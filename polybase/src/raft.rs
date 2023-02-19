use async_trait::async_trait;
use bincode::{deserialize, serialize};
use rand::Rng;
use rmqtt_raft::{
    Config as RaftConfig, Error as RaftError, Mailbox, Raft as RmqttRaft, Result as RaftResult,
    Store as RaftStore,
};
use serde::{Deserialize, Serialize};
use slog::{debug, info};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

use crate::db::{self, Db};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

#[derive(Serialize, Deserialize, Clone)]
pub enum RaftMessage {
    // Basically just a proxy to the db.call() but using Raft ensures that all
    // calls are processed in order
    Call {
        collection_id: String,
        function_name: String,
        record_id: String,
        args: Vec<indexer::RecordValue>,
        auth: Option<indexer::AuthUser>,
    },
    // Commit a set of txns
    Commit {
        // This is the key of the last record change that should be included in
        // the commit. This allows us to keep receiving changes while we are
        // comitting.
        key: [u8; 32],
        // commit_id ensures that we don't commit too often. As any node in the
        // cluster can send a commit message, it is possible that we receive
        // multiple commit messages for the same interval.
        commit_id: usize,
        // We track last commit time so we can determine if a commit message
        // sent to the cluster has been invalidated by an earlier commit (i.e.
        // this prevents over committing due to race conditions)
        // last_commit_id: u64,
    },
    Get {
        id: String,
    },
}

// Main raft with public impl, we also watch this struct for
// drop so we can clean up the commit_interval
pub struct Raft {
    // Shared needs be an Arc, as it is also passed into the
    // commit_interval task
    shared: Arc<RaftShared>,
}

// We need shared access to RaftShared state, so we can access it from
// commit_interval and Raft
struct RaftShared {
    db: Arc<Db>,
    logger: slog::Logger,
    shared: Arc<RaftSharedState>,
    mailbox: Mailbox,
}

// Annoyingly, we cannot reuse RaftShared as we only get access to mailbox
// after RmqttRaft::new returns, but RaftConnector still needs access to db/state
// and we can't wrap in an Arc otherwise we cannot adhere to the RaftStore trait
struct RaftConnector {
    db: Arc<Db>,
    logger: slog::Logger,
    shared: Arc<RaftSharedState>,
}

// Wrapper so we can have common impl for RaftShared and RaftConnector
struct RaftSharedState {
    state: Mutex<RaftState>,
}

struct RaftState {
    commit_id: Option<usize>,
    timer: Instant,
    shutdown: bool,
}

impl Drop for Raft {
    fn drop(&mut self) {
        // Cancel the commit loop
        let mut state = self.shared.shared.state.lock().unwrap();
        state.shutdown = true;

        // TODO: cancel the timer (maybe overkill as it will be max 1 second)
    }
}

impl Raft {
    pub fn new(
        laddr: String,
        peers: Vec<String>,
        db: Arc<Db>,
        logger: slog::Logger,
    ) -> (Self, JoinHandle<()>) {
        let cfg = RaftConfig {
            ..Default::default()
        };

        let shared = Arc::new(RaftSharedState {
            state: Mutex::new(RaftState {
                commit_id: None,
                shutdown: false,
                timer: Instant::now(),
            }),
        });

        let connector = RaftConnector {
            db: db.clone(),
            logger: logger.clone(),
            shared: Arc::clone(&shared),
        };

        let raft = RmqttRaft::new(laddr, connector, logger.clone(), cfg);
        let mailbox = raft.mailbox();

        let shared = Arc::new(RaftShared {
            db: Arc::clone(&db),
            logger: logger.clone(),
            mailbox,
            shared: Arc::clone(&shared),
        });

        // Create the server handle
        let handle = tokio::spawn(raft_init_setup(raft, peers, logger.clone()));

        // Start the loop to commit every ~1 second
        tokio::spawn(commit_interval(Arc::clone(&shared)));

        (Self { shared }, handle)
    }

    // Proxy call() to Raft so that all nodes apply .call()
    // in the same order
    pub async fn call<'a>(
        &self,
        collection_id: String,
        function_name: String,
        record_id: String,
        args: Vec<indexer::RecordValue>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<()> {
        let message = RaftMessage::Call {
            collection_id,
            function_name: function_name.to_string(),
            record_id,
            args,
            auth: auth.cloned(),
        };

        debug!(self.shared.logger, "sending message");

        let message = serialize(&message).unwrap();
        self.shared.mailbox.send(message).await?;
        Ok(())
    }
}

impl RaftShared {
    // Determine if we should send a commit message to the cluster. Any node
    // in the cluster can send a commit message to the cluster, and out of
    // date commit messages will be ignored.
    async fn send_commit(&self) {
        let current_commit_id = self.shared.commit_id();

        if let Some(dur) = self.shared.get_next_interval() {
            // If we're early then sleep until we're due
            tokio::time::sleep(dur).await;

            // In case we shutdown during sleep
            if self.shared.is_shutdown() {
                return;
            }
        }

        if current_commit_id != self.shared.commit_id() {
            // An external commit has been received during the sleep
            return;
        }

        // Only send a commit if we've received a txn since the last commit
        if let Some(last_key) = self.db.last_record_id() {
            let message = RaftMessage::Commit {
                key: last_key,
                commit_id: current_commit_id + 1,
            };
            let message = serialize(&message).unwrap();
            match self.mailbox.send(message).await {
                Ok(_) => {}
                Err(e) => {
                    error!(self.logger, "error sending commit message: {:?}", e);
                }
            }
        }
    }
}

impl RaftSharedState {
    fn receive_commit(&self, commit_id: usize) -> bool {
        let mut state = self.state.lock().unwrap();

        // Last commit exists and has been invalidated
        if let Some(state_commit_id) = state.commit_id {
            if state_commit_id <= commit_id {
                return false;
            }
        }

        // Update the commit time
        state.commit_id = Some(commit_id);

        // Reset timer, so we can calculate time since last commit to determine
        // if we should send a commit message to the cluster
        state.timer = Instant::now();

        true
    }

    fn get_next_interval(&self) -> Option<Duration> {
        let mut state = self.state.lock().unwrap();

        // Time since last interval
        let elapsed = state.timer.elapsed();

        // Reset the timer
        state.timer = Instant::now();

        // We're already behind, so don't sleep
        if elapsed > Duration::from_secs(1) {
            return None;
        }

        Some(Duration::from_secs(1) - elapsed)
    }

    fn commit_id(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.commit_id.unwrap_or(0)
    }

    fn is_shutdown(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.shutdown
    }
}

#[async_trait]
impl RaftStore for RaftConnector {
    // Apply the actual changes to the database, apply is guaranteed to
    // be called in order on all nodes. This is first called on the leader
    // node and if it succeeds, it is called on all other nodes.
    async fn apply(&mut self, message: &[u8]) -> RaftResult<Vec<u8>> {
        let db = self.db.clone();
        let message: RaftMessage = deserialize(message).unwrap();
        match message {
            RaftMessage::Call {
                collection_id,
                function_name,
                record_id,
                args,
                auth,
            } => {
                let auth = auth.as_ref();

                db.call(collection_id, &function_name, record_id, args, auth)
                    .await?;

                Ok(Vec::new())
            }
            RaftMessage::Commit { key, commit_id } => {
                if !self.shared.receive_commit(commit_id) {
                    debug!(self.logger, "Invalid commit: {}", &commit_id);
                    return Ok(Vec::new());
                }

                info!(self.logger, "Committing: {}", &commit_id);

                // Send commit to DB
                self.db.commit(key).await?;

                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn query(&self, query: &[u8]) -> RaftResult<Vec<u8>> {
        // let query: RaftMessage = deserialize(query).unwrap();
        // let data: Vec<u8> = match query {
        //     RaftMessage::Get { key } => {
        //         if let Some(val) = self.get(&key) {
        //             serialize(&val).unwrap()
        //         } else {
        //             Vec::new()
        //         }
        //     }
        //     _ => Vec::new(),
        // };
        // Ok(data)
        Ok(Vec::new())
    }

    // TODO
    async fn snapshot(&self) -> RaftResult<Vec<u8>> {
        // Ok(serialize(&self.cache.read().unwrap().clone())?)
        Ok(Vec::new())
    }

    // TODO
    async fn restore(&mut self, snapshot: &[u8]) -> RaftResult<()> {
        // let new: HashMap<String, String> = deserialize(snapshot).unwrap();
        // let mut db = self.cache.write().unwrap();
        // let _ = std::mem::replace(&mut *db, new);
        Ok(())
    }
}

async fn raft_init_setup(raft: RmqttRaft<RaftConnector>, peers: Vec<String>, logger: slog::Logger) {
    let id: u64 = rand::thread_rng().gen();
    let leader_info = raft.find_leader_info(peers).await.unwrap();
    info!(logger, "leader_info: {:?}", leader_info);

    match leader_info {
        Some((leader_id, leader_addr)) => {
            info!(logger, "running in follower mode");
            raft.join(id, Some(leader_id), leader_addr).await.unwrap();
        }
        None => {
            info!(logger, "running in leader mode");
            raft.lead(id).await.unwrap();
        }
    }
}

async fn commit_interval(shared: Arc<RaftShared>) {
    while !shared.shared.is_shutdown() {
        shared.send_commit().await;
        // tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

impl From<db::DbError> for RaftError {
    fn from(e: db::DbError) -> Self {
        Self::Other(Box::new(e))
    }
}
