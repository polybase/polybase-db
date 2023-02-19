use async_trait::async_trait;
// use bincode::{serde_json::from_slice, serialize};
use rand::Rng;
use rmqtt_raft::{Config as RaftConfig, Mailbox, Raft as RmqttRaft, Store as RmqttRaftStore};
use serde::{Deserialize, Serialize};
use slog::{debug, info};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::db::{self, Db};

#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("raft error: {0}")]
    Raft(#[source] rmqtt_raft::Error),

    #[error("db error: {0}")]
    Db(db::DbError),

    #[error("serializer error: {0}")]
    Serializer(#[source] serde_json::Error),

    #[error("sync receive error: {0}")]
    SyncReceive(#[from] tokio::sync::watch::error::RecvError),

    #[error("sync send error: {0}")]
    SyncSend(#[from] tokio::sync::watch::error::SendError<usize>),
}

pub type Result<T> = std::result::Result<T, RaftError>;

#[derive(Serialize, Deserialize, Clone)]
pub enum RaftMessage {
    // Basically just a proxy to the db.call() but using Raft ensures that all
    // calls are processed in the same order on all nodes
    Call {
        collection_id: String,
        function_name: String,
        record_id: String,
        args: Vec<indexer::RecordValue>,
        auth: Option<indexer::AuthUser>,
    },
    // Commit a set of txns
    Commit {
        // commit_id ensures that we don't commit too often. As any node in the
        // cluster can send a commit message, it is possible that we receive
        // multiple commit messages for the same interval.
        commit_id: usize,
    },
    Get {
        id: String,
    },
}

#[derive(Serialize, Deserialize, Clone)]
struct RaftCallResponse {
    commit_id: usize,
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
    shared: Arc<RaftSharedState>,
    mailbox: Mailbox,
}

// Annoyingly, we cannot reuse RaftShared as we only get access to mailbox
// after RmqttRaft::new returns, but RaftConnector still needs access to db/state
// and we can't wrap in an Arc otherwise it won't adhere to the RaftStore trait
struct RaftConnector {
    db: Arc<Db>,
    shared: Arc<RaftSharedState>,
}

// Wrapper so we can have common impl for RaftShared and RaftConnector
struct RaftSharedState {
    logger: slog::Logger,
    state: Mutex<RaftState>,
}

struct RaftState {
    commit_id: Option<usize>,
    timer: Instant,
    shutdown: bool,
    watcher: (watch::Sender<usize>, watch::Receiver<usize>),
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
            logger: logger.clone(),
            state: Mutex::new(RaftState {
                commit_id: None,
                shutdown: false,
                timer: Instant::now(),
                watcher: watch::channel(0),
            }),
        });

        let connector = RaftConnector {
            db: db.clone(),
            shared: Arc::clone(&shared),
        };

        let raft = RmqttRaft::new(laddr, connector, logger.clone(), cfg);
        let mailbox = raft.mailbox();

        let shared = Arc::new(RaftShared {
            db: Arc::clone(&db),
            mailbox,
            shared: Arc::clone(&shared),
        });

        // Create the server handle
        let handle = tokio::spawn(raft_init_setup(raft, peers, logger.clone()));

        // Start the loop to commit every ~1 second
        tokio::spawn(commit_interval(Arc::clone(&shared)));

        (Self { shared }, handle)
    }

    // Proxy call() to Raft so that all nodes apply .call() in the same order. We need to await
    // the commit before responding to the caller
    pub async fn call<'a>(
        &self,
        collection_id: String,
        function_name: String,
        record_id: String,
        args: Vec<indexer::RecordValue>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<()> {
        debug!(
            self.shared.shared.logger,
            "received call: {collection_id}/{record_id}, {function_name}()"
        );

        let message = RaftMessage::Call {
            collection_id,
            function_name: function_name.to_string(),
            record_id,
            args,
            auth: auth.cloned(),
        };

        let message = serde_json::to_vec(&message).unwrap();
        let resp = self.shared.mailbox.send(message).await?;
        let resp: RaftCallResponse = serde_json::from_slice(&resp)?;

        // Wait for the commit to be applied
        self.shared.shared.wait_for_commit(resp.commit_id).await;

        Ok(())
    }
}

impl RaftShared {
    // Determine if we should send a commit message to the cluster. Any node
    // in the cluster can send a commit message to the cluster, and out of
    // date commit messages (commit_id <= highest seen) will be ignored.
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

        // Check if an external commit has been received during the sleep
        if current_commit_id != self.shared.commit_id() {
            return;
        }

        // Only send a commit if we've received a txn since the last commit
        if self.db.last_record_id().is_some() {
            let message = RaftMessage::Commit {
                commit_id: current_commit_id + 1,
            };
            let message = serde_json::to_vec(&message).unwrap();
            match self.mailbox.send(message).await {
                Ok(_) => {}
                Err(e) => {
                    error!(self.shared.logger, "error sending commit message: {e:?}");
                }
            }
        }
    }
}

impl RaftSharedState {
    fn start_commit(&self, commit_id: usize) -> bool {
        let mut state = self.state.lock().unwrap();

        // Last commit exists and has been invalidated
        if let Some(state_commit_id) = state.commit_id {
            if state_commit_id >= commit_id {
                debug!(self.logger, "commit is out of date"; "local" => state_commit_id, "remote" => commit_id);
                return false;
            }
        }

        // Update the commit id now, to prevent other commits being accepted
        state.commit_id = Some(commit_id);

        // Reset timer, so we can calculate time since last commit to determine
        // if we should send a commit message to the cluster
        state.timer = Instant::now();

        true
    }

    fn end_commit(&self) -> Result<()> {
        let state = self.state.lock().unwrap();
        let tx = &state.watcher.0;
        Ok(tx.send(state.commit_id.unwrap_or(0))?)
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

    fn receiver(&self) -> watch::Receiver<usize> {
        let state = self.state.lock().unwrap();
        state.watcher.1.clone()
    }

    async fn wait_for_commit(&self, commit_id: usize) {
        let state_commit_id = self.commit_id();

        // Check if we already have completed the commit
        // TODO: we may need to track received_commit_id and commit_id
        // so we only release this wait when the commit has been applied.
        if state_commit_id > commit_id {
            return;
        };

        // Clone a new receiver
        let mut rx = self.receiver();

        // Wait for the commit to complete
        while rx.changed().await.is_ok() {
            let committed = rx.borrow();
            if *committed > commit_id {
                return;
            }
        }
    }

    fn is_shutdown(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.shutdown
    }
}

#[async_trait]
impl RmqttRaftStore for RaftConnector {
    // Apply the actual changes to the database, apply is guaranteed to
    // be called in order on all nodes. This is first called on the leader
    // node and if it succeeds, it is called on all other nodes.
    async fn apply(&mut self, message: &[u8]) -> rmqtt_raft::Result<Vec<u8>> {
        let db = self.db.clone();
        let message: RaftMessage = serde_json::from_slice(message).unwrap();
        match message {
            RaftMessage::Call {
                collection_id,
                function_name,
                record_id,
                args,
                auth,
            } => {
                let auth = auth.as_ref();

                debug!(
                    self.shared.logger,
                    "apply call: {collection_id}/{record_id}, {function_name}()"
                );

                db.call(collection_id, &function_name, record_id, args, auth)
                    .await?;

                let commit_id = self.shared.commit_id();
                let resp = serde_json::to_vec(&RaftCallResponse { commit_id }).unwrap();

                Ok(resp)
            }
            RaftMessage::Commit { commit_id } => {
                let key = self.db.last_record_id();

                // Check if we have any changes to commit
                if key.is_none() {
                    debug!(self.shared.logger, "no changes to commit: {commit_id}");
                    return Ok(Vec::new());
                }

                // Now safe to unwrap the key
                let key = key.unwrap();

                if !self.shared.start_commit(commit_id) {
                    return Ok(Vec::new());
                }

                let timer = Instant::now();

                info!(self.shared.logger, "commit started: {commit_id}");

                // Send commit to DB
                self.db.commit(key).await?;

                // Finalise the commit, and notify all call waiters
                self.shared.end_commit()?;

                info!(self.shared.logger, "commit ended: {commit_id}"; "time" => timer.elapsed().as_millis());

                // No resp needed for commit
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    // TODO
    async fn query(&self, query: &[u8]) -> rmqtt_raft::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    // TODO
    async fn snapshot(&self) -> rmqtt_raft::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    // TODO
    async fn restore(&mut self, snapshot: &[u8]) -> rmqtt_raft::Result<()> {
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
    }
}

impl From<db::DbError> for rmqtt_raft::Error {
    fn from(e: db::DbError) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<RaftError> for rmqtt_raft::Error {
    fn from(e: RaftError) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<serde_json::Error> for RaftError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serializer(e)
    }
}

impl From<rmqtt_raft::Error> for RaftError {
    fn from(e: rmqtt_raft::Error) -> Self {
        match e {
            // Unwrap Other error, as it may contain a RaftError (because we are forced to wrap the
            // error in RmqttRaftStore)
            rmqtt_raft::Error::Other(e) => match e.downcast_ref::<RaftError>() {
                Some(_) => *e.downcast::<RaftError>().unwrap(),
                None => Self::Raft(rmqtt_raft::Error::Other(e)),
            },

            // Other rqmtt_raft::Error types can be passed through directly
            _ => Self::Raft(e),
        }
    }
}
