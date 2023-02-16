use slog::{info,debug};
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use rmqtt_raft::{Mailbox, Raft as RmqttRaft, Result as RaftResult, Store as RaftStore, Config as RaftConfig};
use tokio::task::JoinHandle;
use std::sync::{Arc};
use rand::Rng;

use crate::db::Db;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

#[derive(Serialize, Deserialize, Clone)]
pub enum RaftMessage {
    Call { 
        collection_id: String,
        function_name: String,
        record_id: String,
        args: Vec<indexer::RecordValue>,
        auth: Option<indexer::AuthUser>,
    },
    // Commit a set of txns
    Commit {
        key: [u8; 32]
    },
    Get { id: String },
}

pub struct Raft {
    id: u64,
    mailbox: Mailbox,
    logger: slog::Logger,
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

        let id: u64 = rand::thread_rng().gen();

        let raft = RmqttRaft::new(
            laddr,
            SharedDb(db),
            logger.clone(),
            cfg
        );

        let mailbox = raft.mailbox();

        let clogger = logger.clone();

        let handle = tokio::spawn(async move {
            let leader_info = raft.find_leader_info(peers).await.unwrap();
            info!(clogger, "leader_info: {:?}", leader_info);

            match leader_info {
                Some((leader_id, leader_addr)) => {
                    info!(clogger, "running in follower mode");
                    raft.join(id, Some(leader_id), leader_addr).await.unwrap();
                }
                None => {
                    info!(clogger, "running in leader mode");
                    raft.lead(id).await.unwrap();
                }
            }
        });

        (Self{
            id,
            logger: logger.clone(),
            mailbox,
        }, handle)
    }
    
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
            auth: auth.map(|a| a.clone()),
        };

        debug!(self.logger, "sending message");

        let message = serialize(&message).unwrap();
        self.mailbox.send(message).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct SharedDb(Arc<Db>);

#[async_trait]
impl RaftStore for SharedDb {
    async fn apply(&mut self, message: &[u8]) -> RaftResult<Vec<u8>> {
        let message: RaftMessage = deserialize(message).unwrap();
        let message: Vec<u8> = match message {
            RaftMessage::Call { collection_id, function_name, record_id, args, auth  } => {
                let auth = auth.as_ref();
                let db = self.0.clone();
                
                db.call(
                    collection_id,
                    &function_name,
                    record_id,
                    args,
                    auth,
                );

                Vec::new()
            },
            RaftMessage::Commit { key } => {
                let db = self.0.clone();
                db.commit(key);
                Vec::new()
            },
            _ => Vec::new(),
        };
        Ok(message)
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

