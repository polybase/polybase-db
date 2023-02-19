use std::error::Error;
use std::sync::Arc;
use winter_crypto::hashers::Rp64_256;
use winter_crypto::{Digest, Hasher};

use gateway::{Change, Gateway};
use indexer::{Indexer, RecordRoot, RecordValue};

use crate::pending::{self, PendingQueue, PendingQueueError};
use crate::rollup::Rollup;

pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("pending queue error")]
    RecordChangeExists,

    #[error("gateway error")]
    GatewayError(Box<dyn Error + Send + Sync + 'static>),

    #[error("indexer error")]
    IndexerUpdateError(Box<dyn Error + Send + Sync + 'static>),

    #[error("serialize error: {0}")]
    SerializerError(#[from] bincode::Error),

    #[error("rollup error")]
    RollupError,
}

pub struct Db {
    pending: PendingQueue<[u8; 32], Change>,
    gateway: Gateway,
    rollup: Rollup,
    indexer: Arc<Indexer>,
}

impl Db {
    pub fn new(indexer: Arc<Indexer>) -> Self {
        Self {
            pending: PendingQueue::new(),
            rollup: Rollup::new(),
            gateway: gateway::initialize(),
            indexer,
        }
    }

    pub fn last_record_id(&self) -> Option<[u8; 32]> {
        self.pending.back_key()
    }

    pub async fn commit(&self, commit_until_key: [u8; 32]) -> Result<()> {
        // TODO: If there is a commit to collection metadata, we should ignore other changes?

        // Cachce collections
        while let Some(value) = self.pending.pop() {
            let (key, change) = value;
            // Insert into indexer
            match change {
                Change::Create {
                    record,
                    collection_id,
                    record_id,
                } => {
                    self.set(key, collection_id, record_id, record).await?;
                }
                Change::Update {
                    record,
                    collection_id,
                    record_id,
                } => {
                    self.set(key, collection_id, record_id, record).await?;
                }
                Change::Delete {
                    record_id: _,
                    collection_id: _,
                } => {
                    // todo!()
                }
            }

            // Commit up until this point
            if key == commit_until_key {
                break;
            }
        }

        Ok(())
    }

    async fn set(
        &self,
        key: [u8; 32],
        collection_id: String,
        record_id: String,
        record: RecordRoot,
    ) -> Result<()> {
        // Get the indexer collection instance
        let collection = match self.indexer.collection(collection_id.clone()).await {
            Ok(collection) => collection,
            Err(e) => {
                return Err(DbError::IndexerUpdateError(e));
            }
        };

        // Update the indexer
        match collection.set(record_id.clone(), &record).await {
            Ok(_) => {}
            Err(e) => {
                return Err(DbError::IndexerUpdateError(e));
            }
        }

        // Add to the rollup
        match self.rollup.insert(key, &record) {
            Ok(_) => Ok(()),
            Err(_) => Err(DbError::RollupError),
        }
    }

    pub async fn call(
        &self,
        collection_id: String,
        function_name: &str,
        record_id: String,
        args: Vec<RecordValue>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<()> {
        let indexer = Arc::clone(&self.indexer);

        // Get changes
        let changes = match self
            .gateway
            .call(
                &indexer,
                collection_id,
                function_name,
                record_id,
                args,
                auth,
            )
            .await
        {
            Ok(changes) => changes,
            Err(e) => {
                return Err(DbError::GatewayError(e));
            }
        };

        // First we cache the result, as it will be committed later
        for change in changes {
            let (collection_id, record_id) = change.get_path();
            let k = get_key(collection_id, record_id);
            match self.pending.insert(k, change) {
                Ok(_) => {}
                Err(PendingQueueError::KeyExists) => {
                    return Err(DbError::RecordChangeExists);
                }
            }
        }

        Ok(())
    }
}

fn get_key(namespace: &String, id: &String) -> [u8; 32] {
    let b = [namespace.as_bytes(), id.as_bytes()].concat();
    Rp64_256::hash(&b).as_bytes()
}
