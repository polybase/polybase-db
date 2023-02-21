use std::sync::Arc;

use gateway::{Change, Gateway};
use indexer::collection::validate_collection_record;
use indexer::{validate_schema_change, Indexer, RecordRoot};

use crate::hash;
use crate::pending::{PendingQueue, PendingQueueError};
use crate::rollup::Rollup;

pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("existing change for this record exists")]
    RecordChangeExists,

    #[error("collection not found")]
    CollectionNotFound,

    #[error("collection AST is invalid: {0}")]
    CollectionASTInvalid(String),

    #[error(transparent)]
    GatewayError(#[from] gateway::GatewayError),

    #[error("indexer error")]
    IndexerError(#[from] indexer::IndexerError),

    #[error("serialize error")]
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

    pub async fn get(
        &self,
        collection_id: String,
        record_id: String,
    ) -> Result<Option<RecordRoot>> {
        let collection = match self.indexer.collection(collection_id.clone()).await {
            Ok(collection) => collection,
            Err(e) => {
                return Err(DbError::IndexerError(e.into()));
            }
        };

        let record = collection.get_without_auth_check(record_id).await;
        record.map_err(|e| DbError::IndexerError(e.into()))
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
                    record_id,
                    collection_id,
                } => self.delete(key, collection_id, record_id).await?,
            }

            // Commit up until this point
            if key == commit_until_key {
                break;
            }
        }

        Ok(())
    }

    async fn delete(&self, key: [u8; 32], collection_id: String, record_id: String) -> Result<()> {
        let collection = match self.indexer.collection(collection_id.clone()).await {
            Ok(collection) => collection,
            Err(e) => {
                return Err(DbError::IndexerError(e.into()));
            }
        };

        // Update the indexer
        match collection.delete(record_id.clone()).await {
            Ok(_) => {}
            Err(e) => {
                return Err(DbError::IndexerError(e.into()));
            }
        }

        // Remove from rollup
        match self.rollup.delete(key) {
            Ok(_) => Ok(()),
            Err(_) => Err(DbError::RollupError),
        }
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
                return Err(DbError::IndexerError(e.into()));
            }
        };

        // Update the indexer
        match collection.set(record_id.clone(), &record).await {
            Ok(_) => {}
            Err(e) => {
                return Err(DbError::IndexerError(e.into()));
            }
        }

        // Add to the rollup
        match self.rollup.insert(key, &record) {
            Ok(_) => Ok(()),
            Err(_) => Err(DbError::RollupError),
        }
    }

    pub async fn validate_call(
        &self,
        collection_id: String,
        function_name: &str,
        record_id: String,
        args: Vec<serde_json::Value>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<()> {
        let indexer = Arc::clone(&self.indexer);

        // Get changes
        self.gateway
            .call(
                &indexer,
                collection_id,
                function_name,
                record_id,
                args,
                auth,
            )
            .await?;

        Ok(())
    }

    pub async fn call(
        &self,
        collection_id: String,
        function_name: &str,
        record_id: String,
        args: Vec<serde_json::Value>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<String> {
        let indexer = Arc::clone(&self.indexer);
        let mut output_record_id = record_id.clone();

        // Get changes
        let changes = self
            .gateway
            .call(
                &indexer,
                collection_id,
                function_name,
                record_id,
                args,
                auth,
            )
            .await?;

        // First we cache the result, as it will be committed later
        for change in changes {
            let (collection_id, record_id) = change.get_path();
            let k = get_key(collection_id, record_id);

            // Get the ID of created record
            if let Change::Create {
                collection_id: _,
                record_id,
                record: _,
            } = &change
            {
                output_record_id = record_id.clone();
            }

            // Check if we are updating collection schema
            if let Change::Update {
                collection_id,
                record_id,
                record,
                ..
            } = &change
            {
                if collection_id == "Collection" {
                    self.validate_schema_update(collection_id, record_id, record, auth)
                        .await?;
                }
            }

            match self.pending.insert(k, change) {
                Ok(_) => {}
                Err(PendingQueueError::KeyExists) => {
                    return Err(DbError::RecordChangeExists);
                }
            }
        }

        Ok(output_record_id)
    }

    async fn validate_schema_update(
        &self,
        collection_id: &String,
        record_id: &String,
        record: &RecordRoot,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<()> {
        let collection = match self.indexer.collection(collection_id.clone()).await {
            Ok(collection) => collection,
            Err(e) => {
                return Err(DbError::IndexerError(e.into()));
            }
        };

        let old_record = collection
            .get(record_id.clone(), auth)
            .await
            .unwrap()
            .expect("Collection not found");

        let old_ast = old_record
            .get("ast")
            .expect("Collection AST not found in collection record");

        let indexer::RecordValue::IndexValue(indexer::IndexValue::String(old_ast)) = old_ast
            else {
                return Err(DbError::CollectionASTInvalid("Collection AST in old record is not a string".into()));
            };

        let indexer::RecordValue::IndexValue(indexer::IndexValue::String(new_ast)) = record
                .get("ast")
                .expect("Collection AST not found in new collection record") else {
            return Err(DbError::CollectionASTInvalid("Collection AST in new ".into()));
        };

        validate_schema_change(
            record_id.split('/').last().unwrap(),
            serde_json::from_str(old_ast).unwrap(),
            serde_json::from_str(new_ast).unwrap(),
        )
        .map_err(indexer::IndexerError::from)?;

        validate_collection_record(record).map_err(indexer::IndexerError::from)?;

        Ok(())
    }
}

fn get_key(namespace: &String, id: &String) -> [u8; 32] {
    let b = [namespace.as_bytes(), id.as_bytes()].concat();
    hash::hash_bytes(b)
}