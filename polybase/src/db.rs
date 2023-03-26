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

    #[error("collection AST not found in collection record")]
    CollectionASTNotFound,

    #[error("collection AST is invalid: {0}")]
    CollectionASTInvalid(String),

    #[error("cannot update the Collection collection record")]
    CollectionCollectionRecordUpdate,

    #[error(transparent)]
    GatewayError(#[from] gateway::GatewayError),

    #[error("indexer error")]
    IndexerError(#[from] indexer::IndexerError),

    #[error("serialize error")]
    SerializerError(#[from] bincode::Error),

    #[error("serde_json error")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("rollup error")]
    RollupError,
}

pub struct Db {
    pending: PendingQueue<[u8; 32], Change>,
    gateway: Gateway,
    pub rollup: Rollup,
    pub indexer: Arc<Indexer>,
    logger: slog::Logger,
}

impl Db {
    pub fn new(indexer: Arc<Indexer>, logger: slog::Logger) -> Self {
        Self {
            pending: PendingQueue::new(),
            rollup: Rollup::new(),
            gateway: gateway::initialize(),
            indexer,
            logger,
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

    pub async fn commit(&self, commit_until_key: [u8; 32]) {
        // TODO: If there is a commit to collection metadata, we should ignore other changes?

        // Cachce collections
        while let Some(value) = self.pending.pop() {
            let (key, change) = value;

            // Insert into indexer
            let res = match change {
                Change::Create {
                    record,
                    collection_id,
                    record_id,
                } => self.set(key, collection_id, record_id, record).await,
                Change::Update {
                    record,
                    collection_id,
                    record_id,
                } => self.set(key, collection_id, record_id, record).await,
                Change::Delete {
                    record_id,
                    collection_id,
                } => self.delete(key, collection_id, record_id).await,
            };

            // TODO: is the best way to handle an error in commit?
            match res {
                Ok(_) => {}
                Err(e) => warn!(self.logger, "error committing change: {:?}", e),
            }

            // Commit up until this point
            if key == commit_until_key {
                break;
            }
        }

        // match self.rollup.commit() {
        //     Ok(_) => {}
        //     Err(e) => warn!(self.logger, "error committing rollup: {:?}", e),
        // }
    }

    async fn delete(&self, _: [u8; 32], collection_id: String, record_id: String) -> Result<()> {
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

        Ok(())

        // Remove from rollup
        // match self.rollup.delete(key) {
        //     Ok(_) => Ok(()),
        //     Err(_) => Err(DbError::RollupError),
        // }
    }

    async fn set(
        &self,
        _: [u8; 32],
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

        Ok(())

        // Add to the rollup
        // match self.rollup.insert(key, &record) {
        //     Ok(_) => Ok(()),
        //     Err(_) => Err(DbError::RollupError),
        // }
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

        for change in changes {
            let (collection_id, record_id) = change.get_path();
            let k = get_key(collection_id, record_id);

            if self.pending.has(&k) {
                return Err(DbError::RecordChangeExists);
            }
        }

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
                    if record_id == "Collection" {
                        return Err(DbError::CollectionCollectionRecordUpdate);
                    }

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
        collection_id: &str,
        record_id: &str,
        record: &RecordRoot,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<()> {
        let collection = match self.indexer.collection(collection_id.to_owned()).await {
            Ok(collection) => collection,
            Err(e) => {
                return Err(DbError::IndexerError(e.into()));
            }
        };

        let old_record = collection
            .get(record_id.to_owned(), auth)
            .await
            .map_err(indexer::IndexerError::from)?
            .ok_or(DbError::CollectionNotFound)?;

        let old_ast = old_record
            .get("ast")
            .ok_or(DbError::CollectionASTNotFound)?;

        let indexer::RecordValue::String(old_ast) = old_ast
            else {
                return Err(DbError::CollectionASTInvalid("Collection AST in old record is not a string".into()));
            };

        let indexer::RecordValue::String(new_ast) = record
                .get("ast")
                .ok_or(DbError::CollectionASTNotFound)? else {
            return Err(DbError::CollectionASTInvalid("Collection AST in new record is not a string".into()));
        };

        validate_schema_change(
            #[allow(clippy::unwrap_used)] // split always returns at least one element
            record_id.split('/').last().unwrap(),
            serde_json::from_str(old_ast)?,
            serde_json::from_str(new_ast)?,
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
