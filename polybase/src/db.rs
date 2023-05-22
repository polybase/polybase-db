use crate::hash;
use crate::mempool::Mempool;
use crate::rollup::Rollup;
use crate::txn::{self, CallTxn};
use gateway::{Change, Gateway};
use indexer::collection::validate_collection_record;
use indexer::{validate_schema_change, Indexer, RecordRoot};
use solid::proposal::ProposalManifest;
use std::sync::Arc;

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

    #[error("call txn error")]
    CallTxnError(#[from] txn::CallTxnError),

    #[error("rollup error")]
    RollupError,
}

pub struct Db {
    mempool: Mempool<[u8; 32], CallTxn>,
    gateway: Gateway,
    pub rollup: Rollup,
    pub indexer: Arc<Indexer>,
    logger: slog::Logger,
}

impl Db {
    pub fn new(indexer: Arc<Indexer>, logger: slog::Logger) -> Self {
        Self {
            mempool: Mempool::new(),
            rollup: Rollup::new(),
            gateway: gateway::initialize(logger.clone()),
            indexer,
            logger,
        }
    }

    /// Gets a record from the database
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

    /// Applies a call txn
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
                collection_id.clone(),
                function_name,
                record_id.clone(),
                args.clone(),
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
        }

        let txn = CallTxn::new(collection_id, function_name, record_id, args, auth.cloned());
        let hash = txn.hash()?;

        // Wait for txn to be committed
        self.mempool.add_wait(hash, txn).await;

        Ok(output_record_id)
    }

    async fn commit(&self, txn: CallTxn) -> Result<()> {
        let CallTxn {
            collection_id,
            function_name,
            record_id,
            args,
            auth,
        } = &txn;
        let indexer = Arc::clone(&self.indexer);
        // let output_record_id = record_id.clone();

        // Get changes
        let changes = self
            .gateway
            .call(
                &indexer,
                collection_id.clone(),
                &function_name,
                record_id.clone(),
                args.clone(),
                auth.as_ref(),
            )
            .await?;

        for change in changes {
            let (collection_id, record_id) = change.get_path();
            let key = get_key(collection_id, record_id);

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
        }

        let hash = txn.hash()?;

        // Commit changes in mempool
        self.mempool.commit(hash);

        Ok(())
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

// This is the implementation of the `solid::Store` trait for `Arc<Db>`.
pub struct ArcDb(pub Arc<Db>);

impl solid::Store for ArcDb {
    fn propose(&mut self) -> Vec<solid::txn::Txn> {
        self.0
            .mempool
            .get_batch(110)
            .iter()
            .map(|(id, callTxn)| solid::txn::Txn {
                // TODO: remove unwrap
                id: callTxn.hash().unwrap().to_vec(),
                data: callTxn.serialize().unwrap(),
            })
            .collect()
    }

    fn commit(&mut self, manifest: ProposalManifest) -> Vec<u8> {
        let db_ref = self.0.as_ref();

        for txn in manifest.txns {
            let txn = CallTxn::deserialize(&txn.data).unwrap();
            db_ref.commit(txn);
        }

        vec![]
    }

    fn restore(&mut self, snapshot: solid::Snapshot) {
        let db_ref = self.0.as_ref();
        db_ref.indexer.restore(snapshot.data);
    }

    fn snapshot(&self) -> std::result::Result<solid::Snapshot, Box<dyn std::error::Error>> {
        let db_ref = self.0.as_ref();
        let data = db_ref.indexer.snapshot()?;

        Ok(solid::Snapshot {
            proposal: solid::proposal::ProposalManifest::default(),
            data,
        })
    }
}

fn get_key(namespace: &String, id: &String) -> [u8; 32] {
    let b = [namespace.as_bytes(), id.as_bytes()].concat();
    hash::hash_bytes(b)
}
