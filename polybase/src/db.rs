use crate::hash;
use crate::mempool::Mempool;
use crate::rollup::Rollup;
use crate::txn::{self, CallTxn};
use gateway::{Change, Gateway};
use indexer::collection::validate_collection_record;
use indexer::{validate_schema_change, Indexer, RecordRoot};
use serde::{Deserialize, Serialize};
use solid::proposal;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // #[error("existing change for this record exists")]
    // RecordChangeExists,
    #[error("collection not found")]
    CollectionNotFound,

    #[error("collection AST not found in collection record")]
    CollectionASTNotFound,

    #[error("collection AST is invalid: {0}")]
    CollectionASTInvalid(String),

    #[error("cannot update the Collection collection record")]
    CollectionCollectionRecordUpdate,

    #[error(transparent)]
    Gateway(#[from] gateway::GatewayError),

    #[error("indexer error")]
    Indexer(#[from] indexer::IndexerError),

    #[error("collection error")]
    Collection(#[from] indexer::collection::CollectionError),

    #[error("serialize error")]
    Serializer(#[from] bincode::Error),

    #[error("serde_json error")]
    SerdeJson(#[from] serde_json::Error),

    #[error("call txn error")]
    CallTxn(#[from] txn::CallTxnError),

    // #[error("rollup error")]
    // RollupError,
    #[error("tokio send error")]
    TokioSend(#[from] mpsc::error::SendError<CallTxn>),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbSnapshot {
    pub index: Vec<u8>,
}

pub struct Db {
    mempool: Mempool<[u8; 32], CallTxn, usize>,
    gateway: Gateway,
    pub rollup: Rollup,
    pub indexer: Arc<Indexer>,
    // logger: slog::Logger,
    sender: AsyncMutex<mpsc::Sender<CallTxn>>,
    receiver: AsyncMutex<mpsc::Receiver<CallTxn>>,
}

impl Db {
    pub fn new(indexer: Arc<Indexer>, logger: slog::Logger) -> Self {
        let (sender, receiver) = mpsc::channel::<CallTxn>(100);

        Self {
            mempool: Mempool::new(),
            rollup: Rollup::new(),
            gateway: gateway::initialize(logger.clone()),
            indexer,
            // logger,
            sender: AsyncMutex::new(sender),
            receiver: AsyncMutex::new(receiver),
        }
    }

    pub async fn next(&self) -> Option<CallTxn> {
        let mut receiver = self.receiver.lock().await;
        receiver.recv().await
    }

    /// Gets a record from the database
    pub async fn get(
        &self,
        collection_id: String,
        record_id: String,
    ) -> Result<Option<RecordRoot>> {
        let collection = self.indexer.collection(collection_id.clone()).await?;

        let record = collection.get_without_auth_check(record_id).await;
        record.map_err(|e| Error::Indexer(e.into()))
    }

    /// Applies a call txn
    pub async fn call(&self, txn: CallTxn) -> Result<String> {
        let record_id = self.validate_call(&txn).await?;
        let hash = txn.hash()?;

        // Send txn event
        self.sender.lock().await.send(txn.clone()).await?;

        // Wait for txn to be committed
        self.mempool.add_wait(hash, txn).await;

        Ok(record_id)
    }

    pub async fn add_txn(&self, txn: CallTxn) -> Result<String> {
        let record_id = self.validate_call(&txn).await?;
        let hash = txn.hash()?;

        // Send txn event
        self.sender.lock().await.send(txn.clone()).await?;

        // Wait for txn to be committed
        self.mempool.add(hash, txn);

        Ok(record_id)
    }

    async fn validate_call(&self, txn: &CallTxn) -> Result<String> {
        let CallTxn {
            collection_id,
            function_name,
            record_id,
            args,
            auth,
        } = txn;
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
                auth.as_ref(),
            )
            .await?;

        // First we cache the result, as it will be committed later
        for change in changes {
            // let (collection_id, record_id) = change.get_path();
            // let k = get_key(collection_id, record_id);

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
                        return Err(Error::CollectionCollectionRecordUpdate);
                    }

                    self.validate_schema_update(collection_id, record_id, record, auth.as_ref())
                        .await?;
                }
            }
        }

        Ok(output_record_id)
    }

    pub fn propose_txns(&self, height: usize) -> Result<Vec<solid::txn::Txn>> {
        self.mempool
            .lease(height, 110)
            .into_iter()
            .map(|(id, call_txn)| {
                Ok(solid::txn::Txn {
                    // TODO: remove unwrap
                    id: id.to_vec(),
                    data: call_txn.serialize()?,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    pub async fn commit(&self, manifest: proposal::ProposalManifest) -> Result<()> {
        let txns = &manifest.txns;
        let mut keys = vec![];
        for txn in txns.iter() {
            let call_txn = CallTxn::deserialize(&txn.data)?;
            let hash = call_txn.hash()?;
            self.commit_txn(call_txn).await?;
            keys.push(hash);
        }

        // Commit changes in mempool
        self.mempool.commit(manifest.height, keys.iter().collect());

        // TODO: this should be part of a txn with the above!
        self.set_manifest(manifest).await?;

        Ok(())
    }

    pub async fn commit_txn(&self, txn: CallTxn) -> Result<()> {
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
                function_name,
                record_id.clone(),
                args.clone(),
                auth.as_ref(),
            )
            .await?;

        for change in changes {
            let (collection_id, record_id) = change.get_path();
            let key = get_key(collection_id, record_id);

            // Insert into indexer
            match change {
                Change::Create {
                    record,
                    collection_id,
                    record_id,
                } => self.set(key, collection_id, record_id, record).await?,
                Change::Update {
                    record,
                    collection_id,
                    record_id,
                } => self.set(key, collection_id, record_id, record).await?,
                Change::Delete {
                    record_id,
                    collection_id,
                } => self.delete(key, collection_id, record_id).await?,
            };
        }

        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<u8>> {
        let index = self.indexer.snapshot()?;
        let snapshot = DbSnapshot { index };
        let data = bincode::serialize(&snapshot)?;
        Ok(data)
    }

    pub fn restore(&self, data: &[u8]) -> Result<()> {
        let snapshot: DbSnapshot = bincode::deserialize(data)?;
        self.indexer.restore(snapshot.index)?;
        Ok(())
    }

    pub async fn set_manifest(&self, manifest: proposal::ProposalManifest) -> Result<()> {
        let b = bincode::serialize(&manifest)?;
        let value = indexer::RecordValue::Bytes(b);
        let mut record = indexer::RecordRoot::new();
        record.insert("manifest".to_string(), value);
        Ok(self
            .indexer
            .set_system_key("manifest".to_string(), &record)
            .await?)
    }

    pub async fn get_manifest(&self) -> Result<Option<proposal::ProposalManifest>> {
        let record = self.indexer.get_system_key("manifest".to_string()).await?;
        let value = match record.and_then(|mut r| r.remove("manifest")) {
            Some(indexer::RecordValue::Bytes(b)) => b,
            _ => return Ok(None),
        };
        let manifest: proposal::ProposalManifest = bincode::deserialize(&value)?;
        Ok(Some(manifest))
    }

    pub async fn delete(
        &self,
        _: [u8; 32],
        collection_id: String,
        record_id: String,
    ) -> Result<()> {
        // Get the indexer collection instance
        let collection = self.indexer.collection(collection_id.clone()).await?;

        // Update the indexer
        collection.delete(record_id.clone()).await?;

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
        let collection = self.indexer.collection(collection_id.clone()).await?;

        // Update the indexer
        collection.set(record_id.clone(), &record).await?;

        Ok(())
    }

    async fn validate_schema_update(
        &self,
        collection_id: &str,
        record_id: &str,
        record: &RecordRoot,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<()> {
        let collection = self.indexer.collection(collection_id.to_owned()).await?;

        let old_record = collection
            .get(record_id.to_owned(), auth)
            .await
            .map_err(indexer::IndexerError::from)?
            .ok_or(Error::CollectionNotFound)?;

        let old_ast = old_record.get("ast").ok_or(Error::CollectionASTNotFound)?;

        let indexer::RecordValue::String(old_ast) = old_ast
            else {
                return Err(Error::CollectionASTInvalid("Collection AST in old record is not a string".into()));
            };

        let indexer::RecordValue::String(new_ast) = record
                .get("ast")
                .ok_or(Error::CollectionASTNotFound)? else {
            return Err(Error::CollectionASTInvalid("Collection AST in new record is not a string".into()));
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
