use crate::hash;
use crate::mempool::Mempool;
use crate::txn::{self, CallTxn};
use crate::util;
use futures::TryStreamExt;
use gateway::{Change, Gateway};
use indexer::snapshot::{SnapshotChunk, SnapshotIterator};
use indexer::{
    collection::validate_collection_record, validate_schema_change, Cursor, Indexer, ListQuery,
    RecordRoot,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use solid::proposal::{self};
use std::cmp::min;
use std::collections::HashSet;
use std::time::{Duration, SystemTime};
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

pub enum DbWaitResult<T> {
    Updated(T),
    NotModified,
}

#[derive(Debug)]
pub struct DbConfig {
    block_txns_count: usize,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            block_txns_count: 2000,
        }
    }
}

pub struct Db {
    mempool: Mempool<[u8; 32], CallTxn, usize, [u8; 32]>,
    gateway: Gateway,
    indexer: Indexer,
    sender: AsyncMutex<mpsc::Sender<CallTxn>>,
    receiver: AsyncMutex<mpsc::Receiver<CallTxn>>,
    config: DbConfig,
    out_of_sync_height: Mutex<Option<usize>>,
}

impl Db {
    pub fn new(root_dir: String, config: DbConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<CallTxn>(100);

        // Create the indexer
        #[allow(clippy::unwrap_used)]
        let indexer_dir = util::get_indexer_dir(&root_dir).unwrap();
        let indexer = Indexer::new(indexer_dir)?;

        Ok(Self {
            mempool: Mempool::new(),
            gateway: gateway::initialize(),
            indexer,
            sender: AsyncMutex::new(sender),
            receiver: AsyncMutex::new(receiver),
            config,
            out_of_sync_height: Mutex::new(None),
        })
    }

    /// Is the node healthy and up to date
    pub fn is_healthy(&self) -> bool {
        self.out_of_sync_height.lock().is_none()
    }

    /// Set the node as out of sync
    pub fn out_of_sync(&self, height: usize) {
        self.out_of_sync_height.lock().replace(height);
    }

    pub async fn next(&self) -> Option<CallTxn> {
        let mut receiver = self.receiver.lock().await;
        receiver.recv().await
    }

    /// Gets a record from the database
    pub async fn get_without_auth_check(
        &self,
        collection_id: String,
        record_id: String,
    ) -> Result<Option<RecordRoot>> {
        let collection = self.indexer.collection(collection_id.clone()).await?;

        let record = collection.get_without_auth_check(record_id).await;
        record.map_err(|e| Error::Indexer(e.into()))
    }

    pub async fn get(
        &self,
        collection_id: String,
        record_id: String,
        auth: Option<indexer::AuthUser>,
    ) -> Result<Option<RecordRoot>> {
        let collection = self.indexer.collection(collection_id.clone()).await?;
        Ok(collection.get(record_id, auth.as_ref()).await?)
    }

    pub async fn get_wait(
        &self,
        collection_id: String,
        record_id: String,
        auth: Option<indexer::AuthUser>,
        since: f64,
        wait_for: Duration,
    ) -> Result<DbWaitResult<Option<RecordRoot>>> {
        let collection = self.indexer.collection(collection_id.clone()).await?;

        // Wait for a record to create/update for a given amount of time, returns true if the record was created or
        // updated within the given time.
        let updated = wait_for_update(since, wait_for, || async {
            Ok(collection
                .get_record_metadata(&record_id)
                .await?
                .map(|m| m.updated_at))
        })
        .await?;

        Ok(if updated {
            DbWaitResult::Updated(collection.get(record_id, auth.as_ref()).await?)
        } else {
            DbWaitResult::NotModified
        })
    }

    pub async fn list(
        &self,
        collection_id: String,
        query: ListQuery<'_>,
        auth: Option<indexer::AuthUser>,
    ) -> Result<Vec<(Cursor, RecordRoot)>> {
        let collection = self.indexer.collection(collection_id.clone()).await?;

        #[allow(clippy::let_and_return)]
        let records = Ok(collection
            .list(query, &auth.as_ref())
            .await?
            .try_collect::<Vec<_>>()
            .await?);

        records
    }

    pub async fn list_wait(
        &self,
        collection_id: String,
        query: ListQuery<'_>,
        auth: Option<indexer::AuthUser>,
        since: f64,
        wait_for: Duration,
    ) -> Result<DbWaitResult<Vec<(Cursor, RecordRoot)>>> {
        let collection = self.indexer.collection(collection_id.clone()).await?;

        // Wait for a record to create/update for a given amount of time, returns true if the record was created or
        // updated within the given time.
        let updated = wait_for_update(since, wait_for, || async {
            Ok(collection
                .get_metadata()
                .await?
                .map(|m| m.last_record_updated_at))
        })
        .await?;

        Ok(if updated {
            DbWaitResult::Updated(self.list(collection_id, query, auth).await?)
        } else {
            DbWaitResult::NotModified
        })
    }

    /// Applies a call txn
    pub async fn call(&self, txn: CallTxn) -> Result<String> {
        let (record_id, changes) = self.validate_call(&txn).await?;
        let hash = txn.hash()?;

        // Send txn event
        self.sender.lock().await.send(txn.clone()).await?;

        // Wait for txn to be committed
        self.mempool.add_wait(hash, txn, changes).await;

        Ok(record_id)
    }

    pub async fn add_txn(&self, txn: CallTxn) -> Result<String> {
        let (record_id, changes) = self.validate_call(&txn).await?;
        let hash = txn.hash()?;

        // Wait for txn to be committed
        self.mempool.add(hash, txn, changes);

        Ok(record_id)
    }

    async fn validate_call(&self, txn: &CallTxn) -> Result<(String, Vec<[u8; 32]>)> {
        let CallTxn {
            collection_id,
            function_name,
            record_id,
            args,
            auth,
        } = txn;
        // let indexer = Arc::clone(&self.indexer);
        let mut output_record_id = record_id.clone();
        let mut output_records = HashSet::new();

        // Get changes
        let changes = self
            .gateway
            .call(
                &self.indexer,
                collection_id.clone(),
                function_name,
                record_id.clone(),
                args.clone(),
                auth.as_ref(),
            )
            .await?;

        // First we cache the result, as it will be committed later
        for change in changes.iter() {
            let (collection_id, record_id) = change.get_path();
            let key = get_key(collection_id, record_id);
            output_records.insert(key);

            // If we're creating a record then we need to get the record_id from
            // the output from the contstructor call.
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

        Ok((output_record_id, output_records.into_iter().collect()))
    }

    pub fn propose_txns(&self, height: usize) -> Result<Vec<solid::txn::Txn>> {
        // TODO: check txns do not affect the same record
        self.mempool
            .lease_batch(height, self.config.block_txns_count)
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

    pub async fn lease(&self, manifest: &proposal::ProposalManifest) -> Result<()> {
        let txns = &manifest.txns;
        for txn in txns {
            let call_txn = CallTxn::deserialize(&txn.data)?;
            let hash: [u8; 32] = call_txn.hash()?;

            // Add the txn if not existing
            if !self.mempool.contains(&hash) {
                self.add_txn(call_txn).await?;
            }

            self.mempool.lease_txn(&manifest.height, &hash);
        }
        Ok(())
    }

    pub async fn commit(&self, manifest: proposal::ProposalManifest) -> Result<()> {
        let txns = &manifest.txns;
        let mut keys = vec![];

        for txn in txns.iter() {
            let call_txn = CallTxn::deserialize(&txn.data)?;
            let hash: [u8; 32] = call_txn.hash()?;
            match self.commit_txn(call_txn).await {
                Ok(_) => {}
                Err(err) => {
                    return Err(err);
                }
            };
            keys.push(hash);
        }

        let height = manifest.height;

        // Update the txn manifest in rocksdb
        self.set_manifest(manifest).await?;

        // Commit all txns
        self.indexer.commit().await?;

        // Commit changes in mempool (releasing unused txns and removing used ones). This will
        // also release all requests that were waiting for these txns to be committed.
        self.mempool.commit(height, keys.iter().collect());

        // Reset out of sync height if we have now committed beyond the out of sync height
        let mut out_of_sync_height_opt = self.out_of_sync_height.lock();
        if let Some(out_of_sync_height) = *out_of_sync_height_opt {
            if height + 1 >= out_of_sync_height {
                *out_of_sync_height_opt = None;
            }
        }

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
        // let output_record_id = record_id.clone();

        // Get changes
        let changes = self
            .gateway
            .call(
                &self.indexer,
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

    /// Reset all data in the database
    pub fn reset(&self) -> Result<()> {
        Ok(self.indexer.reset()?)
    }

    /// Create a snapshot iterator, that can be used to iterate over the
    /// entire database in chunks
    pub fn snapshot_iter(&self, chunk_size: usize) -> SnapshotIterator {
        self.indexer.snapshot(chunk_size)
    }

    pub fn restore_chunk(&self, chunk: SnapshotChunk) -> Result<()> {
        self.indexer.restore(chunk)?;
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

async fn wait_for_update<F, Fut>(since: f64, wait_for: Duration, check_updated: F) -> Result<bool>
where
    F: Fn() -> Fut,
    Fut: futures::Future<Output = Result<Option<SystemTime>>>,
{
    // Wait for a maximum of 60 seconds
    let wait_for = min(wait_for, Duration::from_secs(60));

    // Calculate the time to wait until
    let wait_until = SystemTime::now() + wait_for;

    // Last time a check was made by the client
    let since = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(since);

    // Loop until the record is updated or the time is up
    while wait_until > SystemTime::now() {
        if let Some(updated_at) = check_updated().await? {
            if updated_at > since {
                return Ok(true);
            }
        }

        // Only check once per second
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    Ok(false)
}
