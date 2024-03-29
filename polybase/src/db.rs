use crate::hash;
use crate::mempool::Mempool;
use crate::txn::{self, CallTxn};
use futures_util::{future, StreamExt};
use gateway::Gateway;
use indexer::{
    adaptor::{IndexerAdaptor, SnapshotValue},
    IndexerChange,
};
use indexer::{auth_user::AuthUser, list_query::ListQuery, Indexer};
use parking_lot::Mutex;
use schema::{
    self, methods,
    record::{
        self, foreign_record_to_json, json_to_record, record_to_json, ForeignRecordReference,
        RecordReference, RecordRoot, RecordValue,
    },
    Schema,
};
use solid::proposal::{self};
use std::cmp::min;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("schema error: {0}")]
    Schema(#[from] schema::Error),

    #[error("record error: {0}")]
    Record(#[from] record::RecordError),

    #[error("record error: {0}")]
    Method(#[from] methods::UserError),

    #[error("user error: {0}")]
    User(#[from] UserError),

    #[error(transparent)]
    Gateway(#[from] gateway::GatewayError),

    #[error("indexer error")]
    Indexer(#[from] indexer::Error),

    #[error("serialize error")]
    Serializer(#[from] bincode::Error),

    #[error("serde_json error")]
    SerdeJson(#[from] serde_json::Error),

    #[error("call txn error")]
    CallTxn(#[from] txn::CallTxnError),

    #[error("tokio send error")]
    TokioSend(#[from] mpsc::error::SendError<CallTxn>),

    #[error("invalid function args response")]
    InvalidFunctionArgsResponse,
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error("collection not found")]
    CollectionNotFound,

    #[error("code is missing definition for collection {name}")]
    MissingDefinitionForCollection { name: String },

    #[error("method {method_name:?} not found in collection {collection_id:?}")]
    FunctionNotFound {
        method_name: String,
        collection_id: String,
    },

    #[error("collection mismatch, expected record in collection {expected_collection_id:?}, got {actual_collection_id:?}")]
    CollectionMismatch {
        expected_collection_id: String,
        actual_collection_id: String,
    },

    #[error("record {record_id:?} was not found in collection {collection_id:?}")]
    RecordNotFound {
        record_id: String,
        collection_id: String,
    },

    // #[error("record ID was modified during call")]
    // RecordIDModified,
    #[error("record does not have a id field")]
    RecordIdNotFound,

    #[error("record ID field is not a string")]
    RecordIdNotString,

    #[error("record id already exists in collection")]
    CollectionIdExists,

    #[error("method {method_name} args invalid, expected {expected} got {actual}")]
    MethodIncorrectNumberOfArguments {
        method_name: String,
        expected: usize,
        actual: usize,
    },

    #[error("you do not have permission to call this function")]
    UnauthorizedCall,
}

pub enum DbWaitResult<T> {
    Updated(T),
    NotModified,
}

#[derive(Debug)]
pub struct DbConfig {
    pub block_txns_count: usize,
    pub migration_batch_size: usize,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            block_txns_count: 2000,
            migration_batch_size: 1000,
        }
    }
}

pub struct Db<A: IndexerAdaptor> {
    mempool: Mempool<[u8; 32], CallTxn, usize, [u8; 32]>,
    gateway: Gateway,
    indexer: Indexer<A>,
    sender: AsyncMutex<mpsc::Sender<CallTxn>>,
    receiver: AsyncMutex<mpsc::Receiver<CallTxn>>,
    config: DbConfig,
    out_of_sync_height: Mutex<Option<usize>>,
}

impl<A: IndexerAdaptor> Db<A> {
    pub async fn new(indexer: Indexer<A>, config: DbConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<CallTxn>(100);

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
    #[tracing::instrument(skip(self))]
    pub async fn get_without_auth_check(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<RecordRoot>> {
        Ok(self
            .indexer
            .get_without_auth_check(collection_id, record_id)
            .await?)
    }

    pub async fn get(
        &self,
        collection_id: &str,
        record_id: &str,
        auth: Option<AuthUser>,
    ) -> Result<Option<RecordRoot>> {
        let public_key = auth.as_ref().map(|a| a.public_key());
        Ok(self
            .indexer
            .get(collection_id, record_id, public_key)
            .await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_wait(
        &self,
        collection_id: &str,
        record_id: &str,
        auth: Option<AuthUser>,
        since: f64,
        wait_for: Duration,
    ) -> Result<DbWaitResult<Option<RecordRoot>>> {
        let public_key = auth.as_ref().map(|a| a.public_key());

        // Wait for a record to create/update for a given amount of time, returns true if the record was created or
        // updated within the given time.
        let updated = wait_for_update(since, wait_for, || async {
            Ok(self
                .indexer
                .last_record_update(collection_id, record_id)
                .await?)
        })
        .await?;

        Ok(if updated {
            DbWaitResult::Updated(
                self.indexer
                    .get(collection_id, record_id, public_key)
                    .await?,
            )
        } else {
            DbWaitResult::NotModified
        })
    }

    #[tracing::instrument(skip(self, query))]
    pub async fn list(
        &self,
        collection_id: &str,
        query: ListQuery<'_>,
        auth: Option<AuthUser>,
    ) -> Result<Vec<RecordRoot>> {
        let public_key = auth.as_ref().map(|a| a.public_key());
        let stream = self.indexer.list(collection_id, query, public_key).await?;

        Ok(stream.collect::<Vec<RecordRoot>>().await)
    }

    #[tracing::instrument(skip(self, query))]
    pub async fn list_wait(
        &self,
        collection_id: &str,
        query: ListQuery<'_>,
        auth: Option<AuthUser>,
        since: f64,
        wait_for: Duration,
    ) -> Result<DbWaitResult<Vec<RecordRoot>>> {
        // Wait for a record to create/update for a given amount of time, returns true if the record was created or
        // updated within the given time.
        let updated = wait_for_update(since, wait_for, || async {
            Ok(self.indexer.last_collection_update(collection_id).await?)
        })
        .await?;

        Ok(if updated {
            DbWaitResult::Updated(self.list(collection_id, query, auth).await?)
        } else {
            DbWaitResult::NotModified
        })
    }

    /// Applies a call txn
    #[tracing::instrument(skip(self))]
    pub async fn call(&self, txn: CallTxn) -> Result<String> {
        let (record_id, changes) = self.call_changes(&txn).await?;
        let hash = txn.hash()?;

        // Send txn event
        self.sender.lock().await.send(txn.clone()).await?;

        // Wait for txn to be committed
        self.mempool
            .add_wait(hash, txn, to_change_keys(&changes))
            .await;

        Ok(record_id)
    }

    #[tracing::instrument(skip(self))]
    pub async fn add_txn(&self, txn: CallTxn) -> Result<String> {
        let (record_id, changes) = self.call_changes(&txn).await?;
        let hash = txn.hash()?;

        // Wait for txn to be committed
        self.mempool.add(hash, txn, to_change_keys(&changes));

        Ok(record_id)
    }

    async fn call_changes(&self, txn: &CallTxn) -> Result<(String, Vec<IndexerChange>)> {
        let CallTxn {
            collection_id,
            record_id,
            function_name: method,
            args,
            auth,
        } = txn;

        let schema = std::sync::Arc::new(self.indexer.get_schema_required(collection_id).await?);
        let public_key = auth.as_ref().map(|a| a.public_key());

        // Get the method
        let method = match schema.get_method(method) {
            Some(method) => method,
            None => {
                return Err(UserError::FunctionNotFound {
                    method_name: method.to_string(),
                    collection_id: collection_id.to_string(),
                })?
            }
        };

        // Check args length
        let required_args_len = method.parameters.iter().filter(|p| p.required).count();
        if args.len() < required_args_len {
            return Err(UserError::MethodIncorrectNumberOfArguments {
                method_name: method.name.clone(),
                expected: required_args_len,
                actual: args.len(),
            })?;
        }

        if args.len() > method.parameters.len() {
            return Err(UserError::MethodIncorrectNumberOfArguments {
                method_name: method.name.clone(),
                expected: method.parameters.len(),
                actual: args.len(),
            })?;
        }

        // Get the js code to run
        let js_code = schema.generate_js();

        // Get current record instance
        let record = if method.name == "constructor" {
            RecordRoot::new()
        } else {
            match self
                .indexer
                .get(collection_id, record_id, public_key)
                .await?
            {
                Some(record) => record,
                None => {
                    return Err(UserError::RecordNotFound {
                        record_id: record_id.to_string(),
                        collection_id: collection_id.to_string(),
                    })?
                }
            }
        };

        // Check user has permission to call
        if method.name != "constructor"
            && !self
                .indexer
                .verify_call(collection_id, &method.name, &schema, &record, public_key)
                .await
        {
            return Err(UserError::UnauthorizedCall)?;
        }

        // Get args as RecordValues (so we can validate them and find the references)
        let input_args = method.args_from_json(args).map_err(Error::from)?;

        // Convert all record references to JSON
        let extended_input_args =
            futures::future::join_all(args.iter().zip(&input_args).map(|(json, val)| async move {
                match val {
                    // TODO(minor): clean up duplicate code
                    RecordValue::ForeignRecordReference(ForeignRecordReference {
                        id,
                        collection_id,
                    }) => {
                        let record = self
                            .indexer
                            .get(collection_id, id, public_key)
                            .await?
                            .ok_or(UserError::RecordNotFound {
                                collection_id: collection_id.to_string(),
                                record_id: id.to_string(),
                            })?;
                        let record = foreign_record_to_json(record, collection_id);
                        Ok(record)
                    }
                    RecordValue::RecordReference(RecordReference { id }) => {
                        let record = self
                            .indexer
                            .get(collection_id, id, public_key)
                            .await?
                            .ok_or(UserError::RecordNotFound {
                                collection_id: collection_id.to_string(),
                                record_id: id.to_string(),
                            })?;
                        Ok(record_to_json(record))
                    }
                    // Keep all other values as JSON
                    _ => Ok(json.clone()),
                }
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<serde_json::Value>>>()?;

        let json_record = &record_to_json(record);

        // Get changes
        let output = self
            .gateway
            .call(
                collection_id,
                &js_code,
                &method.name,
                json_record,
                &extended_input_args,
                auth.as_ref(),
            )
            .await?;

        let output_record_changed = &output.instance != json_record;

        // Output record
        let output_record = json_to_record(&schema, output.instance, false)?;

        // Get output ID
        let output_instance_id = match output_record.get("id") {
            Some(id) => id.clone(),
            None => return Err(UserError::RecordIdNotFound)?,
        };

        // Check output ID is a string
        let RecordValue::String(output_instance_id) = output_instance_id else {
            return Err(UserError::RecordIdNotString)?;
        };

        // Check if already exists
        if let Ok(Some(_)) = self
            .indexer
            .get_without_auth_check(collection_id, &output_instance_id)
            .await
        {
            if method.name == "constructor" {
                return Err(UserError::CollectionIdExists)?;
            }
        }

        // Check output args are same as input, otherwise something strange has happened,
        // possibly someone messing around with the JS code
        if extended_input_args.len() != output.args.len() {
            return Err(Error::InvalidFunctionArgsResponse)?;
        }

        // TODO: check we can't modify records in other collections

        // Validate schema change
        // Update of schema
        if collection_id == "Collection" {
            // Check schema is valid
            let new_schema = Schema::from_record(&output_record).map_err(|err| match err {
                schema::Error::CollectionNotFoundInAST { name } => {
                    Error::User(UserError::MissingDefinitionForCollection { name })
                }
                _ => Error::from(err),
            })?;
            new_schema.validate()?;

            // Existing schema update, validate its allowed
            if method.name != "constructor" {
                let old_schema = self.indexer.get_schema_required(record_id).await?;
                old_schema.validate_schema_change(new_schema)?;
            }
        }

        // Find changes in the args
        let mut changes: Vec<_> = futures::future::join_all(
            extended_input_args
                .into_iter()
                .zip(output.args)
                .zip(input_args)
                .filter(|(_, value)| {
                    matches!(
                        value,
                        RecordValue::ForeignRecordReference(_) | RecordValue::RecordReference(_)
                    )
                })
                .filter(|((input, output), _)| input != output)
                .map(|((_, output), value)| {
                    let schema = std::sync::Arc::clone(&schema);
                    async move {
                        match value {
                            RecordValue::ForeignRecordReference(ForeignRecordReference {
                                id,
                                collection_id,
                            }) => {
                                let schema =
                                    self.indexer.get_schema_required(&collection_id).await?;
                                Ok(IndexerChange::Set {
                                    collection_id,
                                    record_id: id,
                                    record: json_to_record(&schema, output, false)?,
                                })
                            }
                            RecordValue::RecordReference(RecordReference { id }) => {
                                Ok(IndexerChange::Set {
                                    collection_id: collection_id.to_string(),
                                    record_id: id,
                                    record: json_to_record(&schema, output, false)?,
                                })
                            }
                            _ => unreachable!(),
                        }
                    }
                }),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        if output.self_destruct {
            changes.push(IndexerChange::Delete {
                collection_id: collection_id.to_string(),
                record_id: output_instance_id.to_string(),
            });
        } else if method.name == "constructor" || output_record_changed {
            // TODO: if we're creating a new collection, let's do more validation here!
            changes.push(IndexerChange::Set {
                collection_id: collection_id.to_string(),
                record_id: output_instance_id.to_string(),
                record: output_record,
            });
        };

        Ok((output_instance_id.to_string(), changes))
    }

    #[tracing::instrument(skip(self))]
    pub fn propose_txns(&self, height: usize) -> Result<Vec<solid::txn::Txn>> {
        type TxnList = Vec<([u8; 32], CallTxn)>;
        let (mut collection_txns, mut other_txns): (TxnList, TxnList) = self
            .mempool
            .lease_batch(height, self.config.block_txns_count)
            .into_iter()
            .partition(|(_, call_txn)| call_txn.collection_id == "Collection");

        collection_txns
            .drain(..)
            .chain(other_txns.drain(..)) // Chain the two parts
            .map(|(id, call_txn)| {
                Ok(solid::txn::Txn {
                    // TODO: remove unwrap
                    id: id.to_vec(),
                    data: call_txn.serialize()?,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(skip(self))]
    pub async fn commit(&self, manifest: proposal::ProposalManifest) -> Result<()> {
        let txns = &manifest.txns;

        // Convert txns into CallTxn
        let call_txns = txns
            .iter()
            .map(|txn| {
                let call_txn = CallTxn::deserialize(&txn.data)?;
                Ok(call_txn)
            })
            .collect::<Result<Vec<_>>>()?;

        // Get a list of keys to remove from the mempool
        let keys = call_txns
            .iter()
            .map(|call_txn| {
                let hash: [u8; 32] = call_txn.hash()?;
                Ok(hash)
            })
            .collect::<Result<Vec<_>>>()?;

        // Get a list of changes for the indexer
        let mut changes = future::join_all(
            call_txns
                .iter()
                .map(|txn| async move { Ok(self.call_changes(txn).await?.1) }),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

        // Sort collection changes first
        changes.sort_by_key(|item| {
            if match item {
                IndexerChange::Set { collection_id, .. } => collection_id,
                IndexerChange::Delete { collection_id, .. } => collection_id,
            } == "Collection"
            {
                0
            } else {
                1
            }
        });

        let height = manifest.height;

        // Update the txn manifest in rocksdb
        self.set_manifest(manifest).await?;

        // Commit all txns
        self.indexer.commit(height, changes).await?;

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

    /// Reset all data in the database
    pub async fn reset(&self) -> Result<()> {
        Ok(self.indexer.reset().await?)
    }

    /// Create a snapshot iterator, that can be used to iterate over the
    /// entire database in chunks
    pub async fn snapshot_iter(
        &self,
        chunk_size: usize,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<Vec<SnapshotValue>>> + '_ + Send>> {
        self.indexer
            .snapshot(chunk_size)
            .await
            .map(|s| s.map_err(Error::from))
            .boxed()
    }

    pub async fn restore_chunk(&self, chunk: Vec<SnapshotValue>) -> Result<()> {
        Ok(self.indexer.restore(chunk).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn set_manifest(&self, manifest: proposal::ProposalManifest) -> Result<()> {
        let b = bincode::serialize(&manifest)?;
        let value = RecordValue::Bytes(b);
        let mut record = RecordRoot::new();
        record.insert("manifest".to_string(), value);
        Ok(self.indexer.set_system_key("manifest", &record).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_manifest(&self) -> Result<Option<proposal::ProposalManifest>> {
        let record = self.indexer.get_system_key("manifest").await?;
        let value = match record.and_then(|mut r: RecordRoot| r.remove("manifest")) {
            Some(RecordValue::Bytes(b)) => b,
            _ => return Ok(None),
        };
        let manifest: proposal::ProposalManifest = bincode::deserialize(&value)?;
        Ok(Some(manifest))
    }
}

fn get_key(namespace: &str, id: &str) -> [u8; 32] {
    let b = [namespace.as_bytes(), id.as_bytes()].concat();
    hash::hash_bytes(b)
}

fn to_change_keys(changes: &[IndexerChange]) -> Vec<[u8; 32]> {
    changes
        .iter()
        .map(|change| match change {
            IndexerChange::Set {
                collection_id,
                record_id,
                ..
            } => get_key(collection_id, record_id),
            IndexerChange::Delete {
                collection_id,
                record_id,
            } => get_key(collection_id, record_id),
        })
        .collect()
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
