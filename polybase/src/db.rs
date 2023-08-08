use crate::hash;
use crate::mempool::Mempool;
use crate::txn::{self, CallTxn};
use futures_util::StreamExt;
use gateway::Gateway;
use indexer_db_adaptor::adaptor::IndexerAdaptor;
use indexer_db_adaptor::{auth_user::AuthUser, list_query::ListQuery, Indexer};
use indexer_rocksdb::snapshot::{SnapshotChunk, SnapshotIterator};
use parking_lot::Mutex;
use schema::methods;
use schema::record::{
    self, json_to_record, record_to_json, ForeignRecordReference, RecordReference, RecordRoot,
    RecordValue,
};
use serde::{Deserialize, Serialize};
use solid::proposal::{self};
use std::cmp::min;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("collection not found")]
    CollectionNotFound,

    #[error("collection AST not found in collection record")]
    CollectionASTNotFound,

    #[error("collection AST is invalid: {0}")]
    CollectionASTInvalid(String),

    #[error("cannot update the Collection collection record")]
    CollectionCollectionRecordUpdate,

    #[error("invalid function args response")]
    InvalidFunctionArgsResponse,

    #[error("record error: {0}")]
    Record(#[from] record::RecordError),

    #[error("user error: {0}")]
    User(#[from] UserError),

    #[error(transparent)]
    Gateway(#[from] gateway::GatewayError),

    #[error("indexer error")]
    Indexer(#[from] indexer_db_adaptor::Error),

    #[error("serialize error")]
    Serializer(#[from] bincode::Error),

    #[error("serde_json error")]
    SerdeJson(#[from] serde_json::Error),

    #[error("call txn error")]
    CallTxn(#[from] txn::CallTxnError),

    #[error("tokio send error")]
    TokioSend(#[from] mpsc::error::SendError<CallTxn>),
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {
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

    #[error("methods error")]
    Method(#[from] methods::UserError),
}

#[derive(Debug, PartialEq)]
pub enum Change {
    Create {
        collection_id: String,
        record_id: String,
        record: RecordRoot,
    },
    Update {
        collection_id: String,
        record_id: String,
        record: RecordRoot,
    },
    Delete {
        collection_id: String,
        record_id: String,
    },
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
                .last_record_update(&collection_id, &record_id)
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
            Ok(self.indexer.last_collection_update(&collection_id).await?)
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

    async fn call_changes(&self, txn: &CallTxn) -> Result<(String, Vec<Change>)> {
        let CallTxn {
            collection_id,
            record_id,
            function_name: method,
            args,
            auth,
        } = txn;

        let schema = std::sync::Arc::new(self.indexer.get_schema_required(&collection_id).await?);
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

        // Get the js code to run
        let js_code = method.generate_js();

        // Get current record instance
        let record = if method.name == "constructor" {
            RecordRoot::new()
        } else {
            match self
                .indexer
                .get(&collection_id, record_id, public_key)
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
        if method.name != "constructor" {
            self.indexer
                .verify_call(collection_id, &method.name, &schema, &record, public_key);
        }

        // Get args as RecordValues (so we can validate them and find the references)
        let arg_values = method.args_from_json(args).map_err(UserError::from)?;

        // TODO: Validate args against schema

        // Convert all record references to JSON
        let extended_args = futures::future::join_all(args.into_iter().zip(&arg_values).map(
            |(json, val)| async move {
                match val {
                    // TODO(minor): clean up duplicate code
                    RecordValue::ForeignRecordReference(ForeignRecordReference {
                        id,
                        collection_id,
                    }) => {
                        let record = self
                            .indexer
                            .get(&collection_id, &id, public_key)
                            .await?
                            .ok_or(UserError::RecordNotFound {
                                collection_id: collection_id.to_string(),
                                record_id: id.to_string(),
                            })?;
                        Ok(record_to_json(record))
                    }
                    RecordValue::RecordReference(RecordReference { id }) => {
                        let record = self
                            .indexer
                            .get(&collection_id, &id, public_key)
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
            },
        ))
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
                &extended_args,
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

        // let Some(output_instance_id) = output_record.get("id") else {
        //     return Err(UserError::RecordIdNotFound)?;
        // };

        // Check output ID is a string
        let RecordValue::String(output_instance_id) = output_instance_id else {
            return Err(UserError::RecordIdNotString)?;
        };

        // Check output args are same as input, otherwise something strange has happened,
        // possibly someone messing around with the JS code
        if extended_args.len() != output.args.len() {
            return Err(Error::InvalidFunctionArgsResponse)?;
        }

        // TODO: check we can't modify records in other collections

        // Find changes in the args
        let mut changes: Vec<_> = futures::future::join_all(
            extended_args
                .into_iter()
                .zip(output.args)
                .zip(arg_values)
                .filter(|(_, value)| match value {
                    RecordValue::ForeignRecordReference(_) => true,
                    RecordValue::RecordReference(_) => true,
                    _ => false,
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
                                Ok(Change::Update {
                                    collection_id,
                                    record_id: id,
                                    record: json_to_record(&schema, output, false)?,
                                })
                            }
                            RecordValue::RecordReference(RecordReference { id }) => {
                                Ok(Change::Update {
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

        if method.name == "constructor" {
            // TODO: if we're creating a new collection, let's do more validation here!
            changes.push(Change::Create {
                collection_id: collection_id.to_string(),
                record_id: output_instance_id.to_string(),
                record: output_record,
            });
        } else if output.self_destruct {
            changes.push(Change::Delete {
                collection_id: collection_id.to_string(),
                record_id: output_instance_id.to_string(),
            });
        } else if output_record_changed {
            changes.push(Change::Update {
                collection_id: collection_id.to_string(),
                record_id: output_instance_id.to_string(),
                record: output_record,
            });
        }

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

    // #[tracing::instrument(skip(self))]
    pub async fn commit_txn(&self, txn: CallTxn) -> Result<()> {
        // Get changes
        let (_, changes) = self.call_changes(&txn).await?;

        for change in changes.iter() {
            // Insert into indexer
            match change {
                Change::Create {
                    record,
                    collection_id,
                    record_id,
                } => self.set(&collection_id, &record_id, record).await?,
                Change::Update {
                    record,
                    collection_id,
                    record_id,
                } => self.set(collection_id, record_id, record).await?,
                Change::Delete {
                    record_id,
                    collection_id,
                } => self.delete(collection_id, record_id).await?,
            };
        }

        Ok(())
    }

    /// Reset all data in the database
    pub fn reset(&self) -> Result<()> {
        todo!()
        //Ok(self.indexer.reset()?)
    }

    /// Create a snapshot iterator, that can be used to iterate over the
    /// entire database in chunks
    pub fn snapshot_iter(&self, _chunk_size: usize) -> SnapshotIterator {
        todo!()
        //self.indexer.snapshot(chunk_size)
    }

    pub fn restore_chunk(&self, _chunk: SnapshotChunk) -> Result<()> {
        todo!()
        //self.indexer.restore(chunk)?;
        //Ok(())
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

    #[tracing::instrument(skip(self))]
    pub async fn delete(&self, collection_id: &str, record_id: &str) -> Result<()> {
        // Update the indexer
        self.indexer.delete(collection_id, record_id).await?;

        Ok(())
    }

    async fn set(&self, collection_id: &str, record_id: &str, record: &RecordRoot) -> Result<()> {
        // Get the indexer collection instance
        self.indexer.set(&collection_id, record_id, record).await?;

        Ok(())
    }

    // async fn validate_schema_update(
    //     &self,
    //     collection_id: &str,
    //     record_id: &str,
    //     record: &RecordRoot,
    //     auth: Option<&AuthUser>,
    // ) -> Result<()> {
    //     let collection = self.indexer.collection(&collection_id).await?;

    //     let old_record = collection
    //         .get(&record_id, auth)
    //         .await
    //         .map_err(IndexerError::from)?
    //         .ok_or(Error::CollectionNotFound)?;

    //     let old_ast = old_record.get("ast").ok_or(Error::CollectionASTNotFound)?;

    //     let RecordValue::String(old_ast) = old_ast
    //         else {
    //             return Err(Error::CollectionASTInvalid("Collection AST in old record is not a string".into()));
    //         };

    //     let RecordValue::String(new_ast) = record
    //             .get("ast")
    //             .ok_or(Error::CollectionASTNotFound)? else {
    //         return Err(Error::CollectionASTInvalid("Collection AST in new record is not a string".into()));
    //     };

    //     validate_schema_change(
    //         #[allow(clippy::unwrap_used)] // split always returns at least one element
    //         record_id.split('/').last().unwrap(),
    //         serde_json::from_str(&old_ast)?,
    //         serde_json::from_str(new_ast)?,
    //     )
    //     .map_err(IndexerError::from)?;

    //     validate_collection_record(record).map_err(IndexerError::from)?;

    //     Ok(())
    // }
}

fn get_key(namespace: &str, id: &str) -> [u8; 32] {
    let b = [namespace.as_bytes(), id.as_bytes()].concat();
    hash::hash_bytes(b)
}

fn to_change_keys(changes: &Vec<Change>) -> Vec<[u8; 32]> {
    changes
        .iter()
        .map(|change| match change {
            Change::Create {
                collection_id,
                record_id,
                ..
            } => get_key(collection_id, record_id),
            Change::Update {
                collection_id,
                record_id,
                ..
            } => get_key(collection_id, record_id),
            Change::Delete {
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
