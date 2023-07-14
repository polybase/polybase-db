//! The abstract interface for a store (database).
//! Various concrete implemementations can exists, each implementing the
//! contract specified by this interface.

use crate::snapshot::{SnapshotChunk, SnapshotIterator};
use async_trait::async_trait;
use prost::Message;

use crate::{
    keys::{self, Key},
    proto,
    record::RecordRoot,
    store,
};

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("invalid key/value combination")]
    InvalidKeyValueCombination,

    #[error("keys error")]
    KeysError(#[from] keys::KeysError),

    #[error("rocksdb error")]
    RocksDBError(#[from] rocksdb::Error),

    #[error("store error")]
    StoreError(#[from] store::StoreError),

    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),

    #[error("tokio task join error")]
    TokioTaskJoinError(#[from] tokio::task::JoinError),

    #[error("snapshot error")]
    SnapshotError(#[from] crate::snapshot::Error),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;

#[derive(Debug)]
pub enum Value<'a> {
    DataValue(&'a RecordRoot),
    IndexValue(proto::IndexRecord),
}

impl<'a> Value<'a> {
    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        match self {
            Value::DataValue(value) => Ok(bincode::serialize(value)?),
            Value::IndexValue(value) => Ok(value.encode_to_vec()),
        }
    }
}

#[async_trait]
pub trait Database: Send + Sync {
    async fn commit(&self) -> Result<()>;
    async fn set(&self, key: &Key<'_>, value: &Value<'_>) -> Result<()>;
    async fn get(&self, key: &Key<'_>) -> Result<Option<RecordRoot>>;
    async fn delete(&self, key: &Key<'_>) -> Result<()>;

    fn list(
        &self,
        lower_bound: &Key,
        upper_bound: &Key,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>> + '_>>;

    fn destroy(self) -> Result<()>;
    fn reset(&self) -> Result<()>;
    fn snapshot(&self, chunk_size: usize) -> SnapshotIterator;
    fn restore(&self, chunk: SnapshotChunk) -> Result<()>;
}
