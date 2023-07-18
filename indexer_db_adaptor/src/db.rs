//! The abstract interface for a store (database).
//! Various concrete implementations can exists, each implementing the
//! contract specified by this interface.

use crate::record::RecordRoot;
use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("invalid key/value combination")]
    InvalidKeyValueCombination,

    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),

    #[error("tokio task join error")]
    TokioTaskJoinError(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;

#[async_trait]
pub trait Database: Send + Sync {
    type Key: From<String>;
    type Value: From<RecordRoot> + Into<RecordRoot>;

    async fn commit(&self) -> Result<()>;
    async fn set(&self, key: &Self::Key, value: &Self::Value) -> Result<()>;
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>>;
    async fn delete(&self, key: &Self::Key) -> Result<()>;

    fn list(
        &self,
        key: Self::Value, // todo
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>> + '_>>;

    fn destroy(self) -> Result<()>;
    fn reset(&self) -> Result<()>;
}
