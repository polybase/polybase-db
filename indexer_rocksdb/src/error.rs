use crate::keys;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid key/value combination")]
    InvalidKeyValueCombination,

    #[error("keys error")]
    Keys(#[from] keys::KeysError),

    #[error("RocksDB error")]
    RocksDB(#[from] rocksdb::Error),

    #[error("bincode error")]
    Bincode(#[from] bincode::Error),

    #[error("tokio task join error")]
    TokioTaskJoin(#[from] tokio::task::JoinError),

    #[error("snapshot error")]
    Snapshot(#[from] crate::snapshot::Error),

    #[error("index error")]
    Index(#[from] crate::index::Error),
}
