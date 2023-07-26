use super::{DecodeError, EncodeError};

/// An error encountered while persisting or restoring a [`MerkleTree`]
///
/// [`MerkleTree`]: crate::MerkleTree
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Key didn't exist in the database
    #[error("couldn't find key in database: 0x{}", hex::encode(.0))]
    KeyMissing(Vec<u8>),

    /// Error encoding data to binary format
    #[error("error encoding data to binary format: {0}")]
    Encode(#[from] EncodeError),

    /// Error decoding data from binary format
    #[error("error decoding data from binary format: {0}")]
    Decode(#[from] DecodeError),

    /// Rocksdb error
    #[error("rocksdb error: {0}")]
    Unknown(#[from] rocksdb::Error),
}
