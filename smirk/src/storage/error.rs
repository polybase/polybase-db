use crate::hash::Digest;

use super::{DecodeError, EncodeError};

/// An error encountered while persisting or restoring a [`MerkleTree`]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid hash bytes as key
    #[error("the following bytes were used as a key, but were not a valid RPO hash: {0:?}")]
    InvalidHashBytes(Vec<u8>),

    /// Hash mismatch
    #[error("the hash didn't match the computed hash of the stored value - computed: {computed}, stored: {stored}")]
    HashMismatch {
        /// The hash that was computed by hashing the stored value
        computed: Digest,
        /// The hash that was stored in the database
        stored: Digest,
    },

    /// The database referenced data in the structure that was not found in the database
    #[error("no data assocated with {hash}, but found in structure")]
    StructureReferenceMissing {
        /// The hash that was missing
        hash: Digest,
    },

    /// Malformed structure
    #[error("malformed structure: {0}")]
    MalformedStructure(DecodeError),

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
