//! Persistence backends for Merkle trees
//!
//! The main backend provided is [rocksdb][rocksdb], but the design is kept modular to allow
//! possible future storage backends.
//!
//!
//! [rocksdb]: https://github.com/facebook/rocksdb
use std::fmt::Debug;

use crate::{hash::Hashable, tree::MerkleTree};

/// A rocksdb-based storage implementation
pub mod rocksdb;

mod error;
pub use error::Error;
use serde::{Deserialize, Serialize};

/// Types which can act as a storage backend for a Merkle tree
pub trait Storage<K, V>: Debug
where
    K: Ord + 'static,
    V: Hashable + 'static,
{
    /// Persist the given tree to storage
    fn store_tree(&self, tree: &MerkleTree<K, V>) -> Result<(), Error>
    where
        K: Serialize,
        V: Serialize;

    /// Load a tree from storage
    ///
    /// If no tree has been persisted, `None` should be returned
    fn load_tree(&self) -> Result<Option<MerkleTree<K, V>>, Error>
    where
        K: for<'a> Deserialize<'a>,
        V: for<'a> Deserialize<'a>;
}
