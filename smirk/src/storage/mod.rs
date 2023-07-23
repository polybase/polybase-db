//! Persistence layer for [`MerkleTree`]s
use std::{fmt::Debug, path::Path};

use crate::{hash::Hashable, tree::MerkleTree};
use rocksdb::{Transaction, TransactionDB};
use serde::{Deserialize, Serialize};

mod codec;
mod error;

#[cfg(test)]
mod tests;

pub use codec::{DecodeError, EncodeError};
pub use error::Error;

/// A rocksdb-based storage mechanism for [`MerkleTree`]s
///
/// ```rust,no_run
/// # use std::path::Path;
/// # use smirk::storage::Storage;
/// # use smirk::smirk;
/// let storage = Storage::open(Path::new("./db")).unwrap();
///
/// let tree = smirk! {
///   1 => "hello".to_string(),
///   2 => "world".to_string(),
/// };
///
/// storage.store_tree(&tree).unwrap();
///
/// // 2x .unwrap() because it returns `Ok(None)` if no tree has been stored yet
/// let tree_again = storage.load_tree().unwrap().unwrap();
///
/// // the root hashes are the same (since this is what the `Eq` impl for `MerkleTree` uses)
/// assert_eq!(tree, tree_again);
/// ```
///
/// This storage preserves the tree structure, meaning the root hash will not be changed by
/// loading it from storage.
pub struct Storage {
    instance: TransactionDB,
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Storage")
    }
}

impl Storage {
    /// Create a new [`Storage`] from an existing rocksdb instance
    ///
    /// This is useful if you want to create transactions that modify both data managed by smirk,
    /// as well as data external to smirk
    pub fn from_instance(instance: TransactionDB) -> Self {
        Self { instance }
    }

    /// Create a new [`Storage`] by opening a new rocksdb instance at the given path
    pub fn open(path: &Path) -> Result<Self, Error> {
        let instance = TransactionDB::open_default(path)?;
        Ok(Self { instance })
    }

    /// Key used to store the value of the root of the database
    const ROOT_KEY: &[u8] = b"root";
}

impl Storage {
    /// Store a tree
    pub fn store_tree<K, V>(&self, tree: &MerkleTree<K, V>) -> Result<(), Error>
    where
        K: Serialize + 'static + Ord,
        V: Serialize + 'static + Hashable,
    {
        let tx = self.instance.transaction();
        self.store_tree_with_tx(tree, &tx)?;
        tx.commit()?;

        Ok(())
    }

    /// Store a tree with a given transaction
    pub fn store_tree_with_tx<K, V>(
        &self,
        tree: &MerkleTree<K, V>,
        tx: &Transaction<TransactionDB>,
    ) -> Result<(), Error>
    where
        K: Serialize + 'static + Ord,
        V: Serialize + 'static + Hashable,
    {
        codec::write_tree_to_tx(tx, tree)
    }

    /// Load a tree from storage, if it is present
    pub fn load_tree<K, V>(&self) -> Result<Option<MerkleTree<K, V>>, Error>
    where
        K: for<'a> Deserialize<'a> + 'static + Hashable + Ord,
        V: for<'a> Deserialize<'a> + 'static + Hashable,
    {
        let tx = self.instance.transaction();
        let tree = self.load_tree_with_tx(&tx)?;
        tx.commit()?;

        Ok(tree)
    }

    /// Load a tree from storage, if it is present, using the given transaction
    pub fn load_tree_with_tx<K, V>(
        &self,
        tx: &Transaction<TransactionDB>,
    ) -> Result<Option<MerkleTree<K, V>>, Error>
    where
        K: for<'a> Deserialize<'a> + 'static + Hashable + Ord,
        V: for<'a> Deserialize<'a> + 'static + Hashable,
    {
        let key = tx.get(Self::ROOT_KEY)?;

        let Some(key) = key else { return Ok(None) };

        if key.is_empty() {
            return Ok(Some(MerkleTree::new()));
        }

        let node = codec::load_node(tx, &key)?;
        let tree = MerkleTree {
            inner: Some(Box::new(node)),
        };

        Ok(Some(tree))
    }
}
