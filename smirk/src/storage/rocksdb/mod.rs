use std::{fmt::Debug, path::Path};

use rocksdb::{IteratorMode, TransactionDB, DB};
use serde::{Deserialize, Serialize};

use crate::{
    hash::{Digest, Hashable},
    MerkleTree,
};

use self::structure::Structure;

use super::{Error, Storage};

mod codec;
pub use codec::{DecodeError, EncodeError};
mod structure;

/// A struct that acts as a [`Storage`] backend by persisting data in [rocksdb][db]
///
/// Broadly speaking, this type works by:
///  - serializing a tree-like "structure" object to the key `"structure"`
///  - serializing binary encoded key-value pairs to the key `rpo(value)` (note - this is not the
///  hash of a given node, which includes the children in the hash)
///
/// [db]: https://github.com/facebook/rocksdb
pub struct RocksdbStorage {
    pub(crate) instance: TransactionDB,
}

impl Debug for RocksdbStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RocksdbStorage")
    }
}

impl RocksdbStorage {
    /// Create a new [`RocksdbStorage`] from an existing rocksdb instance
    ///
    /// This is useful if you want to create transactions that modify both data managed by smirk,
    /// as well as data external to smirk
    pub fn from_instance(instance: TransactionDB) -> Self {
        Self { instance }
    }

    /// Create a new [`RocksdbStorage`] by opening a new rocksdb instance at the given path
    pub fn open(path: &Path) -> Result<Self, rocksdb::Error> {
        let instance = TransactionDB::open_default(path)?;
        Ok(Self { instance })
    }

    const STRUCTURE_KEY: &[u8] = b"structure";
}

impl<K, V> Storage<K, V> for RocksdbStorage
where
    K: Ord + 'static,
    V: Hashable + 'static,
{
    fn store_tree(&self, tree: &MerkleTree<K, V>) -> Result<(), Error>
    where
        K: Serialize,
        V: Serialize,
    {
        let tx = self.instance.transaction();

        let structure = structure::Structure::from_tree(tree);
        let structure_bytes = codec::encode(&structure).map_err(err)?;

        tx.put(Self::STRUCTURE_KEY, structure_bytes).map_err(err)?;

        for node in tree.iter() {
            let hash = node.value().hash();
            let bytes = codec::encode(&(node.key(), node.value())).map_err(err)?;

            tx.put(&hash.to_bytes(), &bytes).map_err(err)?;
        }

        tx.commit().map_err(err)?;

        Ok(())
    }

    fn load_tree(&self) -> Result<Option<MerkleTree<K, V>>, Error>
    where
        K: for<'a> Deserialize<'a>,
        V: for<'a> Deserialize<'a>,
    {
        let Some(structure_bytes) = self.instance.get(Self::STRUCTURE_KEY).map_err(err)? else { 
            return Ok(None); 
        };

        let structure: Option<Structure> = codec::decode(&structure_bytes).map_err(Error::MalformedStructure)?;
        let Some(structure) = structure else { return Ok(Some(MerkleTree::new())) };

        let mut values = self.instance.iterator(IteratorMode::Start).filter(|result| {
            match result {
                // don't try to deserialize this key
                Ok((hash, _data)) => hash.as_ref() != Self::STRUCTURE_KEY,
                Err(_) => true,
            }
        }).map(|result| {
            let (hash, data) = result.map_err(err)?;

            let hash = get_hash(&hash)?;
            let data: (K, V) = codec::decode(&data).map_err(err)?;

            Ok((hash, data))

        }).collect::<Result<_, Error>>()?;
        
        let tree = structure.to_tree(&mut values)?;

        Ok(Some(tree))
    }
}

fn get_hash(bytes: &[u8]) -> Result<Digest, Error> {
    let hash_bytes = bytes
        .as_ref()
        .try_into()
        .map_err(|_| Error::InvalidHashBytes(bytes.to_vec()))?;

    let hash = Digest::from_bytes(hash_bytes)
        .ok_or_else(|| Error::InvalidHashBytes(hash_bytes.to_vec()))?;

    Ok(hash)
}

fn err<E: std::error::Error + Send + Sync + 'static>(e: E) -> Error {
    Error::Unknown(Box::new(e))
}
