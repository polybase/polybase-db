//! Persistence layer for [`MerkleTree`]s
use std::{fmt::Debug, path::Path};

use crate::{hash::{Hashable, Digest}, tree::MerkleTree};
use rocksdb::{TransactionDB, IteratorMode};
use serde::{Deserialize, Serialize};

mod structure;
mod codec;
pub use codec::{EncodeError, DecodeError};
mod error;
pub use error::Error;

use self::structure::Structure;


/// A rocksdb-based storage mechanism for [`MerkleTree`]s
///
/// ```rust
/// 
/// ```
pub struct Storage {
    instance: TransactionDB,
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Storage")
    }
}

impl Storage {
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

impl Storage {
    /// Store a tree
    pub fn store_tree<K, V>(&self, tree: &MerkleTree<K, V>) -> Result<(), Error>
    where
        K: Serialize + 'static + Ord,
        V: Serialize + 'static + Hashable,
    {
        let tx = self.instance.transaction();

        let structure = Structure::from_tree(tree);
        let structure_bytes = codec::encode(&structure)?;

        tx.put(Self::STRUCTURE_KEY, structure_bytes)?;

        for node in tree.iter() {
            let hash = node.value().hash();
            let bytes = codec::encode(&(node.key(), node.value()))?;

            tx.put(&hash.to_bytes(), &bytes)?;
        }

        tx.commit()?;

        Ok(())
    }

    /// Load a tree from storage, if it is present
    pub fn load_tree<K, V>(&self) -> Result<Option<MerkleTree<K, V>>, Error>
    where
        K: for<'a> Deserialize<'a> + 'static + Ord,
        V: for<'a> Deserialize<'a> + 'static + Hashable,
    {
        let Some(structure_bytes) = self.instance.get(Self::STRUCTURE_KEY)? else { 
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
            let (hash, data) = result?;

            let hash = get_hash(&hash)?;
            let data: (K, V) = codec::decode(&data)?;

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


