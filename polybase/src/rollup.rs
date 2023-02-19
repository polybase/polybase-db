use std::sync::RwLock;
use winter_crypto::hashers::Rp64_256;
use winter_crypto::{Digest, Hasher};

use indexer::RecordRoot;
use rbmerkle::RedBlackTree;

pub type Result<T> = std::result::Result<T, RollupError>;

pub enum RollupError {
    LockError,
    SerializerError(bincode::Error),
}

pub struct Rollup {
    tree: RwLock<RedBlackTree<[u8; 32], Rp64_256>>,
}

impl Rollup {
    pub fn new() -> Self {
        Self {
            tree: RwLock::new(RedBlackTree::<[u8; 32], Rp64_256>::new()),
        }
    }

    pub fn insert(&self, key: [u8; 32], record: &RecordRoot) -> Result<()> {
        // Serialize the record into bytes, so we can capture the hash
        let record_bytes = match bincode::serialize(&record) {
            Ok(b) => b,
            Err(e) => {
                return Err(RollupError::SerializerError(e));
            }
        };

        // Capture the hash of the bin record
        let record_hash = Rp64_256::hash(&record_bytes);

        // Lock the tree
        let mut tree = match self.tree.write() {
            Ok(tree) => tree,
            Err(_) => return Err(RollupError::LockError),
        };

        // Insert the new hash
        tree.insert(key, record_hash);

        Ok(())
    }

    pub fn delete(&self, key: [u8; 32]) -> Result<()> {
        // Lock the tree
        let mut tree = match self.tree.write() {
            Ok(tree) => tree,
            Err(_) => return Err(RollupError::LockError),
        };

        // Delete the hash
        tree.delete(key);

        Ok(())
    }
}
