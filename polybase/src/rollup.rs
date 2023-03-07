use rand::Rng;
use std::sync::RwLock;
use winter_crypto::hashers::Rp64_256;

use crate::hash::{self, hash_bytes};
use indexer::RecordRoot;
use rbmerkle::RedBlackTree;

pub type Result<T> = std::result::Result<T, RollupError>;

#[derive(Debug, thiserror::Error)]
pub enum RollupError {
    #[error("Failed to acquire lock")]
    LockError,
    #[error("Failed to serialize record")]
    SerializerError(bincode::Error),
}

pub struct Rollup {
    state: RwLock<RollupState>,
}

pub struct RollupState {
    tree: RedBlackTree<[u8; 32], Rp64_256>,
    hash: Option<[u8; 32]>,
}

impl Rollup {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(RollupState {
                tree: RedBlackTree::<[u8; 32], Rp64_256>::new(),
                hash: None,
            }),
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
        let record_hash = hash::hash(record_bytes);

        // Lock the tree
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(_) => return Err(RollupError::LockError),
        };

        // Insert the new hash
        state.tree.insert(key, record_hash);

        Ok(())
    }

    pub fn delete(&self, key: [u8; 32]) -> Result<()> {
        // Lock the tree
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(_) => return Err(RollupError::LockError),
        };

        // Delete the hash
        state.tree.delete(key);

        Ok(())
    }

    pub fn hash(&self) -> Result<Option<[u8; 32]>> {
        let state = match self.state.write() {
            Ok(state) => state,
            Err(_) => return Err(RollupError::LockError),
        };
        Ok(state.hash)
    }

    pub fn root(&self) -> Result<[u8; 32]> {
        match self.hash()? {
            Some(hash) => Ok(hash),
            None => self.commit(),
        }
    }

    pub fn commit(&self) -> Result<[u8; 32]> {
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(_) => return Err(RollupError::LockError),
        };
        // let hash = state.tree.root_hash().map(|p| p.as_bytes());
        let random_bytes = rand::thread_rng().gen::<[u8; 32]>();
        let hash = hash_bytes(random_bytes.to_vec());
        state.hash = Some(hash);
        Ok(hash)
    }
}
