use std::fmt::Debug;

use crate::tree::MerkleTree;

pub mod noop;
pub mod rocksdb;

mod error;
pub use error::Error;
use serde::{Deserialize, Serialize};

/// Types which can act as a storage backend for a Merkle tree
pub trait Storage: Debug {
    fn store_tree<T: Serialize>(&self, tree: &MerkleTree<T>) -> Result<(), Error>;

    fn load_tree<T: Clone + Deserialize>(&self) -> Result<Option<MerkleTree<T>>, Error>;
}

#[cfg(test)]
mod tests {
    use crate::testing::{example_tree, TestDb};

    use super::*;

    #[test]
    fn simple_example() {
        let test_db = TestDb::new();
        let tree = example_tree();

        assert!(test_db.load_tree().unwrap().is_none());

        test_db.store_tree(&tree).unwrap();
        let tree_again = test_db.load_tree().unwrap().unwrap();
    }
}
