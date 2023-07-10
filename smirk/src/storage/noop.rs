use crate::MerkleTree;

use super::{Error, Storage};

/// A dummy storage type which does nothing
#[derive(Debug, Default)]
pub struct NoopStorage;

impl Storage for NoopStorage {
    // fn load_tree<T>(&self) -> Result<Option<MerkleTree<T>>, Error> {
    //     Ok(None)
    // }
    //
    // fn store_tree<T>(&self, _tree: &MerkleTree<T>) -> Result<(), Error> {
    //     Ok(())
    // }
}
