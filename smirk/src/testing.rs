use std::ops::Deref;

use tempdir::TempDir;

use crate::{storage::Storage, tree::MerkleTree};

/// Helper struct that makes it easier to test against a rocksdb instance
#[derive(Debug)]
pub struct TestStorage {
    _dir: TempDir,
    db: Storage,
}

impl TestStorage {
    pub fn new() -> Self {
        let dir = TempDir::new("smirk").unwrap();
        let db = Storage::open(dir.path()).unwrap();

        Self { _dir: dir, db }
    }
}

impl Deref for TestStorage {
    type Target = Storage;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

#[test]
fn simple_storage_test() {
    let db = TestStorage::new();

    assert_eq!(db.load_tree().unwrap(), None::<MerkleTree<i32, String>>);

    let tree = (0..10).map(|i| (i, format!("the data is {i}"))).collect();
    db.store_tree(&tree).unwrap();

    let tree_again: MerkleTree<i32, String> = db.load_tree().unwrap().unwrap();

    assert_eq!(tree, tree_again);
}
