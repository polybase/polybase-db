use std::ops::Deref;

use tempdir::TempDir;

use crate::{storage::Storage, MerkleTree};

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

// snapshot test for a well-known tree - if we accidentally change how the hash is calculated, this
// test will fail
#[test]
fn root_hash_snapshot() {
    let tree: MerkleTree<_, _> = (0..100).map(|i| (i, format!("the value is {i}"))).collect();
    insta::assert_snapshot!(tree.root_hash().to_hex());
}
