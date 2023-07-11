use tempdir::TempDir;

use crate::{
    hash::Hashable,
    storage::{rocksdb::RocksdbStorage, Storage},
    tree::MerkleTree,
};

/// Helper struct that makes it easier to test against a rocksdb instance
#[derive(Debug)]
pub struct TestStorage {
    _dir: TempDir,
    db: RocksdbStorage,
}

impl TestStorage {
    pub fn new() -> Self {
        let dir = TempDir::new("smirk").unwrap();
        let db = RocksdbStorage::open(dir.path()).unwrap();

        Self { _dir: dir, db }
    }
}

impl<K, V> Storage<K, V> for TestStorage
where
    K: Ord + 'static,
    V: Hashable + 'static,
{
    fn store_tree(&self, tree: &MerkleTree<K, V>) -> Result<(), crate::storage::Error>
    where
        K: serde::Serialize,
        V: serde::Serialize,
    {
        self.db.store_tree(tree)
    }

    fn load_tree(&self) -> Result<Option<MerkleTree<K, V>>, crate::storage::Error>
    where
        K: for<'a> serde::Deserialize<'a>,
        V: for<'a> serde::Deserialize<'a>,
    {
        self.db.load_tree()
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
