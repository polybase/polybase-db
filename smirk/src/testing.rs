use tempdir::TempDir;

use crate::{
    storage::{rocksdb::RocksDb, Storage},
    tree::{MerkleTree, TreeNode},
};

// 1
// |\
// 2 5
// |\
// 3 4
pub fn example_node() -> TreeNode<i32> {
    let mut node = TreeNode {
        value: 1,
        left: Some(Box::new(TreeNode {
            value: 2,
            left: Some(Box::new(TreeNode::new(3))),
            right: Some(Box::new(TreeNode::new(4))),
            height: 0,
        })),
        right: Some(Box::new(TreeNode::new(5))),
        height: 0,
    };
    node.update_height();
    node
}

pub fn example_tree() -> MerkleTree<i32> {
    MerkleTree {
        inner: Some(Box::new(example_node())),
    }
}

#[derive(Debug)]
pub struct TestDb {
    _dir: TempDir,
    db: RocksDb,
}

impl TestDb {
    pub fn new() -> Self {
        let dir = TempDir::new("smirk").unwrap();
        let db = RocksDb::open(dir.path()).unwrap();

        Self { _dir: dir, db }
    }
}

impl Storage for TestDb {
    fn store_tree<T: AsRef<[u8]>>(
        &self,
        tree: &MerkleTree<T>,
    ) -> Result<(), crate::storage::Error> {
        self.db.store_tree(tree)
    }

    fn load_tree<T: Clone + From<Vec<u8>>>(
        &self,
    ) -> Result<Option<MerkleTree<T>>, crate::storage::Error> {
        self.db.load_tree()
    }
}
