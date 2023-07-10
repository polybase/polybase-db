use tempdir::TempDir;

use crate::{
    hash::Digest,
    // storage::{rocksdb::RocksDb, Storage},
    tree::{MerkleTree, TreeNode},
};

// 1
// |\
// 2 5
// |\
// 3 4
pub fn example_node() -> TreeNode<i32, i32> {
    let mut node = TreeNode {
        key: 1,
        value: 1,
        hash: Digest::NULL,
        left: Some(Box::new(TreeNode {
            key: 2,
            value: 2,
            hash: Digest::NULL,
            left: Some(Box::new(TreeNode::new(3, 3))),
            right: Some(Box::new(TreeNode::new(4, 4))),
            height: 0,
        })),
        right: Some(Box::new(TreeNode::new(5, 5))),
        height: 0,
    };
    node.update_height();
    node
}

pub fn example_tree() -> MerkleTree<i32, i32> {
    MerkleTree {
        inner: Some(Box::new(example_node())),
    }
}

// #[derive(Debug)]
// pub struct TestDb {
//     _dir: TempDir,
//     db: RocksDb,
// }
//
// impl TestDb {
//     pub fn new() -> Self {
//         let dir = TempDir::new("smirk").unwrap();
//         let db = RocksDb::open(dir.path()).unwrap();
//
//         Self { _dir: dir, db }
//     }
// }
//
// impl Storage for TestDb {}
