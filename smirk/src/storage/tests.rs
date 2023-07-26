use super::*;

use pretty_assertions::assert_eq;
use proptest::prop_assert_eq;
use test_strategy::proptest;

use crate::{key_value_hash, smirk, testing::TestStorage};

#[test]
fn empty_db_returns_none() {
    let db = TestStorage::new();

    assert!(db.load_tree::<i32, i32>().unwrap().is_none());
}

// test rocksdb behaviour, since we rely on this for storing empty trees
#[test]
fn store_empty_bytes_does_something() {
    let db = TestStorage::new();
    assert_eq!(db.instance.get(b"hello").unwrap(), None);
    db.instance.put(b"hello", []).unwrap();
    assert_eq!(db.instance.get(b"hello").unwrap(), Some(vec![]));
}

#[test]
fn storing_empty_tree_returns_empty_tree() {
    let db = TestStorage::new();
    let tree = MerkleTree::<i32, i32>::new();

    db.store_tree(&tree).unwrap();

    assert_eq!(db.load_tree().unwrap(), Some(tree));
}

#[test]
fn storing_simple_tree() {
    let db = TestStorage::new();
    let tree = smirk! {
        1 => "hello".to_string(),
        2 => "world".to_string(),
        3 => "foo".to_string(),
    };

    println!("hash: {}", tree.get_node(&3).unwrap().hash());

    db.store_tree(&tree).unwrap();
    let mut tree_again = db.load_tree().unwrap().unwrap();

    let changed = tree_again.recalculate_hash_recursive();
    dbg!(changed);

    assert_eq!(
        tree_again.get_node(&1).unwrap().hash(),
        key_value_hash(&1, "hello")
    );

    assert_eq!(
        tree_again.get_node(&3).unwrap().hash(),
        key_value_hash(&3, "foo")
    );

    assert_eq!(tree, tree_again);
}

#[proptest]
fn storage_round_trip(tree: MerkleTree<i32, String>) {
    let db = TestStorage::new();

    db.store_tree(&tree).unwrap();
    let tree_again = db.load_tree::<i32, String>().unwrap().unwrap();

    prop_assert_eq!(tree, tree_again);
}
