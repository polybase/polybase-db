use test_strategy::proptest;

use crate::{hash::{Hashable, Digest}, smirk, MerkleTree};

#[test]
fn simple_example() {
    let mut tree = smirk! {
        1 => 1,
        2 => 2,
        3 => 3,
    };

    assert_eq!(tree.size(), 3);

    tree.insert(4, 4);
    assert_eq!(tree.size(), 4);

    println!("{tree:#?}");

    let _items: Vec<_> = tree.depth_first().collect();
}

#[test]
fn insert_already_exists() {
    let mut tree = smirk! { 1 => "hello" };

    tree.insert(1, "world");

    assert_eq!(*tree.get(&1).unwrap(), "hello");
}

#[test]
fn new_tree_is_empty() {
    let tree = MerkleTree::<i32, i32>::new();
    assert!(tree.is_empty());
}

#[proptest(cases = 100)]
fn collecting_tree_has_same_length(items: Vec<i32>) {
    let len = items.len();
    let tree: MerkleTree<_, _> = items.into_iter().map(|i| (i, i)).collect();

    assert_eq!(tree.size(), len);
}

#[test]
fn hash_includes_key_and_value() {
    let tree = smirk! { 1 => "hello" };
    let different_key = smirk! { 2 => "hello" };
    let different_value = smirk! { 1 => "world" };

    let hash = |tree: &MerkleTree<i32, &str>| tree.inner.as_ref().unwrap().hash;

    assert_ne!(hash(&tree), hash(&different_key));
    assert_ne!(hash(&tree), hash(&different_value));
}

#[test]
fn hash_of_leaf_is_correct() {
    let tree = smirk! { 1 => "hello" };
    let hash = tree.inner.as_ref().unwrap().hash;

    let expected: Digest = [1.hash(), "hello".hash()].iter().collect();

    assert_eq!(hash, expected);
}
