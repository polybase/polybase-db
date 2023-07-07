use super::*;

#[test]
fn simple_example() {
    let mut tree = MerkleTree::from_iter([1, 2, 3]);

    assert_eq!(tree.size(), 3);

    tree.insert(4);
    assert_eq!(tree.size(), 4);

    println!("{tree:#?}");

    let _items: Vec<_> = tree.depth_first().copied().collect();
}
