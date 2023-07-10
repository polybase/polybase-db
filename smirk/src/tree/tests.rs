use crate::smirk;

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
