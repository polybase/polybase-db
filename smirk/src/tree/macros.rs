/// Macro to generate a [`MerkleTree`] with a more convenient syntax
///
/// ```rust
/// # use smirk::smirk;
/// let tree = smirk! {
///   1 => "hello".to_string(),
///   2 => "world".to_string(),
/// };
///
/// assert_eq!(tree.get(&1).unwrap(), "hello");
/// ```
///
/// [`MerkleTree`]: crate::MerkleTree
#[macro_export]
macro_rules! smirk {
    {} => {{ $crate::MerkleTree::new() }};
    { $key:expr => $value:expr $(,)? } => {{
        let mut tree = $crate::MerkleTree::new();
        tree.insert($key, $value);
        tree
    }};

    { $key:expr => $value:expr, $($t:tt)* } => {{
        let mut tree = smirk!{ $($t)* };
        tree.insert($key, $value);
        tree
    }};
}

#[cfg(test)]
mod tests {
    use crate::MerkleTree;

    #[test]
    fn simple_syntax_test() {
        let tree = smirk! {
            1 => "hello",
            2 => "world"  // without trailing comma
        };

        let other_tree = smirk! {
            1 => "hello",
            2 => "world",  // with trailing comma
        };

        assert_eq!(tree.root_hash(), other_tree.root_hash());

        assert_eq!(*tree.get(&1).unwrap(), "hello");
        assert_eq!(*tree.get(&2).unwrap(), "world");
        assert_eq!(tree.get(&3), None);

        let _many_items = smirk! {
            1 => "hello",
            2 => "world",
            3 => "foo",
            4 => "bar",
        };
        let _many_items_no_trailing = smirk! {
            1 => "hello",
            2 => "world",
            3 => "foo",
            4 => "bar"
        };

        let _single_item = smirk! {
            1 => "hello",
        };

        let _single_item_no_trailing = smirk! {
            1 => "hello"
        };

        let _empty: MerkleTree<i32, i32> = smirk! {};
    }
}
