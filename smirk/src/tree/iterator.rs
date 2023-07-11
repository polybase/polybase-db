use std::iter::empty;

use crate::{hash::Hashable, MerkleTree, TreeNode};

impl<K: Ord, V: Hashable> FromIterator<(K, V)> for MerkleTree<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut tree = MerkleTree::new();

        for (key, value) in iter {
            tree.insert(key, value);
        }

        tree
    }
}

impl<'a, K, V> MerkleTree<K, V> {
    /// Create an [`Iterator`] over the nodes in key order (i.e. the order specified by the `Ord`
    /// impl for `K`)
    ///
    /// ```rust
    /// # use smirk::{smirk, MerkleTree};
    /// let tree = smirk! {
    ///   1 => "hello",
    ///   2 => "world",
    ///   3 => "foo",
    /// };
    ///
    /// let keys: Vec<_> = tree.iter().map(|node| *node.key()).collect();
    ///
    /// assert_eq!(keys, vec![1, 2, 3]);
    /// ```
    pub fn iter(&'a self) -> Iter<'a, K, V> {
        match &self.inner {
            None => Iter::empty(),
            Some(node) => Iter::node(node),
        }
    }
}

pub struct Iter<'a, K, V>(Box<dyn Iterator<Item = &'a TreeNode<K, V>> + 'a>);

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = &'a TreeNode<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'a, K, V> Iter<'a, K, V> {
    fn empty() -> Self {
        Self(Box::new(empty()))
    }

    fn node(node: &'a TreeNode<K, V>) -> Self {
        Self(Box::new(iter(node)))
    }
}

fn iter<'a, K, V>(node: &'a TreeNode<K, V>) -> Box<dyn Iterator<Item = &'a TreeNode<K, V>> + 'a> {
    let left_iter = node.left.iter().flat_map(|node| iter(node));
    let right_iter = node.right.iter().flat_map(|node| iter(node));

    Box::new(left_iter.chain(Some(node)).chain(right_iter))
}

#[cfg(test)]
mod tests {
    use proptest::prop_assert_eq;
    use test_strategy::proptest;

    use super::*;

    #[proptest(cases = 100)]
    fn iter_order_is_correct(mut vec: Vec<i32>) {
        vec.sort();

        let mut tree = MerkleTree::new();

        for elem in &vec {
            tree.insert(*elem, *elem);
        }

        let vec_again: Vec<_> = tree.iter().map(|node| *node.key()).collect();

        prop_assert_eq!(vec, vec_again);
    }
}
