use std::{iter::Chain, option};

use traversal::{Bft, DftPre};

use super::{MerkleTree, TreeNode};

impl<K, V> MerkleTree<K, V> {
    /// Returns an iterator over the keys and values in depth-first order
    pub fn depth_first<'a>(&'a self) -> DepthFirstIter<'a, K, V> {
        match &self.inner {
            None => DepthFirstIter { inner: None },
            Some(node) => node.depth_first(),
        }
    }

    /// Returns an iterator over the keys and values in breadth-first order
    pub fn breadth_first<'a>(&'a self) -> BreadthFirstIter<'a, K, V> {
        match &self.inner {
            None => BreadthFirstIter { inner: None },
            Some(node) => node.breadth_first(),
        }
    }
}

impl<K, V> TreeNode<K, V> {
    /// Get an iterator over the values in this node in depth-first order
    fn depth_first<'a>(&'a self) -> DepthFirstIter<'a, K, V> {
        let inner = DftPre::new(self, children);
        let inner = Box::new(inner.map(|(_, node)| (&node.key, &node.value)));

        DepthFirstIter { inner: Some(inner) }
    }

    /// Get an iterator over the values in this node in breadth-first order
    fn breadth_first(&self) -> BreadthFirstIter<'_, K, V> {
        let inner = Bft::new(self, children);
        let inner = Box::new(inner.map(|(_, node)| (&node.key, &node.value)));

        BreadthFirstIter { inner: Some(inner) }
    }
}

fn children<'a, K, V>(node: &'a TreeNode<K, V>) -> ChildIter<'a, K, V> {
    node.left
        .as_deref()
        .into_iter()
        .chain(node.right.as_deref().into_iter())
}

type ChildIter<'a, K, V> =
    Chain<option::IntoIter<&'a TreeNode<K, V>>, option::IntoIter<&'a TreeNode<K, V>>>;

pub struct DepthFirstIter<'a, K, V> {
    inner: Option<Box<dyn Iterator<Item = (&'a K, &'a V)> + 'a>>,
}

impl<'a, K, V> Iterator for DepthFirstIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.as_mut().map(|iter| iter.next()).flatten()
    }
}

pub struct BreadthFirstIter<'a, K, V> {
    inner: Option<Box<dyn Iterator<Item = (&'a K, &'a V)> + 'a>>,
}

impl<'a, K, V> Iterator for BreadthFirstIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.as_mut().map(|iter| iter.next()).flatten()
    }
}

#[cfg(any(test, feature = "proptest"))]
mod proptest_impls {
    use std::fmt::Debug;

    use crate::hash::Hashable;

    use super::*;

    use proptest::{arbitrary::StrategyFor, prelude::*, strategy::Map};

    impl<K, V> Arbitrary for MerkleTree<K, V>
    where
        K: Debug + Arbitrary + Ord,
        V: Debug + Arbitrary + Hashable,
    {
        type Parameters = ();
        type Strategy = Map<StrategyFor<Vec<(K, V)>>, fn(Vec<(K, V)>) -> Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            any::<Vec<(K, V)>>().prop_map(|v| v.into_iter().collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{hash::Digest, tree::MerkleTree, TreeNode};

    // 1
    // |\
    // 2 5
    // |\
    // 3 4
    fn example_node() -> TreeNode<i32, i32> {
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

    #[test]
    fn depth_first_test() {
        let tree = example_node();
        let items: Vec<_> = tree.depth_first().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]);

        assert_eq!(
            MerkleTree::from_iter::<[(i32, i32); 0]>([])
                .depth_first()
                .count(),
            0
        );
    }

    #[test]
    fn breadth_first_test() {
        let tree = example_node();
        let items: Vec<_> = tree.breadth_first().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(1, 1), (2, 2), (5, 5), (3, 3), (4, 4)]);

        assert_eq!(
            MerkleTree::from_iter::<[(i32, i32); 0]>([])
                .breadth_first()
                .count(),
            0
        );
    }
}
