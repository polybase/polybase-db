use std::{iter::Chain, option};

use traversal::{Bft, DftPre};

use super::{MerkleTree, TreeNode};

impl<T: Ord> FromIterator<T> for MerkleTree<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        // TODO: this is probably pretty inefficient, clean this up later
        let mut tree = MerkleTree::new();

        for elem in iter {
            tree.insert(elem);
        }

        tree
    }
}

impl<T> MerkleTree<T> {
    pub fn depth_first<'a>(&'a self) -> DepthFirstIter<'a, T> {
        match &self.inner {
            None => DepthFirstIter { inner: None },
            Some(node) => node.depth_first(),
        }
    }

    pub fn breadth_first<'a>(&'a self) -> BreadthFirstIter<'a, T> {
        match &self.inner {
            None => BreadthFirstIter { inner: None },
            Some(node) => node.breadth_first(),
        }
    }
}

impl<T> TreeNode<T> {
    /// Get an iterator over the values in this node in depth-first order
    pub fn depth_first<'a>(&'a self) -> DepthFirstIter<'a, T> {
        let inner = DftPre::new(self, children);
        let inner = Box::new(inner.map(|(_, node)| node));

        DepthFirstIter { inner: Some(inner) }
    }

    /// Get an iterator over the values in this node in breadth-first order
    pub fn breadth_first(&self) -> BreadthFirstIter<'_, T> {
        let inner = Bft::new(self, children);
        let inner = Box::new(inner.map(|(_, node)| node));

        BreadthFirstIter { inner: Some(inner) }
    }
}

fn children<'a, T>(node: &'a TreeNode<T>) -> ChildIter<'a, T> {
    node.left
        .as_deref()
        .into_iter()
        .chain(node.right.as_deref().into_iter())
}

type ChildIter<'a, T> = Chain<option::IntoIter<&'a TreeNode<T>>, option::IntoIter<&'a TreeNode<T>>>;

pub struct DepthFirstIter<'a, T> {
    inner: Option<Box<dyn Iterator<Item = &'a TreeNode<T>> + 'a>>,
}

impl<'a, T> Iterator for DepthFirstIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .as_mut()
            .map(|iter| iter.next().map(|node| &node.value))
            .flatten()
    }
}

pub struct BreadthFirstIter<'a, T> {
    inner: Option<Box<dyn Iterator<Item = &'a TreeNode<T>> + 'a>>,
}

impl<'a, T> Iterator for BreadthFirstIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .as_mut()
            .map(|iter| iter.next().map(|node| &node.value))
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use crate::{testing::example_node, tree::MerkleTree};

    #[test]
    fn depth_first_test() {
        let tree = example_node();
        let items: Vec<_> = tree.depth_first().copied().collect();
        assert_eq!(items, vec![1, 2, 3, 4, 5]);

        assert_eq!(
            MerkleTree::from_iter::<[i32; 0]>([]).depth_first().count(),
            0
        );
    }

    #[test]
    fn breadth_first_test() {
        let tree = example_node();
        let items: Vec<_> = tree.breadth_first().copied().collect();
        assert_eq!(items, vec![1, 2, 5, 3, 4]);

        assert_eq!(
            MerkleTree::from_iter::<[i32; 0]>([])
                .breadth_first()
                .count(),
            0
        );
    }
}
