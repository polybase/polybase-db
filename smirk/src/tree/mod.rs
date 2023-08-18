use std::{borrow::Borrow, cmp::Ordering};

use crate::hash::{Digest, Hashable};

/// Batch API for performing many operations on a [`MerkleTree`] at once
pub mod batch;
mod iterator;
pub use iterator::*;

mod impls;
pub use impls::*;

mod macros;
pub mod visitor;

mod path;

mod proof;

mod hash;
pub use hash::key_value_hash;

#[cfg(test)]
mod tests;

/// A Merkle tree with a map-like API
///
/// ```rust
/// # use smirk::{MerkleTree, smirk};
/// let mut tree = MerkleTree::new();
/// tree.insert(123, "hello");
///
/// // or you can use the macro to create a tree
/// let tree = smirk! {
///     123 => "hello",
/// };
///
/// assert_eq!(tree.size(), 1);
/// ```
/// You can use [`MerkleTree::iter`] to get an iterator over tuples of key-value pairs
///
/// The order will be the order specified by the [`Ord`] implementation for the key type
/// ```rust
/// # use smirk::smirk;
/// let tree = smirk! {
///   1 => 123,
///   2 => 234,
///   3 => 345,
/// };
///
/// let pairs: Vec<(i32, i32)> = tree
///     .iter()
///     .map(|node| (*node.key(), *node.value()))
///     .collect();
///
/// assert_eq!(pairs, vec![
///   (1, 123),
///   (2, 234),
///   (3, 345),
/// ]);
/// ```
/// You can also go the other way via [`FromIterator`], just like you would for a [`HashMap`]:
/// ```rust
/// # use smirk::MerkleTree;
/// let pairs = vec![
///   (1, 123),
///   (2, 234),
///   (3, 345),
/// ];
/// let tree: MerkleTree<_, _> = pairs.into_iter().collect();
///
/// assert_eq!(tree.size(), 3);
/// ```
///
/// Broadly speaking, to do anything useful with a Merkle tree, the key type must implement
/// [`Ord`] and [`Hashable`], and the value type must implement [`Hashable`]
///
/// Warning: *DO NOT* use types with interior mutability as either the
/// key or value in this tree, since it can potentially invalidate hashes/ordering guarantees that
/// the tree otherwise maintains.
///
/// If this happens, behaviour of the tree is unspecified, but not
/// undefined. In other words, the usual soundness rules will be upheld, but any function performed
/// on the tree itself may give incorrect results
///
/// [`HashMap`]: std::collections::HashMap
///
#[derive(Debug, Clone)]
pub struct MerkleTree<K, V> {
    pub(crate) inner: Option<Box<TreeNode<K, V>>>,
}

impl<K: Hashable, V: Hashable> PartialEq for MerkleTree<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.root_hash() == other.root_hash()
    }
}

impl<K, V> Hashable for MerkleTree<K, V>
where
    K: Hashable,
    V: Hashable,
{
    fn hash(&self) -> Digest {
        self.root_hash()
    }
}

impl<K, V> Default for MerkleTree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> MerkleTree<K, V> {
    /// Create a new, empty [`MerkleTree`]
    ///
    /// ```rust
    /// # use smirk::MerkleTree;
    /// let tree = MerkleTree::<i32, i32>::new();
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self { inner: None }
    }

    /// Insert a new key-value pair into the tree
    ///
    /// ```rust
    /// # use smirk::MerkleTree;
    /// let mut tree = MerkleTree::new();
    /// tree.insert(1, "hello".to_string());
    ///
    /// assert_eq!(tree.get(&1).unwrap(), "hello");
    /// ```
    /// If the key is already present in the tree, the tree is left unchanged
    ///
    /// Note: inserting a single value will potentially rebalance the tree, and also recompute hash
    /// values, which can be expensive. If you are inserting many items, consider using
    /// [`MerkleTree::apply`]
    pub fn insert(&mut self, key: K, value: V)
    where
        K: Hashable + Ord,
        V: Hashable,
    {
        self.insert_without_update(key, value);
        self.recalculate_hash_recursive();
    }

    /// Basically [`MerkleTree::insert`] but without updating the hashes - performance optimization
    /// for batch API
    pub(crate) fn insert_without_update(&mut self, key: K, value: V)
    where
        K: Hashable + Ord,
        V: Hashable,
    {
        self.inner = Some(Self::insert_node(self.inner.take(), key, value));
    }

    #[allow(clippy::unnecessary_box_returns)]
    fn insert_node(node: Option<Box<TreeNode<K, V>>>, key: K, value: V) -> Box<TreeNode<K, V>>
    where
        K: Hashable + Ord,
        V: Hashable,
    {
        let Some(mut node) = node else { return Box::new(TreeNode::new(key, value, None, None)) };

        match key.cmp(&node.key) {
            Ordering::Equal => {
                node.value = value;
            }
            Ordering::Less => {
                node.left = Some(Self::insert_node(node.left.take(), key, value));
            }
            Ordering::Greater => {
                node.right = Some(Self::insert_node(node.right.take(), key, value));
            }
        }

        node.update_height();
        Self::balance(node)
    }

    fn balance(mut node: Box<TreeNode<K, V>>) -> Box<TreeNode<K, V>> {
        let balance = node.balance_factor();

        if balance > 1 {
            if node.left.as_ref().unwrap().balance_factor() < 0 {
                node.left = Some(Self::rotate_left(node.left.unwrap()));
            }
            node = Self::rotate_right(node);
        } else if balance < -1 {
            if node.right.as_ref().unwrap().balance_factor() > 0 {
                node.right = Some(Self::rotate_right(node.right.unwrap()));
            }
            node = Self::rotate_left(node);
        }

        node
    }

    fn rotate_left(mut root: Box<TreeNode<K, V>>) -> Box<TreeNode<K, V>> {
        let mut new_root = root.right.take().unwrap();
        root.right = new_root.left.take();
        new_root.left = Some(root);

        new_root.left.as_mut().unwrap().update_height();
        new_root.update_height();

        new_root
    }

    fn rotate_right(mut root: Box<TreeNode<K, V>>) -> Box<TreeNode<K, V>> {
        let mut new_root = root.left.take().unwrap();
        root.left = new_root.right.take();
        new_root.right = Some(root);
        new_root.right.as_mut().unwrap().update_height();
        new_root.update_height();

        new_root
    }

    /// The number of elements in the tree
    ///
    /// ```rust
    /// # use smirk::smirk;
    /// let tree = smirk! {
    ///     1 => "hello",
    ///     2 => "world",
    ///     3 => "foo",
    /// };
    ///
    /// assert_eq!(tree.size(), 3);
    /// ```
    #[must_use]
    pub fn size(&self) -> usize {
        struct Counter(usize);
        impl<K, V> visitor::Visitor<K, V> for Counter {
            fn visit(&mut self, _: &K, _: &V) {
                self.0 = self
                    .0
                    .checked_add(1)
                    .expect("this is never going to overflow");
            }
        }

        let mut counter = Counter(0);
        self.visit(&mut counter);

        counter.0
    }

    /// Returns true if and only if the tree contains no elements
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Returns `true` if and only if `key` is present in the tree
    ///
    /// ```rust
    /// # use smirk::smirk;
    /// let tree = smirk! {
    ///   1 => "hello",
    /// };
    ///
    /// assert!(tree.contains(&1));
    /// assert!(!tree.contains(&2));
    /// ```
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        Q: Borrow<K> + ?Sized,
        K: Ord,
    {
        self.get(key).is_some()
    }

    /// The height of this tree
    #[inline]
    #[must_use]
    pub fn height(&self) -> usize {
        match &self.inner {
            None => 0,
            Some(node) => node.height(),
        }
    }

    /// Get the value associated with the given key
    ///
    /// If you need access to the node itself, consider using [`MerkleTree::get_node`]
    /// ```rust
    /// # use smirk::smirk;
    /// let tree = smirk! {
    ///   1 => "hello".to_string(),
    /// };
    ///
    /// assert_eq!(tree.get(&1).unwrap(), "hello");
    /// assert!(tree.get(&2).is_none());
    /// ```
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Borrow<K> + ?Sized,
        K: Ord,
    {
        self.inner.as_ref().and_then(|node| node.get(key))
    }

    /// Get the node associated with the given key
    ///
    /// If you only need access to the value stored in this node, consider using [`MerkleTree::get`]
    /// ```rust
    /// # use smirk::smirk;
    /// # use smirk::hash::Digest;
    /// let tree = smirk! {
    ///   1 => "hello".to_string(),
    /// };
    ///
    /// let node = tree.get_node(&1).unwrap();
    ///
    /// assert_eq!(*node.key(), 1);
    /// assert_eq!(*node.value(), "hello");
    /// let _hash = node.hash();  // the hash of this node plus all the children
    /// ```
    pub fn get_node<Q>(&self, key: &Q) -> Option<&TreeNode<K, V>>
    where
        Q: Borrow<K> + ?Sized,
        K: Ord,
    {
        self.inner.as_ref().and_then(|node| node.get_node(key))
    }
}

/// An individual node in a Merkle tree
#[derive(Debug, Clone)]
pub struct TreeNode<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) hash: Digest,
    pub(crate) left: Option<Box<TreeNode<K, V>>>,
    pub(crate) right: Option<Box<TreeNode<K, V>>>,
    pub(crate) height: usize,
}

impl<K, V> TreeNode<K, V> {
    /// The height of the tree
    ///
    /// For example, in a tree with 3 nodes A, B, C, where A is the parent of B and C:
    ///  - A has height 1
    ///  - B has height 0
    ///  - C has height 0
    #[must_use]
    pub fn height(&self) -> usize {
        self.height
    }

    // pub(crate) for testing only
    pub(crate) fn update_height(&mut self) {
        let left_height = self.left.as_ref().map_or(0, |x| x.height());
        let right_height = self.right.as_ref().map_or(0, |x| x.height());
        self.height = std::cmp::max(left_height, right_height)
            .checked_add(1)
            .expect("this is never going to overflow");
    }

    fn balance_factor(&self) -> isize {
        let left_height = self.left.as_ref().map_or(0, |x| x.height());
        let right_height = self.right.as_ref().map_or(0, |x| x.height());

        let left_height = isize::try_from(left_height).expect("height never overflows");
        let right_height = isize::try_from(right_height).expect("height never overflows");

        left_height
            .checked_sub(right_height)
            .expect("this is never going to over/underflow")
    }

    fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Borrow<K> + ?Sized,
        K: Ord,
    {
        let node = self.get_node(key)?;
        Some(&node.value)
    }

    fn get_node<Q>(&self, key: &Q) -> Option<&TreeNode<K, V>>
    where
        Q: Borrow<K> + ?Sized,
        K: Ord,
    {
        match key.borrow().cmp(&self.key) {
            Ordering::Less => self.left.as_ref().and_then(|node| node.get_node(key)),
            Ordering::Greater => self.right.as_ref().and_then(|node| node.get_node(key)),
            Ordering::Equal => Some(self),
        }
    }
}

impl<K: Hashable, V: Hashable> TreeNode<K, V> {
    pub(crate) fn new(
        key: K,
        value: V,
        left: Option<TreeNode<K, V>>,
        right: Option<TreeNode<K, V>>,
    ) -> Self {
        let hash = Digest::NULL;
        let left = left.map(Box::new);
        let right = right.map(Box::new);

        let mut node = Self {
            key,
            value,
            hash,
            left,
            right,
            height: 0,
        };

        node.update_height();
        node.recalculate_hash_recursive();

        node
    }

    /// The key associated with this node
    pub fn key(&self) -> &K {
        &self.key
    }

    /// The value associated with this node
    pub fn value(&self) -> &V {
        &self.value
    }

    /// The hash of this node and all child nodes
    #[inline]
    pub fn hash(&self) -> Digest {
        self.hash
    }

    /// The hash of the key-value pair contained in this node
    ///
    /// Note: this is unaffected by the value of child nodes
    #[inline]
    pub fn key_value_hash(&self) -> Digest {
        key_value_hash(self.key(), self.value())
    }
}
