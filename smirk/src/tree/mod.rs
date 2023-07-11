use std::{borrow::Borrow, cmp::Ordering};

use crate::hash::{Digest, Hashable, MerklePath};

mod iterator;
pub use iterator::*;

mod impls;
pub use impls::*;

mod macros;
pub mod visitor;

#[cfg(test)]
mod tests;

/// A Merkle tree, with a customizable storage backend and hash function
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
/// You can walk the tree in depth-first or breadth-first ordering:
/// ```rust
/// # use smirk::smirk;
/// let tree = smirk! {
///   1 => 123,
///   2 => 234,
///   3 => 345,
/// };
///
/// for (k, v) in tree.depth_first() {
///     println!("key: {k} - value: {v}");
/// }
///
/// for (k, v) in tree.breadth_first() {
///     println!("key: {k} - value: {v}");
/// }
/// ```
/// Broadly speaking, to do anything useful with a Merkle tree, the key type must implement
/// [`Ord`], and the value type must implement [`Hashable`]
///
/// Warning: *DO NOT* use types with interior mutability as either the
/// key or value in this tree, since it can potentially invalidate hashes/ordering guarantees that
/// the tree otherwise maintains.
///
/// If this happens, behaviour of the tree is unspecified, but not
/// undefined. In other words, the usual soundness rules will be upheld, but any function performed
/// on the tree itself may give incorrect results
#[derive(Debug, Clone)]
pub struct MerkleTree<K, V> {
    pub(crate) inner: Option<Box<TreeNode<K, V>>>,
}

impl<K, V: Hashable> PartialEq for MerkleTree<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.root_hash() == other.root_hash()
    }
}

impl<K, V> MerkleTree<K, V> {
    /// Create a new, empty [`MerkleTree`]
    ///
    /// ```rust
    /// # use smirk::MerkleTree;
    /// let tree = MerkleTree::<i32, i32>::new();
    /// ```
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
    pub fn insert(&mut self, key: K, value: V)
    where
        K: Ord,
        V: Hashable,
    {
        self.inner = Some(Self::insert_node(self.inner.take(), key, value));
    }

    fn insert_node(node: Option<Box<TreeNode<K, V>>>, key: K, value: V) -> Box<TreeNode<K, V>>
    where
        K: Ord,
        V: Hashable,
    {
        let mut node = match node {
            None => return Box::new(TreeNode::new(key, value)),
            Some(node) => node,
        };

        if key < node.key {
            node.left = Some(Self::insert_node(node.left.take(), key, value));
        } else if key > node.key {
            node.right = Some(Self::insert_node(node.right.take(), key, value));
        } else {
            return node; // Duplicates not allowed
        }

        node.update_height();
        node.recalculate_hash_recursive();
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
    pub fn size(&self) -> usize {
        struct Counter(usize);
        impl<K, V> visitor::Visitor<K, V> for Counter {
            fn visit(&mut self, _: &K, _: &V) {
                self.0 += 1;
            }
        }

        let mut counter = Counter(0);
        self.visit(&mut counter);

        counter.0
    }

    /// Returns true if and only if the tree contains no elements
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
    pub fn height(&self) -> usize {
        match &self.inner {
            None => 0,
            Some(node) => node.height() as usize,
        }
    }

    /// Get the value associated with the given key
    ///
    /// If you need access to the node itself, consider using [`MerkleTree::get_node`]
    ///
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
    ///
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

    /// Get the root hash of the Merkle tree
    pub fn root_hash(&self) -> Digest
    where
        V: Hashable,
    {
        match &self.inner {
            None => Digest::NULL, // should this function return an option?
            Some(node) => node.hash(),
        }
    }

    /// Generate a [`MerklePath`] for the a given value
    pub fn path_for<Q>(&self, key: &Q) -> Option<MerklePath>
    where
        Q: Borrow<K> + ?Sized,
        K: Ord,
        V: Hashable,
    {
        let mut components = Vec::with_capacity(self.height());

        let mut opt_node = self.inner.as_deref();

        loop {
            let node = opt_node?;

            components.push(node.hash());

            match key.borrow().cmp(&node.key) {
                Ordering::Less => opt_node = node.left.as_deref(),
                Ordering::Greater => opt_node = node.right.as_deref(),
                Ordering::Equal => {
                    components.reverse();
                    return Some(MerklePath::new(components));
                }
            }
        }
    }

    /// Verify that the given value exists in the tree, by using the provided [`MerklePath`]
    pub fn verify<Q>(&self, path: &MerklePath, value: &V) -> bool
    where
        Q: Ord + Borrow<K> + ?Sized,
        V: Hashable,
    {
        if path.components().last() != Some(&self.root_hash()) {
            return false;
        }

        let mut hash = value.hash();

        for slice in path.components().windows(2) {
            let first = &slice[0];
            let second = &slice[1];

            hash.merge(first);
            if hash != *second {
                return false;
            }
        }

        true
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
    pub(crate) height: isize,
}

impl<K, V> TreeNode<K, V> {
    /// The height of the tree
    ///
    /// For example, in a tree with 3 nodes A, B, C, where A is the parent of B and C:
    ///  - A has height 1
    ///  - B has height 0
    ///  - C has height 0
    #[inline]
    pub fn height(&self) -> isize {
        self.height
    }

    // pub(crate) for testing only
    pub(crate) fn update_height(&mut self) {
        let left_height = self.left.as_ref().map_or(0, |x| x.height());
        let right_height = self.right.as_ref().map_or(0, |x| x.height());
        self.height = 1 + std::cmp::max(left_height, right_height);
    }

    fn balance_factor(&self) -> isize {
        let left_height = self.left.as_ref().map_or(0, |x| x.height());
        let right_height = self.right.as_ref().map_or(0, |x| x.height());
        left_height - right_height
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

impl<K, V: Hashable> TreeNode<K, V> {
    // pub(crate) for testing only
    pub(crate) fn new(key: K, value: V) -> Self {
        let hash = value.hash();

        Self {
            key,
            value,
            hash,
            left: None,
            right: None,
            height: 0,
        }
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

    /// The hash of the value contained in this node
    ///
    /// Note: this is unaffected by the value of child nodes
    #[inline]
    pub fn hash_of_value(&self) -> Digest {
        self.value.hash()
    }

    /// Update the `hash` field of this node, and all child nodes
    pub(crate) fn recalculate_hash_recursive(&mut self) {
        let mut new_hash = self.value.hash();

        if let Some(left) = &mut self.left {
            left.recalculate_hash_recursive();
            new_hash.merge(&left.hash);
        }

        if let Some(right) = &mut self.right {
            right.recalculate_hash_recursive();
            new_hash.merge(&right.hash);
        }

        self.hash = new_hash;
    }
}
