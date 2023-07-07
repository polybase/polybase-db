use std::cmp::Ordering;

use miden_crypto::hash::rpo::{Rpo256, RpoDigest};

use crate::hash::{Hash, MerklePath};

mod impls;
pub mod visitor;

#[cfg(test)]
mod tests;

/// A Merkle tree, with a customizable storage backend and hash function
///
/// ```rust
/// # use smirk::tree::MerkleTree;
/// let mut tree = MerkleTree::new();
/// tree.insert(123);
///
/// assert_eq!(tree.size(), 1);
/// ```
/// You can walk the tree in depth-first or breadth-first ordering:
/// ```rust
/// # use smirk::tree::MerkleTree;
/// let tree = MerkleTree::from_iter([1, 2, 3]);
///
/// for i in tree.depth_first() {
///     println!("{i}");
/// }
///
/// for i in tree.breadth_first() {
///     println!("{i}");
/// }
/// ```
#[derive(Debug, Clone)]
pub struct MerkleTree<T> {
    pub(crate) inner: Option<Box<TreeNode<T>>>,
}

impl<T> MerkleTree<T> {
    /// Create a new [`MerkleTree`] with the given storage backend
    ///
    /// ```rust
    /// # use smirk::tree::MerkleTree;
    /// let tree = MerkleTree::<i32>::new();
    /// ```
    pub fn new() -> Self {
        Self { inner: None }
    }

    pub fn insert(&mut self, value: T)
    where
        T: Ord,
    {
        self.inner = Some(Self::insert_node(self.inner.take(), value));
    }

    fn insert_node(node: Option<Box<TreeNode<T>>>, value: T) -> Box<TreeNode<T>>
    where
        T: Ord,
    {
        let mut node = match node {
            None => return Box::new(TreeNode::new(value)),
            Some(node) => node,
        };

        if value < node.value {
            node.left = Some(Self::insert_node(node.left.take(), value));
        } else if value > node.value {
            node.right = Some(Self::insert_node(node.right.take(), value));
        } else {
            return node; // Duplicates not allowed
        }

        node.update_height();
        Self::balance(node)
    }

    fn balance(mut node: Box<TreeNode<T>>) -> Box<TreeNode<T>> {
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

    fn rotate_left(mut root: Box<TreeNode<T>>) -> Box<TreeNode<T>> {
        let mut new_root = root.right.take().unwrap();
        root.right = new_root.left.take();
        new_root.left = Some(root);

        new_root.left.as_mut().unwrap().update_height();
        new_root.update_height();

        new_root
    }

    fn rotate_right(mut root: Box<TreeNode<T>>) -> Box<TreeNode<T>> {
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
    /// # use smirk::MerkleTree;
    /// let tree = MerkleTree::from_iter([1, 2, 3]);
    ///
    /// assert_eq!(tree.size(), 3);
    /// ```
    pub fn size(&self) -> usize {
        struct Counter(usize);
        impl<T> visitor::Visitor<T> for Counter {
            fn visit(&mut self, _item: &T) {
                self.0 += 1;
            }
        }

        let mut counter = Counter(0);
        self.visit(&mut counter);

        counter.0
    }

    /// The height of this tree
    #[inline]
    pub fn height(&self) -> usize {
        match &self.inner {
            None => 0,
            Some(node) => node.height() as usize,
        }
    }

    /// Return the node associated with the given value
    pub fn get(&self, item: &T) -> Option<&TreeNode<T>>
    where
        T: Ord,
    {
        self.inner.as_ref().and_then(|node| node.get(item))
    }

    pub fn get_mut(&mut self, item: &T) -> Option<&mut TreeNode<T>>
    where
        T: Ord,
    {
        self.inner.as_mut().and_then(|node| node.get_mut(item))
    }

    /// Get the root hash of the Merkle tree
    pub fn root_hash(&self) -> Hash
    where
        T: AsRef<[u8]>,
    {
        match &self.inner {
            None => Hash::NULL, // should this function return an option?
            Some(node) => node.hash(),
        }
    }

    /// Generate a [`MerklePath`] for the a given value
    pub fn path_for(&self, value: &T) -> Option<MerklePath>
    where
        T: Ord + AsRef<[u8]>,
    {
        let mut components = Vec::with_capacity(self.height());

        let mut opt_node = self.inner.as_deref();

        loop {
            let node = opt_node?;

            components.push(node.hash());

            match value.cmp(&node.value) {
                Ordering::Less => opt_node = node.left.as_deref(),
                Ordering::Greater => opt_node = node.right.as_deref(),
                Ordering::Equal => {
                    components.reverse();
                    return Some(MerklePath::new(components));
                }
            }
        }
    }

    pub fn verify(&self, path: &MerklePath, value: &T) -> bool
    where
        T: AsRef<[u8]> + Ord,
    {
        if path.components().last() != Some(&self.root_hash()) {
            return false;
        }

        let mut hash = Hash::calculate(value.as_ref());

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
pub struct TreeNode<T> {
    pub(crate) value: T,
    pub(crate) left: Option<Box<TreeNode<T>>>,
    pub(crate) right: Option<Box<TreeNode<T>>>,
    pub(crate) height: isize,
}

impl<T> TreeNode<T> {
    /// Create a new [`TreeNode`] with no children
    pub fn new(value: T) -> Self {
        Self {
            value,
            left: None,
            right: None,
            height: 0,
        }
    }

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

    pub fn get(&self, item: &T) -> Option<&TreeNode<T>>
    where
        T: Ord,
    {
        match item.cmp(&self.value) {
            Ordering::Less => self.left.as_ref().and_then(|node| node.get(item)),
            Ordering::Greater => self.right.as_ref().and_then(|node| node.get(item)),
            Ordering::Equal => Some(self),
        }
    }

    pub fn get_mut(&mut self, item: &T) -> Option<&mut TreeNode<T>>
    where
        T: Ord,
    {
        match item.cmp(&self.value) {
            Ordering::Less => self.left.as_mut().and_then(|node| node.get_mut(item)),
            Ordering::Greater => self.right.as_mut().and_then(|node| node.get_mut(item)),
            Ordering::Equal => Some(self),
        }
    }

    /// The hash of the value contained in this node (ignoring any children)
    pub fn hash_of_value(&self) -> Hash
    where
        T: AsRef<[u8]>,
    {
        // should we memoize this?
        let bytes = self.value.as_ref();
        Hash::calculate(bytes)
    }

    /// The hash of this value (and all child values)

    // we should probably memoize this
    pub fn hash(&self) -> Hash
    where
        T: AsRef<[u8]>,
    {
        let left = self
            .left
            .as_ref()
            .map(|node| node.hash())
            .unwrap_or(Hash::NULL);
        let this = Hash::calculate(self.value.as_ref());
        let right = self
            .right
            .as_ref()
            .map(|node| node.hash())
            .unwrap_or(Hash::NULL);

        let left_this = Rpo256::merge(&[left.digest(), this.digest()]);
        Hash::from(Rpo256::merge(&[left_this, right.digest()]))
    }
}
