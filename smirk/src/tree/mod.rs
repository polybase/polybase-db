use std::collections::BTreeSet;

use self::element::Element;

pub(crate) mod element;

mod iter;
mod path;
mod raw_api;
mod tree_repr;
mod proof;

#[cfg(any(test, feature = "proptest"))]
pub mod proptest;

/// A sparse Merkle tree
///
/// Conceptually, this type is roughly equivalent to a `HashSet<Element>`, and the API reflects
/// this:
///
/// ```rust
/// # use smirk::*;
/// let mut tree = Tree::<64>::new();
///
/// tree.insert(Element::from(1));
/// tree.insert(Element::from(2));
/// tree.insert(Element::from(3));
///
/// assert!(tree.conains(Element::from(1)));
///
/// for element in tree.elements() {
///     println!("the tree contains {element}");
/// }
/// ```
/// Notably, it also provides [`Tree::insert_and_prove`]
#[derive(Debug, Clone)]
pub struct Tree<const DEPTH: usize> {
    /// The tree-like representation
    tree: tree_repr::Node,
    elements: BTreeSet<Element>,
}

impl<const DEPTH: usize> PartialEq for Tree<DEPTH> {
    fn eq(&self, other: &Self) -> bool {
        self.root_hash() == other.root_hash()
    }
}

impl<const DEPTH: usize> Eq for Tree<DEPTH> {}

impl<const DEPTH: usize> Default for Tree<DEPTH> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<const DEPTH: usize> Tree<DEPTH> {
    /// Creates a new, empty tree
    ///
    /// ```rust
    /// # use smirk::*;
    /// let tree = Tree::<64>::new();
    /// assert!(tree.is_empty());
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            elements: BTreeSet::new(),
            tree: tree_repr::Node::Empty { depth: DEPTH },
        }
    }

    /// The number of elements stored in this tree
    ///
    /// ```rust
    /// # use smirk::*;
    /// let mut tree = Tree::<64>::new();
    ///
    /// assert_eq!(tree.len(), 0);
    ///
    /// tree.insert(Element::from(1));
    /// assert_eq!(tree.len(), 1);
    ///
    /// tree.insert(Element::from(100));
    /// assert_eq!(tree.len(), 2);
    /// ```
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Whether this tree contains no elements
    ///
    /// ```rust
    /// # use smirk::*;
    /// let mut tree = Tree::<64>::new();
    ///
    /// assert_eq!(tree.is_empty(), true);
    ///
    /// tree.insert(Element::from(1));
    ///
    /// assert_eq!(tree.is_empty(), false);
    /// ```
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Insert a non-null element into the tree
    ///
    /// Returns whether the value was newly inserted. That is:
    ///
    /// - If the tree did not previously contain this element, `true` is returned
    /// - If the tree already contained this element, `false` is returned
    /// ```rust
    /// # use smirk::*;
    /// let mut tree = Tree::<64>::new();
    ///
    /// let new = tree.insert(Element::from(1));
    /// assert_eq!(new, true);
    ///
    /// let new = tree.insert(Element::from(1));
    /// assert_eq!(new, false);
    /// ```
    ///
    /// Since this function recalculates all hashes after each insert, it can be quite slow. If you
    /// need to insert many elements at the same time, use [`Tree::insert_all`]
    ///
    /// # Panics
    ///
    /// Panics if the element is [`Element::NULL_HASH`]
    ///
    /// Since the tree uses this value to represent the absence of a value, inserting it is
    /// nonsensical
    #[inline]
    pub fn insert(&mut self, element: Element) -> bool {
        self.raw_insert(element, true)
    }

    /// Insert multiple non-null elements into the tree
    ///
    /// Since this method doesn't recalculate hashes between inserts (only once at the end), it is
    /// significantly faster than repeated calls to [`Tree::insert`]
    ///
    /// ```rust
    /// # use smirk::*;
    /// let mut tree = Tree::<64>::new();
    ///
    /// tree.insert_all((1..=10).map(Element::from_u64));
    ///
    /// assert_eq!(tree.contains(Element::from(1)), true);
    /// assert_eq!(tree.contains(Element::from(10)), true);
    /// assert_eq!(tree.contains(Element::from(11)), false);
    /// ```
    pub fn insert_all<I: Iterator<Item = Element>>(&mut self, elements: I) {
        let mut any_changed = false;

        for element in elements {
            let this_changed = self.raw_insert(element, false);

            if this_changed {
                any_changed = true;
            }
        }

        if any_changed {
            self.tree.recalculate_hashes();
        }
    }

    /// Returns `true` if the tree contains a given element
    ///
    /// ```rust
    /// # use smirk::*;
    /// let mut tree = Tree::<64>::from_iter([
    ///   Element::from(1),
    ///   Element::from(2),
    ///   Element::from(3),
    /// ]);
    ///
    /// assert_eq!(tree.contains(Element::from(1)), true);
    /// assert_eq!(tree.contains(Element::from(2)), true);
    /// assert_eq!(tree.contains(Element::from(3)), true);
    /// assert_eq!(tree.contains(Element::from(4)), false);
    /// ```
    #[inline]
    #[must_use]
    pub fn contains(&self, element: Element) -> bool {
        self.elements.contains(&element)
    }

    /// The root hash of the tree
    ///
    /// This value represents every value contained in the tree, i.e. any changes to the tree will
    /// change the root hash
    ///
    /// ```rust
    /// # use smirk::*;
    /// let mut tree = Tree::<64>::new();
    /// let hash_1 = tree.root_hash();
    ///
    /// tree.insert(Element::from(1));
    /// let hash_2 = tree.root_hash();
    ///
    /// tree.insert(Element::from(2));
    /// let hash_3 = tree.root_hash();
    ///
    /// assert_ne!(hash_1, hash_2);
    /// assert_ne!(hash_1, hash_3);
    /// assert_ne!(hash_2, hash_3);
    /// ```
    /// This value is cached internally, so calls to this function are essentially free
    #[inline]
    #[must_use]
    pub fn root_hash(&self) -> Element {
        self.tree.hash()
    }
}
