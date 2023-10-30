use std::iter::zip;

use crate::{hash_merge, Element, Lsb, Tree};

use super::tree_repr::Node;

/// A Merkle path
///
/// A Merkle path can be used to verify the presence/absence of a value in a merkle tree with a
/// known root hash.
///
#[derive(Debug, Clone)]
pub struct Path<const N: usize> {
    /// The siblings of the element with the shallowest siblings first
    ///
    /// The first N - 1 values are the siblings, and the last value is the root hash of the tree
    ///
    /// Ideally, we would have 2 fields here:
    ///  - `siblings: [Element: {N - 1}]`
    ///  - `root_hash: Element`
    /// Unfortunately, Rust doesn't yet support this. So we just squeeze them together and just
    /// deal with it 🤷
    pub(crate) siblings: [Element; N],

    /// The bits of the element that created this path
    ///
    /// It has length `N - 1`, since a depth of `N` corresponds to `N - 1` left/right choices
    pub(crate) bits: Lsb,
}

impl<const N: usize> Path<N> {
    /// Compute the root hash that wou
    pub fn compute_root_hash(&self, mut element: Element) -> Element {
        let (_root_hash, siblings) = self.siblings.split_last().unwrap();

        let pairs = zip(siblings.iter().rev(), self.bits.into_iter().rev());

        for (&sibling, bit) in pairs {
            match bit {
                // bit is 0, this element is on the left
                false => element = hash_merge(element, sibling),

                // bit is 1, this element is on the right
                true => element = hash_merge(sibling, element),
            }
        }

        element
    }

    /// The root hash of the tree when this path was created
    pub fn actual_root_hash(&self) -> Element {
        *self.siblings.last().unwrap()
    }
}

impl<const DEPTH: usize> Tree<DEPTH> {
    /// Generate a [`Path`] that proves the presence/absence of a particular value at a location in
    /// the tree
    ///
    /// ```rust
    /// # use smirk::*;
    /// let tree = Tree::<64>::from_iter((1..10).map(Element::from_u64));
    ///
    /// let path = tree.path_for(Element::from(1));
    ///
    /// // if we calculate the root hash with the value `1`, the root hash will match the tree
    /// assert_eq!(path.compute_root_hash(Element::from(1)), tree.root_hash())
    ///
    /// ```
    ///
    /// Note that the last `DEPTH` bits of `element` determines the element's location in the tree.
    /// When we traverse to this location, we will either find:
    ///  - an element with the right least significant bits
    ///  - [`Element::NULL_HASH`]
    ///
    /// Given that, this function cannot fail, since every location is conceptually occupied
    /// (either with a real value or [`Element::NULL_HASH`])
    pub fn path_for(&self, element: Element) -> Path<DEPTH> {
        let bits = element.lsb(DEPTH - 1);

        let mut siblings = [Element::NULL_HASH; DEPTH];
        let mut tree = &self.tree;

        for (index, bit) in bits.iter().enumerate() {
            match tree {
                Node::Parent { left, right, .. } => match *bit {
                    // the bit is 0, so we follow the left hash, so right is the sibling
                    false => {
                        siblings[index] = right.hash();
                        tree = left;
                    }

                    // the bit is 1, so we follow the right hash, so left is the sibling
                    true => {
                        siblings[index] = left.hash();
                        tree = right;
                    }
                },
                // if we hit an empty node, we can simply continue in place
                //
                // a depth of `n` corresponds to `n - 1` left/right decisions, so we need to insert
                // `n - 1` elements into the siblings array
                Node::Empty { depth } => {
                    assert_eq!(index + depth, DEPTH);

                    // we don't want to include `depth` here, because it was included when we
                    // calculated the parent
                    for (i, depth) in (1..*depth).rev().enumerate() {
                        siblings[index + i] = Node::Empty { depth }.hash();
                    }

                    // for i in 0..(depth - 1) {
                    //     let depth = DEPTH - index - i - 1;
                    //     siblings[index + i] = Node::Empty { depth }.hash();
                    // }

                    break;
                }
                Node::Leaf(_) => panic!("uh oh"),
            }
        }

        *siblings.last_mut().unwrap() = self.root_hash();

        Path { siblings, bits }
    }
}

#[cfg(test)]
mod tests {

    use test_strategy::proptest;

    use super::*;

    #[proptest]
    fn cached_root_hash_is_correct(tree: Tree<64>, element: Element) {
        let path = tree.path_for(element);
        assert_eq!(path.actual_root_hash(), tree.root_hash());
    }

    #[proptest]
    fn calculated_root_hash_is_correct(tree: Tree<64>, element: Element) {
        let path = tree.path_for(element);

        let element = match tree.contains(element) {
            true => element,
            false => Element::NULL_HASH,
        };

        let computed_root = path.compute_root_hash(element);

        assert_eq!(computed_root, tree.root_hash());
        assert_eq!(computed_root, path.actual_root_hash());
    }

    #[test]
    fn simple_path_example() {
        let mut tree = Tree::<64>::new();
        tree.insert(Element::from(1));

        let path = tree.path_for(Element::from(1));
        assert_eq!(path.actual_root_hash(), tree.root_hash())
    }
}
