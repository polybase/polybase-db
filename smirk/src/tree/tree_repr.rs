use bitvec::slice::BitSlice;

use crate::{empty_tree_hash, hash_merge, Element};

/// A tree-like representation of a sparse tree, for easier computation of merkle paths and hashes
#[derive(Debug, Clone)]
pub(crate) enum Node {
    /// A single leaf at the max depth of the tree
    Leaf(Element),

    /// A tree of depth `depth` containing only null elements
    ///
    /// Since these trees are well-known, all hashes can be computed ahead of time and refered to
    /// by lookup table
    Empty { depth: usize },

    /// A parent of two nodes with a cached hash
    Parent {
        left: Box<Self>,
        right: Box<Self>,
        hash: Element,
        /// if true, the children have changed without recalculating the hash
        hash_dirty: bool,
    },
}

impl Node {
    pub fn hash(&self) -> Element {
        match self {
            Self::Parent { hash, .. } => *hash,
            Self::Leaf(hash) => *hash,
            Self::Empty { depth } => empty_tree_hash(*depth),
        }
    }

    /// Insert an element and return whether the value changed
    ///
    /// This does not update hashes, so is quite fast
    pub fn insert(&mut self, element: Element, bits: &BitSlice<u64>) -> bool {
        println!("{}", bits.len());
        match self {
            Self::Leaf(e) if *e == element => false,
            Self::Leaf(e) => {
                *e = element;
                true
            }
            Self::Parent {
                left,
                right,
                hash_dirty,
                ..
            } => {
                let (head, tail) = bits.split_first().unwrap();
                let changed = match *head {
                    false => left.insert(element, tail),
                    true => right.insert(element, tail),
                };

                if changed {
                    *hash_dirty = true;
                }

                changed
            }
            Self::Empty { depth: 1 } => {
                *self = Self::Leaf(element);
                true
            }

            Self::Empty { depth } => {
                // split an empty tree into two empty subtrees
                *self = Self::Parent {
                    left: Box::new(Self::Empty { depth: *depth - 1 }),
                    right: Box::new(Self::Empty { depth: *depth - 1 }),
                    hash: empty_tree_hash(*depth),
                    hash_dirty: false,
                };

                // now try again
                self.insert(element, bits)
            }
        }
    }

    pub fn recalculate_hashes(&mut self) {
        let Self::Parent {
            left,
            right,
            hash,
            hash_dirty,
        } = self
        else {
            return;
        };

        if *hash_dirty {
            left.recalculate_hashes();
            right.recalculate_hashes();

            *hash = hash_merge(left.hash(), right.hash());
        }

        *hash_dirty = false;
    }

}
