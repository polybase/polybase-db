#![allow(clippy::all)]

use crate::hash::{Digest, Hashable};

mod iter;
mod macros;

pub(crate) struct Tree2<K, V> {
    node: Option<Box<Node2<K, V>>>,
}

impl<K, V> Tree2<K, V> {
    pub fn new() -> Self {
        Self { node: None }
    }

    pub fn with_capacity(capacity: usize) -> Self
    where
        // technically we could relax these bounds, but honestly who cares
        K: Hashable,
        V: Hashable,
    {
        let node = match capacity.next_power_of_two() {
            0 => None,
            _ => Some(Box::new(Node2::with_capacity(capacity))),
        };

        Self { node }
    }

    pub fn root_hash(&self) -> Digest
    where
        K: Hashable,
        V: Hashable,
    {
        self.node.as_deref().map_or(Digest::NULL, Node2::hash)
    }

    pub fn capacity(&self) -> usize {
        todo!();
        // this is a bad impl
        self.iter().count().next_power_of_two()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Node2<K, V> {
    Leaf {
        key: K,
        value: V,
        hash: Digest,
    },
    /// used to pad the tree so we always have a perfectly balanced tree
    EmptyLeaf,
    Parent {
        left: Option<Box<Node2<K, V>>>,
        right: Option<Box<Node2<K, V>>>,
        hash: Digest,
        hash_dirty: bool,
    },
}

impl<K, V> Node2<K, V> {
    /// Basically the [`Clone`] impl, except it panics for non-empty nodes
    ///
    /// This lets us clone nodes to save the hash, without needing to add an unnecessary [`Clone`]
    /// bonud, when we know a node contains only empty children
    fn clone_empty(&self) -> Self {
        match self {
            Self::Leaf { .. } => panic!("non-empty leaf found"),
            Self::EmptyLeaf => Self::EmptyLeaf,
            Self::Parent {
                left,
                right,
                hash,
                hash_dirty,
            } => {
                let left = left.as_deref().map(Self::clone_empty).map(Box::new);
                let right = right.as_deref().map(Self::clone_empty).map(Box::new);
                let hash = hash.clone();
                let hash_dirty = hash_dirty.clone();

                Self::Parent {
                    left,
                    right,
                    hash,
                    hash_dirty,
                }
            }
        }
    }
}

impl<K, V> Node2<K, V>
where
    K: Hashable,
    V: Hashable,
{
    fn with_capacity(capacity: usize) -> Self {
        match capacity {
            0 => unreachable!("we should only ever call this with a positive value"),
            1 => Node2::EmptyLeaf,
            _ => {
                let left = Node2::with_capacity(capacity / 2);
                let right = left.clone_empty(); // IMPORTANT - avoid recalculating the hash
                let hash = [left.hash(), right.hash()].iter().collect();

                Self::Parent {
                    left: Some(Box::new(left)),
                    right: Some(Box::new(right)),
                    hash,
                    hash_dirty: false,
                }
            }
        }
    }

    fn hash(&self) -> Digest {
        match self {
            Self::EmptyLeaf => Digest::NULL,
            Self::Leaf { hash, .. } => *hash,
            Self::Parent {
                hash,
                hash_dirty: false,
                ..
            } => *hash,
            Self::Parent {
                hash,
                hash_dirty: true,
                ..
            } => {
                unreachable!("we should never try to get the hash if any nodes are dirty");
            }
        }
    }
}
