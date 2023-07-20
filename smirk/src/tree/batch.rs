use crate::{hash::Hashable, MerkleTree};

/// An operation that represents an update to the tree
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Operation<K, V> {
    /// Insert the following key-value pair
    Insert(K, V),
}

impl<K, V> Operation<K, V> {
    fn key(&self) -> &K {
        match self {
            Operation::Insert(key, ..) => key,
        }
    }
}

/// A batch of operations that can be applied to a [`MerkleTree`]
///
/// If there are multiple operations
#[derive(Debug, Clone)]
pub struct Batch<K, V> {
    operations: Vec<Operation<K, V>>,
}

impl<K, V> Batch<K, V> {
    /// Create a new [`Batch`] from a list of [`Operation`]s
    ///
    /// Note, if two operations reference the same key, they will be applied in the order they
    /// exist in `operations`. No other guarantees about the order of execution are made
    pub fn from_operations(mut operations: Vec<Operation<K, V>>) -> Self
    where
        K: Ord,
    {
        operations.sort_by(|a, b| a.key().cmp(b.key()));
        Self { operations }
    }
}

impl<K, V> FromIterator<Operation<K, V>> for Batch<K, V>
where
    K: Ord,
{
    fn from_iter<T: IntoIterator<Item = Operation<K, V>>>(iter: T) -> Self {
        let vec = Vec::from_iter(iter);
        Batch::from_operations(vec)
    }
}

impl<K, V> MerkleTree<K, V>
where
    K: Hashable + Ord,
    V: Hashable,
{
    /// Apply a [`Batch`] of operations to the tree
    pub fn apply(&mut self, batch: Batch<K, V>) {
        for operation in batch.operations {
            match operation {
                Operation::Insert(key, value) => self.insert_without_update(key, value),
            }
        }

        if let Some(inner) = self.inner.as_mut() {
            inner.recalculate_hash_recursive();
        }
    }
}
