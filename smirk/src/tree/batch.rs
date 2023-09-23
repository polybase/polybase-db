use miden_crypto::{Felt, FieldElement};

use crate::{
    hash::{Digest, Hashable},
    tree::proof::batch_hash::debug_batch_hash,
    MerkleTree,
};

use super::proof::batch_hash::{prove_batch_hash, BatchHashProof};

/// An operation that represents an update to the tree
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Operation<K, V> {
    /// Insert the following key-value pair
    Insert(K, V),
}

impl<K, V> Operation<K, V> {
    /// The key that this operation modifies
    #[must_use]
    pub fn key(&self) -> &K {
        match self {
            Operation::Insert(key, ..) => key,
        }
    }

    pub(crate) fn hash_triple(&self) -> [Digest; 3]
    where
        K: Hashable,
        V: Hashable,
    {
        const INSERT_DISCRIMMINANT: Digest = make_discrimminant(1);

        // as we add more variants, they should all follow this pattern
        // it's actually cheaper to just calculate the hash in miden than to have a branch, even
        // if we don't need the value hash
        match self {
            Self::Insert(k, v) => [INSERT_DISCRIMMINANT, k.hash(), v.hash()],
        }
    }
}

/// little helper to make things fit on one line
const fn make_discrimminant(i: u64) -> Digest {
    #[allow(clippy::manual_assert)]
    if i == 0 {
        panic!("this is too easy to confuse with `Digest::NULL`");
    }

    let mut elements = [Felt::ZERO; 4];
    elements[3] = Felt::new(i);
    Digest::from_elements_const(elements)
}

impl<K, V> Hashable for Operation<K, V>
where
    K: Hashable,
    V: Hashable,
{
    fn hash(&self) -> Digest {
        // we don't use the FromIter impl here because it saves perf in miden
        // each FromIter call merges the first hash into [`Digest::NULL`], which is an extra hash
        // merge we don't need here
        //
        // this MUST stay consistent with the hashing format in the miden asm, otherwise we'll get
        // different results
        let [a, b, c] = self.hash_triple();
        a.merged_with(b.merged_with(c))
    }
}

/// A batch of operations that can be applied to a [`MerkleTree`]
///
/// If there are multiple operations
#[derive(Debug, Clone)]
pub struct Batch<K, V> {
    // these must remain sorted at all times
    operations: Vec<Operation<K, V>>,
}

impl<K, V> Batch<K, V> {
    /// Create a new [`Batch`] from a list of [`Operation`]s
    ///
    /// Note, if two operations reference the same key, they will be applied in the order they
    /// exist in `operations`. No other guarantees about the order of execution are made
    #[must_use]
    pub fn from_operations(mut operations: Vec<Operation<K, V>>) -> Self
    where
        K: Ord,
    {
        // preserve order of operations, so don't use sort_unstable
        operations.sort_by(|a, b| a.key().cmp(b.key()));
        Self { operations }
    }

    /// Get a slice to the operations in this batch
    #[must_use]
    pub fn operations(&self) -> &[Operation<K, V>] {
        &self.operations
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

impl<K, V> Hashable for Batch<K, V>
where
    K: Hashable + Ord,
    V: Hashable,
{
    fn hash(&self) -> Digest {
        self.operations.iter().map(Hashable::hash).collect()
    }
}

impl<K, V> Batch<K, V>
where
    K: Hashable + Ord,
    V: Hashable,
{
    /// Calculate the hash of a given [`Batch`], and generate a proof that this hash is correct
    /// ```rust
    /// # use smirk::tree::Batch;
    /// let batch: Batch = todo!();
    /// let proof = batch.hash_and_prove();
    ///
    /// assert_eq!(proof.hash(), batch.hash());
    /// assert!(proof.verify().is_ok());
    /// ```
    #[must_use]
    pub fn hash_and_prove(&self) -> BatchHashProof {
        let hash = self.hash();
        let proof = prove_batch_hash(self);

        #[cfg(debug_assertions)]
        {
            if hash != proof.hash() {
                debug_batch_hash(self);
            }
        }

        assert_eq!(
            hash,
            proof.hash(),
            "if these are different, something is very wrong"
        );
        proof
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

#[cfg(any(test, feature = "proptest"))]
pub mod proptest {
    use std::fmt::Debug;

    use proptest::{
        arbitrary::StrategyFor,
        prelude::{any, Arbitrary},
        sample::SizeRange,
        strategy::{Map, Strategy},
    };

    use super::{Batch, Operation};

    impl<K, V> Arbitrary for Batch<K, V>
    where
        K: Debug + Arbitrary + Ord,
        V: Debug + Arbitrary,
    {
        type Parameters = ();
        type Strategy = Map<StrategyFor<Vec<Operation<K, V>>>, fn(Vec<Operation<K, V>>) -> Self>;

        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            any::<Vec<Operation<K, V>>>().prop_map(Batch::from_operations)
        }
    }

    pub fn batch<K, V>(len: impl Into<SizeRange>) -> impl Strategy<Value = Batch<K, V>>
    where
        K: Debug + Arbitrary + Ord,
        V: Debug + Arbitrary,
    {
        proptest::collection::vec(any::<Operation<K, V>>(), len).prop_map(Batch::from_operations)
    }

    impl<K, V> Arbitrary for Operation<K, V>
    where
        K: Debug + Arbitrary,
        V: Debug + Arbitrary,
    {
        type Parameters = ();
        type Strategy = Map<StrategyFor<(K, V)>, fn((K, V)) -> Self>;

        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            any::<(K, V)>().prop_map(|(k, v)| Operation::Insert(k, v))
        }
    }
}
