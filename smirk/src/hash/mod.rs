//! Items relating to hashing data
//!
//! In particular, the [`Digest`] type and the [`Hashable`] trait

use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

use miden_crypto::{
    hash::rpo::{Rpo256, RpoDigest},
    utils::{Deserializable, SliceReader},
    Felt,
};

mod hashable;
pub use hashable::Hashable;
#[cfg(any(test, feature = "proptest"))]
mod proptest_impls;
mod serde_impls;

/// A Rescue-Prime Optimized digest
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Digest(RpoDigest);

impl Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({})", hex::encode(self.0.as_bytes()))
    }
}

impl Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({})", hex::encode(self.0.as_bytes()))
    }
}

impl Hash for Digest {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        <[u8; 32] as Hash>::hash(&self.to_bytes(), state);
    }
}

impl Digest {
    /// The null hash
    ///
    /// This represents the hash of "nothing" (for example, an empty Merkle tree will have this as
    /// the root hash)
    ///
    /// ```rust
    /// # use smirk::hash::Digest;
    /// # use smirk::MerkleTree;
    /// let empty_tree = MerkleTree::<i32, i32>::new();
    /// assert_eq!(empty_tree.root_hash(), Digest::NULL);
    /// ```
    pub const NULL: Digest = Digest(RpoDigest::new([Felt::new(0); 4]));

    /// The length of this hash in bytes
    const LEN: usize = 32;

    /// Get the representation of this hash as a byte array
    #[inline]
    #[must_use]
    pub fn to_bytes(&self) -> [u8; Self::LEN] {
        self.0.as_bytes()
    }

    /// Create a [`Digest`] from the byte array representation
    ///
    /// Note: this returns an `Option` because not all possible byte arrays are valid [`Digest`]s
    ///
    /// Any byte array returned from [`Digets::to_bytes`] will be valid for this function, and the
    /// resulting hash will be equal to the hash that created the byte array
    #[inline]
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Option<Self> {
        let mut reader = SliceReader::new(&bytes);
        RpoDigest::read_from(&mut reader).ok().map(Digest)
    }

    /// Calculate the hash of the given bytes
    #[inline]
    #[must_use]
    pub fn calculate(bytes: &[u8]) -> Self {
        Self(Rpo256::hash(bytes))
    }

    /// Replace `self` with `rpo256(this + other)`
    #[inline]
    pub fn merge(&mut self, other: &Digest) {
        self.0 = Rpo256::merge(&[self.0, other.0]);
    }
}

impl From<RpoDigest> for Digest {
    fn from(value: RpoDigest) -> Self {
        Self(value)
    }
}

/// A Merkle path that can be used to prove the existance of a value in the tree
pub struct MerklePath {
    /// The components of the path, with the root at the end
    components: Vec<Digest>,
}

impl MerklePath {
    /// Create a new [`MerklePath`] from the given components
    ///
    /// The components should be the hashes that form the path, with the root of the tree at the
    /// end
    #[inline]
    #[must_use]
    pub fn new(components: Vec<Digest>) -> Self {
        Self { components }
    }

    /// Get a slice of hashes representing the components of the path
    #[inline]
    #[must_use]
    pub fn components(&self) -> &[Digest] {
        &self.components
    }

    /// Get a mutable slice of hashes representing the components of the path
    #[inline]
    pub fn components_mut(&mut self) -> &mut [Digest] {
        &mut self.components
    }
}

#[cfg(test)]
mod tests {
    use proptest::prop_assert_eq;
    use test_strategy::proptest;

    use super::*;

    #[test]
    fn null_hash_is_all_zeroes() {
        assert_eq!(Digest::NULL.to_bytes(), [0; 32]);
    }

    #[proptest]
    fn digest_bytes_round_trip(digest: Digest) {
        let bytes = digest.to_bytes();
        let digest_again = Digest::from_bytes(bytes).unwrap();

        prop_assert_eq!(digest, digest_again);
    }

    #[proptest]
    fn digest_bytes_serde_round_trip(digest: Digest) {
        let mp_bytes = rmp_serde::to_vec(&digest).unwrap();
        let digest_again: Digest = rmp_serde::from_slice(&mp_bytes).unwrap();

        prop_assert_eq!(digest, digest_again);
    }
}
