//! Items relating to hashing
//!
//! In particular, the [`Digest`] type and the [`Hashable`] trait
//!
//! This module also contains [`MerklePath`], which can be used to verify the existance of a key in
//! a [`MerkleTree`]
//!
//! [`MerkleTree`]: crate::MerkleTree

use std::fmt::{Debug, Display};

use miden_crypto::{
    hash::rpo::{Rpo256, RpoDigest},
    utils::{Deserializable, SliceReader},
    Felt,
};

mod from_iter;
mod hashable;
mod path;
mod serde_impls;

pub use hashable::Hashable;
pub use path::MerklePath;
pub(crate) use path::Stage;

#[cfg(any(test, feature = "proptest"))]
mod proptest_impls;

/// A Rescue-Prime Optimized digest
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Digest(pub(crate) RpoDigest);

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

impl std::hash::Hash for Digest {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        <[u8; 32] as std::hash::Hash>::hash(&self.to_bytes(), state);
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
    ///
    /// These bytes can be converted back to a [`Digest`] using [`Digest::from_bytes`] (though this
    /// function returns an `Option` since it can fail)
    #[inline]
    #[must_use]
    pub fn to_bytes(&self) -> [u8; Self::LEN] {
        self.0.as_bytes()
    }

    /// Create a [`Digest`] from the byte array representation
    ///
    /// Note: this returns an `Option` because not all possible byte arrays are valid [`Digest`]s
    ///
    /// Any byte array returned from [`Digest::to_bytes`] will be valid for this function, and the
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

    /// Convert this [`Digest`] to its hex representation (i.e. the hex encoding of
    /// [`Digest::to_bytes`])
    #[inline]
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.to_bytes())
    }

    /// Replace `self` with `rpo256(this + other)`
    #[inline]
    pub fn merge(&mut self, other: &Digest) {
        self.0 = Rpo256::merge(&[self.0, other.0]);
    }

    /// Return the result of `other` merged into `self`
    #[inline]
    #[must_use]
    pub fn merged_with(self, other: Digest) -> Self {
        Rpo256::merge(&[self.0, other.0]).into()
    }

    /// Construct a [`Digest`] from the field elements of the underlying RPO hash
    #[inline]
    #[must_use]
    pub fn from_elements(elements: [Felt; 4]) -> Self {
        Self(RpoDigest::new(elements))
    }

    /// not sure we want to make this const publicly yet - keep private for now
    #[inline]
    #[must_use]
    pub(crate) const fn from_elements_const(elements: [Felt; 4]) -> Self {
        Self(RpoDigest::new(elements))
    }

    /// Return the field elements that make up this [`Digest`]
    #[inline]
    #[must_use]
    pub fn to_elements(self) -> [Felt; 4] {
        self.0.as_elements().try_into().unwrap()
    }

    /// Return the field elements that make up this [`Digest`] in reverse order
    #[inline]
    #[must_use]
    pub fn to_elements_rev(self) -> [Felt; 4] {
        let mut arr = self.to_elements();
        arr.reverse();
        arr
    }

    /// Utility for seeing what's wrong with hash stuff
    #[allow(unused)]
    pub(crate) fn debug_print(&self) {
        println!("debug print hash =====================");
        println!("self    : {self}");
        println!("elements: {:?}", self.0.as_elements());
        let u64s = self
            .0
            .as_elements()
            .iter()
            .map(Felt::inner)
            .collect::<Vec<_>>();
        println!("u64s    : {:?}", &u64s);
    }
}

impl From<RpoDigest> for Digest {
    fn from(value: RpoDigest) -> Self {
        Self(value)
    }
}

impl From<Digest> for RpoDigest {
    fn from(Digest(value): Digest) -> Self {
        value
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
