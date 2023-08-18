use serde::{Deserialize, Serialize};

use super::Digest;

/// A Merkle path that can be used to prove the existance of a value in the tree
///
/// This type provides [`MerklePath::to_bytes`] and [`MerklePath::from_bytes`] for serialization
/// purposes. It also implements [`Serialize`] and [`Deserialize`], if more control over exact
/// serialization details is needed.
///
/// Note: no [`Arbitrary`] implementation is provided for this type, since it has no public
/// constructors. The only way to create one is to prove the existance of a key-value pair in a
/// [`MerkleTree`].
///
/// Luckily, [`MerkleTree`] *does* implement [`Arbitrary`]
///
/// [`Arbitrary`]: proptest::prelude::Arbitrary
/// [`MerkleTree`]: crate::MerkleTree
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerklePath {
    /// The intermediate stages between the root hash and the target node
    pub(crate) stages: Vec<Stage>,

    /// The root hash of the tree that generated this path
    pub(crate) root_hash: Digest,

    /// The digest of the left sub-tree of the node that contained the target key-value pair
    pub(crate) left: Option<Digest>,

    /// The digest of the right sub-tree of the node that contained the target key-value pair
    pub(crate) right: Option<Digest>,
}

impl MerklePath {
    /// The root hash of the tree that generated this [`MerklePath`]
    #[inline]
    #[must_use]
    pub fn root_hash(&self) -> Digest {
        self.root_hash
    }

    /// Convert this [`MerklePath`] to a canonical serialized representation.
    ///
    /// The exact details of the representation are not specified, other than that it can be
    /// reversed with [`MerklePath::from_bytes`]
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(&self).unwrap()
    }

    /// Create a [`MerklePath`] from its canonical serialized representation
    ///
    /// The exact details of the representation are not specified, other than that it can be
    /// reversed with [`MerklePath::to_bytes`]
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        rmp_serde::from_slice(bytes).ok()
    }
}

/// A stage in a merkle proof (i.e. a single step in the binary search algorithm)
///
///  - `this` is the hash of the key-value pair of the visited node in this stage
///  - `left`/`right` is the root hash of the "other side" of the tree
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Stage {
    Left { this: Digest, right: Option<Digest> },
    Right { this: Digest, left: Option<Digest> },
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use crate::MerkleTree;

    use super::*;

    #[proptest]
    fn path_serialization_round_trip(tree: MerkleTree<i32, String>) {
        for node in tree.iter() {
            let proof = tree.path_for(node.key()).unwrap();
            let bytes = proof.to_bytes();
            let proof_again = MerklePath::from_bytes(&bytes).unwrap();

            assert_eq!(proof, proof_again);
        }
    }
}
