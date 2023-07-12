use std::{borrow::Borrow, cmp::Ordering};

use crate::{
    hash::{Digest, Hashable, MerklePath},
    MerkleTree,
};

impl<K, V: Hashable> MerkleTree<K, V> {
    /// Generate a [`MerklePath`] that proves that a given key exists in the tree
    ///
    /// ```rust
    /// # use smirk::{smirk, MerklePath};
    /// let tree = smirk! {
    ///   1 => "hello",
    ///   2 => "world",
    /// };
    ///
    /// assert!(tree.prove(&1).is_some());
    /// assert!(tree.prove(&2).is_some());
    /// assert!(tree.prove(&3).is_none());
    /// ```
    pub fn prove<Q>(&self, key: &Q) -> Option<MerklePath>
    where
        Q: Borrow<K> + ?Sized,
        K: Ord,
    {
        let Some(mut node) = self.inner.as_deref() else { return None };
        let mut components = Vec::with_capacity(node.height() as usize);

        loop {
            components.push(node.hash());

            match key.borrow().cmp(node.key()) {
                Ordering::Less => node = node.left.as_deref()?,
                Ordering::Greater => node = node.right.as_deref()?,
                Ordering::Equal => {
                    components.reverse();
                    return Some(MerklePath::new(components));
                }
            }
        }
    }

    /// Get the root hash of the Merkle tree
    pub fn root_hash(&self) -> Digest {
        match &self.inner {
            None => Digest::NULL, // should this function return an option?
            Some(node) => node.hash(),
        }
    }

    /// Generate
    /// Verify that the given value exists in the tree, by using the provided [`MerklePath`]
    pub fn verify(&self, path: &MerklePath, value: &V) -> bool
    where
        V: Hashable,
    {
        if path.components().last() != Some(&self.root_hash()) {
            dbg!("not end root hash");
            return false;
        }

        let mut hash = value.hash();

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
