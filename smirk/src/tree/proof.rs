use std::{borrow::Borrow, cmp::Ordering};

use crate::{
    hash::{Digest, Hashable, MerklePath, Stage},
    key_value_hash, MerkleTree,
};

use super::hash::hash_left_right_this;

impl<K: Hashable, V: Hashable> MerkleTree<K, V> {
    /// Generate a [`MerklePath`] that proves that a given key exists in the tree
    ///
    /// ```rust
    /// # use smirk::{smirk, hash::MerklePath};
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
        let mut stages = Vec::with_capacity(node.height());

        loop {
            match key.borrow().cmp(&node.key) {
                Ordering::Less => {
                    let this = key_value_hash(&node.key, &node.value);
                    let right = node.right_hash();
                    let stage = Stage::Left { this, right };
                    stages.push(stage);

                    node = node.left.as_deref()?;
                }
                Ordering::Greater => {
                    let this = key_value_hash(&node.key, &node.value);
                    let left = node.left_hash();
                    let stage = Stage::Right { this, left };
                    stages.push(stage);

                    node = node.left.as_deref()?;
                }
                Ordering::Equal => {
                    let left = node.left_hash();
                    let right = node.right_hash();
                    let root_hash = self.root_hash();

                    let path = MerklePath {
                        stages,
                        root_hash,
                        left,
                        right,
                    };

                    return Some(path);
                }
            }
        }
    }

    /// Get the root hash of the Merkle tree
    ///
    /// The root hash can be viewed as a "summary" of the whole tree - any change to any key or
    /// value will change the root hash. Changing the "layout" of the tree will also change the
    /// root hash
    #[must_use]
    pub fn root_hash(&self) -> Digest {
        match &self.inner {
            None => Digest::NULL, // should this function return an option?
            Some(node) => node.hash,
        }
    }
}

impl MerklePath {
    /// Verify that the given key-value pair exists in the tree that generated this [`MerklePath`]
    #[must_use = "this function indicates a verification failure by returning false"]
    pub fn verify<K: Hashable, V: Hashable>(&self, key: &K, value: &V) -> bool {
        let mut hash = key_value_hash(key, value);

        for stage in self.stages.iter().rev() {
            match *stage {
                Stage::Left { this, right } => hash = hash_left_right_this(this, Some(hash), right),
                Stage::Right { this, left } => hash = hash_left_right_this(this, left, Some(hash)),
            }
        }

        hash == self.root_hash
    }
}

#[cfg(test)]
mod tests {
    use crate::smirk;

    #[test]
    fn simple_proof_example() {
        let tree = smirk! {
            1 => "hello",
            2 => "world",
        };

        let path = tree.prove(&1).unwrap();

        assert!(path.verify(&1, &"hello"));
        assert!(!path.verify(&2, &"hello"));
        assert!(!path.verify(&1, &"world"));

        assert!(tree.prove(&3).is_none());
    }
}
