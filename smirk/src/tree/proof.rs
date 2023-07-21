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

                    node = node.right.as_deref()?;
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
    /// ```rust
    /// # use smirk::smirk;
    /// let mut tree = smirk! { 1 => "hello" };
    /// let hash = tree.root_hash();
    ///
    /// tree.insert(2, "world");
    /// let new_hash = tree.root_hash();
    ///
    /// assert_ne!(hash, new_hash);
    /// ```
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
        hash = hash_left_right_this(hash, self.left, self.right);

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
    use test_strategy::proptest;

    use crate::{smirk, MerkleTree};

    #[test]
    fn simple_proof_example() {
        let tree = smirk! {
            1 => "hello",
            2 => "world",
            3 => "foo",
        };

        let path = tree.prove(&1).unwrap();

        assert!(path.verify(&1, &"hello"));
        assert!(!path.verify(&2, &"hello"));
        assert!(!path.verify(&1, &"world"));

        assert!(tree.prove(&4).is_none());
    }

    #[proptest]
    fn all_proof_root_hash_match(tree: MerkleTree<i32, String>) {
        for node in tree.iter() {
            let proof = tree.prove(node.key()).unwrap();
            assert_eq!(proof.root_hash(), tree.root_hash());
        }
    }

    // we use u8 as the key type to improve the chances of it being in the tree
    #[proptest]
    fn proof_succeeds_iff_key_contained(tree: MerkleTree<u8, String>, key: u8) {
        let tree_contains_key = tree.contains(&key);
        let proof_valid = tree.prove(&key).is_some();

        assert_eq!(tree_contains_key, proof_valid);
    }

    #[proptest]
    fn proof_is_valid(tree: MerkleTree<u8, String>, key: u8) {
        let proof = tree.prove(&key);

        let Some(value) = tree.get(&key) else { return Ok(()); };
        let proof = proof.unwrap();

        let valid = proof.verify(&key, value);
        assert!(valid);

        assert_eq!(tree.root_hash(), proof.root_hash());
    }
}
