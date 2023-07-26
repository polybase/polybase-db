use std::iter::once;

use crate::{
    hash::{Digest, Hashable},
    MerkleTree, TreeNode,
};

impl<K: Hashable, V: Hashable> MerkleTree<K, V> {
    pub(crate) fn recalculate_hash_recursive(&mut self) -> bool {
        match self.inner.as_mut() {
            Some(inner) => inner.recalculate_hash_recursive(),
            None => false,
        }
    }
}

impl<K: Hashable, V: Hashable> TreeNode<K, V> {
    /// The hash of the left subtree (if it exists)
    pub(crate) fn left_hash(&self) -> Option<Digest> {
        self.left.as_ref().map(|node| node.hash)
    }

    /// The hash of the right subtree (if it exists)
    pub(crate) fn right_hash(&self) -> Option<Digest> {
        self.right.as_ref().map(|node| node.hash)
    }

    /// Update the `hash` field of this node, and all child nodes
    pub(crate) fn recalculate_hash_recursive(&mut self) -> bool {
        if let Some(left) = &mut self.left {
            left.recalculate_hash_recursive();
        }

        if let Some(right) = &mut self.right {
            right.recalculate_hash_recursive();
        }

        let this = key_value_hash(self.key(), self.value());
        let left = self.left.as_ref().map(|node| node.hash);
        let right = self.right.as_ref().map(|node| node.hash);
        let new_hash = hash_left_right_this(this, left, right);

        let changed = self.hash != new_hash;

        self.hash = new_hash;

        changed
    }
}

/// Compute the hash of a pair of values (i.e. a key-value pair)
///
/// The hash will change if either input changes:
///
/// ```rust
/// # use smirk::key_value_hash;
/// let hash1 = key_value_hash(&1, "hello");
/// let hash2 = key_value_hash(&2, "hello");
/// let hash3 = key_value_hash(&1, "world");
///
/// assert_ne!(hash1, hash2);
/// assert_ne!(hash1, hash3);
/// assert_ne!(hash2, hash3);
/// ```
///
/// This is guaranteed to be the root hash of a tree with a single entry (with the same key + value)
///
/// ```rust
/// # use smirk::{key_value_hash, smirk};
/// let tree = smirk! { 1 => "hello" };
/// let root_hash = key_value_hash(&1, &"hello");
///
/// assert_eq!(root_hash, tree.root_hash());
/// ```
#[must_use]
pub fn key_value_hash<K: Hashable + ?Sized, V: Hashable + ?Sized>(key: &K, value: &V) -> Digest {
    [key.hash(), value.hash()].into_iter().collect()
}

/// Helper to  a
pub(crate) fn hash_left_right_this(
    this: Digest,
    left: Option<Digest>,
    right: Option<Digest>,
) -> Digest {
    once(this).chain(left).chain(right).collect()
}

#[cfg(test)]
mod tests {
    use proptest::prop_assert_eq;
    use test_strategy::proptest;

    use crate::{key_value_hash, smirk, MerkleTree};

    #[test]
    fn root_hash_is_probably_deterministic() {
        let make = || {
            smirk! {
                1 => "hello",
                2 => "world",
                3 => "foo",
            }
        };

        let root_hash = make().root_hash();

        for _ in 0..1000 {
            let root_hash_again = make().root_hash();
            assert_eq!(root_hash, root_hash_again);
        }
    }

    #[proptest]
    fn root_hash_doesnt_change_when_recalculating(mut tree: MerkleTree<i32, String>) {
        let hash_before = tree.root_hash();
        tree.recalculate_hash_recursive();
        let hash_after = tree.root_hash();

        assert_eq!(hash_before, hash_after);
    }

    #[proptest]
    fn single_element_tree_root_hash_is_kv_hash(key: i32, value: String) {
        let hash = key_value_hash(&key, &value);
        let tree = smirk! { key => value };

        prop_assert_eq!(hash, tree.root_hash());
    }
}
