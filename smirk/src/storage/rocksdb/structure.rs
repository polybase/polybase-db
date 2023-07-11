use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    hash::{Digest, Hashable},
    storage::Error,
    MerkleTree, TreeNode,
};

/// The structure of a tree
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Structure {
    pub hash: Digest,
    pub left: Option<Box<Structure>>,
    pub right: Option<Box<Structure>>,
}

impl Structure {
    pub fn from_tree<K, V: Hashable>(tree: &MerkleTree<K, V>) -> Option<Self> {
        tree.inner.as_deref().map(Self::from_node)
    }

    fn from_node<K, V: Hashable>(node: &TreeNode<K, V>) -> Self {
        let hash = node.value().hash();
        let left = node.left.as_deref().map(Self::from_node).map(Box::new);
        let right = node.left.as_deref().map(Self::from_node).map(Box::new);

        Self { hash, left, right }
    }

    pub fn to_tree<K: Ord, V: Hashable>(
        &self,
        values: &mut HashMap<Digest, (K, V)>,
    ) -> Result<MerkleTree<K, V>, Error> {
        dbg!(values.keys().collect::<Vec<_>>());
        let node = Self::to_node(&self, values)?;

        Ok(MerkleTree {
            inner: Some(Box::new(node)),
        })
    }

    fn to_node<K: Ord, V: Hashable>(
        &self,
        values: &mut HashMap<Digest, (K, V)>,
    ) -> Result<TreeNode<K, V>, Error> {
        let hash = self.hash;
        let Some((key, value)) = values.remove(&hash) else {
            return Err(Error::StructureReferenceMissing { hash });
        };

        let left = self
            .left
            .as_deref()
            .map(|s| Self::to_node(s, values).map(Box::new))
            .transpose()?;
        let right = self
            .right
            .as_deref()
            .map(|s| Self::to_node(s, values).map(Box::new))
            .transpose()?;

        let mut node = TreeNode {
            // this hash is the hash including children, `hash` that is in scope is the hash
            // excluding children, so we just use null for now and clean up later
            hash: Digest::NULL,
            key,
            value,
            right,
            left,
            height: 0,
        };

        node.update_height();
        node.recalculate_hash_recursive();

        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;

    #[proptest]
    fn to_from_structure(tree: MerkleTree<i32, String>) {

    }
}
