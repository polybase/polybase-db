use miden_crypto::{merkle::InnerNodeInfo, Felt};
use miden_prover::AdviceInputs;

use crate::{
    batch::{Batch, Operation},
    hash::{Digest, Hashable},
    MerkleTree, TreeNode,
};

pub(super) fn advice<K, V>(tree: &MerkleTree<K, V>, batch: &Batch<K, V>) -> AdviceInputs
where
    K: Hashable + Ord,
    V: Hashable,
{
    let mut advice = AdviceInputs::default();

    // advice map contains mapping from hash -> field elements corresponding to ordering
    advice.extend_map(compute_key_orderings(tree, batch));

    // merkle store contains tree structure
    advice.extend_merkle_store(tree.iter().map(|node| {
        let left = node.left_hash().unwrap_or(Digest::NULL);
        let right = node.right_hash().unwrap_or(Digest::NULL);
        let value = node.hash();

        let left = left.into();
        let right = right.into();
        let value = value.into();

        InnerNodeInfo { value, left, right }
    }));
    advice
}

fn compute_key_orderings<'a, K, V>(
    tree: &'a MerkleTree<K, V>,
    batch: &'a Batch<K, V>,
) -> impl IntoIterator<Item = ([u8; 32], Vec<Felt>)> + 'a
where
    K: Hashable + Ord,
    V: Hashable,
{
    let batch_keys = batch.operations().iter().map(Operation::key);
    let keys = tree.iter().map(TreeNode::key).chain(batch_keys);

    let mut keys: Vec<_> = keys.collect();
    keys.sort();

    keys.into_iter().enumerate().map(|(index, key)| {
        let key = key.hash().to_bytes();
        let index = index.try_into().expect("usize should be 64 bits");
        let order = Felt::new(index);

        (key, vec![order])
    })
}

#[cfg(test)]
mod tests {
    use crate::smirk;

    use super::*;

    #[test]
    fn key_orderings() {
        let tree = smirk! {
            1 => "hello",
            2 => "world",
        };

        let batch = Batch::from_iter([Operation::Insert(3, "foo")]);

        let orderings: Vec<_> = compute_key_orderings(&tree, &batch).into_iter().collect();

        assert_eq!(
            orderings,
            vec![
                (1.hash().to_bytes(), vec![Felt::new(1)]),
                (2.hash().to_bytes(), vec![Felt::new(2)]),
                (3.hash().to_bytes(), vec![Felt::new(3)]),
            ]
        );
    }
}
