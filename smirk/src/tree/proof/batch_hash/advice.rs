use miden_processor::{AdviceInputs, MemAdviceProvider};

use crate::{
    batch::{Batch, Operation},
    hash::{Digest, Hashable},
    tree::proof::utils,
};

pub(super) fn make_advice<K, V>(batch: &Batch<K, V>) -> MemAdviceProvider
where
    K: Hashable,
    V: Hashable,
{
    let mut advice = AdviceInputs::default();

    utils::add_hashes_to_advice(&mut advice, compute_hashes(batch));

    // let hashes = compute_hashes(batch);
    // advice.extend_stack(hashes.into_iter().rev().flat_map(Digest::to_elements));

    // assert_eq!(
    //     advice.stack().len(),
    //     batch.operations().len().saturating_mul(12), // 3 hashes, 4 elements each
    // );

    MemAdviceProvider::from(advice)
}

fn compute_hashes<K, V>(batch: &Batch<K, V>) -> impl Iterator<Item = Digest> + '_
where
    K: Hashable,
    V: Hashable,
{
    batch
        .operations()
        .iter()
        .flat_map(Operation::hash_triple)
        // .flat_map(|[a, b, c]| [c, b, a])
    // let mut vec = Vec::with_capacity(batch.operations().len().saturating_mul(3));
    //
    //
    // for operation in batch.operations() {
    //     let [a, b, c] = operation.hash_triple();
    //     vec.extend_from_slice(&[c, a, b]);
    // }
    //
    // vec
}
