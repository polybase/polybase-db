use test_strategy::proptest;

use crate::{batch::Batch, MerkleTree};

#[proptest]
fn any_proof_is_valid(mut tree: MerkleTree<i32, String>, batch: Batch<i32, String>) {
    let proof = tree.apply_and_prove(batch);
    assert!(proof.verify());
}


