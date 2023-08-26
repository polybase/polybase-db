use miden_assembly::Assembler;
use miden_crypto::{Felt, FieldElement};
use miden_processor::{MemAdviceProvider, StackInputs};
use proptest::prop_assume;
use test_strategy::proptest;

use crate::{
    batch::Batch,
    hash::{Digest, Hashable},
    tree::hash_left_right_this,
    MerkleTree,
};

#[proptest]
fn any_proof_is_valid(mut tree: MerkleTree<i32, String>, batch: Batch<i32, String>) {
    let proof = tree.apply_and_prove(batch);
    assert!(proof.verify());
}

#[proptest]
fn miden_hash_this_left_right(this: Digest, left: Option<Digest>, right: Option<Digest>) {
    prop_assume!(this != Digest::NULL);
    prop_assume!(left != Some(Digest::NULL));
    prop_assume!(right != Some(Digest::NULL));

    let mut expected = hash_left_right_this(this, left, right).to_elements();
    expected.reverse();
    let mut expected_stack = [Felt::ZERO; 16];
    expected_stack[0..4].copy_from_slice(&expected);

    let s = "begin\nswapw hmerge swapw hmerge\nend";

    let program = Assembler::default().compile(s).unwrap();

    let left = left.unwrap_or(Digest::NULL);
    let right = right.unwrap_or(Digest::NULL);
    let stack = [this.to_elements(), left.to_elements(), right.to_elements()];
    let stack_inputs = StackInputs::new(stack.into_iter().flatten().collect());
    let advice_provider = MemAdviceProvider::default();

    let trace = miden_processor::execute(&program, stack_inputs, advice_provider).unwrap();

    let actual = trace.stack_outputs().stack_top();

    assert_eq!(actual, expected_stack);
}

#[test]
fn foo() {
    let this = 1.hash();
    let left = 2.hash();
    let right = 3.hash();

    let program = Assembler::default().compile("begin swapw hmerge hmerge end").unwrap();

    let mut expected = hash_left_right_this(this, Some(left), Some(right)).to_elements();
    expected.reverse();
    let mut expected_stack = [Felt::ZERO; 16];
    expected_stack[0..4].copy_from_slice(&expected);
    let stack = [this.to_elements(), left.to_elements(), right.to_elements()];
    let stack_inputs = StackInputs::new(stack.into_iter().flatten().collect());
    let advice_provider = MemAdviceProvider::default();
    let trace = miden_processor::execute(&program, stack_inputs, advice_provider).unwrap();

    let actual = trace.stack_outputs().stack_top();

    assert_eq!(actual, expected_stack);
}
