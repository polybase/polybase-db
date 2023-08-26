//! A series of tests I've written to better understand how miden works, rather than to check any
//! specific functionality.
//!
//! Leaving them here in case they're helpful to future contributors

use miden_assembly::Assembler;
use miden_crypto::Felt;
use miden_prover::{AdviceInputs, MemAdviceProvider, ProofOptions, StackInputs};

use crate::hash::{Digest, Hashable};

macro_rules! program {
    ($path:literal) => {
        Assembler::default().compile(include_str!($path)).unwrap()
    };
}

/// When creating a [`StackInputs`] from a Rust vector, the last element is the top of the stack
///
/// When getting the stack outputs back from Miden, the elements are reversed (i.e. the first
/// element is the top of the stack)
#[test]
fn stack_order() {
    let program = program!("./stack_order.masm");
    let stack = StackInputs::new((1..=5).map(Felt::new).collect());
    let advice = MemAdviceProvider::default();
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    assert_eq!(stack_outputs.stack_truncated(6), &[123, 5, 4, 3, 2, 1]);
}

/// `while.true` consumes a `1` or `0` from the top of the stack on each iteration
#[test]
fn while_actually_removes_elements_from_stack() {
    let stack = [5, 0, 1, 1, 1, 1].map(Felt::new).to_vec();

    let program = program!("./while_removes_elements.masm");
    let stack = StackInputs::new(stack);
    let advice = MemAdviceProvider::default();
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    assert_eq!(stack_outputs.stack_truncated(2), [5, 0]);
}

/// When popping multiple elements from the advice stack, they are put onto the stack in the order
/// they are put on the advice stack on the Rust side
#[test]
fn reading_advice_stack() {
    let advice = AdviceInputs::default().with_stack((1..=5).map(Felt::new));

    let program = program!("./reading_advice_stack.masm");
    let stack = StackInputs::default();
    let advice = MemAdviceProvider::from(advice);
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    assert_eq!(stack_outputs.stack_truncated(3), &[3, 2, 1]);
}

/// Calling `adv_push.1` 3x in a row does the same thing as `adv_push.3`
#[test]
fn popping_advice_stack_one_by_one_is_same_as_one_big_pop() {
    let advice = AdviceInputs::default().with_stack((1..=5).map(Felt::new));

    let program = program!("./reading_advice_stack_1by1.masm");
    let stack = StackInputs::default();
    let advice = MemAdviceProvider::from(advice);
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    assert_eq!(stack_outputs.stack_truncated(3), &[3, 2, 1]);
}

/// This is a big one
///
/// We can use `adv.push_mapval`:
///  - look up the key that is the top 4 elements of the operand stack
///  - copy those elements into the advice stack
///  - then we can use `adv_push` to get those values into the operand stack
#[test]
fn reading_advice_map() {
    let advice = AdviceInputs::default()
        .with_map([(
            1.hash().to_bytes(),
            vec![Felt::new(1), Felt::new(2), Felt::new(3), Felt::new(4)],
        )])
        .with_stack(1.hash().to_elements());

    let program = program!("./read_advice_map.masm");
    let stack = StackInputs::default();
    let advice = MemAdviceProvider::from(advice);
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    assert_eq!(stack_outputs.stack_truncated(4), &[4, 3, 2, 1]);
}

#[test]
fn merge_hash() {
    let stack = [1.hash().to_elements(), 2.hash().to_elements()]
        .into_iter()
        .flatten()
        .collect();

    let program = program!("./merge_hash.masm");
    let stack = StackInputs::new(stack);
    let advice = MemAdviceProvider::default();
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    let expected_hash: Digest = [1.hash(), 2.hash()].iter().collect();
    let mut expected_elements = expected_hash.to_elements();
    expected_elements.reverse();

    assert_eq!(&stack_outputs.stack_top()[0..4], expected_elements);
}

#[test]
fn merge_3_hashes() {
    let stack = [1, 2, 3]
        .map(|i| i.hash().to_elements())
        .into_iter()
        .flatten()
        .collect();

    let program = program!("./merge_3_hashes.masm");
    let stack = StackInputs::new(stack);
    let advice = MemAdviceProvider::default();
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    let expected_hash: Digest = [1.hash(), 2.hash()].iter().collect();
    let mut expected_elements = expected_hash.to_elements();
    expected_elements.reverse();

    assert_eq!(&stack_outputs.stack_top()[0..4], expected_elements);
}

#[test]
fn fibonacci() {
    fn fib(n: u64) -> u64 {
        match n {
            0 => 0,
            1 => 1,
            n => fib(n - 1) + fib(n - 2),
        }
    }

    let count = 50;

    let stack = vec![Felt::new(count)];

    let program = program!("./fibonacci.masm");
    let stack = StackInputs::new(stack);
    let advice = MemAdviceProvider::default();
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    assert_eq!(stack_outputs.stack_truncated(1)[0], fib(count));
}
