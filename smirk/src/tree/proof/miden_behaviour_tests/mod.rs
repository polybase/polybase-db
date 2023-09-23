//! A series of tests I've written to better understand how miden works, rather than to check any
//! specific functionality.
//!
//! Leaving them here in case they're helpful to future contributors

use miden_assembly::Assembler;
use miden_crypto::{Felt, FieldElement};
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

// When pushing multiple words, the first word popped is the first word put into the stack
// But the elements are in reverse order
#[test]
fn reading_advice_stack_single_word() {
    let to_word = |i| [Felt::ZERO, Felt::ZERO, Felt::ZERO, Felt::new(i)];
    let words = (1..=5).map(to_word);
    let advice = AdviceInputs::default().with_stack(words.flatten());

    let program = Assembler::default().compile("begin adv_loadw end").unwrap();
    let stack = StackInputs::default();
    let advice = MemAdviceProvider::from(advice);
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    let mut expected = to_word(1);
    expected.reverse();
    assert_eq!(stack_outputs.stack_top()[0..4], expected);
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

/// If it's important that we get elements back in the same format we send them, we should use
/// `stack_top()`
#[test]
fn hash_round_trip() {
    let stack_elements = 1.hash().to_elements();

    let program = Assembler::default().compile("begin end").unwrap();
    let stack = StackInputs::new(stack_elements.to_vec());
    let advice = MemAdviceProvider::default();
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    let elements: Vec<_> = stack_outputs.stack_top()[0..4]
        .iter()
        .rev()
        .copied()
        .collect();
    let hash_again = Digest::from_elements(elements.try_into().unwrap());

    assert_eq!(hash_again, 1.hash());
}

#[test]
fn merge_hash() {
    let stack = [1.hash().to_elements(), 2.hash().to_elements()]
        .map(|mut array| {
            array.reverse();
            array
        })
        .into_iter()
        .flatten()
        .collect();

    println!("{stack:?}");

    let program = program!("./merge_hash.masm");
    let stack = StackInputs::new(stack);
    let advice = MemAdviceProvider::default();

    let iter = miden_processor::execute_iter(&program, stack, advice);

    for state in iter {
        let state = state.unwrap().stack;
        println!("{state:?}");
    }

    // let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();
    //

    let rev = |value: i32| Digest::from_elements(value.hash().to_elements_rev());

    let expected_hash: Digest = [2.hash(), 1.hash()].iter().collect();
    let expected_hash2: Digest = [1.hash(), 2.hash()].iter().collect();
    let expected_hash3: Digest = [rev(2), rev(1)].iter().collect();
    let expected_hash4: Digest = [rev(1), rev(2)].iter().collect();

    dbg!(
        expected_hash.to_elements(),
        expected_hash2.to_elements(),
        expected_hash3.to_elements(),
        expected_hash4.to_elements(),
    );

    panic!(
        "{:?}\n{:?}\n{:?}\n{:?}",
        expected_hash.to_elements(),
        expected_hash2.to_elements(),
        1.hash().to_elements(),
        2.hash().to_elements()
    );
    // // let mut actual_hash = stack_outputs.stack_top()[0..4].to_vec();
    // // actual_hash.reverse();
    // // let actual_hash = Digest::from_elements(actual_hash.try_into().unwrap());
    //
    // assert_eq!(
    //     stack_outputs.stack_top()[0..4].to_vec(),
    //     expected_hash.to_elements().to_vec()
    // );
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
fn reading_advice_stack_by_word() {
    let adv_stack = [1, 1, 1, 0, 2, 2, 2, 0, 3, 3, 3, 0]
        .map(Felt::new)
        .into_iter();
    let advice = AdviceInputs::default().with_stack(adv_stack);

    let program = program!("./reading_adv_stack_by_word.masm");
    let stack = StackInputs::default();
    let advice = MemAdviceProvider::from(advice);
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    let expected = [1, 1, 1, 0, 2, 2, 2, 0, 3, 3, 3, 0];

    assert_eq!(stack_outputs.stack_truncated(12), expected);
}

#[test]
fn stack_is_deeper_than_16() {
    let stack = [1, 1, 1, 0, 2, 2, 2, 0, 3, 3, 3, 0]
        .map(Felt::new)
        .into_iter()
        .collect::<Vec<_>>();

    let program = program!("./stack_is_deeper_than_16.masm");
    let stack = StackInputs::new(stack.clone());
    let advice = MemAdviceProvider::from(AdviceInputs::default());
    let options = ProofOptions::default();

    let (stack_outputs, _proof) = miden_prover::prove(&program, stack, advice, options).unwrap();

    let expected = [0, 3, 3, 3, 0, 2, 2, 2, 0, 1, 1, 1];

    assert_eq!(stack_outputs.stack_truncated(12), expected);
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
