use miden_crypto::{Felt, FieldElement};
use miden_processor::{AdviceInputs, StackOutputs};

use crate::hash::Digest;

/// Extension trait for [`StackOutputs`]
pub trait StackOutputExt {
    fn head<const N: usize>(&self) -> Option<[Felt; N]>;
}

impl StackOutputExt for StackOutputs {
    fn head<const N: usize>(&self) -> Option<[Felt; N]> {
        assert!(N <= 16, "only top 16 elements are visible");

        let mut output = [Felt::ZERO; N];
        let stack = self.stack_top();
        output.copy_from_slice(&stack[0..N]);

        Some(output)
    }
}

pub fn add_hashes_to_advice(advice: &mut AdviceInputs, hashes: impl IntoIterator<Item = Digest>) {
    let hashes = hashes.into_iter().flat_map(Digest::to_elements);
    advice.extend_stack(hashes);
}

pub fn hash_from_stack_output(stack: &StackOutputs) -> Digest {
    let mut arr: [Felt; 4] = stack.stack_top()[0..4].try_into().unwrap();
    arr.reverse();
    Digest::from_elements(arr)
}

#[cfg(test)]
mod tests {
    use miden_assembly::Assembler;
    use miden_processor::{MemAdviceProvider, StackInputs};
    use test_strategy::proptest;

    use crate::hash::Hashable;

    use super::*;

    #[test]
    fn stack_outputs() {
        let program = Assembler::default()
            .compile("begin push.1.2.3.4 end")
            .unwrap();

        let output = miden_processor::execute(
            &program,
            StackInputs::default(),
            MemAdviceProvider::default(),
        )
        .unwrap();

        let stack = output.stack_outputs();

        assert_eq!(stack.head().unwrap(), [4, 3, 2, 1].map(Felt::new));
    }

        #[track_caller]
        fn test_in_out<const N: usize>(hashes: [Digest; N]) {
            let expected_hash: Digest = hashes.into_iter().collect();
            dbg!(expected_hash.to_elements());

            let mut advice = AdviceInputs::default();
            add_hashes_to_advice(&mut advice, hashes);
            let advice = MemAdviceProvider::from(advice);

            let program = format!("begin repeat.{N} padw adv_loadw hmerge end end");
            let program = Assembler::default().compile(program).unwrap();

            let stack = StackInputs::default();

            let stack = miden_processor::execute(&program, stack, advice).unwrap();
            let hash = hash_from_stack_output(stack.stack_outputs());
            dbg!(hash.to_elements());

            assert_eq!(hash, expected_hash, "hashes: {hashes:?}");
        }


    #[test]
    fn hashes_to_from_miden() {
        test_in_out([]);
        test_in_out([1.hash()]);
        test_in_out([1.hash(), 2.hash()]);
        test_in_out([1.hash(), 2.hash(), 3.hash()]);
    }

    #[proptest]
    fn any_hashes_to_from_miden(hashes: [Digest; 100]) {
        test_in_out(hashes);
    }
}
