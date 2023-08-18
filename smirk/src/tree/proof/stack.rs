use miden_crypto::hash::rpo::RpoDigest;
use miden_crypto::{Felt, FieldElement};
use miden_prover::{StackInputs, StackOutputs};

use crate::hash::Digest;
use crate::{
    batch::{Batch, Operation},
    hash::Hashable,
    key_value_hash,
};

/// Create a stack
///
/// For each operation, we push 5 field elements:
///  - 1 for discrimminant
///  - 4 for combined hash of key/value
pub(super) fn make_stack<K, V>(batch: &Batch<K, V>) -> StackInputs
where
    K: Hashable,
    V: Hashable,
{
    let mut stack = vec![];

    for op in batch.operations() {
        stack.push(discrimminant(op));

        match op {
            Operation::Insert(k, v) => {
                let digest = key_value_hash(k, v);
                let digest = RpoDigest::from(digest);
                stack.extend_from_slice(digest.as_elements());
            }
        }
    }

    StackInputs::new(stack)
}

fn discrimminant<K, V>(operation: &Operation<K, V>) -> Felt {
    match operation {
        Operation::Insert(_, _) => Felt::ONE,
    }
}

pub(super) fn hash_from_stack_output(output: &StackOutputs) -> Digest {
    let top = output.stack_top();
    let [a, b, c, d]: [_; 4] = top[0..4].try_into().unwrap();
    assert_eq!(top[4..16], [Felt::ZERO; 12]);
    Digest::from_elements([d, c, b, a])
}

#[cfg(test)]
mod tests {
    use miden_assembly::Assembler;
    use miden_prover::{MemAdviceProvider, ProofOptions};

    use super::*;

    #[test]
    fn hash_from_stack_output_works() {
        let hash = 123.hash();
        let input = RpoDigest::from(hash).as_elements().to_vec();
        assert_eq!(input.len(), 4);
        let (output, _proof) = miden_prover::prove(
            &Assembler::default().compile("begin\nend").unwrap(),
            StackInputs::new(input.clone()),
            MemAdviceProvider::default(),
            ProofOptions::default(),
        )
        .unwrap();

        let hash_again = hash_from_stack_output(&output);

        assert_eq!(hash, hash_again);
    }
}
