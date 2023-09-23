use miden_crypto::{Felt, FieldElement};
use miden_processor::StackInputs;

/// Make the stack inputs for proving the hash of a batch
pub(super) fn make_stack(op_count: u64) -> StackInputs {
    let len = Felt::new(op_count);
    StackInputs::new(vec![Felt::ZERO, Felt::ZERO, Felt::ZERO, len])
}
