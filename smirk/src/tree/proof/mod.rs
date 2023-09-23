use std::sync::OnceLock;

use miden_assembly::Assembler;
use miden_prover::{MemAdviceProvider, Program, ProofOptions, StackOutputs};
use serde::{Deserialize, Serialize};

use crate::{
    batch::Batch,
    hash::{Digest, Hashable},
    tree::proof::{advice::advice, stack::make_stack},
    MerkleTree,
};

pub mod batch_hash;

mod advice;
mod stack;
mod utils;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod miden_behaviour_tests;
mod rust_impl;

type MidenProof = Vec<u8>;

/// A ZK proof that a particular update to the root hash was valid
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UpdateProof {
    old_hash: Digest,
    new_hash: Digest,
    proof_bytes: MidenProof,
}

impl UpdateProof {
    /// Verify that this proof is valid - returns `true` if and only if the proof is valid
    ///
    /// Internally, an [`UpdateProof`] contains the root hashes before and after the operation was
    /// applied, as well as a ZK proof that the state transition was correct, so this struct can
    /// self-validate
    #[must_use = "`verify()` has no side effects - failures are indicated by returning `false`"]
    pub fn verify(&self) -> bool {
        todo!()
    }

    /// The hash of the [`MerkleTree`] after applying the update
    #[inline]
    #[must_use]
    pub fn new_hash(&self) -> Digest {
        self.new_hash
    }

    /// The hash of the [`MerkleTree`] prior to applying the update
    #[inline]
    #[must_use]
    pub fn old_hash(&self) -> Digest {
        self.old_hash
    }
}

impl<K, V> MerkleTree<K, V>
where
    K: Hashable + Ord,
    V: Hashable,
{
    /// Apply the given [`Batch`], returning a proof that the update to the root hash is valid
    /// ```rust
    ///
    /// ```
    ///
    /// The given proof can be verified with [`MerkleTree::verify_proof`]
    ///
    /// The behaviour of this function on `self` is identical to [`MerkleTree::apply`], but it:
    ///  - generates a proof
    ///  - is much slower
    pub fn apply_and_prove(&mut self, batch: Batch<K, V>) -> UpdateProof {
        let old_hash = self.root_hash();
        let (computed_new_hash, proof_bytes) = compute_new_hash_and_prove(self, &batch);
        self.apply(batch);

        let new_hash = self.root_hash();

        assert_eq!(new_hash, computed_new_hash); // TODO: we should think about what we do here

        UpdateProof {
            old_hash,
            new_hash,
            proof_bytes,
        }
    }
}

static PROGRAM: OnceLock<Program> = OnceLock::new();

fn get_program() -> &'static Program {
    PROGRAM.get_or_init(|| {
        Assembler::default()
            .compile(include_str!("./asm/compute_hash.masm"))
            .unwrap()
    })
}

fn compute_new_hash_and_prove<K, V>(
    tree: &MerkleTree<K, V>,
    batch: &Batch<K, V>,
) -> (Digest, MidenProof)
where
    K: Hashable + Ord,
    V: Hashable,
{
    let program = get_program();
    let advice = MemAdviceProvider::from(advice(tree, batch));
    let stack = make_stack(batch);
    let options = ProofOptions::default();

    let (stack_outputs, proof) = miden_prover::prove(program, stack, advice, options).unwrap();

    assert_eq!(stack_outputs.stack().len(), 4);
    let digest = stack::hash_from_stack_output(&stack_outputs);

    todo!()
}
