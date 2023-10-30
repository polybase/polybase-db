use halo2_gadgets::utilities::decompose_running_sum::RunningSumConfig;
use halo2_proofs::{
    pasta::pallas,
    plonk::{Advice, Column, Instance},
};

use crate::circuits::merkle_gadget::MerklePoseidonConfig;

#[derive(Debug, Clone)]
pub struct InsertConfig<const N: usize> {
    /// The config for verifying that the old path exists (with null hash)
    pub old_path: VerifyPathConfig,

    /// The config for verifying that the new path exists (with the real hash)
    pub new_path: VerifyPathConfig,

    /// The hash being inserted into the tree
    pub new_hash: Column<Advice>,

    pub old_root_hash: Column<Instance>,
    pub new_root_hash: Column<Instance>,
 
    pub decompose: RunningSumConfig<pallas::Base, 1>,
}

#[derive(Debug, Clone)]
pub struct VerifyPathConfig {
    /// We need to expose the first column so that we can assign a value to it
    pub first_col: Column<Advice>,
    /// The config for actually performing the verification
    pub merkle: MerklePoseidonConfig,
}
