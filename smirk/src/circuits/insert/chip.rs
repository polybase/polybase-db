use core::array;
use std::marker::PhantomData;

use halo2_gadgets::{
    poseidon::{
        primitives::{Mds, P128Pow5T3, Spec},
        Pow5Chip, Pow5Config,
    },
    utilities::decompose_running_sum::RunningSumConfig,
};
use halo2_proofs::{
    circuit::Chip,
    pasta::pallas,
    plonk::{Advice, Column, ConstraintSystem, Instance},
};

use crate::circuits::merkle_gadget::MerklePoseidonChip;

use super::{
    config::{InsertConfig, VerifyPathConfig},
    Base,
};

pub struct InsertChip<const N: usize> {
    config: InsertConfig<N>,
    _marker: PhantomData<Base>,
}

impl<const N: usize> Chip<Base> for InsertChip<N> {
    type Config = InsertConfig<N>;
    type Loaded = ();

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn loaded(&self) -> &Self::Loaded {
        &()
    }
}

impl<const N: usize> InsertChip<N> {
    pub fn construct(config: InsertConfig<N>) -> Self {
        Self {
            config,
            _marker: PhantomData,
        }
    }

    pub fn configure(
        meta: &mut ConstraintSystem<pallas::Base>,
        old_root_hash: Column<Instance>,
        new_root_hash: Column<Instance>,
        hash_to_insert: Column<Advice>,
    ) -> InsertConfig<N> {
        let old_path = single_path(meta);
        let new_path = single_path(meta);

        let new_hash = meta.advice_column();

        let q_range_check = meta.selector();

        let decompose = RunningSumConfig::configure(meta, q_range_check, hash_to_insert);

        InsertConfig {
            old_path,
            new_path,
            new_hash,
            old_root_hash,
            new_root_hash,
            decompose,
        }
    }
}

/// Configure a single path verification
fn single_path(meta: &mut ConstraintSystem<pallas::Base>) -> VerifyPathConfig {
    let advice = array::from_fn(|_| {
        let column = meta.advice_column();
        meta.enable_equality(column);
        column
    });

    let poseidon_config = poseidon_config(meta);

    let merkle = MerklePoseidonChip::configure(meta, advice, poseidon_config);
    let first_col = advice[0];

    VerifyPathConfig { merkle, first_col }
}

fn poseidon_config(meta: &mut ConstraintSystem<pallas::Base>) -> Pow5Config<pallas::Base, 3, 2> {
    let state = array::from_fn(|_| meta.advice_column());
    let partial_sbox = meta.advice_column();
    let rc_a = array::from_fn(|_| meta.fixed_column());
    let rc_b = array::from_fn(|_| meta.fixed_column());

    meta.enable_constant(rc_b[0]);

    Pow5Chip::configure::<PoseidonSettings>(meta, state, partial_sbox, rc_a, rc_b)
}

/// A struct that forwards its `Spec` impl to [`P128Pow5T3`] (NOT: [`P128Pow5T5`])
///
/// I'm not sure why it doesn't work if I just write [`P128Pow5T3`], but it gives unsatisfied trait
/// bound errors (and it's the same version of the crate, so no idea why it doesn't work - I tried
/// adding `static_assertions` and that passed 🤷)
///
/// [`P128Pow5T5`]: halo2_gadgets::poseidon::primitives::P128Pow5T5
#[derive(Debug)]
pub(super) struct PoseidonSettings;

impl Spec<pallas::Base, 3, 2> for PoseidonSettings {
    fn full_rounds() -> usize {
        <P128Pow5T3 as Spec<pallas::Base, 3, 2>>::full_rounds()
    }

    fn partial_rounds() -> usize {
        <P128Pow5T3 as Spec<pallas::Base, 3, 2>>::partial_rounds()
    }

    fn sbox(val: pallas::Base) -> pallas::Base {
        <P128Pow5T3 as Spec<pallas::Base, 3, 2>>::sbox(val)
    }

    fn secure_mds() -> usize {
        <P128Pow5T3 as Spec<pallas::Base, 3, 2>>::secure_mds()
    }

    fn constants() -> (
        Vec<[pallas::Base; 3]>,
        Mds<pallas::Base, 3>,
        Mds<pallas::Base, 3>,
    ) {
        <P128Pow5T3 as Spec<pallas::Base, 3, 2>>::constants()
    }
}
