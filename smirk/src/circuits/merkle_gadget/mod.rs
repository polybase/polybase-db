use crate::circuits::cond_swap::CondSwapInstructions;
use halo2_gadgets::poseidon::Pow5Config as PoseidonConfig;
use halo2_proofs::{
    circuit::{AssignedCell, Chip, Layouter, Value},
    pasta::pallas,
    plonk::{Advice, Column, ConstraintSystem, Error},
};

use super::cond_swap::{CondSwapChip, CondSwapConfig};

mod poseidon_hash;

/// Merkle Tree Chip based on poseidon hash.
#[derive(Clone, Debug)]
pub struct MerklePoseidonConfig {
    advices: [Column<Advice>; 5],
    cond_swap_config: CondSwapConfig,
    poseidon_config: PoseidonConfig<pallas::Base, 3, 2>,
}

#[derive(Clone, Debug)]
pub struct MerklePoseidonChip {
    config: MerklePoseidonConfig,
}

impl Chip<pallas::Base> for MerklePoseidonChip {
    type Config = MerklePoseidonConfig;
    type Loaded = ();

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn loaded(&self) -> &Self::Loaded {
        &()
    }
}

impl MerklePoseidonChip {
    pub fn configure(
        meta: &mut ConstraintSystem<pallas::Base>,
        advices: [Column<Advice>; 5],
        poseidon_config: PoseidonConfig<pallas::Base, 3, 2>,
    ) -> MerklePoseidonConfig {
        let cond_swap_config = CondSwapChip::configure(meta, advices);

        MerklePoseidonConfig {
            advices,
            cond_swap_config,
            poseidon_config,
        }
    }

    pub fn construct(config: MerklePoseidonConfig) -> Self {
        MerklePoseidonChip { config }
    }
}

#[allow(clippy::type_complexity)]
fn swap(
    merkle_chip: &MerklePoseidonChip,
    layouter: impl Layouter<pallas::Base>,
    pair: (
        AssignedCell<pallas::Base, pallas::Base>,
        Value<pallas::Base>,
    ),
    swap: AssignedCell<pallas::Base, pallas::Base>,
) -> Result<
    (
        AssignedCell<pallas::Base, pallas::Base>,
        AssignedCell<pallas::Base, pallas::Base>,
    ),
    Error,
> {
    let config = merkle_chip.config().cond_swap_config.clone();
    let chip = CondSwapChip::<pallas::Base>::construct(config);
    chip.swap(layouter, pair, swap)
}

#[allow(clippy::type_complexity)]
pub fn merkle_poseidon_gadget(
    mut layouter: impl Layouter<pallas::Base>,
    chip: MerklePoseidonChip,
    note_x: AssignedCell<pallas::Base, pallas::Base>,
    merkle_path: impl IntoIterator<Item = (pallas::Base, AssignedCell<pallas::Base, pallas::Base>)>,
) -> Result<AssignedCell<pallas::Base, pallas::Base>, Error> {
    let mut cur = note_x;
    for (sibling, bit) in merkle_path {
        let pair = {
            let pair = (cur, Value::known(sibling)); // TODO: maybe swap these?
            swap(&chip, layouter.namespace(|| "merkle swap"), pair, bit)?
        };

        cur = poseidon_hash::poseidon_hash_gadget(
            chip.config().poseidon_config.clone(),
            layouter.namespace(|| "merkle poseidon hash"),
            [pair.0, pair.1],
        )?;
    }

    Ok(cur)
}
