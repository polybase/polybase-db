use halo2_proofs::{
    arithmetic::Field,
    circuit::{Layouter, SimpleFloorPlanner, Value},
    plonk::{Circuit, ConstraintSystem},
};

use crate::{
    circuits::{
        merkle_gadget::{merkle_poseidon_gadget, MerklePoseidonChip},
        utils::assign_free_advice,
    },
    Element,
};

use super::{chip::InsertChip, config::InsertConfig, Base};

mod path;
pub use path::MerklePath;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InsertCircuit<const N: usize> {
    /// Public root hash of the tree before inserting the hash
    old_root_hash: Base,
    /// Public root hash of the tree after inserting the hash
    new_root_hash: Base,
    /// Private hash to insert
    new_hash: Base,
    /// Private merkle path to the position in the tree that this hash should be inserted at
    path: MerklePath<N>,
}

impl<const N: usize> InsertCircuit<N> {
    pub fn new(new_hash: Base, path: MerklePath<N>) -> Self {
        let old_root_hash = path.compute_root(Element::NULL_HASH.0);
        let new_root_hash = path.compute_root(new_hash);

        Self {
            old_root_hash,
            new_root_hash,
            new_hash,
            path,
        }
    }
}

impl<const N: usize> Circuit<Base> for InsertCircuit<N> {
    type Config = InsertConfig<N>;
    type FloorPlanner = SimpleFloorPlanner;

    fn without_witnesses(&self) -> Self {
        Self {
            old_root_hash: self.old_root_hash,
            new_root_hash: self.new_root_hash,
            new_hash: Base::default(),
            path: MerklePath::default(),
        }
    }

    fn configure(meta: &mut ConstraintSystem<Base>) -> Self::Config {
        let old_root_hash = meta.instance_column();
        let new_root_hash = meta.instance_column();

        meta.enable_equality(old_root_hash);
        meta.enable_equality(new_root_hash);

        let hash_to_insert = meta.advice_column();

        InsertChip::<N>::configure(meta, old_root_hash, new_root_hash, hash_to_insert)
    }

    fn synthesize(
        &self,
        config: Self::Config,
        mut layouter: impl Layouter<Base>,
    ) -> Result<(), halo2_proofs::plonk::Error> {
        // perform the binary decomposition to get the bit path
        let running_sum = layouter.assign_region(
            || "decompose",
            |mut region| {
                config.decompose.witness_decompose(
                    &mut region,
                    0,
                    Value::known(self.new_hash),
                    false,
                    1,
                    N,
                )
            },
        )?;

        assert_eq!(running_sum.len(), N);

        let path = self.path.make_pairs(running_sum);

        // Verify the old hash

        let old_hash = MerklePoseidonChip::construct(config.old_path.merkle);
        let leaf = assign_free_advice(
            layouter.namespace(|| "witness leaf"),
            config.old_path.first_col,
            Value::known(Base::ZERO),
        )?;

        let root = merkle_poseidon_gadget(
            layouter.namespace(|| "verify old hash"),
            old_hash,
            leaf,
            path.clone(),
        )?;

        let expected_root = {
            let root = self.path.compute_root(Base::ZERO);
            assign_free_advice(
                layouter.namespace(|| "witness leaf"),
                config.old_path.first_col,
                Value::known(root),
            )?
        };

        layouter.assign_region(
            || "constrain old result",
            |mut region| region.constrain_equal(root.cell(), expected_root.cell()),
        )?;

        // Verify new hash
        let new_hash = MerklePoseidonChip::construct(config.new_path.merkle);
        let leaf = assign_free_advice(
            layouter.namespace(|| "witness leaf"),
            config.new_path.first_col,
            Value::known(self.new_hash),
        )?;

        let root = merkle_poseidon_gadget(
            layouter.namespace(|| "verify new hash"),
            new_hash,
            leaf,
            path,
        )?;

        let expected_root = {
            let root = self.path.compute_root(self.new_hash);
            assign_free_advice(
                layouter.namespace(|| "witness leaf"),
                config.new_path.first_col,
                Value::known(root),
            )?
        };

        layouter.assign_region(
            || "constrain new result",
            |mut region| region.constrain_equal(root.cell(), expected_root.cell()),
        )?;

        Ok(())
    }
}
