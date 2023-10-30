use std::iter::zip;

use bitvec::{prelude::BitArray, slice::BitSlice};
use halo2_gadgets::{poseidon::primitives::Hash, utilities::decompose_running_sum::RunningSum};
use halo2_proofs::{arithmetic::Field, circuit::AssignedCell, pasta::group::ff::PrimeFieldBits};

use crate::circuits::insert::{chip::PoseidonSettings, Base};

/// A merkle path
///
/// N is the depth of the tree that generated this path (meaning there are n - 1 siblings)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MerklePath<const N: usize> {
    /// The siblings that form the merkle path
    pub siblings: Vec<Base>,
}

impl<const N: usize> Default for MerklePath<N> {
    fn default() -> Self {
        Self {
            siblings: vec![Base::ZERO; N - 1],
        }
    }
}

impl<const N: usize> MerklePath<N> {
    /// Compute the root hash of a tree with the given hash at this path
    pub fn compute_root(&self, mut hash: Base) -> Base {
        let bits = hash.to_le_bits();
        let bits = Self::last_n_bits(&bits);

        for (is_right, &sibling) in zip(bits, &self.siblings) {
            match *is_right {
                true => hash = hmerge(sibling, hash),
                false => hash = hmerge(hash, sibling),
            }
        }

        hash
    }

    fn last_n_bits(bits: &BitArray<[u64; 4]>) -> &BitSlice<u64> {
        let start_idx = bits.len() - N;
        let slice = &bits[start_idx..];
        assert_eq!(slice.len(), N);
        slice
    }

    pub(crate) fn make_pairs(
        &self,
        bits: RunningSum<Base>,
    ) -> Vec<(Base, AssignedCell<Base, Base>)> {
        core::iter::zip(&self.siblings, bits.iter())
            .map(|(&sibling, cell)| (sibling, cell.clone()))
            .collect()
    }
}

pub(crate) fn hmerge(a: Base, b: Base) -> Base {
    Hash::<_, PoseidonSettings, _, 3, 2>::init().hash([a, b])
}

#[cfg(test)]
mod tests {
    use bitvec::prelude::*;

    use super::*;

    #[test]
    fn last_n_bits_test() {
        let hash_last_bits = 1_234_567u64;
        let expected_bits = hash_last_bits
            .view_bits::<Lsb0>()
            .iter()
            .map(|b| *b)
            .collect::<Vec<_>>();

        let hash = Base::from_raw([1234, 2345, 3456, hash_last_bits]);

        let bits = hash.to_le_bits();
        let bits = MerklePath::<64>::last_n_bits(&bits)
            .iter()
            .map(|b| *b)
            .collect::<Vec<_>>();

        assert_eq!(bits, expected_bits);
    }

    #[test]
    fn simple_root() {
        let siblings = (0..5).map(Base::from).collect::<Vec<_>>();
        let path = MerklePath::<6> {
            siblings: siblings.clone(),
        };

        let root = path.compute_root(Base::from(0));

        // because 0 is the lowest (left-most) possible value, every merge is this way round
        let expected_root = siblings.into_iter().fold(Base::from(0), hmerge);

        assert_eq!(root, expected_root);
    }
}
