#![doc = include_str!("../README.md")]
#![warn(clippy::pedantic)]
#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(clippy::integer_arithmetic)] // explicitly choose wrapping/saturating/checked
#![allow(
    clippy::module_name_repetitions,
    clippy::match_bool,  // overly restrictive style lint
    clippy::bool_assert_comparison,  // overly restrictive style lint
    clippy::derive_partial_eq_without_eq,  // semver hazard
    clippy::missing_panics_doc,  // implementation of lint is buggy
    clippy::missing_errors_doc,  // error is usually obvious from context, this forces useless docs
)]

pub mod hash;
pub mod storage;

mod tree;
use std::time::Instant;

pub use tree::{batch, key_value_hash, visitor::Visitor, MerkleTree, TreeNode};

#[cfg(test)]
mod testing;

#[test]
fn foo() {
    use rand::{Rng, SeedableRng};

    let mut rng = rand_chacha::ChaChaRng::from_seed([0; 32]);
    let mut nums = vec![0; 1000];
    rng.fill(nums.as_mut_slice());

    let instant = Instant::now();
    let mut tree = smirk! {};
    for i in nums {
        tree.insert(i, i);
    }

    println!("{}", instant.elapsed().as_millis());
}
