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
pub use tree::{batch, key_value_hash, visitor::Visitor, MerkleTree, TreeNode};

#[cfg(test)]
mod testing;
