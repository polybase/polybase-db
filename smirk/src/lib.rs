#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::match_bool)]
#![deny(missing_docs)]
#![allow(clippy::doc_markdown)]
//! # Smirk (Sparse MeRKle tree)
//!
//!

mod circuits;
mod hash;
mod proof;
mod tree;

pub use proof::{Proof, Provable, Insert};
pub use tree::element::{Element, Lsb};
pub use tree::Tree;

pub use hash::{empty_tree_hash, hash_merge};

pub(crate) type Base = halo2_proofs::pasta::pallas::Base;
