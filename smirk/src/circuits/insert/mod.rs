//! This module exposes a circuit that proves the following:
//!
//! Given a sparse merkle tree with root hash `old_root_hash`, after inserting the key `hash`, the
//! root hash becomes `new_root_hash`.
//! Additionally, prior to the insert, `hash` was not present in the tree.
//!
//! `old_root_hash`, `new_root_hash`, and `hash` are public inputs.
//! The full contents of the tree remains private.
//!
//! To generate the proof, we do the following:
//!  - we can prove the existence of a particular key at a particular location by generating a path
//!  to that node, and calculating the hashes and verifying that the final hash is equal to the
//!  root hast of the tree.
//!  - given this, we can prove that the key does *not* exist by generating a merkle path and
//!  verifying that it verifies with the null hash value
//!  - then, we can prove that

use crate::{Element, Tree};

type Base = halo2_proofs::pasta::pallas::Base;

mod chip;
pub(crate) mod circuit;
mod config;
// mod proof;

use rand_chacha::rand_core::OsRng;

// pub fn prove_insert<const N: usize>(tree: &Tree<N>, new_hash: Element) -> Proof {
//     proof::create(&mut OsRng, tree, new_hash)
// }
