#![warn(clippy::pedantic)]
#![deny(missing_docs)]

//! Persistent Merkle tree
//!
//! This library provides `MerkleTree`, a Merkle tree that uses the [Rescue-Prime Optimized][rpo]
//! hash function, with a map-like API. There is also a [`Storage`] API for persisting the tree in
//! [rocksdb][db]
//!
//! ```rust
//! # use smirk::{MerkleTree, smirk};
//! let mut tree = MerkleTree::new();
//! tree.insert(1, "hello");
//! tree.insert(2, "world");
//!
//! // or you can use the macro to create a new tree
//! let tree = smirk! {
//!   1 => "hello",
//!   2 => "world",
//! };
//!
//! assert_eq!(tree.get(&1), Some(&"hello"));
//! assert_eq!(tree.get(&2), Some(&"world"));
//! assert_eq!(tree.get(&3), None);
//!
//! ```
//!
//! Types provided by this library implement [`Arbitrary`], for use with [`proptest`], gated behind
//! the `proptest` feature flag.
//!
//! [rpo]: https://eprint.iacr.org/2022/1577.pdf
//! [db]: https://github.com/facebook/rocksdb
//!
//! [`Storage`]: storage::Storage
//! [`Arbitrary`]: proptest::prelude::Arbitrary

pub mod hash;
pub mod storage;

mod tree;
pub use tree::{visitor::Visitor, MerkleTree, TreeNode};

#[cfg(test)]
mod testing;
