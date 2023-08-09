#![warn(clippy::unwrap_used, clippy::expect_used)]

// TODO: we should export schema from here, so that indexer builders
// are using the correct schema

pub mod adaptor;
pub mod auth_user;
pub mod cursor;
// pub mod index;
mod indexer;
pub mod list_query;
pub mod memory;
pub mod where_query;

pub use indexer::{Error, Indexer, IndexerChange, Result, UserError};
