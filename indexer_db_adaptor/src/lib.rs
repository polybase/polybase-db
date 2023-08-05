#![warn(clippy::unwrap_used, clippy::expect_used)]
pub use error::{CollectionError, CollectionUserError};

// TODO: we should export schema from here, so that indexer builders
// are using the correct schema

pub mod ast;
pub mod auth_user;
mod collection_collection;
pub mod cursor;
mod error;
mod indexer;
pub mod list_query;
pub mod memory;
pub mod validation;
pub mod where_query;

pub use indexer::{Error, Indexer, Result};
