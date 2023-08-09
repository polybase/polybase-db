#![warn(clippy::unwrap_used, clippy::expect_used)]

pub mod ast;
mod collection_schema;
pub mod directive;
mod error;
pub mod field_path;
pub mod index;
pub mod index_value;
pub mod methods;
pub mod property;
pub mod publickey;
pub mod record;
mod schema;
pub mod types;
pub mod util;

pub use collection_schema::{COLLECTION_RECORD, COLLECTION_SCHEMA};
pub use error::{Error, Result, UserError};
pub use schema::Schema;
