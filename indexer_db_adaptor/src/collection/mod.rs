pub use error::{CollectionError, CollectionUserError};

pub mod ast;
mod authorization;
#[allow(clippy::module_inception)]
pub mod collection;
mod collection_record;
pub mod cursor;
mod error;
pub mod field_path;
pub mod index;
pub mod record;
pub mod stableast_ext;
mod util;
pub mod validation;
pub mod where_query;
