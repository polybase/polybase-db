use std::error::Error;
use std::path::Path;

mod collection;
mod index;
mod keys;
mod proto;
mod publickey;
mod stableast_ext;
mod store;
mod where_query;

pub use collection::{AuthUser, Collection, Cursor, ListQuery};
pub use index::CollectionIndexField;
pub use keys::{Direction, IndexValue, PathFinder, RecordReference, RecordValue};
pub use publickey::PublicKey;
pub use stableast_ext::FieldWalker;
pub use store::StoreRecordValue;
pub use where_query::WhereQuery;

pub struct Indexer {
    store: store::Store,
}

impl Indexer {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let store = store::Store::open(path)?;
        Ok(Self { store })
    }

    pub fn destroy(self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        self.store.destroy()
    }

    pub fn collection(
        &self,
        id: String,
    ) -> Result<Collection, Box<dyn Error + Send + Sync + 'static>> {
        Collection::load(&self.store, id)
    }
}
