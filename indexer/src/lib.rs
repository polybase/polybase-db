use std::error::Error;
use std::path::Path;

mod collection;
mod index;
mod keys;
mod proto;
mod stableast_ext;
mod store;
mod where_query;

pub use collection::Collection;
pub use keys::IndexValue;
pub use keys::RecordValue;
pub use store::StoreRecordValue;

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
