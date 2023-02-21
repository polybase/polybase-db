use std::path::Path;

pub mod collection;
mod index;
pub mod keys;
mod proto;
pub mod publickey;
mod record;
mod stableast_ext;
mod store;
pub mod where_query;

pub use collection::{validate_schema_change, AuthUser, Collection, Cursor, ListQuery};
pub use index::CollectionIndexField;
pub use keys::Direction;
pub use publickey::PublicKey;
pub use record::{
    json_to_record, record_to_json, Converter, ForeignRecordReference, IndexValue, PathFinder,
    RecordError, RecordRoot, RecordValue,
};
pub use stableast_ext::FieldWalker;
pub use where_query::WhereQuery;

#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("collection error")]
    Collection(#[from] collection::CollectionError),

    #[error("store error")]
    Store(#[from] store::StoreError),

    #[error("index error")]
    Index(#[from] index::IndexError),

    #[error("keys error")]
    Keys(#[from] keys::KeysError),

    #[error(transparent)]
    PublicKey(#[from] publickey::PublicKeyError),

    #[error("record error")]
    Record(#[from] record::RecordError),

    #[error("where query error")]
    WhereQuery(#[from] where_query::WhereQueryError),
}

pub struct Indexer {
    logger: slog::Logger,
    store: store::Store,
}

impl Indexer {
    pub fn new(logger: slog::Logger, path: impl AsRef<Path>) -> store::Result<Self> {
        let store = store::Store::open(path)?;
        Ok(Self { logger, store })
    }

    pub fn destroy(self) -> store::Result<()> {
        self.store.destroy()
    }

    pub async fn collection(&self, id: String) -> collection::Result<Collection> {
        Collection::load(self.logger.clone(), &self.store, id).await
    }
}
