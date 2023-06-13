#![warn(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

pub mod collection;
mod index;
pub mod keys;
mod proto;
pub mod publickey;
mod record;
pub mod snapshot;
mod stableast_ext;
mod store;
pub mod where_query;

pub use collection::{validate_schema_change, AuthUser, Collection, Cursor, ListQuery};
pub use index::CollectionIndexField;
pub use keys::Direction;
pub use publickey::PublicKey;
pub use record::{
    json_to_record, record_to_json, Converter, ForeignRecordReference, IndexValue, PathFinder,
    RecordError, RecordRoot, RecordUserError, RecordValue,
};
use snapshot::SnapshotChunk;
pub use stableast_ext::FieldWalker;
pub use where_query::WhereQuery;

pub type Result<T> = std::result::Result<T, IndexerError>;

// TODO: errors should be named Error and imported as indexer::Error
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
    pub fn new(logger: slog::Logger, path: impl AsRef<Path>) -> Result<Self> {
        let store = store::Store::open(path)?;
        Ok(Self { logger, store })
    }

    pub fn destroy(self) -> Result<()> {
        Ok(self.store.destroy()?)
    }

    pub fn reset(&self) -> Result<()> {
        Ok(self.store.reset()?)
    }

    pub fn snapshot(&self, chunk_size: usize) -> snapshot::SnapshotIterator {
        self.store.snapshot(chunk_size)
    }

    pub fn restore(&self, data: SnapshotChunk) -> Result<()> {
        Ok(self.store.restore(data)?)
    }

    pub async fn collection(&self, id: String) -> Result<Collection> {
        Ok(Collection::load(self.logger.clone(), &self.store, id).await?)
    }

    pub async fn commit(&self) -> Result<()> {
        Ok(self.store.commit().await?)
    }

    pub async fn set_system_key(&self, key: String, data: &RecordRoot) -> Result<()> {
        let system_key = keys::Key::new_system_data(key)?;

        Ok(self
            .store
            .set(&system_key, &store::Value::DataValue(data))
            .await?)
    }

    pub async fn get_system_key(&self, key: String) -> Result<Option<RecordRoot>> {
        let system_key = keys::Key::new_system_data(key)?;
        Ok(self.store.get(&system_key).await?)
    }

    pub fn proof_for(&self, namespace: String, id: String) -> Result<Option<Vec<u8>>> {
        let key = keys::Key::new_data(namespace, id)?;
        let proof = self.store.proof_for(&key)?;
        Ok(proof)
    }

    pub fn root_hash(&self) -> [u8; 32] {
        self.store.root_hash()
    }
}
