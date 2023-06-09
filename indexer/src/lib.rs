#![warn(clippy::unwrap_used, clippy::expect_used)]

use std::{path::Path, sync::Arc};

pub mod collection;
mod index;
mod job_engine;
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
use job_engine::JobEngine;
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

    #[error("job engine error")]
    JobEngineError(#[from] job_engine::JobEngineError),
}

pub struct Indexer {
    logger: slog::Logger,
    store: store::Store,
}

impl Indexer {
    pub fn new(logger: slog::Logger, path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let store = store::Store::open(path)?;
        let indexer = Arc::new(Self { logger, store });

        let shared_indexer = indexer.clone();
        tokio::spawn(Indexer::some_concurrent_task(shared_indexer));

        Ok(indexer)
    }

    async fn some_concurrent_task(indexer: Arc<Indexer>) {
        loop {
            println!("Hello!");
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
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
        Ok(Collection::load(self.logger.clone(), &self, &self.store, id).await?)
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
}
