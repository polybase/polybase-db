#![warn(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

pub mod collection;
mod index;
pub mod keys;
mod migrate;
mod proto;
mod record;
pub mod snapshot;
mod stableast_ext;
mod store;
pub mod where_query;

pub use index::CollectionIndexField;
pub use keys::Direction;

use snapshot::SnapshotChunk;
pub use stableast_ext::FieldWalker;
pub use where_query::WhereQuery;

use collection::{RocksDBCollection, RocksDBCollectionError};
use indexer_db_adaptor::{
    collection::Collection, db::Database, indexer::Indexer, record::RecordRoot,
};

#[derive(Debug, thiserror::Error)]
pub enum RocksDBIndexerError {
    #[error("collection error")]
    Collection(#[from] RocksDBCollectionError),

    #[error("rocksdb store error")]
    RocksDBStore(#[from] store::RocksDBStoreError),

    #[error("index error")]
    Index(#[from] index::IndexError),

    #[error("keys error")]
    Keys(#[from] keys::KeysError),

    #[error(transparent)]
    PublicKey(#[from] indexer_db_adaptor::publickey::PublicKeyError),

    #[error("record error")]
    Record(#[from] indexer_db_adaptor::record::RecordError),

    #[error("where query error")]
    WhereQuery(#[from] where_query::WhereQueryError),

    #[error("migration error")]
    Migration(#[from] migrate::Error),
}

pub type Result<T> = std::result::Result<T, RocksDBIndexerError>;

/// The concrete RocksDBIndexer
pub struct RocksDBIndexer {
    store: store::RocksDBStore,
}

impl RocksDBIndexer {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let store = store::RocksDBStore::open(path)?;
        Ok(Self { store })
    }

    #[tracing::instrument(skip(self))]
    pub fn snapshot(&self, chunk_size: usize) -> snapshot::SnapshotIterator {
        self.store.snapshot(chunk_size)
    }

    #[tracing::instrument(skip(self))]
    pub fn restore(&self, data: SnapshotChunk) -> Result<()> {
        Ok(self.store.restore(data)?)
    }
}

#[async_trait::async_trait]
impl Indexer for RocksDBIndexer {
    type Error = RocksDBIndexerError;
    type Key<'k> = keys::Key<'k>;
    type Value<'v> = store::Value<'v>;
    type ListQuery<'l> = collection::ListQuery<'l>;
    type Cursor = collection::Cursor;
    type CollError = RocksDBCollectionError;

    #[tracing::instrument(skip(self))]
    async fn check_for_migration(&self, migration_batch_size: usize) -> Result<()> {
        let store = &self.store;
        Ok(migrate::check_for_migration(store, migration_batch_size).await?)
    }

    #[tracing::instrument(skip(self))]
    fn destroy(self) -> Result<()> {
        Ok(self.store.destroy()?)
    }

    #[tracing::instrument(skip(self))]
    fn reset(&self) -> Result<()> {
        Ok(self.store.reset()?)
    }

    #[tracing::instrument(skip(self))]
    async fn collection<'k, 'v>(
        &self,
        id: String,
    ) -> Result<
        Box<
            dyn Collection<
                    Error = Self::CollError,
                    Key = crate::keys::Key,
                    Value = store::Value,
                    ListQuery = Self::ListQuery<'_>,
                    Cursor = Self::Cursor,
                > + '_,
        >,
    > {
        Ok(Box::new(RocksDBCollection::load(&self.store, id).await?))
    }

    #[tracing::instrument(skip(self))]
    async fn commit(&self) -> Result<()> {
        Ok(self.store.commit().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn set_system_key(&self, key: String, data: &RecordRoot) -> Result<()> {
        let system_key = keys::Key::new_system_data(key)?;

        Ok(self
            .store
            .set(&system_key, &store::Value::DataValue(data))
            .await?)
    }

    #[tracing::instrument(skip(self))]
    async fn get_system_key(&self, key: String) -> Result<Option<RecordRoot>> {
        let system_key = keys::Key::new_system_data(key)?;
        Ok(self.store.get(&system_key).await?)
    }
}
