#![warn(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

pub mod collection;
mod index;
pub mod keys;
mod migrate;
mod proto;
pub mod publickey;
mod record;
mod rocksdb;
pub mod snapshot;
mod stableast_ext;
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

use indexer_db_adaptor::db;

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

    #[error("migration error")]
    Migration(#[from] migrate::Error),

    #[error("database error")]
    Database(#[from] db::DatabaseError),
}

/// The new Polybase Indexer
/// TODO - move this to indexer_db_adaptor
pub struct Indexer<D>
where
    D: db::Database,
{
    db: D,
}

impl<D> Indexer<D>
where
    D: db::Database,
{
    pub fn new(db: D) -> Result<Self> {
        Ok(Self { db })
    }

    #[tracing::instrument(skip(self))]
    pub fn destroy(self) -> Result<()> {
        Ok(self.db.destroy()?)
    }

    #[tracing::instrument(skip(self))]
    pub fn reset(&self) -> Result<()> {
        Ok(self.db.reset()?)
    }

    // #[tracing::instrument(skip(self))]
    // pub fn snapshot(&self, chunk_size: usize) -> snapshot::SnapshotIterator {
    //     self.db.snapshot(chunk_size)
    // }

    // #[tracing::instrument(skip(self))]
    // pub fn restore(&self, data: SnapshotChunk) -> Result<()> {
    //     Ok(self.db.restore(data.into())?)
    // }

    #[tracing::instrument(skip(self))]
    pub async fn collection(&self, id: String) -> Result<Collection> {
        Ok(Collection::load(&self.db, id).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn commit(&self) -> Result<()> {
        Ok(self.db.commit().await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn set_system_key(&self, key: String, data: &RecordRoot) -> Result<()> {
        todo!();
        //let system_key = keys::Key::new_system_data(key)?;

        //Ok(self
        //    .db
        //    .set(&system_key, &db::Value::DataValue(data))
        //    .await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_system_key(&self, key: String) -> Result<Option<RecordRoot>> {
        todo!();
        //let system_key = keys::Key::new_system_data(key)?;
        //Ok(self.db.get(&system_key).await?)
    }
}

//pub struct Indexer {
//    store: store::Store,
//}
//
//impl Indexer {
//    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
//        let store = store::Store::open(path)?;
//        Ok(Self { store })
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub async fn check_for_migration(&self, migration_batch_size: usize) -> Result<()> {
//        let store = &self.store;
//        Ok(migrate::check_for_migration(store, migration_batch_size).await?)
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub fn destroy(self) -> Result<()> {
//        Ok(self.store.destroy()?)
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub fn reset(&self) -> Result<()> {
//        Ok(self.store.reset()?)
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub fn snapshot(&self, chunk_size: usize) -> snapshot::SnapshotIterator {
//        self.store.snapshot(chunk_size)
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub fn restore(&self, data: SnapshotChunk) -> Result<()> {
//        Ok(self.store.restore(data)?)
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub async fn collection(&self, id: String) -> Result<Collection> {
//        Ok(Collection::load(&self.store, id).await?)
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub async fn commit(&self) -> Result<()> {
//        Ok(self.store.commit().await?)
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub async fn set_system_key(&self, key: String, data: &RecordRoot) -> Result<()> {
//        let system_key = keys::Key::new_system_data(key)?;
//
//        Ok(self
//            .store
//            .set(&system_key, &store::Value::DataValue(data))
//            .await?)
//    }
//
//    #[tracing::instrument(skip(self))]
//    pub async fn get_system_key(&self, key: String) -> Result<Option<RecordRoot>> {
//        let system_key = keys::Key::new_system_data(key)?;
//        Ok(self.store.get(&system_key).await?)
//    }
//}
