#![warn(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

pub mod collection;
mod index;
pub mod keys;
mod migrate;
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
    pub async fn collection(&self, id: String) -> Result<Collection<D>> {
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
