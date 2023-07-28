#![warn(clippy::unwrap_used, clippy::expect_used)]

use crate::collection::{self, collection::Collection, record::RecordRoot};
use crate::store::{self, Store};

pub type Result<T> = std::result::Result<T, IndexerError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("collection error")]
    Collection(#[from] collection::CollectionError),

    #[error("record error")]
    Record(#[from] collection::record::RecordError),

    #[error("store error")]
    Store(#[from] store::Error),
}

pub struct Indexer<S: Store> {
    store: S,
}

impl<S: Store> Indexer<S> {
    pub fn new(db: S) -> Result<Self> {
        Ok(Self { store: db })
    }

    #[tracing::instrument(skip(self))]
    pub async fn check_for_migration(&self, migration_batch_size: usize) -> Result<()> {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    pub async fn destroy(&self) -> Result<()> {
        Ok(self.store.destroy().await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn collection(&self, id: &str) -> Result<Collection<S>> {
        Ok(Collection::load(&self.store, id).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn commit(&self) -> Result<()> {
        Ok(self.store.commit().await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn set_system_key(&self, key: &str, data: &RecordRoot) -> Result<()> {
        Ok(self.store.set_system_key(&key, data).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_system_key(&self, key: &str) -> Result<Option<RecordRoot>> {
        Ok(self.store.get_system_key(&key).await?)
    }
}
