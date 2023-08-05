use crate::where_query::WhereQuery;
use schema::record::RecordRoot;
use std::{pin::Pin, time::SystemTime};

pub type Result<T> = std::result::Result<T, Error>;

pub use schema::index::{Index, IndexDirection, IndexField};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // Custom errors for the store
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Collection collection record not found for collection {id:?}")]
    CollectionCollectionRecordNotFound { id: String },
}

/// The Store trait
#[async_trait::async_trait]
pub trait Indexer: Send + Sync + Clone {
    async fn commit(&self) -> Result<()>;

    async fn set(&self, collection_id: &str, record_id: &str, value: &RecordRoot) -> Result<()>;

    async fn get(&self, collection_id: &str, record_id: &str) -> Result<Option<RecordRoot>>;

    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField],
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>>;

    async fn delete(&self, collection_id: &str, record_id: &str) -> Result<()>;

    async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<SystemTime>>;

    async fn last_collection_update(&self, collection_id: &str) -> Result<Option<SystemTime>>;

    async fn set_system_key(&self, key: &str, data: &RecordRoot) -> Result<()>;

    async fn get_system_key(&self, key: &str) -> Result<Option<RecordRoot>>;

    async fn destroy(&self) -> Result<()>;
}
