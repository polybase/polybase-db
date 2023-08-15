use crate::{indexer::IndexerChange, where_query::WhereQuery};
use schema::{self, record::RecordRoot, Schema};
use std::{pin::Pin, time::SystemTime};

pub type Result<T> = std::result::Result<T, Error>;

pub use schema::{
    index::{Index, IndexDirection, IndexField},
    publickey::PublicKey,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("schema error: {0}")]
    Schema(#[from] schema::Error),

    #[error("Collection collection record not found for collection {id:?}")]
    CollectionCollectionRecordNotFound { id: String },
}

/// The Store trait
#[async_trait::async_trait]
pub trait IndexerAdaptor: Send + Sync {
    // TODO: add a height in here, so we can track where we are up to
    async fn commit(&self, height: usize, changes: Vec<IndexerChange>) -> Result<()>;

    async fn get(&self, collection_id: &str, record_id: &str) -> Result<Option<RecordRoot>>;

    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField],
        reverse: bool,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>>;

    async fn get_schema(&self, collection_id: &str) -> Result<Option<Schema>>;

    async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<SystemTime>>;

    async fn last_collection_update(&self, collection_id: &str) -> Result<Option<SystemTime>>;

    async fn set_system_key(&self, key: &str, data: &RecordRoot) -> Result<()>;

    async fn get_system_key(&self, key: &str) -> Result<Option<RecordRoot>>;
}
