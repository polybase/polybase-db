use crate::collection::{
    index::{Index, IndexField},
    record::RecordRoot,
    where_query::WhereQuery,
};
use std::pin::Pin;
use std::time::SystemTime;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub struct Error(#[source] pub Box<dyn std::error::Error>);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The Store trait
#[async_trait::async_trait]
pub trait Store: Send + Sync + Clone {
    // type Error: std::error::Error + Send + Sync + 'static;
    type Config;

    async fn commit(&self) -> Result<()>;

    async fn set(&self, collection_id: &str, record_id: &str, value: &RecordRoot) -> Result<()>;

    async fn get(&self, collection_id: &str, record_id: &str) -> Result<Option<RecordRoot>>;

    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField<'_>],
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>>;

    async fn delete(&self, collection_id: &str, record_id: &str) -> Result<()>;

    async fn apply_indexes<'a>(
        &self,
        indexes: Vec<Index<'a>>,
        previous: Vec<Index<'a>>,
    ) -> Result<()>;

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
