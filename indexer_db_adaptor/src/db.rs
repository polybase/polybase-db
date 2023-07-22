//! The abstract interface for a store (database backend).
//! Various concrete implementations can exist, each implementing the
//! contract specified by this interface.

use crate::record::RecordRoot;
use async_trait::async_trait;

/// The Database trait
///
/// This trait expresses the essential functionality of the Indexer, regardless
/// of the concrete backend in use (rocksdb, postgres, etc.).
#[async_trait]
pub trait Database: Send + Sync + 'static {
    type Error: std::error::Error;
    type Key<'k>;
    type Value<'v>;
    //type Chunk;
    //type ChunkIterator;

    async fn commit(&self) -> std::result::Result<(), Self::Error>;
    async fn get(&self, key: &Self::Key<'_>) -> Result<Option<RecordRoot>, Self::Error>;
    async fn delete(&self, key: &Self::Key<'_>) -> std::result::Result<(), Self::Error>;

    async fn set(
        &self,
        key: &Self::Key<'_>,
        value: &Self::Value<'_>,
    ) -> std::result::Result<(), Self::Error>;

    fn list(
        &self,
        lower_bound: &Self::Key<'_>, // todo
        upper_bound: &Self::Key<'_>, // todo
        reverse: bool,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<(Box<[u8]>, Box<[u8]>), Self::Error>> + '_>,
        Self::Error,
    >;

    fn destroy(self) -> std::result::Result<(), Self::Error>;
    fn reset(&self) -> std::result::Result<(), Self::Error>;
    //fn snapshot(&self, chunk_size: usize) -> Self::ChunkIterator;
    //fn restore(&self, chunk: Self::Chunk) -> Result<()>;
}
