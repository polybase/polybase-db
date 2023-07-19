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
    type Err;
    type Key: From<String>;
    type Value: From<RecordRoot> + Into<RecordRoot>;
    //type Chunk;
    //type ChunkIterator;

    async fn commit(&self) -> std::result::Result<(), Self::Err>;
    async fn set(&self, key: &Self::Key, value: &Self::Value)
        -> std::result::Result<(), Self::Err>;
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Err>;
    async fn delete(&self, key: &Self::Key) -> std::result::Result<(), Self::Err>;

    fn list(
        &self,
        key: Self::Value, // todo
        reverse: bool,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<(Box<[u8]>, Box<[u8]>), Self::Err>> + '_>,
        Self::Err,
    >;

    fn destroy(self) -> std::result::Result<(), Self::Err>;
    fn reset(&self) -> std::result::Result<(), Self::Err>;
    //fn snapshot(&self, chunk_size: usize) -> Self::ChunkIterator;
    //fn restore(&self, chunk: Self::Chunk) -> Result<()>;
}
