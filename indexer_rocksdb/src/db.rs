use crate::{
    record::RecordRoot,
    snapshot::{SnapshotChunk, SnapshotIterator},
};

#[async_trait::async_trait]
pub(crate) trait Database: Send + Sync {
    type Error;
    type Key<'k>;
    type Value<'v>;

    async fn commit(&self) -> Result<(), Self::Error>;

    async fn set(&self, key: &Self::Key<'_>, value: &Self::Value<'_>) -> Result<(), Self::Error>;

    async fn get(&self, key: &Self::Key<'_>) -> Result<Option<RecordRoot>, Self::Error>;

    async fn delete(&self, key: &Self::Key<'_>) -> Result<(), Self::Error>;

    //fn list(
    //    &self,
    //    lower_bound: &Self::Key<'_>, // todo
    //    upper_bound: &Self::Key<'_>, // todo
    //    reverse: bool,
    //) -> std::result::Result<
    //    Box<dyn Iterator<Item = std::result::Result<(Box<[u8]>, Box<[u8]>), Self::Error>> + '_>,
    //    Self::Error,
    //>;

    fn destroy(self) -> Result<(), Self::Error>;

    fn reset(&self) -> Result<(), Self::Error>;

    fn snapshot(&self, chunk_size: usize) -> SnapshotIterator;

    fn restore(&self, chunk: SnapshotChunk) -> Result<(), Self::Error>;
}
