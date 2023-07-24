use crate::record::RecordRoot;

/// The Database trait
#[async_trait::async_trait]
pub trait Database: Send + Sync {
    type Error;
    type Key<'k>;
    type Value<'v>;

    async fn commit(&self) -> Result<(), Self::Error>;

    async fn set(&self, key: &Self::Key<'_>, value: &Self::Value<'_>) -> Result<(), Self::Error>;

    async fn get(&self, key: &Self::Key<'_>) -> Result<Option<RecordRoot>, Self::Error>;

    async fn delete(&self, key: &Self::Key<'_>) -> Result<(), Self::Error>;

    fn destroy(self) -> Result<(), Self::Error>;

    fn reset(&self) -> Result<(), Self::Error>;

    async fn set_system_key(&self, key: String, data: &RecordRoot) -> Result<(), Self::Error>;

    async fn get_system_key(&self, key: String) -> Result<Option<RecordRoot>, Self::Error>;
}
