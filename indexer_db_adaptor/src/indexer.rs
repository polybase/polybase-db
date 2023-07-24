use crate::{collection::Collection, record::RecordRoot};

/// The generic Indexer trait
#[async_trait::async_trait]
pub trait Indexer {
    type Error;
    type CollError;
    type Key<'k>
    where
        Self: 'k;
    type Value<'v>
    where
        Self: 'v;
    type ListQuery<'l>
    where
        Self: 'l;
    type Cursor;

    async fn check_for_migration(&self, migration_batch_size: usize) -> Result<(), Self::Error>;

    fn destroy(self) -> Result<(), Self::Error>;

    fn reset(&self) -> Result<(), Self::Error>;

    async fn collection<'k, 'v>(
        &self,
        id: String,
    ) -> Result<
        Box<
            dyn Collection<
                    Error = Self::CollError,
                    Key = Self::Key<'_>,
                    Value = Self::Value<'_>,
                    ListQuery = Self::ListQuery<'_>,
                    Cursor = Self::Cursor,
                > + '_,
        >,
        Self::Error,
    >;

    async fn commit(&self) -> Result<(), Self::Error>;

    async fn set_system_key(&self, key: String, data: &RecordRoot) -> Result<(), Self::Error>;

    async fn get_system_key(&self, key: String) -> Result<Option<RecordRoot>, Self::Error>;
}
