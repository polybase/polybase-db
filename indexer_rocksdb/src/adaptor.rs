use crate::{error, store::Store};
use indexer_db_adaptor::{
    collection::{
        cursor::Cursor,
        index::{Index, IndexField},
        record::RecordRoot,
        where_query::WhereQuery,
    },
    store::{Result, Store as StoreAdaptor},
};
use std::{path::Path, pin::Pin, time::SystemTime};

pub struct RocksDBAdaptor {
    store: Store,
}

impl RocksDBAdaptor {
    pub fn new(config: impl AsRef<Path>) -> Self {
        Self {
            store: Store::open(config).unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl StoreAdaptor for RocksDBAdaptor {
    type Config = String;

    async fn commit(&self) -> Result<()> {
        self.store.commit().await?;
        Ok(())
    }

    // TODO: this will be pulled from existing collection logic
    async fn set(&self, collection_id: &str, record_id: &str, value: &RecordRoot) -> Result<()> {
        todo!()
    }

    async fn get(&self, collection_id: &str, record_id: &str) -> Result<Option<RecordRoot>> {
        todo!()
    }

    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery,
        order_by: &[IndexField<'_>],
        cursor_before: Option<Cursor<'_>>,
        cursor_after: Option<Cursor<'_>>,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        todo!()
    }

    async fn delete(&self, collection_id: &str, record_id: &str) -> Result<()> {
        todo!()
    }

    async fn apply_indexes<'a>(
        &self,
        indexes: Vec<Index<'a>>,
        previous: Vec<Index<'a>>,
    ) -> Result<()> {
        todo!()
    }

    async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<SystemTime>> {
        todo!()
    }

    async fn last_collection_update(&self, collection_id: &str) -> Result<Option<SystemTime>> {
        todo!()
    }

    async fn set_system_key(&self, key: &str, data: &RecordRoot) -> Result<()> {
        todo!()
    }

    async fn get_system_key(&self, key: &str) -> Result<Option<RecordRoot>> {
        todo!()
    }

    async fn destroy(&self) -> Result<()> {
        todo!()
    }
}

impl From<error::Error> for indexer_db_adaptor::store::Error {
    fn from(err: error::Error) -> Self {
        Self(Box::new(err))
    }
}
