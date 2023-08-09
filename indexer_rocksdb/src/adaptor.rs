use crate::keys::{self, Key};
use crate::result_stream::convert_stream;
use crate::{
    key_range::{self, key_range, KeyRange},
    proto,
    store::{self, Store},
};
use futures::{StreamExt, TryStreamExt};
use indexer_db_adaptor::{
    adaptor::{self, IndexerAdaptor},
    // indexer::{IndexField, Result, Store as StoreAdaptor},
    where_query::WhereQuery,
    IndexerChange,
};
use prost::Message;
use schema::field_path::FieldPath;
use schema::{
    index::IndexField,
    record::{RecordRoot, RecordValue},
    Schema,
};
use std::{
    path::Path,
    pin::Pin,
    time::{Duration, SystemTime},
};

pub struct CollectionMetadata {
    pub last_record_updated_at: SystemTime,
}

pub struct RecordMetadata {
    pub updated_at: SystemTime,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("metadata is missing lastRecordUpdatedAt")]
    MetadataMissingLastRecordUpdatedAt,

    #[error("no index found matching the query")]
    NoIndexFoundMatchingTheQuery,

    #[error("system time error")]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("store error")]
    StoreError(#[from] store::StoreError),

    #[error("key error")]
    KeyError(#[from] keys::KeysError),

    #[error("key range error")]
    KeyRange(#[from] key_range::Error),

    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("indexer adaptor error")]
    IndexerAdaptor(#[from] adaptor::Error),
}

#[derive(Clone)]
pub struct RocksDBAdaptor {
    store: Store,
}

impl RocksDBAdaptor {
    pub fn new(config: impl AsRef<Path>) -> Self {
        Self {
            store: Store::open(config).unwrap(),
        }
    }

    pub async fn _get(&self, collection_id: &str, record_id: &str) -> Result<Option<RecordRoot>> {
        let key = keys::Key::new_data(collection_id.to_string(), record_id.to_string())?;

        let Some(value) = self.store.get(&key).await? else {
            return Ok(None);
        };

        Ok(Some(value))
    }

    pub async fn _list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField],
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        let schema = self.get_schema(collection_id).await?.unwrap();

        // Select the best index for the query
        let Some(index) = schema.indexes.iter().find(|index| where_query.matches(index, order_by)) else {
            // This should never be called, as we also do a check in the calling indexer
            return Err(Error::NoIndexFoundMatchingTheQuery)?;
        };

        // Borrwed key range of the query
        let key_range = key_range(
            &where_query,
            &schema,
            collection_id.to_string(),
            index
                .fields
                .iter()
                .map(|f| &f.path)
                .collect::<Vec<_>>()
                .as_slice(),
            &index.fields.iter().map(|f| f.direction).collect::<Vec<_>>(),
        )?;

        // Owned key range of the query
        let key_range = KeyRange {
            lower: key_range.lower.with_static(),
            upper: key_range.upper.with_static(),
        };

        // Looking at the provided sort order, to know if we need to reverse the results
        // based on the index direction
        let reverse = index.should_list_in_reverse(order_by);

        let res = futures::stream::iter(self.store.list(
            &key_range.lower,
            &key_range.upper,
            reverse,
        )?)
        .try_filter_map(|res| async {
            let (k, v) = res;

            // let index_key = Cursor::new(keys::Key::deserialize(&k)?.with_static())?;
            let index_record = proto::IndexRecord::decode(&v[..])?;
            let data_key = keys::Key::deserialize(&index_record.id)?;
            let data = match self.store.get(&data_key).await? {
                Some(d) => d,
                None => return Ok(None),
            };

            Ok(Some(data))
        })
        // .try_filter_map(|r| async {
        //     match r {
        //         Some(r) => Ok(Some(RecordRoot::decode(&r[..])?)),
        //         None => Ok(None),
        //     }
        // })
        .take(limit.unwrap_or(usize::MAX));

        let stream = convert_stream(Box::pin(res))?;

        Ok(stream.boxed())
    }

    async fn update_metadata(&self, collection_id: &str, time: &SystemTime) -> Result<()> {
        let collection_metadata_key = Key::new_system_data(format!("{}/metadata", collection_id))?;

        self.store
            .set(
                &collection_metadata_key,
                &store::Value::DataValue(&RecordRoot(
                    [(
                        "lastRecordUpdatedAt".to_string(),
                        RecordValue::String(
                            time.duration_since(SystemTime::UNIX_EPOCH)?
                                .as_millis()
                                .to_string(),
                        ),
                    )]
                    .into(),
                )),
            )
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_metadata(&self, collection_id: &str) -> Result<Option<CollectionMetadata>> {
        let collection_metadata_key =
            keys::Key::new_system_data(format!("{}/metadata", &collection_id))?;

        let Some(record) = self.store.get(&collection_metadata_key).await? else {
            return Ok(None);
        };

        let last_record_updated_at = match record.get_path(&FieldPath::from("lastRecordUpdatedAt"))
        {
            Some(RecordValue::String(s)) => {
                SystemTime::UNIX_EPOCH + Duration::from_millis(s.parse()?)
            }
            _ => return Err(Error::MetadataMissingLastRecordUpdatedAt),
        };

        Ok(Some(CollectionMetadata {
            last_record_updated_at,
        }))
    }
}

#[async_trait::async_trait]
impl IndexerAdaptor for RocksDBAdaptor {
    async fn commit(&self, height: usize, changes: Vec<IndexerChange>) -> adaptor::Result<()> {
        self.store.commit().await.map_err(Error::from)?;
        Ok(())
    }

    async fn get(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> adaptor::Result<Option<RecordRoot>> {
        Ok(self._get(collection_id, record_id).await?)
    }

    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField],
    ) -> adaptor::Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        Ok(self
            ._list(collection_id, limit, where_query, order_by)
            .await?)
    }

    async fn get_schema(&self, collection_id: &str) -> adaptor::Result<Option<Schema>> {
        let record = match self._get("Collection", collection_id).await? {
            Some(record) => record,
            None => return Ok(None),
        };

        Ok(Some(
            Schema::from_record(&record).map_err(adaptor::Error::Schema)?,
        ))
    }

    async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> adaptor::Result<Option<SystemTime>> {
        todo!()
    }

    async fn last_collection_update(
        &self,
        collection_id: &str,
    ) -> adaptor::Result<Option<SystemTime>> {
        let metadata = self.get_metadata(collection_id).await?;
        Ok(metadata.map(|m| m.last_record_updated_at))
    }

    async fn set_system_key(&self, key: &str, data: &RecordRoot) -> adaptor::Result<()> {
        todo!()
    }

    async fn get_system_key(&self, key: &str) -> adaptor::Result<Option<RecordRoot>> {
        todo!()
    }

    async fn destroy(&self) -> adaptor::Result<()> {
        todo!()
    }
}

impl From<Error> for adaptor::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::IndexerAdaptor(e) => e,
            _ => Self::Store(Box::new(err)),
        }
    }
}
