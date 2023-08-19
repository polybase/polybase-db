use crate::keys;
use crate::result_stream::convert_stream;
use crate::{
    key_range::{self, key_range, KeyRange},
    proto, snapshot,
    store::{self, Store},
};
use async_recursion::async_recursion;
use futures::{StreamExt, TryStreamExt};
use indexer::{
    adaptor::{self, IndexerAdaptor, SnapshotValue},
    where_query::WhereQuery,
    IndexerChange,
};
use prost::Message;
use schema::COLLECTION_SCHEMA;
use schema::{
    field_path::FieldPath,
    index::{IndexDirection, IndexField},
    record::{json_to_record, record_to_json, RecordRoot, RecordValue},
    Schema,
};
use std::{
    collections::HashMap,
    path::Path,
    pin::Pin,
    time::{Duration, SystemTime},
};
use tracing::{self, error, warn};

pub struct CollectionMetadata {
    pub last_record_updated_at: SystemTime,
}

pub struct RecordMetadata {
    pub updated_at: SystemTime,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("collection not found")]
    CollectionNotFound,

    #[error("metadata is missing lastRecordUpdatedAt")]
    MetadataMissingLastRecordUpdatedAt,

    #[error("metadata is missing updatedAt")]
    MetadataMissingUpdatedAt,

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

    #[error("prost decode error")]
    ProstDecode(#[from] prost::DecodeError),

    #[error("schema error")]
    Schema(#[from] schema::Error),

    #[error("schema error")]
    Record(#[from] schema::record::RecordError),
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

    pub fn snapshot(&self, chunk_size: usize) -> snapshot::SnapshotIterator {
        self.store.snapshot(chunk_size)
    }

    pub fn restore(&self, data: snapshot::SnapshotChunk) -> Result<()> {
        Ok(self.store.restore(data)?)
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
        reverse: bool,
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
        let reverse_index = index.should_list_in_reverse(order_by);

        // Switch the order if we need to reverse the results
        let reverse_index = if reverse {
            !reverse_index
        } else {
            reverse_index
        };

        let res = futures::stream::iter(self.store.list(
            &key_range.lower,
            &key_range.upper,
            reverse_index,
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
        .take(limit.unwrap_or(usize::MAX));

        let stream = convert_stream(Box::pin(res))?;

        Ok(stream.boxed())
    }

    #[tracing::instrument(skip(self))]
    pub async fn set(
        &self,
        collection_id: &str,
        record_id: &str,
        record: &RecordRoot,
        schema: &Schema,
    ) -> Result<()> {
        let data_key = keys::Key::new_data(collection_id.to_string(), record_id.to_string())?;

        if collection_id == "Collection" && record_id != "Collection" {
            let old_schema = self.get_schema(record_id).await?;
            if let Some(old_schema) = old_schema {
                let new_schema = Schema::from_record(record)?;
                self.rebuild(record_id, &new_schema, &old_schema).await?;
            }
        }

        // Get the old record before the set
        let old_record = self.get(collection_id, record_id).await?;

        self.store
            .set(&data_key, &store::Value::DataValue(record))
            .await?;

        self.update_metadata(collection_id, &SystemTime::now())
            .await?;

        self.update_record_metadata(collection_id, record_id, &SystemTime::now())
            .await?;

        // If old record exists
        if let Some(old_value) = &old_record {
            // delete the indexes for the old values
            self.delete_indexes(collection_id, record_id, old_value, schema)
                .await;
        }

        self.add_indexes(collection_id, record_id, &data_key, record, schema)
            .await;

        Ok(())
    }

    pub async fn _get_system_record(&self, key: &str) -> Result<Option<RecordRoot>> {
        let key = keys::Key::new_system_data(key.to_string())?;

        match self.store.get(&key).await? {
            Some(record) => Ok(Some(record)),
            None => Ok(None),
        }
    }

    pub async fn _set_system_record(&self, key: &str, record: &RecordRoot) -> Result<()> {
        let key = keys::Key::new_system_data(key.to_string())?;

        self.store
            .set(&key, &store::Value::DataValue(record))
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_metadata(&self, collection_id: &str) -> Result<Option<CollectionMetadata>> {
        let collection_metadata_key = format!("{}/metadata", &collection_id);

        let Some(record) = self._get_system_record(&collection_metadata_key).await? else {
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

    async fn update_metadata(&self, collection_id: &str, time: &SystemTime) -> Result<()> {
        let collection_metadata_key = &format!("{}/metadata", collection_id);

        self._set_system_record(
            collection_metadata_key,
            &RecordRoot(
                [(
                    "lastRecordUpdatedAt".to_string(),
                    RecordValue::String(
                        time.duration_since(SystemTime::UNIX_EPOCH)?
                            .as_millis()
                            .to_string(),
                    ),
                )]
                .into(),
            ),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_record_metadata(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<RecordMetadata>> {
        let record_metadata_key = format!("{}/records/{}/metadata", collection_id, record_id);

        let Some(record) = self._get_system_record(&record_metadata_key).await? else {
            return Ok(None);
        };

        let updated_at = match record.get_path(&"updatedAt".into()) {
            Some(RecordValue::String(s)) => {
                SystemTime::UNIX_EPOCH + Duration::from_millis(s.parse()?)
            }
            _ => return Err(Error::MetadataMissingUpdatedAt),
        };

        Ok(Some(RecordMetadata { updated_at }))
    }

    #[tracing::instrument(skip(self))]
    pub async fn update_record_metadata(
        &self,
        collection_id: &str,
        record_id: &str,
        updated_at: &SystemTime,
    ) -> Result<()> {
        let record_metadata_key = format!("{}/records/{}/metadata", collection_id, record_id);

        self._set_system_record(
            &record_metadata_key,
            &RecordRoot(
                [(
                    "updatedAt".into(),
                    RecordValue::String(
                        updated_at
                            .duration_since(SystemTime::UNIX_EPOCH)?
                            .as_millis()
                            .to_string(),
                    ),
                )]
                .into(),
            ),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn add_indexes(
        &self,
        collection_id: &str,
        record_id: &str,
        data_key: &keys::Key<'_>,
        record: &RecordRoot,
        schema: &Schema,
    ) {
        let index_value = store::Value::IndexValue(proto::IndexRecord {
            id: match data_key.serialize() {
                Ok(data) => data,
                Err(e) => {
                    error!("failed to serialize data key: {e}");
                    return;
                }
            },
        });

        for index in schema.indexes.iter() {
            if let Err(indexing_failure) = async {
                let index_key = keys::index_record_key_with_record(
                    collection_id.to_string(),
                    &index.fields.iter().map(|f| &f.path).collect::<Vec<_>>(),
                    &index.fields.iter().map(|f| f.direction).collect::<Vec<_>>(),
                    record,
                )?;

                self.store.set(&index_key, &index_value).await?;

                Ok::<_, Error>(())
            }
            .await
            {
                error!(
                    record = record_id,
                    index = index
                        .fields
                        .iter()
                        .map(|f| f.path.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    "indexing failure: {indexing_failure}"
                );
            }
        }
    }

    async fn delete_indexes(
        &self,
        collection_id: &str,
        record_id: &str,
        record: &RecordRoot,
        schema: &Schema,
    ) {
        for index in schema.indexes.iter() {
            if let Err(deindexing_failure) = async {
                let index_key = keys::index_record_key_with_record(
                    collection_id.to_string(),
                    &index.fields.iter().map(|f| &f.path).collect::<Vec<_>>(),
                    &index.fields.iter().map(|f| f.direction).collect::<Vec<_>>(),
                    record,
                )?;

                self.store.delete(&index_key).await?;

                Ok::<_, Error>(())
            }
            .await
            {
                error!(
                    record = record_id,
                    index = index
                        .fields
                        .iter()
                        .map(|f| f.path.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    "failed to delete index: {deindexing_failure}"
                );
            }
        }
    }

    pub async fn delete(
        &self,
        collection_id: &str,
        record_id: &str,
        schema: &Schema,
    ) -> Result<()> {
        let Some(record) = self._get(collection_id, record_id).await? else {
            return Ok(());
        };

        let key = keys::Key::new_data(collection_id.to_string(), record_id.to_string())?;

        self.store.delete(&key).await?;

        let now = SystemTime::now();
        self.update_metadata(collection_id, &now).await?;
        self.update_record_metadata(collection_id, record_id.clone(), &now)
            .await?;

        self.delete_indexes(collection_id, record_id, &record, schema)
            .await;

        Ok(())
    }

    #[async_recursion]
    async fn rebuild(
        &self,
        collection_id: &str,
        new_schema: &Schema,
        old_schema: &Schema,
    ) -> Result<()> {
        // Check if we need to update the indexes
        if new_schema == old_schema {
            // Collection code was not changed, no need to rebuild anything
            return Ok(());
        }

        let start_key = keys::Key::new_index(
            collection_id.to_string(),
            &[&"id".into()],
            &[IndexDirection::Ascending],
            vec![],
        )?;

        let end_key = start_key.clone().wildcard();

        // Loop through every record in the collection
        for entry in self.store.list(&start_key, &end_key, false)? {
            let (id, value) = entry?;

            let index_record = proto::IndexRecord::decode(&value[..])?;
            let data_key = keys::Key::deserialize(&index_record.id)?;

            // Get old record
            let record = self.store.get(&data_key).await?;

            // Record is missing, skip it
            let Some(record) = record else {
                warn!(collection_id = collection_id, "Record is missing, skipping");
                continue;
            };

            // Record is missing id, skip it
            let record_id = match record.id() {
                Ok(id) => id,
                Err(_) => {
                    warn!(
                        collection_id = collection_id,
                        "Record is missing id, skipping"
                    );
                    continue;
                }
            };

            // We convert the record to json and then back to a record, in order to case values
            // TODO: there must be a better way to do this!
            let json_data = record_to_json(record.clone());
            let new_data = json_to_record(new_schema, json_data, true)?;

            // Delete from the old collection object (loaded from old ast), to delete the old data and indexes
            self.delete(collection_id, record_id, old_schema).await?;

            // Insert into the new collection object, to create the new data and indexes
            self.set(collection_id, record_id, &new_data, new_schema)
                .await?;
        }

        Ok(())
    }

    pub async fn store_commit(&self) -> Result<()> {
        Ok(self.store.commit().await?)
    }
}

#[async_trait::async_trait]
impl IndexerAdaptor for RocksDBAdaptor {
    async fn commit(&self, height: usize, changes: Vec<IndexerChange>) -> adaptor::Result<()> {
        let mut schemas = HashMap::<String, Schema>::new();

        for change in changes.iter() {
            match change {
                IndexerChange::Set {
                    collection_id,
                    record_id,
                    record,
                } => {
                    if collection_id == "Collection" && !schemas.contains_key(record_id) {
                        let schema = Schema::from_record(record)?;
                        schemas.insert(record_id.to_string(), schema);
                    }

                    let schema = match schemas.entry(collection_id.to_string()) {
                        std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
                        std::collections::hash_map::Entry::Vacant(v) => {
                            let fetched_schema = self
                                .get_schema(collection_id)
                                .await?
                                .ok_or(Error::CollectionNotFound)?;
                            v.insert(fetched_schema.clone())
                        }
                    };

                    self.set(collection_id, record_id, record, schema).await?;
                }
                IndexerChange::Delete {
                    collection_id,
                    record_id,
                } => {
                    let schema = self
                        .get_schema(collection_id)
                        .await?
                        .ok_or(Error::CollectionNotFound)?;
                    self.delete(collection_id, record_id, &schema).await?;
                }
            }
        }
        self.store_commit().await?;
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
        reverse: bool,
    ) -> adaptor::Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        Ok(self
            ._list(collection_id, limit, where_query, order_by, reverse)
            .await?)
    }

    async fn get_schema(&self, collection_id: &str) -> adaptor::Result<Option<Schema>> {
        if collection_id == "Collection" {
            return Ok(Some(COLLECTION_SCHEMA.clone()));
        }

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
        let metadata = self.get_record_metadata(collection_id, record_id).await?;
        Ok(metadata.map(|m| m.updated_at))
    }

    async fn last_collection_update(
        &self,
        collection_id: &str,
    ) -> adaptor::Result<Option<SystemTime>> {
        let metadata = self.get_metadata(collection_id).await?;
        Ok(metadata.map(|m| m.last_record_updated_at))
    }

    async fn set_system_key(&self, key: &str, data: &RecordRoot) -> adaptor::Result<()> {
        Ok(self._set_system_record(key, data).await?)
    }

    async fn get_system_key(&self, key: &str) -> adaptor::Result<Option<RecordRoot>> {
        Ok(self._get_system_record(key).await?)
    }

    async fn snapshot(
        &self,
        chunk_size: usize,
    ) -> Pin<Box<dyn futures::Stream<Item = adaptor::Result<Vec<SnapshotValue>>> + '_ + Send>> {
        let res = futures::stream::iter(self.store.snapshot(chunk_size));
        let stream = Box::pin(res.map(|s| {
            s.map_err(store::StoreError::from)
                .map_err(Error::from)
                .map_err(adaptor::Error::from)
        }));

        stream.boxed()
    }

    async fn restore(&self, chunk: Vec<SnapshotValue>) -> adaptor::Result<()> {
        Ok(self.store.restore(chunk).map_err(Error::from)?)
    }

    async fn reset(&self) -> adaptor::Result<()> {
        Ok(self.store.reset().map_err(Error::from)?)
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
