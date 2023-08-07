use crate::collection::{
    index::{Index, IndexDirection, IndexField},
    record::{RecordRoot, RecordValue},
    where_query::WhereQuery,
};
use crate::store::{Error, Result, Store};
use std::{collections::HashMap, pin::Pin, sync::Arc, time::SystemTime};
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum MemoryStoreError {
    #[error("error during `get`")]
    Get,
    #[error("error during `list`")]
    List,
}

#[derive(Clone)]
pub struct MemoryStore {
    state: Arc<Mutex<MemoryStoreState>>,
}

struct MemoryStoreState {
    data: HashMap<String, Collection>,
    system_data: HashMap<String, RecordRoot>,
}

struct Collection {
    pub data: HashMap<String, Record>,
    pub last_updated: SystemTime,
}

struct Record {
    pub data: RecordRoot,
    pub last_updated: SystemTime,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryStoreState {
                data: HashMap::new(),
                system_data: HashMap::new(),
            })),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Store for MemoryStore {
    type Config = ();

    async fn commit(&self) -> Result<()> {
        Ok(())
    }

    async fn set(&self, collection_id: &str, record_id: &str, value: &RecordRoot) -> Result<()> {
        let mut state = self.state.lock().await;

        let collection = match state.data.get_mut(collection_id) {
            Some(collection) => collection,
            // TODO: we should implement Store trait error for missing collection
            None => {
                state.data.insert(
                    collection_id.to_string(),
                    Collection {
                        data: HashMap::from([(
                            record_id.to_string(),
                            Record {
                                data: value.clone(),
                                last_updated: SystemTime::now(),
                            },
                        )]),
                        last_updated: SystemTime::now(),
                    },
                );

                state
                    .data
                    .get_mut(collection_id)
                    .ok_or(Error(Box::new(MemoryStoreError::Get)))?
            }
        };

        collection.data.insert(
            record_id.to_string(),
            Record {
                data: value.clone(),
                last_updated: SystemTime::now(),
            },
        );

        Ok(())
    }

    async fn get(&self, collection_id: &str, record_id: &str) -> Result<Option<RecordRoot>> {
        let state = self.state.lock().await;

        if let Some(record) = state
            .data
            .get(collection_id)
            .and_then(|col| col.data.get(record_id))
        {
            return Ok(Some(record.data.clone()));
        }

        Ok(None)
    }

    // todo : remove this
    #[allow(unused_variables)]
    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField<'_>],
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        let state = self.state.lock().await;

        let collection = match state.data.get(collection_id) {
            Some(collection) => collection,
            None => return Ok(Box::pin(futures::stream::iter(vec![]))),
        };

        // Loop through every record and filter based on the where query
        // TODO
        let mut records: Vec<RecordRoot> = collection
            .data
            .values()
            .map(|value| value.data.clone())
            // TODO: implement the filter/sort/cursor, we'll just loop through
            // every record to find the match
            // .filter_map(|(key, value)| {
            //     let record = RecordRoot::from(value.clone());
            //     if where_query.matches(&record) {
            //         Some(record)
            //     } else {
            //         None
            //     }
            // })
            .collect();

        // sorting
        // TODO
        for IndexField { path, direction } in order_by {
            records.sort_by(|a, b| {
                if let Some(rec_a) = a.get(path[0].as_ref()) {
                    if let Some(rec_b) = b.get(path[0].as_ref()) {
                        match (rec_a, rec_b) {
                            (RecordValue::Number(na), RecordValue::Number(nb)) => match direction {
                                IndexDirection::Ascending => na.partial_cmp(nb).unwrap(),
                                IndexDirection::Descending => nb.partial_cmp(na).unwrap(),
                            },
                            (RecordValue::String(sa), RecordValue::String(sb)) => match direction {
                                IndexDirection::Ascending => sa.cmp(sb),
                                IndexDirection::Descending => sb.cmp(sa),
                            },
                            (RecordValue::Boolean(ba), RecordValue::Boolean(bb)) => match direction
                            {
                                IndexDirection::Ascending => ba.cmp(bb),
                                IndexDirection::Descending => bb.cmp(ba),
                            },
                            _ => std::cmp::Ordering::Equal,
                        }
                    } else {
                        std::cmp::Ordering::Equal
                    }
                } else {
                    std::cmp::Ordering::Equal
                }
            });
        }

        Ok(Box::pin(futures::stream::iter(
            records.into_iter().take(limit.unwrap_or(usize::MAX)),
        )))
    }

    async fn delete(&self, collection_id: &str, record_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;

        let collection = match state.data.get_mut(collection_id) {
            Some(collection) => collection,
            // TODO: we should implement Store trait error for missing collection
            None => return Ok(()),
        };

        collection.data.remove(record_id);

        Ok(())
    }

    async fn apply_indexes<'a>(&self, _indexes: Vec<Index<'a>>, _: Vec<Index<'a>>) -> Result<()> {
        Ok(())
    }

    async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<SystemTime>> {
        let state = self.state.lock().await;

        if let Some(last_updated) = state
            .data
            .get(collection_id)
            .and_then(|col| col.data.get(record_id))
            .map(|record| record.last_updated)
        {
            return Ok(Some(last_updated));
        }

        Ok(None)
    }

    async fn last_collection_update(&self, collection_id: &str) -> Result<Option<SystemTime>> {
        let state = self.state.lock().await;

        if let Some(last_updated) = state
            .data
            .get(collection_id)
            .map(|record| record.last_updated)
        {
            return Ok(Some(last_updated));
        }

        Ok(None)
    }

    async fn set_system_key(&self, key: &str, data: &RecordRoot) -> Result<()> {
        let mut state = self.state.lock().await;

        state.system_data.insert(key.to_string(), data.clone());

        Ok(())
    }

    async fn get_system_key(&self, key: &str) -> Result<Option<RecordRoot>> {
        let state = self.state.lock().await;

        Ok(state.system_data.get(key).cloned())
    }

    async fn destroy(&self) -> Result<()> {
        let mut state = self.state.lock().await;

        state.data.clear();
        state.system_data.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_memory_store_set_and_get() {
        let store = MemoryStore::new();

        let collection_id = "test_collection";
        let record_id = "test_record";
        let record_data = [
            ("id".into(), RecordValue::String("id1".into())),
            ("name".into(), RecordValue::String("Bob".into())),
        ]
        .into();

        store
            .set(collection_id, record_id, &record_data)
            .await
            .unwrap();

        let retrieved_data = store.get(collection_id, record_id).await.unwrap().unwrap();
        assert_eq!(retrieved_data, record_data);
    }

    #[tokio::test]
    async fn test_memory_store_list() {
        let store = MemoryStore::new();

        let collection_id = "test_collection";
        let record1_data = [
            ("id".into(), RecordValue::String("id1".into())),
            ("name".into(), RecordValue::String("Bob".into())),
            ("age".into(), RecordValue::Number(42.0)),
        ]
        .into();

        let record2_data = [
            ("id".into(), RecordValue::String("id2".into())),
            ("name".into(), RecordValue::String("Dave".into())),
            ("age".into(), RecordValue::Number(23.0)),
        ]
        .into();

        store
            .set(collection_id, "record1", &record1_data)
            .await
            .unwrap();
        store
            .set(collection_id, "record2", &record2_data)
            .await
            .unwrap();

        let records = store
            .list(collection_id, None, WhereQuery::default(), &[])
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(2, records.len());
    }

    #[tokio::test]
    #[ignore]
    async fn test_memory_store_list_where_query() {
        todo!();
    }

    #[tokio::test]
    async fn test_memory_store_delete() {
        let store = MemoryStore::new();
        let collection_id = "test_collection";
        let record_id = "test_record";
        let record_data = [
            ("id".into(), RecordValue::String("id1".into())),
            ("name".into(), RecordValue::String("Bob".into())),
        ]
        .into();

        store
            .set(collection_id, record_id, &record_data)
            .await
            .unwrap();

        let retrieved_data = store.get(collection_id, record_id).await.unwrap().unwrap();
        assert_eq!(retrieved_data, record_data);

        store.delete(collection_id, record_id).await.unwrap();
        let deleted_data = store.get(collection_id, record_id).await.unwrap();
        assert!(deleted_data.is_none());
    }

    #[tokio::test]
    async fn test_memory_store_last_update() {
        let store = MemoryStore::new();
        let collection_id = "test_collection";
        let record_id = "test_record";
        let record_data = [
            ("id".into(), RecordValue::String("id1".into())),
            ("name".into(), RecordValue::String("Bob".into())),
        ]
        .into();

        store
            .set(collection_id, record_id, &record_data)
            .await
            .unwrap();

        let last_update = store
            .last_record_update(collection_id, record_id)
            .await
            .unwrap()
            .unwrap();

        let now = SystemTime::now();
        assert!(last_update >= now - Duration::from_secs(5) && last_update <= now);
    }

    #[tokio::test]
    async fn test_memory_store_system_key() {
        let store = MemoryStore::new();
        let key = "system_key";
        let record_data = RecordRoot::new();

        store.set_system_key(key, &record_data).await.unwrap();
        let retrieved_data = store.get_system_key(key).await.unwrap().unwrap();

        assert_eq!(retrieved_data, record_data);
    }

    #[tokio::test]
    async fn test_memory_store_destroy() {
        let store = MemoryStore::new();

        let collection_id = "test_collection";
        let record_id = "test_record";
        let record_data = RecordRoot::new();
        store
            .set(collection_id, record_id, &record_data)
            .await
            .unwrap();

        let system_data = RecordRoot::new();
        store
            .set_system_key("some_system_key", &system_data)
            .await
            .unwrap();

        store.destroy().await.unwrap();

        let retrieved_data = store.get(collection_id, record_id).await.unwrap();
        let system_data = store.get_system_key("some_system_key").await.unwrap();

        assert!(retrieved_data.is_none());
        assert!(system_data.is_none());
    }
}
