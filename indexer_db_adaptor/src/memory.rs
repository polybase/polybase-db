use crate::collection::{
    record::{IndexValue, RecordRoot, RecordValue},
    where_query::WhereQuery,
};
use crate::store::{Result, Store};
use schema::index::{IndexDirection, IndexField};
use std::{collections::HashMap, pin::Pin, sync::Arc, time::SystemTime};
use tokio::sync::Mutex;

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

                state.data.get_mut(collection_id).unwrap()
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
        order_by: &[IndexField],
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        let state = self.state.lock().await;

        let collection = match state.data.get(collection_id) {
            Some(collection) => collection,
            None => return Ok(Box::pin(futures::stream::empty())),
        };

        // Loop through every record and filter based on the where query
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
            .take(limit.unwrap_or(usize::MAX))
            .collect();

        // sorting
        for IndexField { path, direction } in order_by {
            records.sort_by(|a, b| {
                if let Some(rec_a) = a.get(&path.0[0]) {
                    if let Some(rec_b) = b.get(&path.0[0]) {
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

        Ok(Box::pin(futures::stream::iter(records)))
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
