use crate::adaptor::{Error, IndexerAdaptor, Result};
use crate::where_query::{WhereInequality, WhereNode, WhereQuery};
use schema::field_path::FieldPath;
use schema::index_value::IndexValue;
use schema::Schema;
use schema::{
    index::{IndexDirection, IndexField},
    record::{RecordRoot, RecordValue},
};
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

    pub async fn set(
        &self,
        collection_id: &str,
        record_id: &str,
        value: &RecordRoot,
    ) -> Result<()> {
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
                    .ok_or(Error::Store(Box::new(MemoryStoreError::Get)))?
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

    pub async fn delete(&self, collection_id: &str, record_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;

        let collection = match state.data.get_mut(collection_id) {
            Some(collection) => collection,
            // TODO: we should implement Store trait error for missing collection
            None => return Ok(()),
        };

        collection.data.remove(record_id);

        Ok(())
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

fn record_matches(where_query: &WhereQuery<'_>, record: &RecordRoot) -> bool {
    println!("where_query = {where_query:?}");

    for (rec_key, rec_val) in record.iter() {
        println!("rec_key = {rec_key:?}, rec_val = {rec_val:?}");
        if let Some(where_val) = where_query.0.get(&FieldPath(vec![rec_key.clone()])) {
            match where_val {
                WhereNode::Equality(ref eq_val) => {
                    return eq_val.0.clone() == IndexValue::try_from(rec_val.clone()).unwrap();
                }
                WhereNode::Inequality(ref ineq_val) => {
                    let WhereInequality { gt, gte, lt, lte } = ineq_val.clone();

                    if let Some(gt_val) = gt {
                        let rec_val = IndexValue::try_from(rec_val.clone()).unwrap();

                        match (gt_val.0, rec_val) {
                            (IndexValue::Number(wnum), IndexValue::Number(rec_num)) => {
                                return rec_num > wnum;
                            }
                            (IndexValue::String(wstr), IndexValue::String(rec_str)) => {
                                return rec_str > wstr;
                            }

                            (IndexValue::Boolean(wbool), IndexValue::Boolean(rec_bool)) => {
                                return rec_bool & !wbool;
                            }
                            _ => return false,
                        }
                    }

                    if let Some(gte_val) = gte {
                        let rec_val = IndexValue::try_from(rec_val.clone()).unwrap();

                        match (gte_val.0, rec_val) {
                            (IndexValue::Number(wnum), IndexValue::Number(rec_num)) => {
                                return rec_num >= wnum;
                            }
                            (IndexValue::String(wstr), IndexValue::String(rec_str)) => {
                                return rec_str >= wstr;
                            }

                            (IndexValue::Boolean(wbool), IndexValue::Boolean(rec_bool)) => {
                                return rec_bool >= wbool;
                            }
                            _ => return false,
                        }
                    }

                    if let Some(lt_val) = lt {
                        let rec_val = IndexValue::try_from(rec_val.clone()).unwrap();

                        match (lt_val.0, rec_val) {
                            (IndexValue::Number(wnum), IndexValue::Number(rec_num)) => {
                                return rec_num < wnum;
                            }
                            (IndexValue::String(wstr), IndexValue::String(rec_str)) => {
                                return rec_str < wstr;
                            }

                            (IndexValue::Boolean(wbool), IndexValue::Boolean(rec_bool)) => {
                                return !rec_bool & wbool;
                            }
                            _ => return false,
                        }
                    }

                    if let Some(lte_val) = lte {
                        let rec_val = IndexValue::try_from(rec_val.clone()).unwrap();

                        match (lte_val.0, rec_val) {
                            (IndexValue::Number(wnum), IndexValue::Number(rec_num)) => {
                                return rec_num <= wnum;
                            }
                            (IndexValue::String(wstr), IndexValue::String(rec_str)) => {
                                return rec_str <= wstr;
                            }

                            (IndexValue::Boolean(wbool), IndexValue::Boolean(rec_bool)) => {
                                return rec_bool <= wbool;
                            }
                            _ => return false,
                        }
                    }
                }
            }
        }
    }

    true // todo
}

#[async_trait::async_trait]
impl IndexerAdaptor for MemoryStore {
    async fn commit(&self) -> Result<()> {
        Ok(())
    }

    async fn get_schema(&self, collection_id: &str) -> Result<Option<Schema>> {
        let record = match self.get("Collection", collection_id).await? {
            Some(record) => record,
            None => return Ok(None),
        };
        Ok(Some(Schema::from_record(&record)?))
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
            None => return Ok(Box::pin(futures::stream::iter(vec![]))),
        };

        // Loop through every record and filter based on the where query
        // TODO
        let mut records: Vec<RecordRoot> = collection
            .data
            .values()
            .map(|value| value.data.clone())
            .filter_map(|record| {
                let record = record.clone();
                if record_matches(&where_query, &record) {
                    Some(record)
                } else {
                    None
                }
            })
            .collect();

        // sort based on order_by
        // TODO
        for IndexField { path, direction } in order_by {
            records.sort_by(|a, b| {
                // how to handle Vec<String>?
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

        Ok(Box::pin(futures::stream::iter(
            records.into_iter().take(limit.unwrap_or(usize::MAX)),
        )))
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
    use crate::where_query::{WhereInequality, WhereValue};
    use std::borrow::Cow;

    use super::*;
    use futures::StreamExt;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_memory_store_set_and_get() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";
        let record_id = "test_record";

        let mut record_data = RecordRoot::new();
        record_data.insert("id".into(), RecordValue::String("id1".into()));
        record_data.insert("name".into(), RecordValue::String("Bob".into()));

        store
            .set(collection_id, record_id, &record_data)
            .await
            .unwrap();

        let retrieved_data = store.get(collection_id, record_id).await.unwrap().unwrap();
        assert_eq!(retrieved_data, record_data);
    }

    #[tokio::test]
    async fn test_memory_store_list() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let mut record1_data = RecordRoot::new();
        record1_data.insert("id".into(), RecordValue::String("id1".into()));
        record1_data.insert("name".into(), RecordValue::String("Bob".into()));
        record1_data.insert("age".into(), RecordValue::Number(42.0));

        let mut record2_data = RecordRoot::new();
        record2_data.insert("id".into(), RecordValue::String("id2".into()));
        record2_data.insert("name".into(), RecordValue::String("Dave".into()));
        record2_data.insert("age".into(), RecordValue::Number(23.0));

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
    async fn test_memory_store_list_where_query_single_equality() {
        use std::borrow::Cow;

        let store = MemoryStore::default();
        let collection_id = "test_collection";

        let mut record1_data = RecordRoot::new();

        record1_data.insert("id".into(), RecordValue::String("id1".into()));
        record1_data.insert("name".into(), RecordValue::String("Bob".into()));
        record1_data.insert("age".into(), RecordValue::Number(42.0));

        let mut record2_data = RecordRoot::new();

        record2_data.insert("id".into(), RecordValue::String("id2".into()));
        record2_data.insert("name".into(), RecordValue::String("Dave".into()));
        record2_data.insert("age".into(), RecordValue::Number(23.0));

        let mut record3_data = RecordRoot::new();
        record3_data.insert("id".into(), RecordValue::String("id3".into()));
        record3_data.insert("name".into(), RecordValue::String("Wanda".into()));
        record3_data.insert("age".into(), RecordValue::Number(19.0));

        store
            .set(collection_id, "record1", &record1_data)
            .await
            .unwrap();
        store
            .set(collection_id, "record2", &record2_data)
            .await
            .unwrap();
        store
            .set(collection_id, "record3", &record3_data)
            .await
            .unwrap();

        let where_query = WhereQuery(
            [(
                FieldPath(["id".to_string()].into()),
                WhereNode::Equality(WhereValue(IndexValue::String(Cow::Owned("id2".into())))),
            )]
            .into(),
        );

        let records = store
            .list(collection_id, None, where_query, &[])
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 1);
        assert_eq!(records[0], record2_data);
    }

    #[tokio::test]
    async fn test_where_sort() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let mut record1_data = RecordRoot::new();
        record1_data.insert("id".into(), RecordValue::String("id1".into()));
        record1_data.insert("name".into(), RecordValue::String("Bob".into()));
        record1_data.insert("age".into(), RecordValue::Number(42.0));
        record1_data.insert("place".into(), RecordValue::String("Timbuktu".into()));

        let mut record2_data = RecordRoot::new();
        record2_data.insert("id".into(), RecordValue::String("id2".into()));
        record2_data.insert("name".into(), RecordValue::String("Bobby".into()));
        record2_data.insert("age".into(), RecordValue::Number(42.0));
        record2_data.insert("place".into(), RecordValue::String("Ougadougou".into()));

        let mut record3_data = RecordRoot::new();
        record3_data.insert("id".into(), RecordValue::String("id3".into()));
        record3_data.insert("name".into(), RecordValue::String("Bobbers".into()));
        record3_data.insert("age".into(), RecordValue::Number(42.0));
        record3_data.insert("place".into(), RecordValue::String("Oshkosh".into()));

        store
            .set(collection_id, "rec1", &record1_data)
            .await
            .unwrap();

        store
            .set(collection_id, "rec2", &record2_data)
            .await
            .unwrap();

        store
            .set(collection_id, "rec3", &record3_data)
            .await
            .unwrap();

        store.commit().await.unwrap();

        let where_query = WhereQuery(
            [(
                FieldPath(["name".to_string()].into()),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue(IndexValue::String("Bob".into()))),
                    gte: None,
                    lt: None,
                    lte: None,
                }),
            )]
            .into(),
        );

        let order_by = vec![IndexField {
            path: vec!["name".to_string()].into(),
            direction: IndexDirection::Descending,
        }];

        let records = store
            .list(collection_id, None, where_query, &order_by)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(records.len(), 2);
        assert_eq!(records[0], record2_data);
        assert_eq!(records[1], record3_data);
    }

    #[tokio::test]
    async fn test_memory_store_list_order_by() {
        use std::borrow::Cow;

        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let mut record1_data = RecordRoot::new();
        record1_data.insert("id".into(), RecordValue::String("id1".into()));
        record1_data.insert("name".into(), RecordValue::String("Bob".into()));
        record1_data.insert("age".into(), RecordValue::Number(42.0));

        let mut record2_data = RecordRoot::new();
        record2_data.insert("id".into(), RecordValue::String("id2".into()));
        record2_data.insert("name".into(), RecordValue::String("Bob".into()));
        record2_data.insert("age".into(), RecordValue::Number(23.0));

        let mut record3_data = RecordRoot::new();
        record3_data.insert("id".into(), RecordValue::String("id3".into()));
        record3_data.insert("name".into(), RecordValue::String("Wanda".into()));
        record3_data.insert("age".into(), RecordValue::Number(23.0));

        let mut record4_data = RecordRoot::new();
        record4_data.insert("id".into(), RecordValue::String("id4".into()));
        record4_data.insert("name".into(), RecordValue::String("Bob".into()));
        record4_data.insert("age".into(), RecordValue::Number(89.0));

        store
            .set(collection_id, "record1", &record1_data)
            .await
            .unwrap();

        store
            .set(collection_id, "record2", &record2_data)
            .await
            .unwrap();

        store
            .set(collection_id, "record3", &record3_data)
            .await
            .unwrap();

        store
            .set(collection_id, "record4", &record4_data)
            .await
            .unwrap();

        store.commit().await.unwrap();

        let where_query = WhereQuery(
            [(
                FieldPath(["name".to_string()].into()),
                WhereNode::Equality(WhereValue(IndexValue::String(Cow::Owned("Bob".into())))),
            )]
            .into(),
        );

        let order_by = vec![
            IndexField {
                path: vec!["name".to_string()].into(),
                direction: IndexDirection::Ascending,
            },
            IndexField {
                path: vec!["id".to_string()].into(),
                direction: IndexDirection::Descending,
            },
        ];

        let mut records = store
            .list(collection_id, None, where_query, &order_by)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(records.len(), 3);

        let third = records.pop().unwrap();
        let second = records.pop().unwrap();
        let first = records.pop().unwrap();

        assert_eq!(first, record4_data);
        assert_eq!(second, record2_data);
        assert_eq!(third, record1_data);
    }

    #[tokio::test]
    async fn test_memory_store_delete() {
        let store = MemoryStore::new();
        let collection_id = "test_collection";
        let record_id = "test_record";

        let mut record_data = RecordRoot::new();
        record_data.insert("id".into(), RecordValue::String("id1".into()));
        record_data.insert("name".into(), RecordValue::String("Bob".into()));

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
        let mut record_data = RecordRoot::new();

        record_data.insert("id".into(), RecordValue::String("id1".into()));
        record_data.insert("name".into(), RecordValue::String("Bob".into()));

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
