use crate::adaptor::{Error, Result, SnapshotValue};
use crate::where_query::{WhereInequality, WhereNode, WhereQuery};
use crate::IndexerAdaptor;
use crate::IndexerChange;
use schema::{
    field_path::FieldPath,
    index::{IndexDirection, IndexField},
    index_value::IndexValue,
    record::{RecordRoot, RecordValue},
    Schema,
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

    pub async fn destroy(&self) -> Result<()> {
        let mut state = self.state.lock().await;

        state.data.clear();
        state.system_data.clear();

        Ok(())
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

fn record_matches(where_query: &WhereQuery<'_>, record: &RecordRoot) -> Result<bool> {
    // we need to match all conditions for the record against the where query for the record to
    // qualify as a match.
    let mut rec_field_matches: Vec<Result<bool>> = Vec::new();

    for (rec_key, rec_val) in record.iter() {
        if let Some(where_val) = where_query.0.get(&FieldPath(vec![rec_key.clone()])) {
            match where_val {
                WhereNode::Equality(ref eq_val) => {
                    rec_field_matches.push(Ok(eq_val.0.clone()
                        == IndexValue::try_from(rec_val.clone())
                            .map_err(|e| Error::Store(Box::new(e)))?));
                }
                WhereNode::Inequality(ref ineq_val) => {
                    let WhereInequality { gt, gte, lt, lte } = *ineq_val.clone();

                    if let Some(gt_val) = gt {
                        let rec_val = IndexValue::try_from(rec_val.clone())
                            .map_err(|e| Error::Store(Box::new(e)))?;

                        rec_field_matches.push(Ok(match (gt_val.0, rec_val) {
                            (IndexValue::Number(wnum), IndexValue::Number(rec_num)) => {
                                rec_num > wnum
                            }
                            (IndexValue::String(wstr), IndexValue::String(rec_str)) => {
                                rec_str > wstr
                            }

                            (IndexValue::Boolean(wbool), IndexValue::Boolean(rec_bool)) => {
                                rec_bool & !wbool
                            }
                            _ => true,
                        }));
                    }

                    if let Some(gte_val) = gte {
                        let rec_val = IndexValue::try_from(rec_val.clone())
                            .map_err(|e| Error::Store(Box::new(e)))?;

                        rec_field_matches.push(Ok(match (gte_val.0, rec_val) {
                            (IndexValue::Number(wnum), IndexValue::Number(rec_num)) => {
                                rec_num >= wnum
                            }
                            (IndexValue::String(wstr), IndexValue::String(rec_str)) => {
                                rec_str >= wstr
                            }

                            (IndexValue::Boolean(wbool), IndexValue::Boolean(rec_bool)) => {
                                rec_bool >= wbool
                            }
                            _ => true,
                        }));
                    }

                    if let Some(lt_val) = lt {
                        let rec_val = IndexValue::try_from(rec_val.clone())
                            .map_err(|e| Error::Store(Box::new(e)))?;

                        rec_field_matches.push(Ok(match (lt_val.0, rec_val) {
                            (IndexValue::Number(wnum), IndexValue::Number(rec_num)) => {
                                rec_num < wnum
                            }
                            (IndexValue::String(wstr), IndexValue::String(rec_str)) => {
                                rec_str < wstr
                            }

                            (IndexValue::Boolean(wbool), IndexValue::Boolean(rec_bool)) => {
                                !rec_bool & wbool
                            }
                            _ => true,
                        }));
                    }

                    if let Some(lte_val) = lte {
                        let rec_val = IndexValue::try_from(rec_val.clone())
                            .map_err(|e| Error::Store(Box::new(e)))?;

                        rec_field_matches.push(Ok(match (lte_val.0, rec_val) {
                            (IndexValue::Number(wnum), IndexValue::Number(rec_num)) => {
                                rec_num <= wnum
                            }
                            (IndexValue::String(wstr), IndexValue::String(rec_str)) => {
                                rec_str <= wstr
                            }

                            (IndexValue::Boolean(wbool), IndexValue::Boolean(rec_bool)) => {
                                rec_bool <= wbool
                            }
                            _ => true,
                        }));
                    }
                }
            }
        }
    }

    Ok(rec_field_matches
        .iter()
        .all(|res| res.as_ref().map(|&b| b).unwrap_or(false)))
}

#[async_trait::async_trait]
impl IndexerAdaptor for MemoryStore {
    async fn commit(&self, _height: usize, changes: Vec<IndexerChange>) -> Result<()> {
        for change in changes {
            match change {
                IndexerChange::Set {
                    collection_id,
                    record_id,
                    record,
                } => {
                    self.set(&collection_id, &record_id, &record).await?;
                }

                IndexerChange::Delete {
                    collection_id,
                    record_id,
                } => {
                    self.delete(&collection_id, &record_id).await?;
                }
            }
        }

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

    #[allow(unused_variables)]
    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField],
        reverse: bool,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        let state = self.state.lock().await;

        let collection = match state.data.get(collection_id) {
            Some(collection) => collection,
            None => return Ok(Box::pin(futures::stream::iter(vec![]))),
        };

        // Loop through every record and filter based on the where query
        let mut records: Vec<RecordRoot> = collection
            .data
            .values()
            .map(|value| value.data.clone())
            .filter_map(|record| {
                let record = record.clone();

                match record_matches(&where_query, &record) {
                    Ok(matches) => {
                        if matches {
                            Some(record)
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            })
            .collect();

        // sort the matching records based on order_by
        for IndexField { path, direction } in order_by {
            records.sort_by(|a, b| {
                let joined_path = path.0.join("."); // vector of fields
                if let Some(rec_a) = a.get(&joined_path) {
                    if let Some(rec_b) = b.get(&joined_path) {
                        match (rec_a, rec_b) {
                            (RecordValue::Number(na), RecordValue::Number(nb)) => match direction {
                                IndexDirection::Ascending => {
                                    na.partial_cmp(nb).unwrap_or(std::cmp::Ordering::Greater)
                                }
                                IndexDirection::Descending => {
                                    nb.partial_cmp(na).unwrap_or(std::cmp::Ordering::Greater)
                                }
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

                            (RecordValue::PublicKey(pka), RecordValue::PublicKey(pkb)) => {
                                match direction {
                                    IndexDirection::Ascending => {
                                        pka.partial_cmp(pkb).unwrap_or(std::cmp::Ordering::Greater)
                                    }
                                    IndexDirection::Descending => {
                                        pkb.partial_cmp(pka).unwrap_or(std::cmp::Ordering::Greater)
                                    }
                                }
                            }

                            (
                                RecordValue::ForeignRecordReference(fra),
                                RecordValue::ForeignRecordReference(frb),
                            ) => match direction {
                                IndexDirection::Ascending => {
                                    fra.partial_cmp(frb).unwrap_or(std::cmp::Ordering::Greater)
                                }
                                IndexDirection::Descending => {
                                    frb.partial_cmp(fra).unwrap_or(std::cmp::Ordering::Greater)
                                }
                            },
                            _ => std::cmp::Ordering::Equal,
                        }
                    } else {
                        std::cmp::Ordering::Equal // todo - PublicKey and ForeignRecordReference
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

    async fn snapshot(
        &self,
        _: usize,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<Vec<SnapshotValue>>> + '_ + Send>> {
        todo!()
    }

    async fn restore(&self, _: Vec<SnapshotValue>) -> Result<()> {
        todo!()
    }

    async fn reset(&self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::where_query::{WhereInequality, WhereValue};

    use super::*;
    use futures::StreamExt;
    use std::borrow::Cow;
    use tokio::time::Duration;

    fn create_record_root(fields: &[&str], values: &[RecordValue]) -> RecordRoot {
        let mut record_root = RecordRoot::new();

        for (field, value) in fields.iter().zip(values) {
            record_root.insert(field.to_string(), value.clone());
        }

        record_root
    }

    #[tokio::test]
    async fn test_memory_store_set_and_get() {
        let store = MemoryStore::default();
        let mut changes = Vec::new();

        let collection_id = "test_collection";
        let record_id = "test_record";

        let record_data = create_record_root(
            &["id", "name"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
            ],
        );

        changes.push(IndexerChange::Set {
            collection_id: collection_id.to_string(),
            record_id: record_id.to_string(),
            record: record_data.clone(),
        });

        store.commit(0, changes).await.unwrap();

        let retrieved_data = store.get(collection_id, record_id).await.unwrap().unwrap();
        assert_eq!(retrieved_data, record_data);
    }

    #[tokio::test]
    async fn test_memory_store_list() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let record1_data = create_record_root(
            &["id", "name", "age"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Number(42.0),
            ],
        );

        let record2_data = create_record_root(
            &["id", "name", "age"],
            &[
                RecordValue::String("id2".into()),
                RecordValue::String("Dave".into()),
                RecordValue::Number(23.0),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record1".into(),
                record: record1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record2".to_string(),
                record: record2_data.clone(),
            },
        ];

        store.commit(9, changes).await.unwrap();

        let records = store
            .list(collection_id, None, WhereQuery::default(), &[], false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(2, records.len());
    }

    #[tokio::test]
    async fn test_memory_store_list_where_query_single_equality() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let record1_data = create_record_root(
            &["id", "name", "age"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Number(42.0),
            ],
        );

        let record2_data = create_record_root(
            &["id", "name", "age"],
            &[
                RecordValue::String("id2".into()),
                RecordValue::String("Dave".into()),
                RecordValue::Number(23.0),
            ],
        );
        let record3_data = create_record_root(
            &["id", "name", "age"],
            &[
                RecordValue::String("id3".into()),
                RecordValue::String("Wanda".into()),
                RecordValue::Number(19.0),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record1".to_string(),
                record: record1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record2".to_string(),
                record: record2_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record3".to_string(),
                record: record3_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let where_query = WhereQuery(
            [(
                FieldPath(["id".to_string()].into()),
                WhereNode::Equality(WhereValue(IndexValue::String(Cow::Owned("id2".into())))),
            )]
            .into(),
        );

        let records = store
            .list(collection_id, None, where_query, &[], false)
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

        let record1_data = create_record_root(
            &["id", "name", "age", "place"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Number(42.0),
                RecordValue::String("Timbuktu".into()),
            ],
        );

        let record2_data = create_record_root(
            &["id", "name", "age", "place"],
            &[
                RecordValue::String("id2".into()),
                RecordValue::String("Bobby".into()),
                RecordValue::Number(21.0),
                RecordValue::String("Timbuktu".into()),
            ],
        );

        let record3_data = create_record_root(
            &["id", "name", "age", "place"],
            &[
                RecordValue::String("id3".into()),
                RecordValue::String("Bobbers".into()),
                RecordValue::Number(89.0),
                RecordValue::String("Timbuktu".into()),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "rec1".to_string(),
                record: record1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "rec2".to_string(),
                record: record2_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "rec3".to_string(),
                record: record3_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let where_query = WhereQuery(
            [(
                FieldPath(["name".to_string()].into()),
                WhereNode::Inequality(Box::new(WhereInequality {
                    gt: Some(WhereValue(IndexValue::String("Bob".into()))),
                    gte: None,
                    lt: None,
                    lte: None,
                })),
            )]
            .into(),
        );

        let order_by = vec![IndexField {
            path: vec!["name".to_string()].into(),
            direction: IndexDirection::Descending,
        }];

        let records = store
            .list(collection_id, None, where_query, &order_by, false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(records.len(), 2);
        assert_eq!(records[0], record2_data);
        assert_eq!(records[1], record3_data);

        let where_query = WhereQuery(
            [(
                FieldPath(["name".to_string()].into()),
                WhereNode::Inequality(Box::new(WhereInequality {
                    gt: Some(WhereValue(IndexValue::String("Bob".into()))),
                    gte: None,
                    lt: None,
                    lte: None,
                })),
            )]
            .into(),
        );

        let order_by = vec![IndexField {
            path: vec!["name".to_string()].into(),
            direction: IndexDirection::Ascending,
        }];

        let records = store
            .list(collection_id, None, where_query, &order_by, false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(records.len(), 2);
        assert_eq!(records[0], record3_data);
        assert_eq!(records[1], record2_data);
    }

    #[tokio::test]
    async fn test_memory_store_list_order_by() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let record1_data = create_record_root(
            &["id", "name"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Number(42.0),
            ],
        );
        let record2_data = create_record_root(
            &["id", "name"],
            &[
                RecordValue::String("id2".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Number(23.0),
            ],
        );
        let record3_data = create_record_root(
            &["id", "name"],
            &[
                RecordValue::String("id3".into()),
                RecordValue::String("Wanda".into()),
                RecordValue::Number(23.0),
            ],
        );
        let record4_data = create_record_root(
            &["id", "name"],
            &[
                RecordValue::String("id4".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Number(89.0),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record1".to_string(),
                record: record1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record2".to_string(),
                record: record2_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record3".to_string(),
                record: record3_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record4".to_string(),
                record: record4_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

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
            .list(collection_id, None, where_query, &order_by, false)
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
    async fn test_where_nested_fields() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let record1_data = create_record_root(
            &["id", "info.name"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
            ],
        );

        let record2_data = create_record_root(
            &["id", "info.name"],
            &[
                RecordValue::String("id2".into()),
                RecordValue::String("Dave".into()),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record1".to_string(),
                record: record1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record2".to_string(),
                record: record2_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let where_query = WhereQuery(
            [(
                FieldPath(["info.name".to_string()].into()),
                WhereNode::Equality(WhereValue(IndexValue::String(Cow::Owned("Bob".into())))),
            )]
            .into(),
        );

        let records = store
            .list(collection_id, None, where_query, &[], false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 1);
        assert_eq!(records[0], record1_data);
    }

    #[tokio::test]
    async fn test_sort_nested_fields() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let record1_data = create_record_root(
            &["id", "info.name"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
            ],
        );

        let record2_data = create_record_root(
            &["id", "info.name"],
            &[
                RecordValue::String("id2".into()),
                RecordValue::String("Dave".into()),
            ],
        );

        let record3_data = create_record_root(
            &["id", "info.name"],
            &[
                RecordValue::String("id3".into()),
                RecordValue::String("Wanda".into()),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record1".to_string(),
                record: record1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record2".to_string(),
                record: record2_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record3".to_string(),
                record: record3_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let order_by = vec![IndexField {
            path: vec!["info".to_string(), "name".to_string()].into(),
            direction: IndexDirection::Ascending,
        }];

        let records = store
            .list(collection_id, None, WhereQuery::default(), &order_by, false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 3);
        assert_eq!(records[0], record1_data);
        assert_eq!(records[1], record2_data);
        assert_eq!(records[2], record3_data);

        let order_by = vec![IndexField {
            path: vec!["info".to_string(), "name".to_string()].into(),
            direction: IndexDirection::Descending,
        }];

        let records = store
            .list(collection_id, None, WhereQuery::default(), &order_by, false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 3);
        assert_eq!(records[0], record3_data);
        assert_eq!(records[1], record2_data);
        assert_eq!(records[2], record1_data);
    }

    #[tokio::test]
    async fn test_where_boolean_field() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let record1_data = create_record_root(
            &["id", "name", "isActive"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Boolean(true),
            ],
        );

        let record2_data = create_record_root(
            &["id", "name", "isActive"],
            &[
                RecordValue::String("id2".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Boolean(false),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record1".to_string(),
                record: record1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record2".to_string(),
                record: record2_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let where_query = WhereQuery(
            [
                (
                    FieldPath(["name".to_string()].into()),
                    WhereNode::Equality(WhereValue(IndexValue::String(Cow::Owned("Bob".into())))),
                ),
                (
                    FieldPath(["isActive".to_string()].into()),
                    WhereNode::Equality(WhereValue(IndexValue::Boolean(true))),
                ),
            ]
            .into(),
        );

        let records = store
            .list(collection_id, None, where_query, &[], false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 1);
        assert_eq!(records[0], record1_data);

        let where_query = WhereQuery(
            [
                (
                    FieldPath(["name".to_string()].into()),
                    WhereNode::Equality(WhereValue(IndexValue::String(Cow::Owned("Bob".into())))),
                ),
                (
                    FieldPath(["isActive".to_string()].into()),
                    WhereNode::Equality(WhereValue(IndexValue::Boolean(false))),
                ),
            ]
            .into(),
        );

        let records = store
            .list(collection_id, None, where_query, &[], false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 1);
        assert_eq!(records[0], record2_data);
    }

    #[tokio::test]
    async fn test_sort_boolean_field() {
        let store = MemoryStore::default();

        let collection_id = "test_collection";

        let record1_data = create_record_root(
            &["id", "name", "isActive"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Boolean(true),
            ],
        );

        let record2_data = create_record_root(
            &["id", "name", "isActive"],
            &[
                RecordValue::String("id2".into()),
                RecordValue::String("Bob".into()),
                RecordValue::Boolean(false),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record1".to_string(),
                record: record1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: collection_id.into(),
                record_id: "record2".to_string(),
                record: record2_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let order_by = vec![IndexField {
            path: vec!["isActive".to_string()].into(),
            direction: IndexDirection::Ascending,
        }];

        let records = store
            .list(collection_id, None, WhereQuery::default(), &order_by, false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 2);
        assert_eq!(records[0], record2_data);
        assert_eq!(records[1], record1_data);

        let order_by = vec![IndexField {
            path: vec!["isActive".to_string()].into(),
            direction: IndexDirection::Descending,
        }];

        let records = store
            .list(collection_id, None, WhereQuery::default(), &order_by, false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 2);
        assert_eq!(records[0], record1_data);
        assert_eq!(records[1], record2_data);
    }

    #[tokio::test]
    async fn test_where_foreign_record_reference() {
        use schema::record::ForeignRecordReference;

        let store = MemoryStore::new();

        let user_collection = "test/User";
        let user1_data = create_record_root(&["id"], &[RecordValue::String("0".into())]);
        let user2_data = create_record_root(&["id"], &[RecordValue::String("1".into())]);

        let changes = vec![
            IndexerChange::Set {
                collection_id: user_collection.into(),
                record_id: "user1".to_string(),
                record: user1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: user_collection.into(),
                record_id: "user2".to_string(),
                record: user2_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let account_collection = "test/Account";
        let account1_data = create_record_root(
            &["id", "user"],
            &[
                RecordValue::String("0".into()),
                RecordValue::ForeignRecordReference(ForeignRecordReference {
                    collection_id: user_collection.to_string(),
                    id: "0".to_string(),
                }),
            ],
        );
        let account2_data = create_record_root(
            &["id", "user"],
            &[
                RecordValue::String("1".into()),
                RecordValue::ForeignRecordReference(ForeignRecordReference {
                    collection_id: user_collection.to_string(),
                    id: "1".to_string(),
                }),
            ],
        );

        let account3_data = create_record_root(
            &["id", "user"],
            &[
                RecordValue::String("2".into()),
                RecordValue::ForeignRecordReference(ForeignRecordReference {
                    collection_id: user_collection.to_string(),
                    id: "0".to_string(),
                }),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: account_collection.into(),
                record_id: "account1".to_string(),
                record: account1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: account_collection.into(),
                record_id: "account2".to_string(),
                record: account2_data.clone(),
            },
            IndexerChange::Set {
                collection_id: account_collection.into(),
                record_id: "account3".to_string(),
                record: account3_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let where_query = WhereQuery(
            [(
                FieldPath(["user".to_string()].into()),
                WhereNode::Equality(WhereValue(IndexValue::ForeignRecordReference(Cow::Owned(
                    ForeignRecordReference {
                        collection_id: user_collection.to_string(),
                        id: "1".to_string(),
                    },
                )))),
            )]
            .into(),
        );

        let records = store
            .list(account_collection, None, where_query, &[], false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 1);
        assert_eq!(records[0], account2_data);
    }

    #[tokio::test]
    async fn test_sort_foreign_record_reference() {
        use schema::record::ForeignRecordReference;

        let store = MemoryStore::new();

        let user_collection = "test/User";
        let user1_data = create_record_root(&["id"], &[RecordValue::String("0".into())]);
        let user2_data = create_record_root(&["id"], &[RecordValue::String("1".into())]);

        let changes = vec![
            IndexerChange::Set {
                collection_id: user_collection.into(),
                record_id: "user1".to_string(),
                record: user1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: user_collection.into(),
                record_id: "user2".to_string(),
                record: user2_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let account_collection = "test/Account";
        let account1_data = create_record_root(
            &["id", "user"],
            &[
                RecordValue::String("0".into()),
                RecordValue::ForeignRecordReference(ForeignRecordReference {
                    collection_id: user_collection.to_string(),
                    id: "0".to_string(),
                }),
            ],
        );
        let account2_data = create_record_root(
            &["id", "user"],
            &[
                RecordValue::String("1".into()),
                RecordValue::ForeignRecordReference(ForeignRecordReference {
                    collection_id: user_collection.to_string(),
                    id: "1".to_string(),
                }),
            ],
        );

        let account3_data = create_record_root(
            &["id", "user"],
            &[
                RecordValue::String("2".into()),
                RecordValue::ForeignRecordReference(ForeignRecordReference {
                    collection_id: user_collection.to_string(),
                    id: "0".to_string(),
                }),
            ],
        );

        let changes = vec![
            IndexerChange::Set {
                collection_id: account_collection.into(),
                record_id: "account1".to_string(),
                record: account1_data.clone(),
            },
            IndexerChange::Set {
                collection_id: account_collection.into(),
                record_id: "account2".to_string(),
                record: account2_data.clone(),
            },
            IndexerChange::Set {
                collection_id: account_collection.into(),
                record_id: "account3".to_string(),
                record: account3_data.clone(),
            },
        ];

        store.commit(0, changes).await.unwrap();

        let where_query = WhereQuery(
            [(
                FieldPath(["user".to_string()].into()),
                WhereNode::Equality(WhereValue(IndexValue::ForeignRecordReference(Cow::Owned(
                    ForeignRecordReference {
                        collection_id: user_collection.to_string(),
                        id: "0".to_string(),
                    },
                )))),
            )]
            .into(),
        );

        let order_by = vec![IndexField {
            path: vec!["id".to_string()].into(),
            direction: IndexDirection::Ascending,
        }];

        let records = store
            .list(
                account_collection,
                None,
                where_query.clone(),
                &order_by,
                false,
            )
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 2);
        assert_eq!(records[0], account1_data);
        assert_eq!(records[1], account3_data);

        let order_by = vec![IndexField {
            path: vec!["id".to_string()].into(),
            direction: IndexDirection::Descending,
        }];

        let records = store
            .list(account_collection, None, where_query, &order_by, false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(records.len() == 2);
        assert_eq!(records[0], account3_data);
        assert_eq!(records[1], account1_data);
    }

    #[tokio::test]
    async fn test_memory_store_delete() {
        let store = MemoryStore::new();
        let collection_id = "test_collection";
        let record_id = "test_record";

        let record_data = create_record_root(
            &["id", "name"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
            ],
        );

        let changes = vec![IndexerChange::Set {
            collection_id: collection_id.into(),
            record_id: record_id.to_string(),
            record: record_data.clone(),
        }];

        store.commit(0, changes).await.unwrap();

        let retrieved_data = store.get(collection_id, record_id).await.unwrap().unwrap();
        assert_eq!(retrieved_data, record_data);

        let changes = vec![IndexerChange::Delete {
            collection_id: collection_id.into(),
            record_id: record_id.to_string(),
        }];

        store.commit(0, changes).await.unwrap();

        let deleted_data = store.get(collection_id, record_id).await.unwrap();
        assert!(deleted_data.is_none());
    }

    #[tokio::test]
    async fn test_memory_store_last_update() {
        let store = MemoryStore::new();
        let collection_id = "test_collection";
        let record_id = "test_record";

        let record_data = create_record_root(
            &["id", "name"],
            &[
                RecordValue::String("id1".into()),
                RecordValue::String("Bob".into()),
            ],
        );

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
        let changes = vec![IndexerChange::Set {
            collection_id: collection_id.into(),
            record_id: record_id.to_string(),
            record: record_data.clone(),
        }];

        store.commit(0, changes).await.unwrap();

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
