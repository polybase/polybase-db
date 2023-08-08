use super::{
    ast::collection_ast_from_record,
    collection_collection::{get_collection_collection_schema, COLLECTION_COLLECTION_RECORD},
    cursor::{Cursor, CursorDirection},
    error::{CollectionError, CollectionUserError, Result},
    where_query,
};
use crate::store::Store;
use async_recursion::async_recursion;
use futures::StreamExt;
use schema::{
    index,
    publickey::PublicKey,
    record::{PathFinder, RecordRoot, RecordValue},
    util::normalize_name,
    Schema,
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, time::SystemTime};
use tracing::warn;

#[derive(Clone)]
pub struct Collection<'a, S: Store> {
    store: &'a S,

    // TODO: make private
    pub schema: Schema,
}

pub struct CollectionMetadata {
    pub last_record_updated_at: SystemTime,
}

pub struct RecordMetadata {
    pub updated_at: SystemTime,
}

pub struct ListQuery<'a> {
    pub limit: Option<usize>,
    pub where_query: where_query::WhereQuery<'a>,
    pub order_by: &'a [index::IndexField],
    pub cursor_before: Option<Cursor<'a>>,
    pub cursor_after: Option<Cursor<'a>>,
}

impl<'a, S: Store + 'a> Collection<'a, S> {
    fn new(store: &'a S, schema: Schema) -> Self {
        Self { store, schema }
    }

    pub(crate) fn collection_collection(store: &'a S) -> Self {
        Self::new(store, get_collection_collection_schema())
    }

    #[tracing::instrument(skip(store))]
    pub(crate) async fn load(store: &'a S, id: &str) -> Result<Collection<'a, S>> {
        let collection_collection = Self::collection_collection(store);

        if id == "Collection" {
            return Ok(collection_collection);
        }

        let Some(record) = collection_collection.get_without_auth_check(id).await? else {
            return Err(CollectionUserError::CollectionNotFound { name: id.to_string() })?;
        };

        let id = match record.get("id") {
            Some(RecordValue::String(id)) => id,
            Some(_) => return Err(CollectionError::CollectionRecordIDIsNotAString),
            None => return Err(CollectionError::CollectionRecordMissingID),
        };

        let short_collection_name = normalize_name(id.as_str());
        let collection_ast = collection_ast_from_record(&record, &short_collection_name)?;

        Ok(Self {
            store,
            schema: Schema::new(&collection_ast),
        })
    }

    // fn id(&self) -> &str {
    //     self.schema.id()
    // }

    #[tracing::instrument(skip(self))]
    #[async_recursion]
    pub(crate) async fn user_can_read(
        &self,
        record: &RecordRoot,
        user: &Option<&AuthUser>,
    ) -> Result<bool> {
        if self.schema.read_all {
            return Ok(true);
        }

        let Some(user) = user else {
            return Ok(false);
        };

        let mut authorized = false;
        for (key, value) in record {
            let mut record_references = vec![];
            let mut foreign_record_references = vec![];

            #[allow(clippy::unwrap_used)] // We never return an error
            value
                .walk_all::<std::convert::Infallible>(
                    &mut vec![Cow::Borrowed(key)],
                    &mut |path: &[Cow<'_, str>], value| {
                        if !self.schema.read_fields().any(|rf| rf.0 == path) {
                            return Ok(());
                        }

                        match value {
                            RecordValue::PublicKey(record_pk) if record_pk == &user.public_key => {
                                authorized = true;
                            }
                            RecordValue::ForeignRecordReference(fr) => {
                                foreign_record_references.push(fr.clone());
                            }
                            RecordValue::RecordReference(r) => {
                                record_references.push(r.clone());
                            }
                            RecordValue::Array(arr) => {
                                for value in arr {
                                    match value {
                                        RecordValue::PublicKey(record_pk)
                                            if record_pk == &user.public_key =>
                                        {
                                            authorized = true;
                                        }
                                        RecordValue::ForeignRecordReference(fr) => {
                                            foreign_record_references.push(fr.clone());
                                        }
                                        RecordValue::RecordReference(r) => {
                                            record_references.push(r.clone());
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }

                        Ok(())
                    },
                )
                .unwrap();

            // Get records from this collection
            for record_reference in record_references {
                let Some(record) = self.get(record_reference.id.as_str(), Some(user)).await? else {
                    continue;
                };

                if self
                    .has_delegate_access(&record, &Some(user))
                    .await
                    .unwrap_or(false)
                {
                    authorized = true;
                }
            }

            // Get records from another collection
            for foreign_record_reference in foreign_record_references {
                let collection =
                    Collection::load(self.store, foreign_record_reference.collection_id.as_str())
                        .await?;

                let Some(record) = collection
                    .get(foreign_record_reference.id.as_str(), Some(user))
                    .await?
                else {
                    continue;
                };

                if collection
                    .has_delegate_access(&record, &Some(user))
                    .await
                    .unwrap_or(false)
                {
                    authorized = true;
                }
            }
        }

        if !authorized {
            authorized = self.has_delegate_access(record, &Some(user)).await?;
        }

        Ok(authorized)
    }

    /// Returns true if the user is one of the delegates for the record
    #[tracing::instrument(skip(self, record))]
    #[async_recursion]
    pub async fn has_delegate_access(
        &self,
        record: &(impl PathFinder + Sync),
        user: &Option<&AuthUser>,
    ) -> Result<bool> {
        let delegate_fields = self.schema.delegate_fields();

        let Some(user) = user else { return Ok(false) };

        for delegate_value in delegate_fields.map(|df| record.find_path(&df.0)) {
            let Some(delegate_value) = delegate_value else {
                continue;
            };

            if Self::check_delegate_value(self, delegate_value, user).await? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    #[async_recursion]
    async fn check_delegate_value(
        collection: &Collection<'_, S>,
        delegate_value: &RecordValue,
        user: &AuthUser,
    ) -> Result<bool> {
        match delegate_value {
            RecordValue::PublicKey(pk) if pk == &user.public_key => {
                return Ok(true);
            }
            RecordValue::RecordReference(r) => {
                let Some(record) = collection.get(&r.id, Some(user)).await? else {
                    return Ok(false);
                };

                if collection
                    .has_delegate_access(&record, &Some(user))
                    .await
                    .unwrap_or(false)
                {
                    return Ok(true);
                }
            }
            RecordValue::ForeignRecordReference(fr) => {
                let collection = Collection::load(collection.store, &fr.collection_id).await?;

                let Some(record) = collection.get(&fr.id, Some(user)).await? else {
                    return Ok(false);
                };

                if collection
                    .has_delegate_access(&record, &Some(user))
                    .await
                    .unwrap_or(false)
                {
                    return Ok(true);
                }
            }
            RecordValue::Array(arr) => {
                for item in arr {
                    if Self::check_delegate_value(collection, item, user).await? {
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }

        Ok(false)
    }

    #[tracing::instrument(skip(self))]
    pub async fn set(&self, record_id: &str, value: &RecordRoot) -> Result<()> {
        match value.get("id") {
            Some(rv) => match rv {
                RecordValue::String(id) => {
                    if record_id != id {
                        return Err(CollectionError::RecordIDArgDoesNotMatchRecordDataID);
                    }
                }
                _ => return Err(CollectionError::RecordIDMustBeAString),
            },
            None => return Err(CollectionError::RecordMissingID),
        }

        // Get the old value before we update, so we can provide the old indexes
        let old_value = self.get_without_auth_check(record_id.clone()).await?;

        // self.store.set(self.schema, record_id, value).await?;

        // We have an update to a Collection, that means we need to update the collection schema, not just
        // the record
        if self.schema.id() == "Collection" && record_id != "Collection" {
            if let Some(old_collection_record) = old_value {
                // let old_collection_ast =
                //     collection_ast_from_record(&old_collection_record, &self.schema.name())?;
                // let old_indexes = indexes_from_ast(&old_collection_ast);

                // let new_collection_ast = collection_ast_from_record(value, &self.schema.name())?;
                // let new_indexes = indexes_from_ast(&new_collection_ast);

                // Notify the store that the indexes have changed
                // self.store.apply_indexes(new_indexes, old_indexes).await?;
            }
        }

        Ok(())
    }

    pub async fn get(
        &self,
        record_id: &str,
        user: Option<&AuthUser>,
    ) -> Result<Option<RecordRoot>> {
        let Some(value) = self.get_without_auth_check(record_id).await? else {
            return Ok(None);
        };

        if !self.user_can_read(&value, &user).await? {
            return Err(CollectionUserError::UnauthorizedRead)?;
        }

        Ok(Some(value))
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_without_auth_check(&self, record_id: &str) -> Result<Option<RecordRoot>> {
        if self.id() == "Collection" && record_id == "Collection" {
            return Ok(Some(COLLECTION_COLLECTION_RECORD.clone()));
        }

        let Some(value) = self.store.get(&self.id(), record_id).await? else {
            return Ok(None);
        };

        Ok(Some(value))
    }

    #[tracing::instrument(skip(self))]
    pub async fn delete(&self, record_id: &str) -> Result<()> {
        self.store.delete(self.schema.id(), record_id).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn list(
        &'a self,
        ListQuery {
            limit,
            where_query,
            order_by,
            cursor_before,
            cursor_after,
        }: ListQuery<'_>,
        user: &'a Option<&'a AuthUser>,
    ) -> Result<impl futures::Stream<Item = Result<RecordRoot>> + '_> {
        if !self
            .schema
            .indexes
            .iter()
            .any(|index| where_query.matches(index, order_by))
        {
            return Err(CollectionUserError::NoIndexFoundMatchingTheQuery)?;
        }

        // TODO: remove clone
        let mut where_query = where_query.clone();

        match (cursor_before, cursor_after) {
            (Some(before), None) => {
                where_query.apply_cursor(before, CursorDirection::Before, order_by)
            }
            (None, Some(after)) => {
                where_query.apply_cursor(after, CursorDirection::After, order_by)
            }
            (Some(_), Some(_)) => {
                return Err(CollectionUserError::InvalidCursorBeforeAndAfterSpecified)?;
            }
            (None, None) => (),
        };

        let stream = self
            .store
            .list(&self.id(), limit, where_query, order_by)
            .await?;

        Ok(stream
            .filter_map(|r| async {
                match self.user_can_read(&r, user).await {
                    Ok(false) => None,
                    Ok(true) => Some(Ok(r)),
                    Err(e) => {
                        // TODO: should we propagate this error?
                        warn!("failed to check if user can read record: {e:#?}",);
                        None
                    }
                }
            })
            .take(limit.unwrap_or(usize::MAX)))
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::MemoryStore;
    use futures::TryStreamExt;
    use polylang::stableast;
    use schema::{field_path::FieldPath, index::IndexDirection};
    use std::collections::HashMap;

    use super::*;

    // #[tokio::test]
    // async fn test_collection_collection_load() {
    //     let store = MemoryStore::new();
    //     let collection = Collection::load(&store, "Collection").await.unwrap();

    //     assert_eq!(collection.schema.id(), "Collection");
    //     assert_eq!(
    //         collection.schema.authorization,
    //         Authorization {
    //             read_all: true,
    //             call_all: true,
    //             read_fields: vec![],
    //             delegate_fields: vec![]
    //         }
    //     );
    //     assert_eq!(collection.schema.indexes.len(), 4);
    //     assert_eq!(
    //         collection.schema.indexes[0],
    //         index::Index::new(vec![index::IndexField::new(
    //             vec!["id".into()],
    //             IndexDirection::Ascending
    //         )])
    //     );
    // }

    async fn create_collection<'a>(
        store: &'a MemoryStore,
        ast: stableast::Root<'_>,
    ) -> Vec<Collection<'a, MemoryStore>> {
        let collection_collection = Collection::load(store, "Collection").await.unwrap();

        let ast_json = serde_json::to_string(&ast).unwrap();

        let mut collections = vec![];
        for collection in ast.0.iter().filter_map(|node| match node {
            stableast::RootNode::Collection(c) => Some(c),
            _ => None,
        }) {
            let mut id = collection.namespace.value.to_string();
            if !id.is_empty() {
                id.push('/');
            }

            id.push_str(&collection.name);

            collection_collection
                .set(&id, &{
                    let mut map = HashMap::new();

                    map.insert("id".to_string(), RecordValue::String(id.clone()));
                    map.insert("ast".to_string(), RecordValue::String(ast_json.clone()));

                    map
                })
                .await
                .unwrap();

            store.commit().await.unwrap();

            collections.push(Collection::load(store, &id).await.unwrap());
        }

        collections
    }

    #[tokio::test]
    async fn test_create_collection() {
        let store = MemoryStore::new();

        let collection_account = create_collection(
            &store,
            stableast::Root(vec![stableast::RootNode::Collection(
                stableast::Collection {
                    namespace: stableast::Namespace { value: "ns".into() },
                    name: "Account".into(),
                    attributes: vec![
                        stableast::CollectionAttribute::Property(stableast::Property {
                            name: "id".into(),
                            type_: stableast::Type::Primitive(stableast::Primitive {
                                value: stableast::PrimitiveType::String,
                            }),
                            directives: vec![],
                            required: true,
                        }),
                        stableast::CollectionAttribute::Property(stableast::Property {
                            name: "balance".into(),
                            type_: stableast::Type::Primitive(stableast::Primitive {
                                value: stableast::PrimitiveType::Number,
                            }),
                            directives: vec![],
                            required: true,
                        }),
                        stableast::CollectionAttribute::Property(stableast::Property {
                            name: "info".into(),
                            type_: stableast::Type::Object(stableast::Object {
                                fields: vec![stableast::ObjectField {
                                    name: "name".into(),
                                    type_: stableast::Type::Primitive(stableast::Primitive {
                                        value: stableast::PrimitiveType::String,
                                    }),
                                    required: true,
                                }],
                            }),
                            directives: vec![],
                            required: true,
                        }),
                    ],
                },
            )]),
        )
        .await
        .into_iter()
        .next()
        .unwrap();

        store.commit().await.unwrap();

        assert_eq!(collection_account.schema.id(), "ns/Account");
        // assert_eq!(
        //     collection_account.schema.authorization,
        //     Authorization {
        //         read_all: false,
        //         call_all: false,
        //         read_fields: vec![],
        //         delegate_fields: vec![],
        //     }
        // );
        assert_eq!(collection_account.schema.indexes.len(), 3);
        assert_eq!(
            collection_account.schema.indexes[0],
            index::Index::new(vec![index::IndexField::new(
                vec!["id"].into(),
                IndexDirection::Ascending
            )])
        );
        assert_eq!(
            collection_account.schema.indexes[1],
            index::Index::new(vec![index::IndexField::new(
                vec!["balance"].into(),
                IndexDirection::Ascending
            )])
        );
        assert_eq!(
            collection_account.schema.indexes[2],
            index::Index::new(vec![index::IndexField::new(
                vec!["info", "name"].into(),
                IndexDirection::Ascending
            )])
        );
    }

    // #[tokio::test]
    // async fn test_collection_set_get() {
    //     let store = MemoryStore::new();

    //     let collection = Collection::new(
    //         &store,
    //         "test".to_string(),
    //         vec![],
    //         Authorization {
    //             read_all: true,
    //             call_all: true,
    //             read_fields: vec![],
    //             delegate_fields: vec![],
    //         },
    //     );

    //     let value = HashMap::from([
    //         ("id".to_string(), RecordValue::String("1".into())),
    //         ("name".to_string(), RecordValue::String("test".into())),
    //     ]);

    //     collection.set("1", &value).await.unwrap();
    //     store.commit().await.unwrap();

    //     let record = collection.get("1", None).await.unwrap().unwrap();
    //     assert_eq!(record.get("id").unwrap(), &RecordValue::String("1".into()));
    //     assert_eq!(
    //         record.get("name").unwrap(),
    //         &RecordValue::String("test".into())
    //     );
    // }

    #[tokio::test]
    async fn test_collection_set_list() {
        let store = MemoryStore::new();

        {
            let collection = Collection::load(&store, "Collection").await.unwrap();
            collection
                .set(
                    "test/test",
                    &RecordRoot::from([
                        ("id".to_owned(), RecordValue::String("test/test".to_owned())),
                        (
                            "ast".to_owned(),
                            RecordValue::String(
                                serde_json::to_string_pretty(&stableast::Root(vec![
                                    stableast::RootNode::Collection(stableast::Collection {
                                        namespace: stableast::Namespace {
                                            value: "test".into(),
                                        },
                                        name: "test".into(),
                                        attributes: vec![
                                            stableast::CollectionAttribute::Directive(
                                                polylang::stableast::Directive {
                                                    name: "public".into(),
                                                    arguments: vec![],
                                                },
                                            ),
                                            stableast::CollectionAttribute::Property(
                                                stableast::Property {
                                                    name: "id".into(),
                                                    type_: stableast::Type::Primitive(
                                                        stableast::Primitive {
                                                            value: stableast::PrimitiveType::String,
                                                        },
                                                    ),
                                                    directives: vec![],
                                                    required: true,
                                                },
                                            ),
                                            stableast::CollectionAttribute::Property(
                                                stableast::Property {
                                                    name: "name".into(),
                                                    type_: stableast::Type::Primitive(
                                                        stableast::Primitive {
                                                            value: stableast::PrimitiveType::String,
                                                        },
                                                    ),
                                                    directives: vec![],
                                                    required: true,
                                                },
                                            ),
                                        ],
                                    }),
                                ]))
                                .unwrap(),
                            ),
                        ),
                    ]),
                )
                .await
                .unwrap();
        }

        store.commit().await.unwrap();

        let collection = Collection::load(&store, "test/test").await.unwrap();

        let value_1 = HashMap::from([
            ("id".to_string(), RecordValue::String("1".into())),
            ("name".to_string(), RecordValue::String("test".into())),
        ]);
        collection.set("1", &value_1).await.unwrap();

        let value_2 = HashMap::from([
            ("id".to_string(), RecordValue::String("2".into())),
            ("name".to_string(), RecordValue::String("test".into())),
        ]);
        collection.set("2", &value_2).await.unwrap();

        store.commit().await.unwrap();

        let mut results = collection
            .list(
                ListQuery {
                    limit: None,
                    where_query: where_query::WhereQuery(
                        [(
                            FieldPath(vec!["name".into()]),
                            where_query::WhereNode::Equality(where_query::WhereValue(
                                "test".into(),
                            )),
                        )]
                        .into(),
                    ),
                    order_by: &[
                        index::IndexField {
                            path: vec!["name"].into(),
                            direction: IndexDirection::Ascending,
                        },
                        index::IndexField {
                            path: vec!["id"].into(),
                            direction: IndexDirection::Descending,
                        },
                    ],
                    cursor_before: None,
                    cursor_after: None,
                },
                &None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        let second = results.pop().unwrap();
        let first = results.pop().unwrap();

        assert_eq!(first, value_2);
        assert_eq!(second, value_1);
    }

    #[tokio::test]
    async fn test_collection_set_list_default_query() {
        let store = MemoryStore::new();

        {
            let collection = Collection::load(&store, "Collection").await.unwrap();
            collection
                .set(
                    "test/test",
                    &RecordRoot::from([
                        ("id".to_owned(), RecordValue::String("test/test".to_owned())),
                        (
                            "ast".to_owned(),
                            RecordValue::String(
                                serde_json::to_string_pretty(&stableast::Root(vec![
                                    stableast::RootNode::Collection(stableast::Collection {
                                        namespace: stableast::Namespace {
                                            value: "test".into(),
                                        },
                                        name: "test".into(),
                                        attributes: vec![
                                            stableast::CollectionAttribute::Directive(
                                                polylang::stableast::Directive {
                                                    name: "public".into(),
                                                    arguments: vec![],
                                                },
                                            ),
                                            stableast::CollectionAttribute::Property(
                                                stableast::Property {
                                                    name: "id".into(),
                                                    type_: stableast::Type::Primitive(
                                                        stableast::Primitive {
                                                            value: stableast::PrimitiveType::String,
                                                        },
                                                    ),
                                                    directives: vec![],
                                                    required: true,
                                                },
                                            ),
                                            stableast::CollectionAttribute::Property(
                                                stableast::Property {
                                                    name: "name".into(),
                                                    type_: stableast::Type::Primitive(
                                                        stableast::Primitive {
                                                            value: stableast::PrimitiveType::String,
                                                        },
                                                    ),
                                                    directives: vec![],
                                                    required: true,
                                                },
                                            ),
                                        ],
                                    }),
                                ]))
                                .unwrap(),
                            ),
                        ),
                    ]),
                )
                .await
                .unwrap();
        }

        store.commit().await.unwrap();

        let collection = Collection::load(&store, "test/test").await.unwrap();

        let value_1 = HashMap::from([
            ("id".to_string(), RecordValue::String("1".into())),
            ("name".to_string(), RecordValue::String("test".into())),
        ]);
        collection.set("1", &value_1).await.unwrap();

        let value_2 = HashMap::from([
            ("id".to_string(), RecordValue::String("2".into())),
            ("name".to_string(), RecordValue::String("test".into())),
        ]);
        collection.set("2", &value_2).await.unwrap();

        store.commit().await.unwrap();

        let results = collection
            .list(
                ListQuery {
                    limit: None,
                    where_query: where_query::WhereQuery([].into()),
                    order_by: &[],
                    cursor_before: None,
                    cursor_after: None,
                },
                &None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
    }
}
