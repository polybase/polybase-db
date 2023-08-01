use super::{
    ast::{collection_ast_from_json, collection_ast_from_record, indexes_from_ast},
    authorization::Authorization,
    collection_record::COLLECTION_COLLECTION_RECORD,
    cursor::{Cursor, CursorDirection},
    error::{CollectionError, CollectionUserError, Result},
    field_path::FieldPath,
    index::{self, Index, IndexDirection},
    record::{PathFinder, RecordRoot, RecordValue},
    stableast_ext::{self, FieldWalker},
    util::{self, normalize_name},
    where_query,
};
use crate::{publickey::PublicKey, store::Store};
use async_recursion::async_recursion;
use futures::StreamExt;
use polylang::stableast;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, time::SystemTime};
use tracing::warn;

#[derive(Clone)]
pub struct Collection<'a, S: Store> {
    store: &'a S,
    collection_id: String,
    indexes: Vec<index::Index<'a>>,
    authorization: Authorization,
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
    pub order_by: &'a [index::IndexField<'a>],
    pub cursor_before: Option<Cursor<'a>>,
    pub cursor_after: Option<Cursor<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthUser {
    public_key: PublicKey,
}

impl AuthUser {
    pub fn new(public_key: PublicKey) -> Self {
        Self { public_key }
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

impl<'a, S: Store + 'a> Collection<'a, S> {
    fn new(
        store: &'a S,
        collection_id: String,
        indexes: Vec<Index<'a>>,
        authorization: Authorization,
    ) -> Self {
        Self {
            store,
            collection_id,
            indexes,
            authorization,
        }
    }

    pub(crate) fn collection_collection(store: &'a S) -> Self {
        Self::new(
            store,
            "Collection".to_string(),
            vec![
                index::Index::new(vec![index::IndexField::new(
                    vec![Cow::Borrowed("id")],
                    IndexDirection::Ascending,
                )]),
                index::Index::new(vec![index::IndexField::new(
                    vec![Cow::Borrowed("name")],
                    IndexDirection::Ascending,
                )]),
                index::Index::new(vec![index::IndexField::new(
                    vec![Cow::Borrowed("lastRecordUpdated")],
                    IndexDirection::Ascending,
                )]),
                index::Index::new(vec![index::IndexField::new(
                    vec![Cow::Borrowed("publicKey")],
                    IndexDirection::Ascending,
                )]),
            ],
            Authorization {
                read_all: true,
                call_all: true,
                read_fields: vec![],
                delegate_fields: vec![],
            },
        )
    }

    #[tracing::instrument(skip(store))]
    pub(crate) async fn load(store: &'a S, id: &str) -> Result<Collection<'a, S>> {
        if id == "Collection" {
            return Ok(Self::collection_collection(store));
        }

        let collection_collection = Self::collection_collection(store);
        let Some(record) = collection_collection.get(id, None).await? else {
            return Err(CollectionUserError::CollectionNotFound { name: id.to_string() })?;
        };

        let id = match record.get("id") {
            Some(RecordValue::String(id)) => id,
            Some(_) => return Err(CollectionError::CollectionRecordIDIsNotAString),
            None => return Err(CollectionError::CollectionRecordMissingID),
        };

        let short_collection_name = normalize_name(id.as_str());

        let collection_ast = collection_ast_from_record(&record, &short_collection_name)?;
        let indexes = indexes_from_ast(&collection_ast);

        let is_public = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "public"));
        let is_read_all = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "read" && d.arguments.is_empty()));
        let is_call_all = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "call" && d.arguments.is_empty()));

        Ok(Self {
            store,
            collection_id: id.to_string(),
            indexes,
            authorization: Authorization {
                read_all: is_public || is_read_all,
                call_all: is_public || is_call_all,
                read_fields: collection_ast
                    .attributes
                    .iter()
                    .filter_map(|attr| match attr {
                        stableast::CollectionAttribute::Property(prop) => Some(prop),
                        _ => None,
                    })
                    .filter_map(|prop| {
                        prop.directives
                            .iter()
                            .find(|dir: &&stableast::Directive<'_>| dir.name == "read")
                            .map(|_| FieldPath(vec![prop.name.to_string()]))
                    })
                    .collect::<Vec<_>>(),
                delegate_fields: {
                    let mut delegate_fields = vec![];

                    collection_ast.walk_fields(&mut vec![], &mut |path, field| {
                        if let stableast_ext::Field::Property(p) = field {
                            if p.directives.iter().any(|dir| dir.name == "delegate") {
                                delegate_fields
                                    .push(FieldPath(path.iter().map(|p| p.to_string()).collect()));
                            }
                        };
                    });

                    delegate_fields
                },
            },
        })
    }

    async fn ast<'ast>(
        &self,
        ast_json_holder: &'ast mut Option<String>,
    ) -> Result<stableast::Collection<'ast>> {
        let Some(record) = Self::load(self.store, "Collection")
            .await?
            .get(self.id(), None)
            .await? else {
            return Err(CollectionError::CollectionCollectionRecordNotFound {
                id: self.collection_id.clone(),
            });
        };

        let ast_json = match record.get("ast") {
            Some(RecordValue::String(ast_json)) => ast_json,
            Some(_) => return Err(CollectionError::CollectionRecordASTIsNotAString),
            None => return Err(CollectionError::CollectionRecordMissingAST),
        };

        *ast_json_holder = Some(ast_json.clone());
        #[allow(clippy::unwrap_used)]
        let ast_json = ast_json_holder.as_ref().unwrap();

        collection_ast_from_json(ast_json, self.name().as_str())
    }

    pub fn id(&self) -> &str {
        &self.collection_id
    }

    pub fn name(&self) -> String {
        Self::normalize_name(self.collection_id.as_str())
    }

    pub fn normalize_name(collection_id: &str) -> String {
        util::normalize_name(collection_id)
    }

    pub fn namespace(&self) -> &str {
        let Some(slash_index) = self.collection_id.rfind('/') else {
            return "";
        };

        &self.collection_id[0..slash_index]
    }

    #[tracing::instrument(skip(self))]
    #[async_recursion]
    pub(crate) async fn user_can_read(
        &self,
        record: &RecordRoot,
        user: &Option<&AuthUser>,
    ) -> Result<bool> {
        if self.authorization.read_all {
            return Ok(true);
        }

        let read_fields = &self.authorization.read_fields;

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
                    &mut |path, value| {
                        if !read_fields.iter().any(|rf| rf.0 == path) {
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
        let delegate_fields = &self.authorization.delegate_fields;

        let Some(user) = user else { return Ok(false) };

        for delegate_value in delegate_fields.iter().map(|df| record.find_path(&df.0)) {
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

        self.store.set(self.id(), record_id, value).await?;

        // We have an update to a Collection, that means we need to update the collection schema, not just
        // the record
        if self.collection_id == "Collection" && record_id != "Collection" {
            if let Some(old_collection_record) = old_value {
                let old_collection_ast =
                    collection_ast_from_record(&old_collection_record, &self.name())?;
                let old_indexes = indexes_from_ast(&old_collection_ast);

                let new_collection_ast = collection_ast_from_record(value, &self.name())?;
                let new_indexes = indexes_from_ast(&new_collection_ast);

                // Notify the store that the indexes have changed
                self.store.apply_indexes(new_indexes, old_indexes).await?;
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
        if self.collection_id == "Collection" && record_id == "Collection" {
            return Ok(Some(COLLECTION_COLLECTION_RECORD.clone()));
        }

        let Some(value) = self.store.get(self.id(), record_id).await? else {
            return Ok(None);
        };

        Ok(Some(value))
    }

    // todo - remove this
    pub async fn get_metadata(&self) -> Result<Option<CollectionMetadata>> {
        todo!()
    }

    // todo - remove this
    pub async fn get_record_metadata(&self, _record_id: &str) -> Result<Option<RecordMetadata>> {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    pub async fn delete(&self, record_id: &str) -> Result<()> {
        self.store.delete(self.id(), record_id).await?;
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
            .indexes
            .iter()
            .any(|index| index.matches(&where_query, order_by))
        {
            return Err(CollectionUserError::NoIndexFoundMatchingTheQuery)?;
        }

        let mut ast_holder = None;
        let _ast = self.ast(&mut ast_holder).await?;

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
            .list(self.id(), limit, where_query, order_by)
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
    use std::collections::HashMap;

    use super::*;

    #[tokio::test]
    async fn test_collection_collection_load() {
        let store = MemoryStore::new();
        let collection = Collection::load(&store, "Collection").await.unwrap();

        assert_eq!(collection.collection_id, "Collection");
        assert_eq!(
            collection.authorization,
            Authorization {
                read_all: true,
                call_all: true,
                read_fields: vec![],
                delegate_fields: vec![]
            }
        );
        assert_eq!(collection.indexes.len(), 4);
        assert_eq!(
            collection.indexes[0],
            index::Index::new(vec![index::IndexField::new(
                vec!["id".into()],
                IndexDirection::Ascending
            )])
        );
    }

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

        assert_eq!(collection_account.collection_id, "ns/Account");
        assert_eq!(
            collection_account.authorization,
            Authorization {
                read_all: false,
                call_all: false,
                read_fields: vec![],
                delegate_fields: vec![],
            }
        );
        assert_eq!(collection_account.indexes.len(), 3);
        assert_eq!(
            collection_account.indexes[0],
            index::Index::new(vec![index::IndexField::new(
                vec!["id".into()],
                IndexDirection::Ascending
            )])
        );
        assert_eq!(
            collection_account.indexes[1],
            index::Index::new(vec![index::IndexField::new(
                vec!["balance".into()],
                IndexDirection::Ascending
            )])
        );
        assert_eq!(
            collection_account.indexes[2],
            index::Index::new(vec![index::IndexField::new(
                vec!["info".into(), "name".into()],
                IndexDirection::Ascending
            )])
        );
    }

    #[tokio::test]
    async fn test_collection_set_get() {
        let store = MemoryStore::new();

        let collection = Collection::new(
            &store,
            "test".to_string(),
            vec![],
            Authorization {
                read_all: true,
                call_all: true,
                read_fields: vec![],
                delegate_fields: vec![],
            },
        );

        let value = HashMap::from([
            ("id".to_string(), RecordValue::String("1".into())),
            ("name".to_string(), RecordValue::String("test".into())),
        ]);

        collection.set("1", &value).await.unwrap();
        store.commit().await.unwrap();

        let record = collection.get("1", None).await.unwrap().unwrap();
        assert_eq!(record.get("id").unwrap(), &RecordValue::String("1".into()));
        assert_eq!(
            record.get("name").unwrap(),
            &RecordValue::String("test".into())
        );
    }

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
                            path: vec!["name".into()],
                            direction: IndexDirection::Ascending,
                        },
                        index::IndexField {
                            path: vec!["id".into()],
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
