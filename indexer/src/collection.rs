use std::{borrow::Cow, collections::HashMap, error::Error};

use base64::Engine;
use once_cell::sync::Lazy;
use polylang::stableast::{self, Record};
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::{
    index,
    keys::{self, PathFinder},
    proto,
    publickey::PublicKey,
    stableast_ext::FieldWalker,
    store::{self, RecordValue, StoreRecordValue},
    where_query::{self, FieldPath},
    IndexValue, RecordReference,
};

static COLLECTION_COLLECTION_RECORD: Lazy<String> = Lazy::new(|| {
    let mut hm = HashMap::new();

    hm.insert(
        Cow::Borrowed("id"),
        keys::RecordValue::IndexValue(keys::IndexValue::String(Cow::Borrowed("collections"))),
    );

    let code = r#"
@public
collection Collection {
    id: string;
    name?: string;
    lastRecordUpdated?: string;
    code?: string;
    ast?: string;
    publicKey?: PublicKey;

    @index(publicKey);
    @index([lastRecordUpdated, desc]);

    constructor (id: string, code: string) {
        this.id = id;
        this.code = code;
        this.ast = parse(code, id);
        if (ctx.publicKey) this.publicKey = ctx.publicKey;
    }

    updateCode (code: string) {
        if (this.publicKey != ctx.publicKey) {
            throw error('invalid owner');
        }
        this.code = code;
        this.ast = parse(code, this.id);
    }
}
"#;

    hm.insert(
        Cow::Borrowed("code"),
        keys::RecordValue::IndexValue(keys::IndexValue::String(Cow::Borrowed(code))),
    );

    let mut program = None;
    let (_, stable_ast) = polylang::parse(code, "", &mut program).unwrap();
    hm.insert(
        Cow::Borrowed("ast"),
        keys::RecordValue::IndexValue(keys::IndexValue::String(Cow::Owned(
            serde_json::to_string(&stable_ast).unwrap(),
        ))),
    );

    serde_json::to_string(&hm).unwrap()
});

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Authorization<'a> {
    Public,
    Private(PrivateAuthorization<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PrivateAuthorization<'a> {
    pub(crate) read_fields: Vec<where_query::FieldPath<'a>>,
    pub(crate) delegate_fields: Vec<where_query::FieldPath<'a>>,
}

#[derive(Clone)]
pub struct Collection<'a> {
    store: &'a store::Store,
    collection_id: String,
    indexes: Vec<index::CollectionIndex<'a>>,
    authorization: Authorization<'a>,
}

pub struct ListQuery<'a> {
    pub limit: Option<usize>,
    pub where_query: &'a where_query::WhereQuery<'a>,
    pub order_by: &'a [index::CollectionIndexField<'a>],
    pub cursor_before: Option<Cursor>,
    pub cursor_after: Option<Cursor>,
}

#[derive(Debug)]
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

#[derive(Debug, Clone)]
pub struct Cursor(keys::Key<'static>);

impl Cursor {
    fn new(key: keys::Key<'static>) -> Result<Self, String> {
        match key {
            keys::Key::Index { .. } => {}
            _ => return Err("Invalid key type, expected index".to_string()),
        }

        Ok(Self(key))
    }
}

impl Serialize for Cursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let buf = self.0.serialize().map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&base64::engine::general_purpose::URL_SAFE.encode(buf))
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let buf = base64::engine::general_purpose::URL_SAFE
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        let key = keys::Key::deserialize(&buf).map_err(serde::de::Error::custom)?;
        Self::new(key.to_static()).map_err(serde::de::Error::custom)
    }
}

impl<'a> Collection<'a> {
    fn new(
        store: &'a store::Store,
        collection_id: String,
        indexes: Vec<index::CollectionIndex<'a>>,
        authorization: Authorization<'a>,
    ) -> Self {
        Self {
            store,
            collection_id,
            indexes,
            authorization,
        }
    }

    pub(crate) fn load(
        store: &'a store::Store,
        id: String,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let collection_collection = Self::new(
            store,
            "Collection".to_string(),
            vec![index::CollectionIndex::new(vec![
                index::CollectionIndexField::new(
                    vec![Cow::Borrowed("id")],
                    keys::Direction::Ascending,
                ),
            ])],
            Authorization::Public,
        );

        if id == "Collection" {
            return Ok(collection_collection);
        }

        let Some(collection) = collection_collection.get( id, None)? else {
            return Err("Collection not found".into());
        };

        let record = collection.borrow_record();
        let id = match record.get("id") {
            Some(keys::RecordValue::IndexValue(keys::IndexValue::String(id))) => id,
            Some(_) => return Err("Collection record id is not a string".into()),
            None => return Err("Collection record missing id".into()),
        };

        let ast: stableast::Root = match record.get("ast") {
            Some(keys::RecordValue::IndexValue(keys::IndexValue::String(ast))) => {
                serde_json::from_str(ast)?
            }
            Some(_) => return Err("Collection record AST is not a string".into()),
            None => return Err("Collection record missing AST".into()),
        };

        let short_collection_name = id.split('/').last().unwrap();
        let Some(collection_ast) = ast.0.iter().find(|ast| matches!(ast, stableast::RootNode::Collection(c) if c.name == short_collection_name)) else {
            return Err("Collection record AST does not contain collection".into());
        };

        let collection_ast = match collection_ast {
            stableast::RootNode::Collection(c) => c,
            _ => unreachable!(),
        };

        let mut indexes = collection_ast
            .attributes
            .iter()
            .filter_map(|attr| match attr {
                stableast::CollectionAttribute::Index(index) => Some(index::CollectionIndex::new(
                    index
                        .fields
                        .iter()
                        .map(|field| {
                            index::CollectionIndexField::new(
                                field
                                    .field_path
                                    .iter()
                                    .map(|p| Cow::Owned(p.to_string()))
                                    .collect(),
                                match field.direction {
                                    stableast::Direction::Asc => keys::Direction::Ascending,
                                    stableast::Direction::Desc => keys::Direction::Descending,
                                },
                            )
                        })
                        .collect(),
                )),
                _ => None,
            })
            .chain([index::CollectionIndex::new(vec![])].into_iter())
            .collect::<Vec<_>>();

        collection_ast.walk_fields(&mut vec![], &mut |path, field| {
            if let stableast::Type::Primitive(_) = field.type_() {
                let new_index = |direction| {
                    index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                        path.iter().map(|p| Cow::Owned(p.to_string())).collect(),
                        direction,
                    )])
                };
                let new_index_asc = new_index(keys::Direction::Ascending);
                let new_index_desc = new_index(keys::Direction::Descending);

                if !indexes.contains(&new_index_asc) && !indexes.contains(&new_index_desc) {
                    indexes.push(new_index_asc);
                }
            }
        });

        let is_public = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "public"));

        Ok(Self {
            store,
            collection_id: id.to_string(),
            indexes,
            authorization: if is_public {
                Authorization::Public
            } else {
                Authorization::Private(PrivateAuthorization {
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
                                .find(|dir| dir.name == "read")
                                .map(|_| {
                                    where_query::FieldPath(vec![Cow::Owned(prop.name.to_string())])
                                })
                        })
                        .collect::<Vec<_>>(),
                    delegate_fields: {
                        let mut delegate_fields = vec![];

                        collection_ast.walk_fields(&mut vec![], &mut |path, field| {
                            if let crate::stableast_ext::Field::Property(p) = field {
                                if p.directives.iter().any(|dir| dir.name == "delegate") {
                                    delegate_fields.push(where_query::FieldPath(
                                        path.iter().map(|p| Cow::Owned(p.to_string())).collect(),
                                    ));
                                }
                            };
                        });

                        delegate_fields
                    },
                })
            },
        })
    }

    pub fn id(&self) -> &str {
        &self.collection_id
    }

    pub fn name(&self) -> &str {
        self.collection_id.split('/').last().unwrap()
    }

    pub fn namespace(&self) -> &str {
        let Some(slash_index) = self.collection_id.rfind('/') else {
            return "";
        };

        &self.collection_id[0..slash_index]
    }

    pub(crate) fn user_can_read(
        &self,
        record: &RecordValue,
        user: &Option<&AuthUser>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let read_fields = match &self.authorization {
            Authorization::Public => return Ok(true),
            Authorization::Private(pa) => &pa.read_fields,
        };

        let Some(user) = user else {
            return Ok(false);
        };

        let mut authorized = false;
        for (key, value) in record {
            value
                .walk_all::<std::convert::Infallible>(
                    &mut vec![Cow::Borrowed(key)],
                    &mut |path, value| {
                        if !read_fields.iter().any(|rf| rf.0 == path) {
                            return Ok(());
                        }

                        match value {
                            keys::RecordValue::IndexValue(keys::IndexValue::PublicKey(
                                record_pk,
                            )) if record_pk.as_ref().as_ref() == &user.public_key => {
                                authorized = true;
                            }
                            keys::RecordValue::Map(_) => {
                                let Ok(record_reference) = keys::RecordReference::try_from(value) else {
                                    return Ok(());
                                };

                                let Ok(collection) = record_reference.collection_id.map_or(Ok(Cow::Borrowed(self)), |collection_id| {
                                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Cow::Owned(Collection::load(self.store, collection_id)?))
                                }) else {
                                    // TODO: log the error
                                    return Ok(());
                                };

                                let Ok(Some(record)) = collection.get(record_reference.id, Some(user)) else {
                                    return Ok(());
                                };

                                if collection.has_delegate_access(record.borrow_record(), &Some(user)).unwrap_or(false) {
                                    authorized = true;
                                }
                            }
                            _ => {}
                        }

                        Ok(())
                    },
                )
                .unwrap(); // We never return an error
        }

        if !authorized {
            authorized =
                self.has_delegate_access(&keys::RecordValue::Map(record.clone()), &Some(user))?;
        }

        Ok(authorized)
    }

    fn user_can_read_lazy<'b>(
        &self,
        record_getter: impl FnOnce() -> Result<
            Option<&'b RecordValue<'b>>,
            Box<dyn Error + Send + Sync + 'static>,
        >,
        user: Option<&AuthUser>,
    ) -> Result<bool, Box<dyn Error + Send + Sync + 'static>> {
        match &self.authorization {
            Authorization::Public => Ok(true),
            Authorization::Private(_) => match (record_getter()?, user) {
                (None, _) => Ok(true),
                (Some(_), None) => Ok(false),
                (Some(old_value), Some(auth_user)) => {
                    Ok(self.user_can_read(old_value, &Some(auth_user))?)
                }
            },
        }
    }

    pub fn has_delegate_access<'b>(
        &self,
        record: &impl PathFinder<'b>,
        user: &Option<&AuthUser>,
    ) -> Result<bool, Box<dyn Error + Send + Sync + 'static>> {
        let delegate_fields = match &self.authorization {
            Authorization::Public => return Ok(true),
            Authorization::Private(pa) => &pa.delegate_fields,
        };
        dbg!(&delegate_fields);

        let Some(user) = user else { return Ok(false) };

        for delegate_value in delegate_fields.iter().map(|df| record.find_path(&df.0)) {
            let Some(delegate_value) = delegate_value else {
                continue;
            };

            match delegate_value {
                keys::RecordValue::IndexValue(IndexValue::PublicKey(pk))
                    if pk.as_ref().as_ref() == &user.public_key =>
                {
                    return Ok(true);
                }
                keys::RecordValue::Map(_) => {
                    let Ok(record_ref) = RecordReference::try_from(delegate_value) else {
                        continue;
                    };

                    let collection = match record_ref.collection_id {
                        Some(collection_id) => {
                            Cow::Owned(Collection::load(self.store, collection_id)?)
                        }
                        None => Cow::Borrowed(self),
                    };

                    let Some(record) = collection.get(record_ref.id, Some(user))? else {
                        continue;
                    };

                    if collection
                        .has_delegate_access(record.borrow_record(), &Some(user))
                        .unwrap_or(false)
                    {
                        return Ok(true);
                    }
                }
                _ => {}
            }
        }

        Ok(false)
    }

    pub fn set(
        &self,
        id: String,
        value: &HashMap<Cow<str>, keys::RecordValue>,
        auth_user: Option<&AuthUser>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        match value.get("id") {
            Some(rv) => match rv {
                keys::RecordValue::IndexValue(keys::IndexValue::String(record_id)) => {
                    if &id != record_id {
                        return Err("id must match the record_id".into());
                    }
                }
                _ => return Err("id must be a string".into()),
            },
            None => return Err("id is required".into()),
        }

        let data_key = keys::Key::new_data(self.collection_id.clone(), id)?;
        let store_record_value = self.store.get(&data_key)?;
        if !self.user_can_read_lazy(
            || Ok(store_record_value.as_ref().map(|sv| sv.borrow_record())),
            auth_user,
        )? {
            return Err("unauthorized".into());
        }

        self.store
            .set(&data_key, &store::Value::DataValue(Cow::Borrowed(value)))?;

        // TODO: ignore index failures
        let index_value = store::Value::IndexValue(proto::IndexRecord {
            id: data_key.serialize()?,
        });
        for index in self.indexes.iter() {
            let index_key = keys::index_record_key_with_record(
                self.collection_id.clone(),
                &index.fields.iter().map(|f| &f.path[..]).collect::<Vec<_>>(),
                &index.fields.iter().map(|f| f.direction).collect::<Vec<_>>(),
                value,
            )?;

            self.store.set(&index_key, &index_value)?;
        }

        Ok(())
    }

    pub fn get(
        &self,
        id: String,
        user: Option<&AuthUser>,
    ) -> Result<Option<StoreRecordValue>, Box<dyn Error + Send + Sync + 'static>> {
        if self.collection_id == "Collection" && id == "Collection" {
            return Ok(Some(StoreRecordValue::new_from_static(
                COLLECTION_COLLECTION_RECORD.as_bytes(),
            )?));
        }

        let key = keys::Key::new_data(self.collection_id.clone(), id)?;
        let Some(value) = self.store.get(&key)? else {
            return Ok(None);
        };

        if !self.user_can_read(value.borrow_record(), &user)? {
            return Err("unauthorized".into());
        }

        Ok(Some(value))
    }

    pub fn list(
        &'a self,
        query: ListQuery,
        user: &'a Option<&'a AuthUser>,
    ) -> Result<
        impl Iterator<
                Item = Result<
                    (Cursor, StoreRecordValue<'a>),
                    Box<dyn Error + Send + Sync + 'static>,
                >,
            > + '_,
        Box<dyn Error + Send + Sync + 'static>,
    > {
        let Some(index) = self.indexes.iter().find(|index| index.matches(query.where_query, query.order_by)) else {
            return Err("No index found matching the query".into());
        };

        let key_range = query
            .where_query
            .to_key_range(
                self.collection_id.clone(),
                &index.fields.iter().map(|f| &f.path[..]).collect::<Vec<_>>(),
                &index.fields.iter().map(|f| f.direction).collect::<Vec<_>>(),
            )
            .map_err(|e| e.to_string())?;

        let key_range = where_query::KeyRange {
            lower: key_range.lower.to_static(),
            upper: key_range.upper.to_static(),
        };

        let mut reverse = index.should_list_in_reverse(query.order_by);
        let key_range = match (query.cursor_after, query.cursor_before) {
            (Some(mut after), _) => {
                after.0.immediate_successor_value_mut()?;
                where_query::KeyRange {
                    lower: after.0,
                    upper: key_range.upper,
                }
            }
            (_, Some(before)) => {
                reverse = !reverse;
                where_query::KeyRange {
                    lower: key_range.lower,
                    upper: before.0,
                }
            }
            (None, None) => key_range,
        };

        Ok(self
            .store
            .list(&key_range.lower, &key_range.upper, reverse)?
            .map(|res| -> Result<_, Box<dyn Error + Send + Sync + 'static>> {
                let (k, v) = res?;

                let index_key = Cursor::new(keys::Key::deserialize(&k)?.to_static())?;
                let index_record = proto::IndexRecord::decode(&v[..])?;
                let data_key = keys::Key::deserialize(&index_record.id)?;
                let data = match self.store.get(&data_key)? {
                    Some(d) => d,
                    None => return Ok(None),
                };

                Ok(Some((index_key, data)))
            })
            .filter_map(|r| match r {
                // Skip records that we couldn't find by the data key
                Ok(None) => None,
                Ok(Some(x)) => Some(Ok(x)),
                Err(e) => Some(Err(e)),
            })
            .filter_map(
                |r| -> Option<Result<_, Box<dyn Error + Send + Sync + 'static>>> {
                    match r {
                        Ok((cursor, sv)) => {
                            if !self
                                .user_can_read(sv.borrow_record(), user)
                                // TODO: should we propagate this error?
                                .unwrap_or(false)
                            {
                                // Skip records that the user can't read
                                return None;
                            }

                            Some(Ok((cursor, sv)))
                        }
                        Err(e) => Some(Err(e)),
                    }
                },
            )
            .take(query.limit.unwrap_or(usize::MAX)))
    }
}

#[cfg(test)]
mod tests {
    use crate::{publickey, store::tests::TestStore};

    use super::*;

    #[test]
    fn test_collection_collection_load() {
        let store = TestStore::default();
        let collection = Collection::load(&store, "Collection".to_string()).unwrap();

        assert_eq!(collection.collection_id, "Collection");
        assert_eq!(collection.authorization, Authorization::Public);
        assert_eq!(collection.indexes.len(), 1);
        assert_eq!(
            collection.indexes[0],
            index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                vec!["id".into()],
                keys::Direction::Ascending
            )])
        );
    }

    fn create_collection<'a>(store: &'a TestStore, ast: stableast::Root) -> Vec<Collection<'a>> {
        let collection_collection = Collection::load(store, "Collection".to_string()).unwrap();

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
                .set(
                    id.clone(),
                    &{
                        let mut map = HashMap::new();

                        map.insert(
                            Cow::Borrowed("id"),
                            keys::RecordValue::IndexValue(keys::IndexValue::String(Cow::Borrowed(
                                &id,
                            ))),
                        );
                        map.insert(
                            Cow::Borrowed("ast"),
                            keys::RecordValue::IndexValue(keys::IndexValue::String(Cow::Owned(
                                ast_json.clone(),
                            ))),
                        );

                        map
                    },
                    None,
                )
                .unwrap();

            collections.push(Collection::load(store, id).unwrap());
        }

        collections
    }

    #[test]
    fn test_create_collection() {
        let store = TestStore::default();

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
        .into_iter()
        .next()
        .unwrap();

        assert_eq!(collection_account.collection_id, "ns/Account");
        assert_eq!(
            collection_account.authorization,
            Authorization::Private(PrivateAuthorization {
                read_fields: vec![],
                delegate_fields: vec![],
            })
        );
        assert_eq!(collection_account.indexes.len(), 3);
        assert_eq!(
            collection_account.indexes[0],
            index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                vec!["id".into()],
                keys::Direction::Ascending
            )])
        );
        assert_eq!(
            collection_account.indexes[1],
            index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                vec!["balance".into()],
                keys::Direction::Ascending
            )])
        );
        assert_eq!(
            collection_account.indexes[2],
            index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                vec!["info".into(), "name".into()],
                keys::Direction::Ascending
            )])
        );
    }

    #[test]
    fn test_collection_set_get() {
        let store = TestStore::default();
        let collection = Collection::new(&store, "test".to_string(), vec![], Authorization::Public);

        let value_json = r#"{"id": "1", "name": "test" }"#;
        let value =
            serde_json::from_str::<HashMap<Cow<str>, keys::RecordValue>>(value_json).unwrap();

        collection.set("1".into(), &value, None).unwrap();

        let record = collection.get("1".into(), None).unwrap().unwrap();
        assert_eq!(
            record.borrow_record().get("id").unwrap(),
            &keys::RecordValue::IndexValue(keys::IndexValue::String("1".into()))
        );
        assert_eq!(
            record.borrow_record().get("name").unwrap(),
            &keys::RecordValue::IndexValue(keys::IndexValue::String("test".into()))
        );
    }

    #[test]
    fn test_collection_set_list() {
        let store = TestStore::default();
        let collection = Collection::new(
            &store,
            "test".to_string(),
            vec![index::CollectionIndex {
                fields: vec![
                    index::CollectionIndexField {
                        path: vec!["name".into()],
                        direction: keys::Direction::Ascending,
                    },
                    index::CollectionIndexField {
                        path: vec!["id".into()],
                        direction: keys::Direction::Ascending,
                    },
                ],
            }],
            Authorization::Public,
        );

        let value_1_json = r#"{"id": "1", "name": "test" }"#;
        let value_1 =
            serde_json::from_str::<HashMap<Cow<str>, keys::RecordValue>>(value_1_json).unwrap();
        collection.set("1".into(), &value_1, None).unwrap();

        let value_2_json = r#"{"id": "2", "name": "test" }"#;
        let value_2 =
            serde_json::from_str::<HashMap<Cow<str>, keys::RecordValue>>(value_2_json).unwrap();
        collection.set("2".into(), &value_2, None).unwrap();

        let mut results = collection
            .list(
                ListQuery {
                    limit: None,
                    where_query: &where_query::WhereQuery(
                        [(
                            where_query::FieldPath(vec!["name".into()]),
                            where_query::WhereNode::Equality(where_query::WhereValue::String(
                                "test".into(),
                            )),
                        )]
                        .into(),
                    ),
                    order_by: &[
                        index::CollectionIndexField {
                            path: vec!["name".into()],
                            direction: keys::Direction::Ascending,
                        },
                        index::CollectionIndexField {
                            path: vec!["id".into()],
                            direction: keys::Direction::Descending,
                        },
                    ],
                    cursor_before: None,
                    cursor_after: None,
                },
                &None,
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(results.len(), 2);
        let (_, second) = results.pop().unwrap();
        let (_, first) = results.pop().unwrap();

        assert_eq!(first.borrow_record(), &value_2);
        assert_eq!(second.borrow_record(), &value_1);
    }

    #[test]
    fn test_collection_auth() {
        let store = TestStore::default();
        let collection = Collection::new(
            &store,
            "test".to_string(),
            vec![],
            Authorization::Private(PrivateAuthorization {
                read_fields: vec![where_query::FieldPath(vec!["owner".into()])],
                delegate_fields: vec![],
            }),
        );

        let auth_user = AuthUser {
            public_key: publickey::PublicKey::random(),
        };

        collection
            .set(
                "1".into(),
                &[
                    (
                        "id".into(),
                        keys::RecordValue::IndexValue(keys::IndexValue::String("1".into())),
                    ),
                    (
                        "owner".into(),
                        keys::RecordValue::IndexValue(keys::IndexValue::PublicKey(Box::new(
                            Cow::Borrowed(&auth_user.public_key),
                        ))),
                    ),
                ]
                .into(),
                Some(&auth_user),
            )
            .unwrap();

        // Update without a key fails
        assert_eq!(
            collection
                .set(
                    "1".into(),
                    &[(
                        "id".into(),
                        keys::RecordValue::IndexValue(keys::IndexValue::String("1".into())),
                    )]
                    .into(),
                    None,
                )
                .unwrap_err()
                .to_string(),
            "unauthorized"
        );

        // Update with a different key fails
        assert_eq!(
            collection
                .set(
                    "1".into(),
                    &[(
                        "id".into(),
                        keys::RecordValue::IndexValue(keys::IndexValue::String("1".into())),
                    )]
                    .into(),
                    Some(&AuthUser {
                        public_key: publickey::PublicKey::random(),
                    }),
                )
                .unwrap_err()
                .to_string(),
            "unauthorized"
        );

        // Update with the key stored in `owner` works
        collection
            .set(
                "1".into(),
                &[
                    (
                        "id".into(),
                        keys::RecordValue::IndexValue(keys::IndexValue::String("1".into())),
                    ),
                    (
                        "owner".into(),
                        keys::RecordValue::IndexValue(keys::IndexValue::PublicKey(Box::new(
                            Cow::Borrowed(&auth_user.public_key),
                        ))),
                    ),
                    (
                        "name".into(),
                        keys::RecordValue::IndexValue(keys::IndexValue::String("John".into())),
                    ),
                ]
                .into(),
                Some(&auth_user),
            )
            .unwrap();
    }
}
