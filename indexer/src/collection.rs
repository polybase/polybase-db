use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crate::{
    index,
    job_engine::jobs,
    json_to_record, keys, proto,
    publickey::PublicKey,
    record::{self, PathFinder, RecordRoot, RecordValue},
    record_to_json,
    stableast_ext::FieldWalker,
    store::{self},
    where_query, Indexer, IndexerError,
};
use async_recursion::async_recursion;
use base64::Engine;
use futures::StreamExt;
use once_cell::sync::Lazy;
use polylang::stableast;
use prost::Message;
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, CollectionError>;

#[derive(Debug, thiserror::Error)]
pub enum CollectionError {
    #[error(transparent)]
    UserError(#[from] CollectionUserError),

    #[error("collection {name} not found in AST")]
    CollectionNotFoundInAST { name: String },

    #[error("collection record ID is not a string")]
    CollectionRecordIDIsNotAString,

    #[error("collection record AST is not a string")]
    CollectionRecordASTIsNotAString,

    #[error("collection record missing ID")]
    CollectionRecordMissingID,

    #[error("collection record missing AST")]
    CollectionRecordMissingAST,

    #[error("metadata is missing lastRecordUpdatedAt")]
    MetadataMissingLastRecordUpdatedAt,

    #[error("metadata is missing updatedAt")]
    MetadataMissingUpdatedAt,

    #[error("record ID argument does not match record data ID value")]
    RecordIDArgDoesNotMatchRecordDataID,

    #[error("record ID must be a string")]
    RecordIDMustBeAString,

    #[error("record is missing ID field")]
    RecordMissingID,

    #[error("Collection collection record not found for collection {id:?}")]
    CollectionCollectionRecordNotFound { id: String },

    #[error("keys error")]
    KeysError(#[from] keys::KeysError),

    #[error("store error")]
    StoreError(#[from] store::StoreError),

    #[error("where query error")]
    WhereQueryError(#[from] where_query::WhereQueryError),

    #[error("record error")]
    RecordError(#[from] record::RecordError),

    #[error("parse int error")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("system time error")]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("serde_json error")]
    SerdeJSONError(#[from] serde_json::Error),

    #[error("prost decode error")]
    ProstDecodeError(#[from] prost::DecodeError),

    // jobs run inside the job engine may raise this error
    #[error("indexer error")]
    IndexerError(#[from] Box<IndexerError>),
}

#[derive(Debug, thiserror::Error)]
pub enum CollectionUserError {
    #[error("collection {name} not found")]
    CollectionNotFound { name: String },

    #[error("no index found matching the query")]
    NoIndexFoundMatchingTheQuery,

    #[error("unauthorized read")]
    UnauthorizedRead,

    #[error("invalid index key")]
    InvalidCursorKey,

    #[error("collection id is missing namespace")]
    CollectionIdMissingNamespace,

    #[error("collection name cannot start with '$'")]
    CollectionNameCannotStartWithDollarSign,

    #[error("collection must have an 'id' field")]
    CollectionMissingIdField,

    #[error("collection 'id' field must be a string")]
    CollectionIdFieldMustBeString,

    #[error("collection 'id' field cannot be optional")]
    CollectionIdFieldCannotBeOptional,

    #[error("code is missing definition for collection {name}")]
    MissingDefinitionForCollection { name: String },

    #[error("index field {field:?} not found in schema")]
    IndexFieldNotFoundInSchema { field: String },

    #[error("cannot index field {field:?} of type array")]
    IndexFieldCannotBeAnArray { field: String },

    #[error("cannot index field {field:?} of type map")]
    IndexFieldCannotBeAMap { field: String },

    #[error("cannot index field {field:?} of type object")]
    IndexFieldCannotBeAnObject { field: String },

    #[error("cannot index field {field:?} of type bytes")]
    IndexFieldCannotBeBytes { field: String },

    #[error("collection directive {directive:?} cannot have arguments")]
    CollectionDirectiveCannotHaveArguments { directive: &'static str },

    #[error("unknown collection directives {directives:?}")]
    UnknownCollectionDirectives { directives: Vec<String> },
}

static COLLECTION_COLLECTION_RECORD: Lazy<RecordRoot> = Lazy::new(|| {
    let mut hm = HashMap::new();

    hm.insert(
        "id".to_string(),
        RecordValue::String("Collection".to_string()),
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
        "code".to_string(),
        // The replaces are for clients <=0.3.23
        RecordValue::String(code.replace("@public", "").replace("PublicKey", "string")),
    );

    let mut program = None;
    #[allow(clippy::unwrap_used)]
    let (_, stable_ast) = polylang::parse(code, "", &mut program).unwrap();
    hm.insert(
        "ast".to_string(),
        #[allow(clippy::unwrap_used)]
        RecordValue::String(serde_json::to_string(&stable_ast).unwrap()),
    );

    hm
});

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Authorization {
    /// Anyone can read the collection.
    pub(crate) read_all: bool,
    /// Anyone can call the collection functions.
    pub(crate) call_all: bool,
    /// PublicKeys/Delegates in this list can read the collection.
    pub(crate) read_fields: Vec<where_query::FieldPath>,
    /// PublicKeys/Delegates in this list have delegate permissions,
    /// i.e. if someone @read's a field with a record from this collection,
    /// anyone in the delegate list can read that record.
    pub(crate) delegate_fields: Vec<where_query::FieldPath>,
}

#[derive(Clone)]
pub struct Collection<'a> {
    logger: slog::Logger,
    indexer: &'a Indexer,
    collection_id: String,
    indexes: Vec<index::CollectionIndex<'a>>,
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
    pub where_query: where_query::WhereQuery,
    pub order_by: &'a [index::CollectionIndexField<'a>],
    pub cursor_before: Option<Cursor>,
    pub cursor_after: Option<Cursor>,
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

#[derive(Debug, Clone)]
pub struct Cursor(keys::Key<'static>);

impl Cursor {
    fn new(key: keys::Key<'static>) -> Result<Self> {
        match key {
            keys::Key::Index { .. } => {}
            _ => return Err(CollectionUserError::InvalidCursorKey)?,
        }

        Ok(Self(key))
    }

    pub fn immediate_successor(mut self) -> Result<Self> {
        self.0.immediate_successor_value_mut()?;
        Ok(self)
    }
}

impl Serialize for Cursor {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let buf = self.0.serialize().map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(buf))
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let buf = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        let key = keys::Key::deserialize(&buf).map_err(serde::de::Error::custom)?;
        Self::new(key.with_static()).map_err(serde::de::Error::custom)
    }
}

fn collection_ast_from_root<'a>(
    ast: stableast::Root<'a>,
    collection_name: &str,
) -> Option<stableast::Collection<'a>> {
    ast.0.into_iter().find_map(|node| match node {
        polylang::stableast::RootNode::Collection(collection)
            if collection.name == collection_name =>
        {
            Some(collection)
        }
        _ => None,
    })
}

pub fn collection_ast_from_json<'a>(
    ast_json: &'a str,
    collection_name: &str,
) -> Result<stableast::Collection<'a>> {
    let ast = serde_json::from_str::<polylang::stableast::Root>(ast_json)?;
    let Some(collection_ast) = collection_ast_from_root(ast, collection_name) else {
        return Err(CollectionError::CollectionNotFoundInAST { name: collection_name.to_string() });
    };

    Ok(collection_ast)
}

pub fn validate_schema_change(
    collection_name: &str,
    old_ast: stableast::Root,
    new_ast: stableast::Root,
) -> Result<()> {
    let Some(_old_ast) = collection_ast_from_root(old_ast, collection_name) else {
        return Err(CollectionError::CollectionNotFoundInAST { name: collection_name.to_string() });
    };
    let Some(_new_ast) = collection_ast_from_root(new_ast, collection_name) else {
        return Err(CollectionError::CollectionNotFoundInAST { name: collection_name.to_string() });
    };

    Ok(())
}

pub fn validate_collection_record(record: &RecordRoot) -> Result<()> {
    let (namespace, name) = if let Some(RecordValue::String(id)) = record.get("id") {
        let Some((namespace, name)) = id.rsplit_once('/') else {
                return Err(CollectionUserError::CollectionIdMissingNamespace)?;
            };

        (namespace, name)
    } else {
        unreachable!()
    };

    if namespace.is_empty() {
        return Err(CollectionUserError::CollectionIdMissingNamespace.into());
    }

    if name.starts_with('$') {
        return Err(CollectionUserError::CollectionNameCannotStartWithDollarSign.into());
    }

    let Some(ast) = record.get("ast") else {
        return Err(CollectionError::CollectionRecordMissingAST);
    };

    let ast = match ast {
        RecordValue::String(ast) => ast,
        _ => return Err(CollectionError::CollectionRecordASTIsNotAString),
    };

    let ast = serde_json::from_str::<polylang::stableast::Root>(ast)?;

    let Some(collection) = collection_ast_from_root(ast, &Collection::normalize_name( name)) else {
        return Err(CollectionUserError::MissingDefinitionForCollection { name: name.to_owned() }.into());
    };

    let properties = collection
        .attributes
        .iter()
        .filter_map(|a| match a {
            stableast::CollectionAttribute::Property(p) => Some(p),
            _ => None,
        })
        .collect::<Vec<_>>();

    let Some(id_property) = properties.iter().find(|p| p.name == "id") else {
        return Err(CollectionUserError::CollectionMissingIdField.into());
    };

    if id_property.type_
        != stableast::Type::Primitive(stableast::Primitive {
            value: stableast::PrimitiveType::String,
        })
    {
        return Err(CollectionUserError::CollectionIdFieldMustBeString.into());
    }

    if !id_property.required {
        return Err(CollectionUserError::CollectionIdFieldCannotBeOptional.into());
    }

    let indexes = collection
        .attributes
        .iter()
        .filter_map(|a| match a {
            stableast::CollectionAttribute::Index(i) => Some(i),
            _ => None,
        })
        .collect::<Vec<_>>();

    for index in indexes {
        for index_field in &index.fields {
            let Some(field) = collection.find_field(&index_field.field_path) else {
                return Err(CollectionUserError::IndexFieldNotFoundInSchema {
                    field: index_field.field_path.join("."),
                }
                .into());
            };

            match field.type_() {
                stableast::Type::Array(_) => {
                    return Err(CollectionUserError::IndexFieldCannotBeAnArray {
                        field: index_field.field_path.join("."),
                    }
                    .into());
                }
                stableast::Type::Map(_) => {
                    return Err(CollectionUserError::IndexFieldCannotBeAMap {
                        field: index_field.field_path.join("."),
                    }
                    .into());
                }
                stableast::Type::Object(_) => {
                    return Err(CollectionUserError::IndexFieldCannotBeAnObject {
                        field: index_field.field_path.join("."),
                    }
                    .into());
                }
                stableast::Type::Primitive(stableast::Primitive {
                    value: stableast::PrimitiveType::Bytes,
                }) => {
                    return Err(CollectionUserError::IndexFieldCannotBeBytes {
                        field: index_field.field_path.join("."),
                    }
                    .into());
                }
                _ => {}
            }
        }
    }

    let directives = collection
        .attributes
        .iter()
        .filter_map(|a| match a {
            stableast::CollectionAttribute::Directive(d) => Some(d),
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(public_directive) = directives.iter().find(|d| d.name == "public") {
        if !public_directive.arguments.is_empty() {
            return Err(
                CollectionUserError::CollectionDirectiveCannotHaveArguments {
                    directive: "public",
                }
                .into(),
            );
        }
    }
    if let Some(read_directive) = directives.iter().find(|d| d.name == "read") {
        if !read_directive.arguments.is_empty() {
            return Err(
                CollectionUserError::CollectionDirectiveCannotHaveArguments { directive: "read" }
                    .into(),
            );
        }
    }
    if let Some(call_directive) = directives.iter().find(|d| d.name == "call") {
        if !call_directive.arguments.is_empty() {
            return Err(
                CollectionUserError::CollectionDirectiveCannotHaveArguments { directive: "call" }
                    .into(),
            );
        }
    }

    const VALID_COLLECTION_DIRECTIVES: &[&str] = &["public", "read", "call"];
    let unknown_directives = directives
        .iter()
        .filter(|d| !VALID_COLLECTION_DIRECTIVES.contains(&d.name.as_ref()))
        .map(|d| d.name.as_ref().to_owned())
        .collect::<Vec<_>>();
    if !unknown_directives.is_empty() {
        return Err(CollectionUserError::UnknownCollectionDirectives {
            directives: unknown_directives,
        }
        .into());
    }

    Ok(())
}

impl<'a> Collection<'a> {
    fn new(
        logger: slog::Logger,
        indexer: &'a Indexer,
        collection_id: String,
        indexes: Vec<index::CollectionIndex<'a>>,
        authorization: Authorization,
    ) -> Self {
        Self {
            logger: logger.new(slog::o!("collection" => collection_id.clone())),
            indexer,
            collection_id,
            indexes,
            authorization,
        }
    }

    pub(crate) async fn load(
        logger: slog::Logger,
        indexer: &'a Indexer,
        id: String,
    ) -> Result<Collection<'a>> {
        let collection_collection = Self::new(
            logger.clone(),
            indexer,
            "Collection".to_string(),
            vec![
                index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                    vec![Cow::Borrowed("id")],
                    keys::Direction::Ascending,
                )]),
                index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                    vec![Cow::Borrowed("name")],
                    keys::Direction::Ascending,
                )]),
                index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                    vec![Cow::Borrowed("lastRecordUpdated")],
                    keys::Direction::Ascending,
                )]),
                index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                    vec![Cow::Borrowed("publicKey")],
                    keys::Direction::Ascending,
                )]),
            ],
            Authorization {
                read_all: true,
                call_all: true,
                read_fields: vec![],
                delegate_fields: vec![],
            },
        );

        if id == "Collection" {
            return Ok(collection_collection);
        }

        let Some(record) = collection_collection.get(id.clone(), None).await? else {
            return Err(CollectionUserError::CollectionNotFound { name: id })?;
        };

        let id = match record.get("id") {
            Some(RecordValue::String(id)) => id,
            Some(_) => return Err(CollectionError::CollectionRecordIDIsNotAString),
            None => return Err(CollectionError::CollectionRecordMissingID),
        };

        let short_collection_name = Collection::normalize_name(id.as_str());

        let collection_ast: stableast::Collection = match record.get("ast") {
            Some(RecordValue::String(ast)) => {
                collection_ast_from_json(ast, short_collection_name.as_str())?
            }
            Some(_) => return Err(CollectionError::CollectionRecordASTIsNotAString),
            None => return Err(CollectionError::CollectionRecordMissingAST),
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
            let indexable = matches!(
                field.type_(),
                stableast::Type::Primitive(_) | stableast::Type::PublicKey(_)
            );

            if indexable {
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

        // Sort indexes by number of fields, so that we use the most specific index first
        indexes.sort_by(|a, b| a.fields.len().cmp(&b.fields.len()));

        let is_public = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "public"));
        let is_read_all = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "read" && d.arguments.is_empty()));
        let is_call_all = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "call" && d.arguments.is_empty()));

        Ok(Self {
            logger: logger.clone(),
            indexer,
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
                            .find(|dir| dir.name == "read")
                            .map(|_| where_query::FieldPath(vec![prop.name.to_string()]))
                    })
                    .collect::<Vec<_>>(),
                delegate_fields: {
                    let mut delegate_fields = vec![];

                    collection_ast.walk_fields(&mut vec![], &mut |path, field| {
                        if let crate::stableast_ext::Field::Property(p) = field {
                            if p.directives.iter().any(|dir| dir.name == "delegate") {
                                delegate_fields.push(where_query::FieldPath(
                                    path.iter().map(|p| p.to_string()).collect(),
                                ));
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
        let Some(record) = Self::load(self.logger.clone(), self.indexer, "Collection".to_owned())
            .await?
            .get(self.collection_id.clone(), None)
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
        #[allow(clippy::unwrap_used)] // split always returns at least one element
        let last_part = collection_id.split('/').last().unwrap();

        last_part.replace('-', "_")
    }

    pub fn namespace(&self) -> &str {
        let Some(slash_index) = self.collection_id.rfind('/') else {
            return "";
        };

        &self.collection_id[0..slash_index]
    }

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
                let Some(record) = self.get(record_reference.id, Some(user)).await? else {
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
                let collection = Collection::load(
                    self.logger.clone(),
                    self.indexer,
                    foreign_record_reference.collection_id,
                )
                .await?;

                let Some(record) = collection
                    .get(foreign_record_reference.id, Some(user))
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

            #[async_recursion]
            async fn check_delegate_value(
                self_col: &Collection<'_>,
                delegate_value: &RecordValue,
                user: &AuthUser,
            ) -> Result<bool> {
                match delegate_value {
                    RecordValue::PublicKey(pk) if pk == &user.public_key => {
                        return Ok(true);
                    }
                    RecordValue::RecordReference(r) => {
                        let Some(record) = self_col.get(r.id.clone(), Some(user)).await? else {
                            return Ok(false);
                        };

                        if self_col
                            .has_delegate_access(&record, &Some(user))
                            .await
                            .unwrap_or(false)
                        {
                            return Ok(true);
                        }
                    }
                    RecordValue::ForeignRecordReference(fr) => {
                        let collection = Collection::load(
                            self_col.logger.clone(),
                            self_col.indexer,
                            fr.collection_id.clone(),
                        )
                        .await?;

                        let Some(record) = collection.get(fr.id.clone(), Some(user)).await? else {
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
                            if check_delegate_value(self_col, item, user).await? {
                                return Ok(true);
                            }
                        }
                    }
                    _ => {}
                }

                Ok(false)
            }

            if check_delegate_value(self, delegate_value, user).await? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn update_metadata(&self, time: &SystemTime) -> Result<()> {
        let collection_metadata_key =
            keys::Key::new_system_data(format!("{}/metadata", &self.collection_id))?;

        self.indexer
            .store
            .set(
                &collection_metadata_key,
                &store::Value::DataValue(
                    &[(
                        "lastRecordUpdatedAt".into(),
                        RecordValue::String(
                            time.duration_since(SystemTime::UNIX_EPOCH)?
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

    pub async fn get_metadata(&self) -> Result<Option<CollectionMetadata>> {
        let collection_metadata_key =
            keys::Key::new_system_data(format!("{}/metadata", &self.collection_id))?;

        let Some(record) = self.indexer.store.get(&collection_metadata_key).await? else {
            return Ok(None);
        };

        let last_record_updated_at = match record.find_path(&["lastRecordUpdatedAt"]) {
            Some(RecordValue::String(s)) => {
                SystemTime::UNIX_EPOCH + Duration::from_millis(s.parse()?)
            }
            _ => return Err(CollectionError::MetadataMissingLastRecordUpdatedAt),
        };

        Ok(Some(CollectionMetadata {
            last_record_updated_at,
        }))
    }

    pub async fn update_record_metadata(
        &self,
        record_id: String,
        updated_at: &SystemTime,
    ) -> Result<()> {
        let record_metadata_key = keys::Key::new_system_data(format!(
            "{}/records/{}/metadata",
            &self.collection_id, record_id
        ))?;

        self.indexer
            .store
            .set(
                &record_metadata_key,
                &store::Value::DataValue(
                    &[(
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

    pub async fn get_record_metadata(&self, record_id: &str) -> Result<Option<RecordMetadata>> {
        let record_metadata_key = keys::Key::new_system_data(format!(
            "{}/records/{}/metadata",
            &self.collection_id, record_id
        ))?;

        let Some(record) = self.indexer.store.get(&record_metadata_key).await? else {
            return Ok(None);
        };

        let updated_at = match record.find_path(&["updatedAt"]) {
            Some(RecordValue::String(s)) => {
                SystemTime::UNIX_EPOCH + Duration::from_millis(s.parse()?)
            }
            _ => return Err(CollectionError::MetadataMissingUpdatedAt),
        };

        Ok(Some(RecordMetadata { updated_at }))
    }

    pub async fn set(&self, id: String, value: &RecordRoot) -> Result<()> {
        match value.get("id") {
            Some(rv) => match rv {
                RecordValue::String(record_id) => {
                    if &id != record_id {
                        return Err(CollectionError::RecordIDArgDoesNotMatchRecordDataID);
                    }
                }
                _ => return Err(CollectionError::RecordIDMustBeAString),
            },
            None => return Err(CollectionError::RecordMissingID),
        }

        let collection_before = if self.collection_id == "Collection" {
            match Collection::load(self.logger.clone(), self.indexer, id.clone()).await {
                Ok(c) => Some(c),
                Err(CollectionError::UserError(CollectionUserError::CollectionNotFound {
                    ..
                })) => None,
                Err(err) => return Err(err),
            }
        } else {
            None
        };

        let old_value = self.get_without_auth_check(id.clone()).await?;

        let data_key = keys::Key::new_data(self.collection_id.clone(), id.clone())?;

        self.indexer
            .store
            .set(&data_key, &store::Value::DataValue(value))
            .await?;

        self.update_metadata(&SystemTime::now()).await?;
        self.update_record_metadata(id.clone(), &SystemTime::now())
            .await?;

        if let Some(old_value) = &old_value {
            // delete the indexes for the old values
            self.delete_indexes(&id, old_value).await;
        }

        self.indexer
            .enqueue_job(jobs::Job::new(
                self.collection_id.clone(),
                1,
                jobs::JobState::AddIndexes {
                    collection_id: self.collection_id.clone(),
                    id: id.clone(),
                    record: value.clone(),
                },
                true, // is_last_job
            ))
            .await
            .map_err(|e| CollectionError::from(Box::new(e)))?;

        self.indexer
            .await_job_completion(self.collection_id.clone())
            .await
            .map_err(|e| CollectionError::from(Box::new(e)))?;

        if self.collection_id == "Collection" && id != "Collection" {
            if let Some(collection_before) = collection_before {
                // Unwrap is safe because collection_before had to load the existing record.
                #[allow(clippy::unwrap_used)]
                let old_value = old_value.unwrap();

                let target_col = Collection::load(self.logger.clone(), self.indexer, id).await?;

                target_col.rebuild(collection_before, &old_value).await?;
            }
        }

        Ok(())
    }

    pub async fn get(&self, id: String, user: Option<&AuthUser>) -> Result<Option<RecordRoot>> {
        if self.collection_id == "Collection" && id == "Collection" {
            return Ok(Some(COLLECTION_COLLECTION_RECORD.clone()));
        }

        let key = keys::Key::new_data(self.collection_id.clone(), id)?;
        let Some(value) = self.indexer.store.get(&key).await? else {
            return Ok(None);
        };

        if !self.user_can_read(&value, &user).await? {
            return Err(CollectionUserError::UnauthorizedRead)?;
        }

        Ok(Some(value))
    }

    pub async fn get_without_auth_check(&self, id: String) -> Result<Option<RecordRoot>> {
        if self.collection_id == "Collection" && id == "Collection" {
            return Ok(Some(COLLECTION_COLLECTION_RECORD.clone()));
        }

        let key = keys::Key::new_data(self.collection_id.clone(), id)?;
        let Some(value) = self.indexer.store.get(&key).await? else {
            return Ok(None);
        };

        Ok(Some(value))
    }

    pub(crate) async fn add_indexes(
        &self,
        record_id: &str,
        data_key: &keys::Key<'_>,
        record: &RecordRoot,
    ) {
        let index_value = store::Value::IndexValue(proto::IndexRecord {
            id: match data_key.serialize() {
                Ok(data) => data,
                Err(e) => {
                    slog::crit!(self.logger, "failed to serialize data key: {e}");
                    return;
                }
            },
        });

        for index in self.indexes.iter() {
            if let Err(indexing_failure) = async {
                let index_key = keys::index_record_key_with_record(
                    self.collection_id.clone(),
                    &index.fields.iter().map(|f| &f.path[..]).collect::<Vec<_>>(),
                    &index.fields.iter().map(|f| f.direction).collect::<Vec<_>>(),
                    record,
                )?;

                self.indexer.store.set(&index_key, &index_value).await?;

                Ok::<_, CollectionError>(())
            }
            .await
            {
                slog::crit!(
                    self.logger,
                    "indexing failure: {indexing_failure}";
                    "record" => record_id,
                    "index" => index.fields.iter().map(|f| f.path.join(".")).collect::<Vec<_>>().join(", ")
                );
            }
        }
    }

    async fn delete_indexes(&self, record_id: &str, record: &RecordRoot) {
        for index in self.indexes.iter() {
            if let Err(deindexing_failure) = async {
                let index_key = keys::index_record_key_with_record(
                    self.collection_id.clone(),
                    &index.fields.iter().map(|f| &f.path[..]).collect::<Vec<_>>(),
                    &index.fields.iter().map(|f| f.direction).collect::<Vec<_>>(),
                    record,
                )?;

                self.indexer.store.delete(&index_key).await?;

                Ok::<_, CollectionError>(())
            }
            .await
            {
                slog::crit!(
                    self.logger,
                    "failed to delete index: {deindexing_failure}";
                    "record" => record_id,
                    "index" => index.fields.iter().map(|f| f.path.join(".")).collect::<Vec<_>>().join(", ")
                );
            }
        }
    }

    pub async fn delete(&self, id: String) -> Result<()> {
        let Some(record) = self.get_without_auth_check(id.clone()).await? else {
            return Ok(());
        };

        let key = keys::Key::new_data(self.collection_id.clone(), id.clone())?;

        self.indexer.store.delete(&key).await?;

        let now = SystemTime::now();
        self.update_metadata(&now).await?;
        self.update_record_metadata(id.clone(), &now).await?;

        self.delete_indexes(&id, &record).await;

        Ok(())
    }

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
    ) -> Result<impl futures::Stream<Item = Result<(Cursor, RecordRoot)>> + '_> {
        let Some(index) = self.indexes.iter().find(|index| index.matches(&where_query, order_by)) else {
            return Err(CollectionUserError::NoIndexFoundMatchingTheQuery)?;
        };

        let mut ast_holder = None;
        let ast = self.ast(&mut ast_holder).await?;

        let key_range = where_query.key_range(
            &ast,
            self.collection_id.clone(),
            &index.fields.iter().map(|f| &f.path[..]).collect::<Vec<_>>(),
            &index.fields.iter().map(|f| f.direction).collect::<Vec<_>>(),
        )?;

        let key_range = where_query::KeyRange {
            lower: key_range.lower.with_static(),
            upper: key_range.upper.with_static(),
        };

        let mut reverse = index.should_list_in_reverse(order_by);
        let key_range = match (cursor_after, cursor_before) {
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

        Ok(futures::stream::iter(self.indexer.store.list(
            &key_range.lower,
            &key_range.upper,
            reverse,
        )?)
        .map(|res| async {
            let (k, v) = res?;

            let index_key = Cursor::new(keys::Key::deserialize(&k)?.with_static())?;
            let index_record = proto::IndexRecord::decode(&v[..])?;
            let data_key = keys::Key::deserialize(&index_record.id)?;
            let data = match self.indexer.store.get(&data_key).await? {
                Some(d) => d,
                None => return Ok(None),
            };

            Ok(Some((index_key, data)))
        })
        .filter_map(|r| async {
            match r.await {
                // Skip records that we couldn't find by the data key
                Ok(None) => None,
                Ok(Some(x)) => Some(Ok(x)),
                Err(e) => Some(Err(e)),
            }
        })
        .filter_map(|r| async {
            match r {
                Ok((cursor, record)) => {
                    match self.user_can_read(&record, user).await {
                        Ok(false) => None,
                        Ok(true) => Some(Ok((cursor, record))),
                        Err(e) => {
                            // TODO: should we propagate this error?
                            slog::warn!(
                                self.logger,
                                "failed to check if user can read record: {e:#?}",
                            );
                            None
                        }
                    }
                }
                Err(e) => Some(Err(e)),
            }
        })
        .take(limit.unwrap_or(usize::MAX)))
    }

    #[async_recursion]
    async fn rebuild(
        &self,
        // The old collection record, loaded before the AST was changed
        old_collection: Collection<'async_recursion>,
        old_collection_record: &RecordRoot,
    ) -> Result<()> {
        let collection_collection =
            Collection::load(self.logger.clone(), self.indexer, "Collection".to_string()).await?;
        let meta = collection_collection
            .get(self.id().to_string(), None)
            .await?;
        let Some(meta) = meta else {
            return Err(CollectionUserError::CollectionNotFound { name: self.name() })?;
        };

        let collection_ast = match meta.get("ast") {
            Some(RecordValue::String(ast)) => collection_ast_from_json(ast, self.name().as_str())?,
            _ => return Err(CollectionError::CollectionRecordMissingAST),
        };

        let old_collection_ast = match old_collection_record.get("ast") {
            Some(RecordValue::String(ast)) => collection_ast_from_json(ast, self.name().as_str())?,
            _ => return Err(CollectionError::CollectionRecordMissingAST),
        };

        if collection_ast == old_collection_ast {
            // Collection code was not changed, no need to rebuild anything
            return Ok(());
        }

        // TODO: diff old and new ASTs to determine which indexes need to be rebuilt
        // For now, let's just rebuild all indexes

        let start_key = keys::Key::new_index(
            self.id().to_string(),
            &[&["id"]],
            &[keys::Direction::Ascending],
            vec![],
        )?;
        let end_key = start_key.clone().wildcard();
        for entry in self.indexer.store.list(&start_key, &end_key, false)? {
            let (_, value) = entry?;
            let index_record = proto::IndexRecord::decode(&value[..])?;
            let data_key = keys::Key::deserialize(&index_record.id)?;
            let data = self.indexer.store.get(&data_key).await?;
            let Some(data) = data else {
                continue;
            };
            let Some(RecordValue::String(id)) = data.get("id") else {
                return Err(CollectionError::RecordMissingID);
            };
            let id = id.clone();

            let json_data = record_to_json(data)?;
            let new_data = json_to_record(&collection_ast, json_data, true)?;
            // Delete from the old collection object (loaded from old ast), to delete the old data and indexes
            old_collection.delete(id.clone()).await?;
            // Insert into the new collection object (loaded from new ast), to create the new data and indexes
            self.set(id.clone(), &new_data).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use slog::Drain;
    use std::{ops::Deref, sync::Arc};

    use super::*;

    fn logger() -> slog::Logger {
        let decorator = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }

    pub(crate) struct TestIndexer(Option<Arc<Indexer>>);

    impl Default for TestIndexer {
        fn default() -> Self {
            let temp_dir = std::env::temp_dir();
            let mut path = temp_dir.join(format!(
                "test-collection-rocksdb-store-{}",
                rand::random::<u32>()
            ));
            path.push("data/indexer.db");

            Self(Some(Indexer::new(logger(), path).unwrap()))
        }
    }

    impl Drop for TestIndexer {
        fn drop(&mut self) {
            if let Some(indexer) = self.0.take() {
                if let Ok(indexer) = Arc::try_unwrap(indexer) {
                    indexer.destroy().unwrap();
                }
            }
        }
    }

    impl Deref for TestIndexer {
        type Target = Indexer;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    #[tokio::test]
    async fn test_collection_collection_load() {
        let indexer = TestIndexer::default();
        let collection = Collection::load(logger(), &indexer, "Collection".to_string())
            .await
            .unwrap();

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
            index::CollectionIndex::new(vec![index::CollectionIndexField::new(
                vec!["id".into()],
                keys::Direction::Ascending
            )])
        );
    }

    async fn create_collection<'a>(
        indexer: &'a Indexer,
        ast: stableast::Root<'_>,
    ) -> Vec<Collection<'a>> {
        let collection_collection = Collection::load(logger(), indexer, "Collection".to_string())
            .await
            .unwrap();

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
                .set(id.clone(), &{
                    let mut map = HashMap::new();

                    map.insert("id".to_string(), RecordValue::String(id.clone()));
                    map.insert("ast".to_string(), RecordValue::String(ast_json.clone()));

                    map
                })
                .await
                .unwrap();

            indexer.store.commit().await.unwrap();

            collections.push(Collection::load(logger(), indexer, id).await.unwrap());
        }

        collections
    }

    #[tokio::test]
    async fn test_create_collection() {
        let indexer = TestIndexer::default();

        let collection_account = create_collection(
            &indexer,
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

        indexer.store.commit().await.unwrap();

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

    #[tokio::test]
    async fn test_collection_set_get() {
        let indexer = TestIndexer::default();

        let collection = create_collection(
            &indexer,
            stableast::Root(vec![stableast::RootNode::Collection(
                stableast::Collection {
                    namespace: stableast::Namespace { value: "ns".into() },
                    name: "test".into(),
                    attributes: vec![
                        stableast::CollectionAttribute::Property(stableast::Property {
                            name: "id".into(),
                            type_: stableast::Type::Primitive(stableast::Primitive {
                                value: stableast::PrimitiveType::String,
                            }),
                            directives: vec![],
                            required: true,
                        }),
                        stableast::CollectionAttribute::Directive(stableast::Directive {
                            name: "read".into(),
                            arguments: vec![],
                        }),
                    ],
                },
            )]),
        )
        .await
        .into_iter()
        .next()
        .unwrap();

        indexer.store.commit().await.unwrap();

        let value = HashMap::from([("id".to_string(), RecordValue::String("1".into()))]);
        collection.set("1".into(), &value).await.unwrap();

        indexer.store.commit().await.unwrap();

        let record = collection.get("1".into(), None).await.unwrap().unwrap();
        assert_eq!(record.get("id").unwrap(), &RecordValue::String("1".into()));
    }

    #[tokio::test]
    async fn test_collection_set_list() {
        let indexer = TestIndexer::default();

        {
            let collection = Collection::load(logger(), &indexer, "Collection".to_owned())
                .await
                .unwrap();
            collection
                .set(
                    "test/test".to_owned(),
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

        indexer.store.commit().await.unwrap();

        let collection = Collection::load(logger(), &indexer, "test/test".to_owned())
            .await
            .unwrap();

        indexer.store.commit().await.unwrap();

        let value_1 = HashMap::from([
            ("id".to_string(), RecordValue::String("1".into())),
            ("name".to_string(), RecordValue::String("test".into())),
        ]);
        collection.set("1".into(), &value_1).await.unwrap();

        let value_2 = HashMap::from([
            ("id".to_string(), RecordValue::String("2".into())),
            ("name".to_string(), RecordValue::String("test".into())),
        ]);
        collection.set("2".into(), &value_2).await.unwrap();

        indexer.store.commit().await.unwrap();

        let mut results = collection
            .list(
                ListQuery {
                    limit: None,
                    where_query: where_query::WhereQuery(
                        [(
                            where_query::FieldPath(vec!["name".into()]),
                            where_query::WhereNode::Equality(where_query::WhereValue(
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
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        let (_, second) = results.pop().unwrap();
        let (_, first) = results.pop().unwrap();

        assert_eq!(first, value_2);
        assert_eq!(second, value_1);
    }
}
