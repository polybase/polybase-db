use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crate::{
    db::Database,
    publickey::PublicKey,
    record::{self, PathFinder, RecordRoot, RecordValue},
};
use async_recursion::async_recursion;
use base64::Engine;
use futures::StreamExt;
use once_cell::sync::Lazy;
use polylang::stableast;
use prost::Message;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

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

static COLLECTION_COLLECTION_RECORD: Lazy<crate::record::RecordRoot> = Lazy::new(|| {
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

/// The generic collection functionality
#[async_trait::async_trait]
pub trait Collection<'a> {
    type Key;
    type Value;
    type ListQuery;
    type Cursor;

    async fn get_without_auth_check(&self, id: String) -> Result<Option<RecordRoot>>;

    async fn get(&self, id: String, user: Option<&AuthUser>) -> Result<Option<RecordRoot>>;

    async fn get_record_metadata(&self, record_id: &str) -> Result<Option<RecordMetadata>>;

    async fn list(
        &'a self,
        list_query: Self::ListQuery,
        user: &'a Option<&'a AuthUser>,
    ) -> Result<Box<dyn futures::Stream<Item = Result<(Self::Cursor, RecordRoot)>> + 'a>>;

    async fn get_metadata(&self) -> Result<Option<CollectionMetadata>>;
}

pub struct CollectionMetadata {
    pub last_record_updated_at: SystemTime,
}

pub struct RecordMetadata {
    pub updated_at: SystemTime,
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
