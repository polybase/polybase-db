use std::time::SystemTime;

use crate::db::Database;
use crate::publickey::PublicKey;
use crate::record::RecordError;
use serde::{Deserialize, Serialize};

/// The generic collection functionality

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
    RecordError(#[from] RecordError),

    #[error("parse int error")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("system time error")]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("serde_json error")]
    SerdeJSONError(#[from] serde_json::Error),

    #[error("prost decode error")]
    ProstDecodeError(#[from] prost::DecodeError),

    #[error(transparent)]
    ConcreteCollectionError(#[from] Box<dyn std::error::Error>),
}

unsafe impl Send for CollectionError {}

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

#[derive(Clone)]
pub struct Collection<'c, D: Database> {
    pub store: &'c D,
    pub collection_id: String,
}

impl<'c, D> Collection<'c, D>
where
    D: Database,
{
    pub fn id(&self) -> &str {
        &self.collection_id
    }

    pub fn normalize_name(collection_id: &str) -> String {
        #[allow(clippy::unwrap_used)] // split always returns at least one element
        let last_part = collection_id.split('/').last().unwrap();

        last_part.replace('-', "_")
    }

    pub fn name(&self) -> String {
        Self::normalize_name(self.collection_id.as_str())
    }

    pub fn namespace(&self) -> &str {
        let Some(slash_index) = self.collection_id.rfind('/') else {
            return "";
        };

        &self.collection_id[0..slash_index]
    }
}

pub struct CollectionMetadata {
    pub last_record_updated_at: SystemTime,
}

pub struct RecordMetadata {
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthUser {
    pub public_key: PublicKey,
}

impl AuthUser {
    pub fn new(public_key: PublicKey) -> Self {
        Self { public_key }
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}
