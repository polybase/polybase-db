use super::record::{self, IndexValueError};
use super::{cursor, where_query};
use crate::store;

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

    #[error("store error")]
    Store(#[from] store::Error),

    #[error("index value error")]
    IndexValue(#[from] IndexValueError),

    #[error("cursor error")]
    CursorError(#[from] cursor::Error),
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

    #[error("invalid cursor, before and after cannot be used together")]
    InvalidCursorBeforeAndAfterSpecified,

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

    #[error("record user error")]
    RecordUserError(#[from] record::RecordUserError),
}
