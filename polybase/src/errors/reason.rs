use derive_more::Display;

use crate::errors::code::ErrorCode;

#[derive(Debug, Display, PartialEq)]
pub enum ReasonCode {
    #[display(fmt = "record/not-found")]
    RecordNotFound,

    #[display(fmt = "record/id-not-string")]
    RecordIdNotString,

    #[display(fmt = "record/collection-id-not-found")]
    RecordCollectionIdNotFound,

    #[display(fmt = "record/field-not-object")]
    RecordFieldNotObject,

    #[display(fmt = "record/id-modified")]
    RecordIDModified,

    #[display(fmt = "index/missing-index")]
    IndexesMissingIndex,

    #[display(fmt = "function/invalidated-id")]
    FunctionInvalidatedId,

    #[display(fmt = "function/not-found")]
    FunctionNotFound,

    #[display(fmt = "function/invalid-args")]
    FunctionInvalidArgs,

    #[display(fmt = "function/invalid-call")]
    FunctionInvalidCall,

    #[display(fmt = "function/javascript-exception")]
    FunctionJavaScriptException,

    #[display(fmt = "function/collection-error")]
    FunctionCollectionError,

    #[display(fmt = "constructor/no-id-assigned")]
    ConstructorNoId,

    #[display(fmt = "collection/invalid-call")]
    CollectionRecordIdNotFound,

    #[display(fmt = "collection/mismatch")]
    CollectionMismatch,

    #[display(fmt = "collection/not-found")]
    CollectionNotFound,

    #[display(fmt = "collection/id-exists")]
    CollectionIdExists,

    #[display(fmt = "collection/invalid-id")]
    CollectionInvalidId,

    #[display(fmt = "collection/invalid-schema")]
    CollectionInvalidSchema,

    #[display(fmt = "collection/alter-public-key")]
    CollectionCannotChangeFieldTypeToPublicKey,

    #[display(fmt = "indexer/missing-index")]
    IndexerMissingIndex,

    #[display(fmt = "indexer/query-paths-directions-length")]
    IndexerQueryPathsAndDirectionsLengthMismatch,

    #[display(fmt = "indexer/query-inequality-not-last")]
    IndexerQueryInequalityNotLast,

    #[display(fmt = "indexer/invalid-cursor")]
    IndexerInvalidCursorKey,

    #[display(fmt = "unauthorized")]
    Unauthorized,

    #[display(fmt = "internal")]
    Internal,
}

impl ReasonCode {
    pub fn code(&self) -> ErrorCode {
        match self {
            ReasonCode::RecordNotFound => ErrorCode::NotFound,
            ReasonCode::RecordIdNotString => ErrorCode::InvalidArgument,
            ReasonCode::RecordCollectionIdNotFound => ErrorCode::NotFound,
            ReasonCode::RecordFieldNotObject => ErrorCode::InvalidArgument,
            ReasonCode::RecordIDModified => ErrorCode::FailedPrecondition,
            ReasonCode::IndexesMissingIndex => ErrorCode::FailedPrecondition,
            ReasonCode::FunctionInvalidatedId => ErrorCode::FailedPrecondition,
            ReasonCode::FunctionNotFound => ErrorCode::NotFound,
            ReasonCode::FunctionInvalidArgs => ErrorCode::InvalidArgument,
            ReasonCode::FunctionInvalidCall => ErrorCode::InvalidArgument,
            ReasonCode::FunctionJavaScriptException => ErrorCode::FailedPrecondition,
            ReasonCode::FunctionCollectionError => ErrorCode::FailedPrecondition,
            ReasonCode::ConstructorNoId => ErrorCode::InvalidArgument,
            ReasonCode::CollectionNotFound => ErrorCode::NotFound,
            ReasonCode::CollectionIdExists => ErrorCode::AlreadyExists,
            ReasonCode::CollectionInvalidId => ErrorCode::InvalidArgument,
            ReasonCode::CollectionInvalidSchema => ErrorCode::InvalidArgument,
            ReasonCode::CollectionCannotChangeFieldTypeToPublicKey => ErrorCode::InvalidArgument,
            ReasonCode::IndexerMissingIndex => ErrorCode::FailedPrecondition,
            ReasonCode::CollectionMismatch => ErrorCode::InvalidArgument,
            ReasonCode::CollectionRecordIdNotFound => ErrorCode::NotFound,
            ReasonCode::IndexerQueryInequalityNotLast => ErrorCode::InvalidArgument,
            ReasonCode::IndexerQueryPathsAndDirectionsLengthMismatch => ErrorCode::InvalidArgument,
            ReasonCode::IndexerInvalidCursorKey => ErrorCode::InvalidArgument,
            ReasonCode::Unauthorized => ErrorCode::PermissionDenied,
            ReasonCode::Internal => ErrorCode::Internal,
        }
    }

    pub fn from_gateway_error(err: &gateway::GatewayUserError) -> Self {
        match err {
            gateway::GatewayUserError::RecordNotFound { .. } => ReasonCode::RecordNotFound,

            gateway::GatewayUserError::RecordIdNotString => ReasonCode::RecordIdNotString,

            gateway::GatewayUserError::RecordCollectionIdNotFound => {
                ReasonCode::RecordCollectionIdNotFound
            }

            gateway::GatewayUserError::RecordFieldNotObject => ReasonCode::RecordFieldNotObject,

            gateway::GatewayUserError::RecordIDModified => ReasonCode::RecordIDModified,

            gateway::GatewayUserError::CollectionNotFound { .. } => ReasonCode::CollectionNotFound,

            gateway::GatewayUserError::CollectionIdExists => ReasonCode::CollectionIdExists,

            gateway::GatewayUserError::CollectionRecordIdNotFound => {
                ReasonCode::CollectionRecordIdNotFound
            }

            gateway::GatewayUserError::CollectionMismatch { .. } => ReasonCode::CollectionMismatch,

            gateway::GatewayUserError::FunctionNotFound { .. } => ReasonCode::FunctionNotFound,

            gateway::GatewayUserError::FunctionIncorrectNumberOfArguments { .. } => {
                ReasonCode::FunctionInvalidArgs
            }

            gateway::GatewayUserError::UnauthorizedCall => ReasonCode::Unauthorized,

            gateway::GatewayUserError::JavaScriptException { .. } => {
                ReasonCode::FunctionJavaScriptException
            }

            gateway::GatewayUserError::CollectionFunctionError { .. } => {
                ReasonCode::FunctionCollectionError
            }

            gateway::GatewayUserError::FunctionInvalidArgumentType { .. } => {
                ReasonCode::FunctionInvalidArgs
            }

            gateway::GatewayUserError::ConstructorMustAssignId => ReasonCode::ConstructorNoId,
        }
    }

    pub fn from_where_query_error(err: &indexer::where_query::WhereQueryUserError) -> Self {
        match err {
            indexer::where_query::WhereQueryUserError::PathsAndDirectionsLengthMismatch {
                ..
            } => ReasonCode::IndexerQueryPathsAndDirectionsLengthMismatch,
            indexer::where_query::WhereQueryUserError::InequalityNotLast => {
                ReasonCode::IndexerQueryInequalityNotLast
            }
        }
    }

    pub fn from_collection_error(err: &indexer::collection::CollectionUserError) -> Self {
        match err {
            indexer::collection::CollectionUserError::CollectionNotFound { .. } => {
                ReasonCode::CollectionNotFound
            }
            indexer::collection::CollectionUserError::CannotChangeFieldTypeToPublicKey {
                ..
            } => ReasonCode::CollectionCannotChangeFieldTypeToPublicKey,
            indexer::collection::CollectionUserError::NoIndexFoundMatchingTheQuery => {
                ReasonCode::IndexerMissingIndex
            }
            indexer::collection::CollectionUserError::UnauthorizedRead => ReasonCode::Unauthorized,
            indexer::collection::CollectionUserError::InvalidCursorKey => {
                ReasonCode::IndexerInvalidCursorKey
            }
            indexer::collection::CollectionUserError::CollectionIdMissingNamespace => {
                ReasonCode::CollectionInvalidId
            }
            indexer::collection::CollectionUserError::CollectionNameCannotStartWithDollarSign => {
                ReasonCode::CollectionInvalidId
            }
            indexer::collection::CollectionUserError::MissingDefinitionForCollection { .. } => {
                ReasonCode::CollectionInvalidSchema
            }
            indexer::collection::CollectionUserError::CollectionMissingIdField => {
                ReasonCode::CollectionInvalidSchema
            }
            indexer::collection::CollectionUserError::CollectionIdFieldMustBeString => {
                ReasonCode::CollectionInvalidSchema
            }
            indexer::collection::CollectionUserError::CollectionIdFieldCannotBeOptional => {
                ReasonCode::CollectionInvalidSchema
            }
            indexer::collection::CollectionUserError::IndexFieldNotFoundInSchema { .. } => {
                ReasonCode::CollectionInvalidSchema
            }
            indexer::collection::CollectionUserError::IndexFieldCannotBeAnArray { .. } => {
                ReasonCode::CollectionInvalidSchema
            }
            indexer::collection::CollectionUserError::IndexFieldCannotBeAMap { .. } => {
                ReasonCode::CollectionInvalidSchema
            }
            indexer::collection::CollectionUserError::IndexFieldCannotBeAnObject { .. } => {
                ReasonCode::CollectionInvalidSchema
            }
            indexer::collection::CollectionUserError::IndexFieldCannotBeBytes { .. } => {
                ReasonCode::CollectionInvalidSchema
            }
        }
    }
}