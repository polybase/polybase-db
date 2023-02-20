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

    #[display(fmt = "collection/invalid-call")]
    CollectionRecordIdNotFound,

    #[display(fmt = "collection/mismatch")]
    CollectionMismatch,

    #[display(fmt = "collection/not-found")]
    CollectionNotFound,

    #[display(fmt = "collection/id-exist")]
    CollectionIdExists,

    #[display(fmt = "collection/invalid-id")]
    CollectionInvalidId,

    #[display(fmt = "collection/invalid-schema")]
    CollectionInvalidSchema,

    #[display(fmt = "indexer/query/paths-directions-length")]
    IndexerQueryPathsAndDirectionsLengthMismatch,

    #[display(fmt = "indexer/query/inequality-not-last")]
    IndexerQueryInequalityNotLast,

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
            ReasonCode::CollectionNotFound => ErrorCode::NotFound,
            ReasonCode::CollectionIdExists => ErrorCode::AlreadyExists,
            ReasonCode::CollectionInvalidId => ErrorCode::InvalidArgument,
            ReasonCode::CollectionInvalidSchema => ErrorCode::InvalidArgument,
            ReasonCode::CollectionMismatch => ErrorCode::InvalidArgument,
            ReasonCode::CollectionRecordIdNotFound => ErrorCode::NotFound,
            ReasonCode::IndexerQueryInequalityNotLast => ErrorCode::InvalidArgument,
            ReasonCode::IndexerQueryPathsAndDirectionsLengthMismatch => ErrorCode::InvalidArgument,
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
}
