use derive_more::Display;

use crate::{auth, db, errors::code::ErrorCode};

#[derive(Debug, Display, PartialEq)]
pub enum ReasonCode {
    #[display(fmt = "record/not-found")]
    RecordNotFound,

    #[display(fmt = "record/id-not-string")]
    RecordIdNotString,

    #[display(fmt = "record/collection-id-not-found")]
    RecordCollectionIdNotFound,

    #[display(fmt = "record/not-object")]
    RecordNotObject,

    #[display(fmt = "record/field-not-object")]
    RecordFieldNotObject,

    #[display(fmt = "record/id-modified")]
    RecordIDModified,

    #[display(fmt = "record/missing-field")]
    RecordMissingField,

    #[display(fmt = "record/invalid-field")]
    RecordInvalidField,

    #[allow(unused)]
    #[display(fmt = "index/missing-index")]
    IndexesMissingIndex,

    #[allow(unused)]
    #[display(fmt = "function/invalidated-id")]
    FunctionInvalidatedId,

    #[display(fmt = "function/not-found")]
    FunctionNotFound,

    #[display(fmt = "function/invalid-args")]
    FunctionInvalidArgs,

    #[allow(unused)]
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

    #[display(fmt = "indexer/missing-index")]
    IndexerMissingIndex,

    #[display(fmt = "indexer/query-paths-directions-length")]
    IndexerQueryPathsAndDirectionsLengthMismatch,

    #[display(fmt = "indexer/query-inequality-not-last")]
    IndexerQueryInequalityNotLast,

    #[display(fmt = "indexer/invalid-cursor")]
    IndexerInvalidCursorKey,

    #[display(fmt = "auth/invalid-signature")]
    AuthInvalidSignature,

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
            ReasonCode::RecordNotObject => ErrorCode::InvalidArgument,
            ReasonCode::RecordFieldNotObject => ErrorCode::InvalidArgument,
            ReasonCode::RecordIDModified => ErrorCode::FailedPrecondition,
            ReasonCode::RecordMissingField => ErrorCode::InvalidArgument,
            ReasonCode::RecordInvalidField => ErrorCode::InvalidArgument,
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
            ReasonCode::IndexerMissingIndex => ErrorCode::FailedPrecondition,
            ReasonCode::CollectionMismatch => ErrorCode::InvalidArgument,
            ReasonCode::CollectionRecordIdNotFound => ErrorCode::NotFound,
            ReasonCode::IndexerQueryInequalityNotLast => ErrorCode::InvalidArgument,
            ReasonCode::IndexerQueryPathsAndDirectionsLengthMismatch => ErrorCode::InvalidArgument,
            ReasonCode::IndexerInvalidCursorKey => ErrorCode::InvalidArgument,
            ReasonCode::AuthInvalidSignature => ErrorCode::InvalidArgument,
            ReasonCode::Unauthorized => ErrorCode::PermissionDenied,
            ReasonCode::Internal => ErrorCode::Internal,
        }
    }

    pub fn from_db_error(err: &db::UserError) -> Self {
        match err {
            db::UserError::FunctionNotFound { .. } => ReasonCode::FunctionNotFound,
            db::UserError::CollectionMismatch { .. } => ReasonCode::CollectionMismatch,
            db::UserError::RecordNotFound { .. } => ReasonCode::RecordNotFound,
            db::UserError::RecordIdNotString => ReasonCode::RecordIdNotString,
            db::UserError::RecordIdNotFound => ReasonCode::RecordCollectionIdNotFound,
            // db::UserError::RecordIDModified => ReasonCode::RecordIDModified,
            db::UserError::Method(err) => Self::from_schema_method_error(err),
        }
    }

    pub fn from_gateway_error(err: &gateway::GatewayUserError) -> Self {
        match err {
            // gateway::GatewayUserError::RecordNotFound { .. } => ReasonCode::RecordNotFound,

            // gateway::GatewayUserError::RecordIdNotString => ReasonCode::RecordIdNotString,

            // gateway::GatewayUserError::RecordCollectionIdNotFound => {
            //     ReasonCode::RecordCollectionIdNotFound
            // }

            // gateway::GatewayUserError::RecordFieldNotObject => ReasonCode::RecordFieldNotObject,
            gateway::GatewayUserError::RecordIDModified => ReasonCode::RecordIDModified,

            // gateway::GatewayUserError::CollectionNotFound { .. } => ReasonCode::CollectionNotFound,
            // gateway::GatewayUserError::CollectionIdExists => ReasonCode::CollectionIdExists,

            // gateway::GatewayUserError::CollectionRecordIdNotFound => {
            //     ReasonCode::CollectionRecordIdNotFound
            // }

            // gateway::GatewayUserError::CollectionMismatch { .. } => ReasonCode::CollectionMismatch,

            // gateway::GatewayUserError::FunctionNotFound { .. } => ReasonCode::FunctionNotFound,
            gateway::GatewayUserError::UnauthorizedCall => ReasonCode::Unauthorized,

            gateway::GatewayUserError::JavaScriptException { .. } => {
                ReasonCode::FunctionJavaScriptException
            }

            gateway::GatewayUserError::CollectionFunctionError { .. } => {
                ReasonCode::FunctionCollectionError
            }

            gateway::GatewayUserError::ConstructorMustAssignId => ReasonCode::ConstructorNoId,

            gateway::GatewayUserError::FunctionTimedOut => ReasonCode::FunctionJavaScriptException,
        }
    }

    pub fn from_where_query_error(
        err: &indexer_db_adaptor::where_query::WhereQueryUserError,
    ) -> Self {
        match err {
            indexer_db_adaptor::where_query::WhereQueryUserError::PathsAndDirectionsLengthMismatch {
                ..
            } => ReasonCode::IndexerQueryPathsAndDirectionsLengthMismatch,
            indexer_db_adaptor::where_query::WhereQueryUserError::InequalityNotLast => {
                ReasonCode::IndexerQueryInequalityNotLast
            }
            indexer_db_adaptor::where_query::WhereQueryUserError::CannotFilterOrSortByField(..) => {
                ReasonCode::IndexerMissingIndex
            }
        }
    }

    // pub fn from_schema_error(err: &schema::UserError) -> Self {
    //     match err {
    //         schema::UserError::Method(err) => Self::from_schema_method_error(err),
    //     }
    // }

    pub fn from_schema_method_error(err: &schema::methods::UserError) -> Self {
        match err {
            schema::methods::UserError::MethodIncorrectNumberOfArguments { .. } => {
                ReasonCode::FunctionInvalidArgs
            }
            schema::methods::UserError::MethodInvalidArgumentType { .. } => {
                ReasonCode::FunctionInvalidArgs
            }
        }
    }

    pub fn from_indexer_error(err: &indexer_db_adaptor::UserError) -> Self {
        match err {
            indexer_db_adaptor::UserError::CollectionNotFound { .. } => {
                ReasonCode::CollectionNotFound
            }

            indexer_db_adaptor::UserError::CursorBeforeAndAfter { .. } => {
                ReasonCode::IndexerInvalidCursorKey
            }

            indexer_db_adaptor::UserError::UnauthorizedRead { .. } => ReasonCode::Unauthorized,
            // indexer_db_adaptor::CollectionUserError::NoIndexFoundMatchingTheQuery => {
            //     ReasonCode::IndexerMissingIndex
            // }
            // indexer_db_adaptor::CollectionUserError::UnauthorizedRead => ReasonCode::Unauthorized,
            // indexer_db_adaptor::CollectionUserError::InvalidCursorKey => {
            //     ReasonCode::IndexerInvalidCursorKey
            // }
            // indexer_db_adaptor::CollectionUserError::InvalidCursorBeforeAndAfterSpecified => {
            //     ReasonCode::IndexerInvalidCursorKey
            // }
            // indexer_db_adaptor::CollectionUserError::CollectionIdMissingNamespace => {
            //     ReasonCode::CollectionInvalidId
            // }
            // indexer_db_adaptor::CollectionUserError::CollectionNameCannotStartWithDollarSign => {
            //     ReasonCode::CollectionInvalidId
            // }
            // indexer_db_adaptor::CollectionUserError::MissingDefinitionForCollection { .. } => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::CollectionMissingIdField => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::CollectionIdFieldMustBeString => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::CollectionIdFieldCannotBeOptional => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::IndexFieldNotFoundInSchema { .. } => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::IndexFieldCannotBeAnArray { .. } => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::IndexFieldCannotBeAMap { .. } => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::IndexFieldCannotBeAnObject { .. } => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::IndexFieldCannotBeBytes { .. } => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::CollectionDirectiveCannotHaveArguments {
            //     ..
            // } => ReasonCode::CollectionInvalidSchema,
            // indexer_db_adaptor::CollectionUserError::UnknownCollectionDirectives { .. } => {
            //     ReasonCode::CollectionInvalidSchema
            // }
            // indexer_db_adaptor::CollectionUserError::RecordUserError(_) => {
            //     ReasonCode::RecordInvalidField
            // }
            // indexer_db_adaptor::CollectionUserError::Schema(_) => {
            //     ReasonCode::CollectionInvalidSchema
            // }
        }
    }

    pub fn from_record_user_error(err: &schema::record::RecordUserError) -> Self {
        match err {
            schema::record::RecordUserError::RecordRootShouldBeAnObject { .. } => {
                ReasonCode::RecordNotObject
            }
            schema::record::RecordUserError::MissingField { .. } => ReasonCode::RecordMissingField,
            schema::record::RecordUserError::InvalidFieldValueType { .. } => {
                ReasonCode::RecordInvalidField
            }
            schema::record::RecordUserError::UnexpectedFields { .. } => {
                ReasonCode::RecordInvalidField
            }
        }
    }

    pub fn from_auth_error(_err: &auth::AuthUserError) -> Self {
        ReasonCode::AuthInvalidSignature
    }
}
