use derive_more::Display;

use crate::errors::code::ErrorCode;

#[derive(Debug, Display)]
pub enum ReasonCode {
    #[display(fmt = "record/not-found")]
    RecordNotFound,

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

    #[display(fmt = "collection/id-exist")]
    CollectionIdExists,

    #[display(fmt = "collection/invalid-id")]
    CollectionInvalidId,

    #[display(fmt = "collection/invalid-schema")]
    CollectionInvalidSchema,
}

impl ReasonCode {
    pub fn code(&self) -> ErrorCode {
        match self {
            ReasonCode::RecordNotFound => ErrorCode::NotFound,
            ReasonCode::IndexesMissingIndex => ErrorCode::FailedPrecondition,
            ReasonCode::FunctionInvalidatedId => ErrorCode::FailedPrecondition,
            ReasonCode::FunctionNotFound => ErrorCode::NotFound,
            ReasonCode::FunctionInvalidArgs => ErrorCode::InvalidArgument,
            ReasonCode::FunctionInvalidCall => ErrorCode::InvalidArgument,
            ReasonCode::CollectionIdExists => ErrorCode::AlreadyExists,
            ReasonCode::CollectionInvalidId => ErrorCode::InvalidArgument,
            ReasonCode::CollectionInvalidSchema => ErrorCode::InvalidArgument,
        }
    }
}
