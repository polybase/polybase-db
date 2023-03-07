use actix_web::{
    http::{header::ContentType, StatusCode},
    HttpResponse,
};
use serde::Serialize;
use std::{error::Error, fmt::Display};

use super::reason::ReasonCode;
use crate::{
    auth,
    db::{self},
};
use crate::{
    raft::{self},
    rollup,
};

#[derive(Debug)]
pub struct HTTPError {
    pub reason: ReasonCode,
    source: Option<Box<dyn Error>>,
    // pub backtrace: Backtrace,
}

#[derive(Serialize)]
pub struct ErrorOutput {
    error: ErrorDetail,
}

#[derive(Serialize)]
pub struct ErrorDetail {
    code: String,
    reason: String,
    message: String,
}

impl HTTPError {
    pub fn new(reason: ReasonCode, source: Option<Box<dyn std::error::Error>>) -> HTTPError {
        HTTPError { reason, source }
    }
}

impl Display for HTTPError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.reason.code(), self.reason)
    }
}

impl std::error::Error for HTTPError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref())
    }
}

impl actix_web::error::ResponseError for HTTPError {
    fn error_response(&self) -> HttpResponse {
        let error = ErrorOutput {
            error: ErrorDetail {
                code: self.reason.code().to_string(),
                reason: self.reason.to_string(),
                message: self
                    .source
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
            },
        };
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(serde_json::to_string(&error).unwrap())
    }

    fn status_code(&self) -> StatusCode {
        self.reason.code().status_code()
    }
}

impl From<gateway::GatewayError> for HTTPError {
    fn from(err: gateway::GatewayError) -> Self {
        match err {
            // We only need to match the user errors
            gateway::GatewayError::UserError(e) => e.into(),
            gateway::GatewayError::IndexerError(e) => e.into(),
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<gateway::GatewayUserError> for HTTPError {
    fn from(err: gateway::GatewayUserError) -> Self {
        HTTPError::new(ReasonCode::from_gateway_error(&err), Some(Box::new(err)))
    }
}

impl From<db::DbError> for HTTPError {
    fn from(err: db::DbError) -> Self {
        match err {
            db::DbError::CollectionNotFound => {
                HTTPError::new(ReasonCode::CollectionNotFound, Some(Box::new(err)))
            }
            db::DbError::GatewayError(e) => e.into(),
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<raft::RaftError> for HTTPError {
    fn from(err: raft::RaftError) -> Self {
        match err {
            raft::RaftError::Db(e) => e.into(),
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<indexer::collection::CollectionError> for HTTPError {
    fn from(err: indexer::collection::CollectionError) -> Self {
        match err {
            indexer::collection::CollectionError::UserError(e) => e.into(),
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<indexer::collection::CollectionUserError> for HTTPError {
    fn from(err: indexer::collection::CollectionUserError) -> Self {
        HTTPError::new(ReasonCode::from_collection_error(&err), Some(Box::new(err)))
    }
}

impl From<indexer::where_query::WhereQueryUserError> for HTTPError {
    fn from(err: indexer::where_query::WhereQueryUserError) -> Self {
        HTTPError::new(
            ReasonCode::from_where_query_error(&err),
            Some(Box::new(err)),
        )
    }
}

impl From<indexer::IndexerError> for HTTPError {
    fn from(err: indexer::IndexerError) -> Self {
        match err {
            // Collection
            indexer::IndexerError::Collection(e) => match e {
                indexer::collection::CollectionError::UserError(e) => e.into(),
                _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(e))),
            },
            // WhereQuery
            indexer::IndexerError::WhereQuery(e) => match e {
                indexer::where_query::WhereQueryError::UserError(e) => e.into(),
                _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(e))),
            },
            // Other errors are internal
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<auth::AuthError> for HTTPError {
    fn from(err: auth::AuthError) -> Self {
        match err {
            auth::AuthError::User(e) => e.into(),
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<auth::AuthUserError> for HTTPError {
    fn from(err: auth::AuthUserError) -> Self {
        HTTPError::new(ReasonCode::from_auth_error(&err), Some(Box::new(err)))
    }
}

impl From<rollup::RollupError> for HTTPError {
    fn from(err: rollup::RollupError) -> Self {
        HTTPError::new(ReasonCode::Internal, Some(Box::new(err)))
    }
}
