use actix_web::{
    http::{header::ContentType, StatusCode},
    HttpResponse,
};
use serde::Serialize;
use std::{error::Error, fmt::Display};

use super::reason::ReasonCode;
use crate::db::{self};
use crate::raft::{self};

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

// impl std::error::Error for actix_web::Error {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         self.source.as_ref().map(|e| e.as_ref())
//     }
// }

impl actix_web::error::ResponseError for HTTPError {
    fn error_response(&self) -> HttpResponse {
        let error = ErrorOutput {
            error: ErrorDetail {
                code: self.reason.code().to_string(),
                reason: self.reason.to_string(),
                message: self.source.as_ref().unwrap().to_string(),
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
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<gateway::GatewayUserError> for HTTPError {
    fn from(err: gateway::GatewayUserError) -> Self {
        let reason = ReasonCode::from_gateway_error(&err);
        HTTPError::new(reason, Some(Box::new(err)))
    }
}

impl From<db::DbError> for HTTPError {
    fn from(err: db::DbError) -> Self {
        match err {
            db::DbError::CollectionNotFound => {
                HTTPError::new(ReasonCode::CollectionNotFound, Some(Box::new(err)))
            }
            // Fwd the gateway error
            db::DbError::GatewayError(e) => e.into(),
            // TODO: once we have better errors populated by Indexer/Gateway, we can map
            // those errors here
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<raft::RaftError> for HTTPError {
    fn from(err: raft::RaftError) -> Self {
        match err {
            raft::RaftError::Db(e) => e.into(),
            // TODO: once we have better errors populated by Indexer/Gateway, we can map
            // those errors here
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}
