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
        #[allow(clippy::unwrap_used)]
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
            // gateway::GatewayError::IndexerError(e) => e.into(),
            _ => HTTPError::new(ReasonCode::Internal, Some(Box::new(err))),
        }
    }
}

impl From<gateway::GatewayUserError> for HTTPError {
    fn from(err: gateway::GatewayUserError) -> Self {
        HTTPError::new(ReasonCode::from_gateway_error(&err), Some(Box::new(err)))
    }
}

impl From<db::Error> for HTTPError {
    fn from(err: db::Error) -> Self {
        match err {
            db::Error::Schema(e) => e.into(),
            db::Error::Record(e) => e.into(),
            db::Error::Method(e) => e.into(),
            db::Error::User(e) => e.into(),
            db::Error::Gateway(e) => e.into(),
            db::Error::Indexer(e) => e.into(),
            db::Error::Serializer(_) => internal_error(err),
            db::Error::SerdeJson(_) => internal_error(err),
            db::Error::CallTxn(_) => internal_error(err),
            db::Error::TokioSend(_) => internal_error(err),
            db::Error::InvalidFunctionArgsResponse => internal_error(err),
        }
    }
}

impl From<db::UserError> for HTTPError {
    fn from(err: db::UserError) -> Self {
        HTTPError::new(ReasonCode::from_db_error(&err), Some(Box::new(err)))
    }
}

impl From<indexer::cursor::Error> for HTTPError {
    fn from(err: indexer::cursor::Error) -> Self {
        HTTPError::new(ReasonCode::Internal, Some(Box::new(err)))
    }
}

impl From<indexer::where_query::WhereQueryError> for HTTPError {
    fn from(err: indexer::where_query::WhereQueryError) -> Self {
        match err {
            indexer::where_query::WhereQueryError::UserError(e) => e.into(),
            indexer::where_query::WhereQueryError::RecordError(
                schema::record::RecordError::UserError(e),
            ) => e.into(),
            indexer::where_query::WhereQueryError::RecordError(e) => internal_error(e),
        }
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

impl From<schema::Error> for HTTPError {
    fn from(err: schema::Error) -> Self {
        match err {
            schema::Error::User(e) => e.into(),
            schema::Error::Method(e) => e.into(),
            _ => internal_error(err),
        }
    }
}

impl From<schema::UserError> for HTTPError {
    fn from(err: schema::UserError) -> Self {
        HTTPError::new(ReasonCode::from_schema_error(&err), Some(Box::new(err)))
    }
}

impl From<schema::record::RecordError> for HTTPError {
    fn from(err: schema::record::RecordError) -> Self {
        match err {
            schema::record::RecordError::UserError(e) => e.into(),
            _ => internal_error(err),
        }
    }
}

impl From<schema::record::RecordUserError> for HTTPError {
    fn from(err: schema::record::RecordUserError) -> Self {
        HTTPError::new(
            ReasonCode::from_record_user_error(&err),
            Some(Box::new(err)),
        )
    }
}

impl From<schema::methods::UserError> for HTTPError {
    fn from(err: schema::methods::UserError) -> Self {
        HTTPError::new(
            ReasonCode::from_schema_method_error(&err),
            Some(Box::new(err)),
        )
    }
}

impl From<indexer::Error> for HTTPError {
    fn from(err: indexer::Error) -> Self {
        match err {
            indexer::Error::User(e) => e.into(),
            indexer::Error::WhereQuery(e) => e.into(),
            indexer::Error::Adaptor(e) => internal_error(e),
        }
    }
}

impl From<indexer::UserError> for HTTPError {
    fn from(err: indexer::UserError) -> Self {
        HTTPError::new(ReasonCode::from_indexer_error(&err), Some(Box::new(err)))
    }
}

impl From<auth::AuthError> for HTTPError {
    fn from(err: auth::AuthError) -> Self {
        match err {
            auth::AuthError::User(e) => e.into(),
            auth::AuthError::ToStr(e) => internal_error(e),
            // TODO: Should some of these be user errors?
            auth::AuthError::Secp256k1(e) => internal_error(e),
            auth::AuthError::Payload(e) => internal_error(e),
        }
    }
}

impl From<auth::AuthUserError> for HTTPError {
    fn from(err: auth::AuthUserError) -> Self {
        HTTPError::new(ReasonCode::from_auth_error(&err), Some(Box::new(err)))
    }
}

fn internal_error(err: impl std::error::Error + 'static) -> HTTPError {
    HTTPError::new(ReasonCode::Internal, Some(Box::new(err)))
}
