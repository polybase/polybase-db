use actix_web::{
    http::{header::ContentType, StatusCode},
    HttpResponse,
};

use serde::Serialize;
use std::{error::Error, fmt::Display};

use super::reason::ReasonCode;
// use crate::db::{self};

#[derive(Debug)]
pub struct HTTPError {
    reason: ReasonCode,
    source: Option<Box<dyn Error>>,
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
        eprintln!("Error: {}", self);

        // Log out each error
        let mut error: &dyn std::error::Error = self;
        while let Some(source) = error.source() {
            println!("  Caused by: {}", source);
            error = source;
        }

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

// impl From<db::DbError> for HTTPError {
//     fn from(err: db::DbError) -> Self {
//         match err {
//             db::DbError::RecordNotFound { source: _ } => {
//                 HTTPError::new(ReasonCode::RecordNotFound, Some(Box::new(err)))
//             }
//             db::DbError::IndexerErr { source: _ } => {
//                 HTTPError::new(ReasonCode::KeyTooLong, Some(Box::new(err)))
//             }
//         }
//     }
// }
