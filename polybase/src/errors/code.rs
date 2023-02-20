use actix_web::http::StatusCode;
use derive_more::Display;

#[derive(Debug, Display)]
pub enum ErrorCode {
    // #[display(fmt = "bad-request")]
    // BadRequest,
    #[display(fmt = "invalid-argument")]
    InvalidArgument,

    #[display(fmt = "failed-precondition")]
    FailedPrecondition,

    // #[display(fmt = "out-of-range")]
    // OutOfRange,
    #[display(fmt = "unauthenticated")]
    Unauthenticated,

    #[display(fmt = "permission-denied")]
    PermissionDenied,

    #[display(fmt = "not-found")]
    NotFound,

    // #[display(fmt = "aborted")]
    // Aborted,
    #[display(fmt = "already-exists")]
    AlreadyExists,

    // #[display(fmt = "resource-exhausted")]
    // ResourceExhausted,

    // #[display(fmt = "cancelled")]
    // Cancelled,

    // #[display(fmt = "unavailable")]
    // Unavailable,
    #[display(fmt = "internal")]
    Internal,
    // #[display(fmt = "deadline-exceeded")]
    // DeadlineExceeded,
}

impl ErrorCode {
    pub fn status_code(&self) -> StatusCode {
        match self {
            // ErrorCode::BadRequest => StatusCode::BAD_REQUEST,
            ErrorCode::InvalidArgument => StatusCode::BAD_REQUEST,
            ErrorCode::FailedPrecondition => StatusCode::BAD_REQUEST,
            // ErrorCode::OutOfRange => StatusCode::BAD_REQUEST,
            ErrorCode::Unauthenticated => StatusCode::UNAUTHORIZED,
            ErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            // ErrorCode::Aborted => StatusCode::CONFLICT,
            ErrorCode::AlreadyExists => StatusCode::CONFLICT,
            // ErrorCode::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            // ErrorCode::Cancelled => StatusCode::NOT_ACCEPTABLE,
            // ErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            // ErrorCode::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        }
    }
}
