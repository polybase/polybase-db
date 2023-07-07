use crate::hash::Hash;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("A key referenced this hash as a child, but it wasn't present: {0}")]
    MissingKeyReferenced(Hash),
    #[error("the `structure` key was not defined")]
    Unknown(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<rocksdb::Error> for Error {
    fn from(value: rocksdb::Error) -> Self {
        Self::Unknown(Box::new(value))
    }
}
