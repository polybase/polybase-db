use indexer_db_adaptor::adaptor;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("schema error")]
    Schema(#[from] schema::Error),
    // #[error("Collection collection record not found for collection {id:?}")]
    // CollectionCollectionRecordNotFound { id: String },
    #[error("adaptor error")]
    Adaptor(#[from] adaptor::Error),
}

impl From<Error> for adaptor::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Sqlx(e) => adaptor::Error::Store(Box::new(e)),
            Error::Schema(e) => adaptor::Error::Store(Box::new(e)),
            Error::Adaptor(e) => e,
        }
    }
}
