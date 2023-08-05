pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("schema error")]
    Schema(#[from] schema::Error),
    // #[error("Collection collection record not found for collection {id:?}")]
    // CollectionCollectionRecordNotFound { id: String },
    #[error("indexer error")]
    Indexer(#[from] indexer_db_adaptor::Error),
}

impl From<Error> for indexer_db_adaptor::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Sqlx(e) => indexer_db_adaptor::Error::Store(Box::new(e)),
            Error::Schema(e) => indexer_db_adaptor::Error::Store(Box::new(e)),
            Error::Indexer(e) => e,
        }
    }
}
