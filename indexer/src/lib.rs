#![warn(clippy::unwrap_used, clippy::expect_used)]

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub mod collection;
mod index;
mod job_engine;
pub mod keys;
mod proto;
pub mod publickey;
mod record;
pub mod snapshot;
mod stableast_ext;
mod store;
pub mod where_query;

use job_engine::{job_store, jobs};

pub use collection::{validate_schema_change, AuthUser, Collection, Cursor, ListQuery};
pub use index::CollectionIndexField;
pub use keys::Direction;
pub use publickey::PublicKey;
pub use record::{
    json_to_record, record_to_json, Converter, ForeignRecordReference, IndexValue, PathFinder,
    RecordError, RecordRoot, RecordUserError, RecordValue,
};
use snapshot::SnapshotChunk;
pub use stableast_ext::FieldWalker;
pub use where_query::WhereQuery;

pub type Result<T> = std::result::Result<T, IndexerError>;

// TODO: errors should be named Error and imported as indexer::Error
#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("collection error")]
    Collection(#[from] collection::CollectionError),

    #[error("store error")]
    Store(#[from] store::StoreError),

    #[error("index error")]
    Index(#[from] index::IndexError),

    #[error("keys error")]
    Keys(#[from] keys::KeysError),

    #[error(transparent)]
    PublicKey(#[from] publickey::PublicKeyError),

    #[error("record error")]
    Record(#[from] record::RecordError),

    #[error("where query error")]
    WhereQuery(#[from] where_query::WhereQueryError),

    #[error("job engine error")]
    JobEngineError(#[from] job_engine::JobEngineError),

    #[error("job store error")]
    JobStoreError(#[from] job_store::JobStoreError),
}

pub struct Indexer {
    logger: slog::Logger,
    store: store::Store,
    job_store: Arc<job_store::JobStore>,
}

impl Indexer {
    pub fn new(logger: slog::Logger, path: impl AsRef<Path>) -> Result<Arc<Self>> {
        // initialise the job store
        let indexer_store_path = path.as_ref().to_path_buf();
        let job_store_path = Self::get_job_store_path(indexer_store_path.as_ref());
        let job_store_logger = logger.clone();
        let job_store = job_store::JobStore::open(job_store_path, job_store_logger)?;
        let job_store = Arc::new(job_store);

        let job_engine_job_store = job_store.clone();
        let job_engine_logger = logger.clone();

        let store = store::Store::open(path)?;
        let indexer = Arc::new(Self {
            logger,
            store,
            job_store,
        });

        let job_engine_indexer = indexer.clone();

        tokio::spawn(job_engine::check_for_jobs_to_execute(
            job_engine_indexer,
            job_engine_job_store,
            job_engine_logger,
        ));

        Ok(indexer)
    }

    pub fn destroy(self) -> Result<()> {
        Ok(self.store.destroy()?)
    }

    pub fn reset(&self) -> Result<()> {
        Ok(self.store.reset()?)
    }

    pub fn snapshot(&self, chunk_size: usize) -> snapshot::SnapshotIterator {
        self.store.snapshot(chunk_size)
    }

    pub fn restore(&self, data: SnapshotChunk) -> Result<()> {
        Ok(self.store.restore(data)?)
    }

    pub async fn collection(&self, id: String) -> Result<Collection> {
        Ok(Collection::load(self.logger.clone(), &self, &self.store, id).await?)
    }

    pub async fn commit(&self) -> Result<()> {
        Ok(self.store.commit().await?)
    }

    pub async fn set_system_key(&self, key: String, data: &RecordRoot) -> Result<()> {
        let system_key = keys::Key::new_system_data(key)?;

        Ok(self
            .store
            .set(&system_key, &store::Value::DataValue(data))
            .await?)
    }

    pub async fn get_system_key(&self, key: String) -> Result<Option<RecordRoot>> {
        let system_key = keys::Key::new_system_data(key)?;
        Ok(self.store.get(&system_key).await?)
    }

    pub async fn enqueue_job(&self, job: jobs::Job) -> Result<()> {
        job_engine::enqueue_job(job, self.job_store.clone()).await?;
        Ok(())
    }

    async fn delete_job(&self, job: jobs::Job) -> Result<()> {
        job_engine::delete_job(job, self.job_store.clone()).await;
        Ok(())
    }

    pub async fn await_job_completion(&self, job_group: impl AsRef<str>) -> Result<()> {
        Ok(job_engine::await_job_completion(job_group, self.job_store.clone()).await?)
    }

    fn get_job_store_path(path: &Path) -> PathBuf {
        // this is always guaranteed to be present if it's reached this far
        #[allow(clippy::unwrap_used)]
        path.parent().unwrap().join("jobs.db")
    }
}
