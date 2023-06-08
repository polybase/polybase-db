mod job_store;
pub mod jobs;

use slog::debug;
use std::collections::VecDeque;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use std::collections::HashMap;
use tokio::{sync::Mutex, time};

use crate::keys::KeysError;
use crate::{keys, Indexer, IndexerError};
use job_store::JobStore;
use jobs::{Job, JobState};

pub(crate) type Result<T> = std::result::Result<T, JobEngineError>;

#[derive(Debug, thiserror::Error)]
pub enum JobEngineError {
    #[error("job store error")]
    JobStoreError(#[from] job_store::JobStoreError),

    #[error("tokio task join error")]
    TokioTaskJoinError(#[from] tokio::task::JoinError),
}

const JOBS_CHECK_INTERVAL: u64 = 100;

#[derive(Debug, thiserror::Error)]
pub enum JobExecutionError {
    #[error("job execution error: indexer error")]
    IndexerError(#[from] IndexerError),

    #[error("job execution error: keys error")]
    KeysError(#[from] KeysError),
}

pub enum JobExecutionResultState {
    Okay,
    Shutdown,
}

pub type JobExecutionResult = std::result::Result<JobExecutionResultState, JobExecutionError>;

/// The indexer Job Engine
pub struct JobEngine {
    job_store: Arc<Mutex<job_store::JobStore>>,
    shutdown: Arc<Mutex<bool>>,
    logger: slog::Logger,
}

impl JobEngine {
    pub(crate) async fn new(
        indexer_store_path: impl AsRef<Path>,
        logger: slog::Logger,
        indexer: &Indexer,
    ) -> Result<Self> {
        // initialise the job store
        let job_store_path = get_job_store_path(indexer_store_path.as_ref());
        let job_store = job_store::JobStore::open(job_store_path)?;
        let job_store = Arc::new(Mutex::new(job_store));

        let shared_job_store = job_store.clone();
        let shutdown = Arc::new(Mutex::new(false));
        let shared_shutdown = shutdown.clone();

        let job_engine = Self {
            job_store,
            shutdown,
            logger: logger.clone(),
        };

        // TODO - figure out a way to make this work - `await`ing here will block the
        // constructor, but not awaiting it will not run the jobs!
        // The constructor itself needs to be async because other wise the `process_job_groups`
        // method will block, and that needs to be async because if we spawn tasks for the jobs, we
        // cannot get a handle to the `indexer`!.
        job_engine.process_job_groups(shared_job_store, shared_shutdown, &indexer);

        Ok(job_engine)
    }

    async fn delete_job(&self, job: Job) {
        let store = self.job_store.clone();
        let store = store.lock().await;
        let _ = store.delete_job(job).await;
    }

    async fn process_job(&self, indexer: &Indexer, job: Job) -> JobExecutionResult {
        match job.job_state {
            JobState::JobType1 { num } => {
                println!("Got a num {num:?}");
                Ok(JobExecutionResultState::Okay)
            }
            JobState::JobType2 { ref string, num } => {
                println!("Got a string {string:?} and a number {num:?}");
                Ok(JobExecutionResultState::Okay)
            }
            JobState::JobType3 { b } => {
                println!("Got a bool: {b:?}");
                Ok(JobExecutionResultState::Okay)
            }

            JobState::AddIndexes {
                ref collection_id,
                ref id,
                ref record,
            } => {
                let collection = indexer.collection(collection_id.clone()).await?;
                let data_key = keys::Key::new_data(collection_id.clone(), id.clone())?;
                collection.add_indexes(&id, &data_key, &record).await;
                Ok(JobExecutionResultState::Okay)
            }
        }
    }

    async fn process_jobs(
        &self,
        indexer: &Indexer,
        store: Arc<Mutex<JobStore>>,
        jobs: VecDeque<Job>,
    ) -> Vec<JobExecutionResult> {
        let mut results = Vec::new();
        for job in jobs {
            results.push(self.process_job(indexer, job.clone()).await);
            self.delete_job(job).await;
        }

        results
    }

    async fn process_job_groups(
        &self,
        shared_job_store: Arc<Mutex<job_store::JobStore>>,
        shared_shutdown: Arc<Mutex<bool>>,
        indexer: &Indexer,
    ) -> JobExecutionResult {
        let mut interval = time::interval(time::Duration::from_millis(JOBS_CHECK_INTERVAL));

        loop {
            let shutdown = shared_shutdown.lock().await;
            if *shutdown {
                return Ok(JobExecutionResultState::Shutdown);
            }

            interval.tick().await;

            let jobs_map = {
                let store = shared_job_store.lock().await;
                store.get_jobs().await.unwrap_or(HashMap::new())
            };

            for (_job_group, jobs) in jobs_map {
                let store = shared_job_store.clone();
                let results = self.process_jobs(indexer, store, jobs).await;
            }
        }
    }

    /// Shut down the Job Engine.
    pub async fn shutdown(&self) {
        debug!(self.logger, "Shutting down the Job Engine");

        let shutdown = self.shutdown.clone();
        let mut shutdown = shutdown.lock().await;
        *shutdown = true;

        debug!(self.logger, "Finished shutting down the Job Engine");
    }

    /// Save a job for processing later - this will push the jobs into the jobs store
    /// and the runner will query the jobs store for the jobs to execute.
    pub async fn enqueue_job(&self, job: Job) -> Result<()> {
        let store = self.job_store.clone();
        let store = store.lock().await;
        store.save_job(job).await?;

        Ok(())
    }

    /// Check if the job group has finished executing
    pub(crate) async fn check_job_group_completion(&self, job_group: String) -> Result<bool> {
        let store = self.job_store.clone();
        let store = store.lock().await;
        Ok(store.is_job_group_complete(&job_group).await?)
    }
}

fn get_job_store_path(path: &Path) -> PathBuf {
    // this is always guaranteed to be present if it's reached this far
    #[allow(clippy::unwrap_used)]
    path.parent().unwrap().join("jobs.db")
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures::executor::block_on;
    use slog::Drain;
    use std::ops::{Deref, DerefMut};

    pub(crate) struct TestIndexer(Option<Indexer>);

    impl Default for TestIndexer {
        fn default() -> Self {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!(
                "test-gateway-rocksdb-store-{}",
                rand::random::<u32>()
            ));

            Self(Some(Indexer::new(logger(), path).unwrap()))
        }
    }

    impl Drop for TestIndexer {
        fn drop(&mut self) {
            if let Some(indexer) = self.0.take() {
                indexer.destroy().unwrap();
            }
        }
    }

    impl Deref for TestIndexer {
        type Target = Indexer;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl DerefMut for TestIndexer {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }

    pub(crate) struct TestJobEngine(Option<JobEngine>);

    fn logger() -> slog::Logger {
        let decorator = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }

    impl Default for TestJobEngine {
        fn default() -> Self {
            let temp_dir = std::env::temp_dir();
            let indexer_path = temp_dir.join(format!(
                "test-indexer-job-engine-store-{}",
                rand::random::<u32>(),
            ));

            Self(block_on(async {
                Some(
                    JobEngine::new(indexer_path, logger(), &TestIndexer::default())
                        .await
                        .unwrap(),
                )
            }))
        }
    }

    impl Drop for TestJobEngine {
        fn drop(&mut self) {
            if let Some(job_engine) = self.0.take() {
                block_on(async {
                    let _ = job_engine.shutdown().await;
                });
            }
        }
    }

    impl Deref for TestJobEngine {
        type Target = JobEngine;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl DerefMut for TestJobEngine {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }
}
