use slog::debug;

mod job_store;
pub mod jobs;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::future;
use std::collections::HashMap;
use tokio::{sync::Mutex, time};

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

/// The indexer Job Engine
pub struct JobEngine {
    job_store: Arc<Mutex<job_store::JobStore>>,
    shutdown: Arc<Mutex<bool>>,
    logger: slog::Logger,
}

impl JobEngine {
    pub(crate) fn new(indexer_store_path: impl AsRef<Path>, logger: slog::Logger) -> Result<Self> {
        // initialise the job store
        let job_store_path = get_job_store_path(indexer_store_path.as_ref());
        let job_store = job_store::JobStore::open(job_store_path)?;
        let job_store = Arc::new(Mutex::new(job_store));

        let shared_job_store = job_store.clone();
        let shutdown = Arc::new(Mutex::new(false));

        // todo - move this into a function
        let shared_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(time::Duration::from_millis(JOBS_CHECK_INTERVAL));

            loop {
                let shutdown = shared_shutdown.lock().await;
                if *shutdown {
                    break;
                }

                interval.tick().await;

                let jobs_map = {
                    let store = shared_job_store.lock().await;
                    store.get_jobs().await.unwrap_or(HashMap::new())
                };

                let mut tasks = Vec::new();
                for (_job_group, jobs) in jobs_map {
                    let store = shared_job_store.clone();

                    tasks.push(tokio::spawn(async move {
                        for job in jobs {
                            // todo - change these when the JobState enum is properly defined, and
                            match job.job_state {
                                JobState::JobType1 { num } => println!("Got a num {num:?}"),
                                JobState::JobType2 { ref string, num } => {
                                    println!("Got a string {string:?} and a number {num:?}")
                                }
                                JobState::JobType3 { b } => println!("Got a bool: {b:?}"),
                            }

                            // delete the job
                            {
                                let store = store.lock().await;
                                let _ = store.delete_job(job).await;
                            }
                        }
                    }));
                }

                // wait for all the tasks to finish
                future::join_all(tasks).await;
            }
        });

        Ok(Self {
            job_store,
            shutdown,
            logger: logger.clone(),
        })
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
    pub(crate) async fn check_job_group_completion(&self, job: Job) -> Result<bool> {
        let store = self.job_store.clone();
        let store = store.lock().await;
        Ok(store.is_job_group_complete(&job.job_group).await?)
    }
}

fn get_job_store_path(path: &Path) -> PathBuf {
    // this is always guaranteed to be present if it's reached this far
    #[allow(clippy::unwrap_used)]
    path.parent().unwrap().join("jobs.db")
}
