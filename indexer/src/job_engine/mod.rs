pub(super) mod job_store;
pub mod jobs;

use futures::future;
use slog::{crit, info};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::time::{interval, Duration};

use crate::{collection, job_engine::jobs::JobState, keys};

use super::{Indexer, IndexerError};
use job_store::JobStore;
use jobs::Job;

// job execution

#[derive(Debug, thiserror::Error)]
pub enum JobExecutionError {
    #[error("job execution error: indexer error")]
    Indexer(#[from] IndexerError),

    #[error("job execution error: keys error")]
    Keys(#[from] keys::KeysError),

    #[error("job execution error: collection error")]
    Collection(#[from] collection::CollectionError),
}

pub(super) type JobExecutionResult = std::result::Result<(), JobExecutionError>;

// job engine

#[derive(Debug, thiserror::Error)]
pub enum JobEngineError {
    #[error("job store error")]
    JobStoreError(#[from] job_store::JobStoreError),

    #[error("tokio task join error")]
    TokioTaskJoinError(#[from] tokio::task::JoinError),
}

pub(super) type JobEngineResult<T> = std::result::Result<T, JobEngineError>;

const JOBS_CHECK_INTERVAL: u64 = 500; // 500ms

/// Check for jobs that are queued up for execution - this is the Job Engine's executor task
/// that will poll the jobs store for jobs, and execute the jobs within each job group in
/// sequential order.
/// This will also automatically start running during Polybase startup and persist till
/// server stop.
pub(super) async fn check_for_jobs_to_execute(
    indexer: Arc<Indexer>,
    job_store: Arc<JobStore>,
    logger: slog::Logger,
) {
    let mut interval = interval(Duration::from_millis(JOBS_CHECK_INTERVAL));

    loop {
        let job_store = job_store.clone();
        interval.tick().await;

        info!(
            logger,
            "[Job Engine] Checking for jobs in the jobs store to execute"
        );
        let jobs_map = get_jobs(job_store.clone()).await.unwrap_or(HashMap::new());

        if !jobs_map.is_empty() {
            info!(logger, "[Job Engine] Found jobs queued for execution");

            let mut job_group_tasks = Vec::new();
            // execute each job group concurrently
            for (_, jobs) in jobs_map {
                let indexer = indexer.clone();
                let logger = logger.clone();

                let job_store = job_store.clone();
                job_group_tasks.push(tokio::spawn(async move {
                    // execute each job within a job group sequentially
                    for job in jobs {
                        let indexer = indexer.clone();

                        if let Err(e) = execute_job(job.clone(), indexer, logger.clone()).await {
                            crit!(
                                logger,
                                "[Job Engine] Error while executing job {:#?}: {e:?}",
                                job.clone()
                            );
                        }

                        if let Err(e) = delete_job(job.clone(), job_store.clone()).await {
                            crit!(
                                logger,
                                "[Job Engine] Error while deleting job {:#?}: {e:?}",
                                job.clone();
                            )
                        }
                    }
                }));
            }

            // wait for all jobs in the job group to finish execution
            let _job_exec_results = future::join_all(job_group_tasks).await;
        } else {
            info!(logger, "[Job Engine] Found no queued jobs");
        }
    }
}

async fn execute_job(job: Job, indexer: Arc<Indexer>, logger: slog::Logger) -> JobExecutionResult {
    let job_str = format!("{:#?}", job);

    info!(logger, "[Job Engine] Executing job {job_str:#?}");

    let job_exec_res = match job.job_state {
        JobState::RebuildCollectionIndexes {
            collection_id,
            record_id,
        } => {
            let collection = indexer.collection(collection_id.clone()).await?;
            let old_value = collection.get_without_auth_check(record_id.clone()).await?;

            let collection_before = if collection_id == "Collection" {
                match collection::Collection::load(logger.clone(), &indexer, record_id.clone())
                    .await
                {
                    Ok(c) => Some(c),
                    Err(collection::CollectionError::UserError(
                        collection::CollectionUserError::CollectionNotFound { .. },
                    )) => None,
                    Err(err) => return Err(JobExecutionError::from(err)),
                }
            } else {
                None
            };

            if collection_id == "Collection" && record_id != "Collection" {
                if let Some(collection_before) = collection_before {
                    // Unwrap is safe because collection_before had to load the existing record.
                    #[allow(clippy::unwrap_used)]
                    let old_value = old_value.unwrap();

                    let target_col =
                        collection::Collection::load(logger.clone(), &indexer, record_id).await?;

                    target_col.rebuild(collection_before, &old_value).await?;
                }
            }

            Ok(())
        }
    };

    info!(logger, "[Job Engine] Finished executing : job {job_str:#?}");

    job_exec_res
}

/// Save a job in the jobs store for execution
pub(super) async fn enqueue_job(job: Job, job_store: Arc<JobStore>) -> JobEngineResult<()> {
    Ok(job_store.save_job(job).await?)
}

/// Delete the job from the jobs store
pub(super) async fn delete_job(job: Job, job_store: Arc<JobStore>) -> JobEngineResult<()> {
    Ok(job_store.delete_job(job).await?)
}

/// Wait for the all the jobs in the job group to finish execution
pub(super) async fn await_job_completion(
    job_group: impl AsRef<str>,
    job_store: Arc<JobStore>,
) -> JobEngineResult<()> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        match job_store.is_job_group_complete(job_group.as_ref()).await {
            Ok(true) => break,
            Ok(false) => continue,
            Err(e) => return Err(JobEngineError::from(e)),
        }
    }
    Ok(())
}

async fn get_jobs(job_store: Arc<JobStore>) -> JobEngineResult<HashMap<String, VecDeque<Job>>> {
    Ok(job_store.get_jobs().await?)
}
