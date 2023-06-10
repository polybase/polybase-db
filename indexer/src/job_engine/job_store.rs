use bincode::{deserialize, serialize};
use rocksdb::{
    Options, SingleThreaded, TransactionDB, TransactionDBOptions, WriteBatchWithTransaction,
};
use slog::debug;
use std::collections::{HashMap, VecDeque};
use std::{convert::AsRef, path::Path, sync::Arc};

use super::jobs::Job;

pub type JobStoreResult<T> = std::result::Result<T, JobStoreError>;

#[derive(Debug, thiserror::Error)]
pub enum JobStoreError {
    #[error("invalid/corrupted job value")]
    InvalidOrCorruptedJobValue,

    #[error("invalid/metadata handle")]
    InvalidMetadataHandle,

    #[error("failed/job group metadata missing")]
    UnableToQueryJobGroupCompletion,

    #[error("RocksDB error")]
    RocksDBError(#[from] rocksdb::Error),

    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),

    #[error("tokio task join error")]
    TokioTaskJoinError(#[from] tokio::task::JoinError),
}

/// The indexer Job Store
pub(crate) struct JobStore {
    db: Arc<TransactionDB<SingleThreaded>>,
    logger: slog::Logger,
}

impl JobStore {
    /// Create a new instance of the jobs store if it does not exist, creating a column family
    /// `metadata` to store job group information.
    /// Otherwise, open the existing store.`
    pub fn open(path: impl AsRef<Path>, logger: slog::Logger) -> JobStoreResult<Self> {
        debug!(logger, "[Job Store] Opening jobs store");

        let mut options = Options::default();
        options.create_if_missing(true);

        let cf_names = TransactionDB::<SingleThreaded>::list_cf(&Options::default(), &path);

        let txn_db = if cf_names.is_err() || !cf_names?.contains(&"metadata".to_string()) {
            let mut txn_db = TransactionDB::<SingleThreaded>::open(
                &options,
                &TransactionDBOptions::default(),
                path,
            )?;

            let mut cf_opts = Options::default();
            cf_opts.create_if_missing(true);
            txn_db.create_cf("metadata", &cf_opts)?;
            txn_db
        } else {
            TransactionDB::open_cf(
                &options,
                &TransactionDBOptions::default(),
                path.as_ref(),
                vec!["metadata"],
            )?
        };

        debug!(logger, "[Job Store] Finished opening jobs store");

        Ok(Self {
            db: Arc::new(txn_db),
            logger,
        })
    }

    /// Persist the job in the `jobs.db` rocksdb database.
    pub(crate) async fn save_job(&self, job: Job) -> JobStoreResult<()> {
        let job_str = format!("{job:#?}");

        debug!(self.logger, "[Job Engine] Saving job {job_str}");

        let job_key = format!("{}|{}", job.job_group, job.id).into_bytes();
        let job_bytes = serialize(&job)?;

        // check if there is metadata for this job group. If not, add it.
        let job_group_metadata_key = job.job_group.into_bytes();
        let job_group_metadata_cf = self
            .db
            .cf_handle("metadata")
            .ok_or(JobStoreError::InvalidMetadataHandle)?;
        let job_group_metadata_exists = self
            .db
            .get_cf(job_group_metadata_cf, &job_group_metadata_key)?
            .is_some();

        if !job_group_metadata_exists {
            let job_group_metadata_bytes = serialize(&true)?;

            let mut batch = WriteBatchWithTransaction::<true>::default();
            batch.put(&job_key, &job_bytes);
            batch.put_cf(
                job_group_metadata_cf,
                &job_group_metadata_key,
                &job_group_metadata_bytes,
            );
            self.db.write(batch)?;
        } else {
            let mut batch = WriteBatchWithTransaction::<true>::default();
            batch.put(&job_key, &job_bytes);
            self.db.write(batch)?;
        }

        debug!(self.logger, "[Job Engine] Finished saving job {job_str}");

        Ok(())
    }

    /// Retrieve a map of jobs with `job_group` as the key, and a queue of jobs as the
    /// value.
    pub(crate) async fn get_jobs(&self) -> JobStoreResult<HashMap<String, VecDeque<Job>>> {
        debug!(self.logger, "[Job Engine] Getting queued jobs");

        let mut jobs_map = HashMap::new();

        let mut iter = self.db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            let value = iter
                .value()
                .ok_or(JobStoreError::InvalidOrCorruptedJobValue)?;

            let job: Job = deserialize(value)?;
            let job_group = job.job_group.clone();

            let jobs_list = jobs_map.entry(job_group).or_insert(VecDeque::new());
            jobs_list.push_back(job);
            iter.next();
        }

        debug!(self.logger, "[Job Engine] Finished getting queued jobs");

        Ok(jobs_map)
    }

    /// Delete the persisted job from the `jobs.db` rocksdb database.
    pub(crate) async fn delete_job(&self, job: Job) -> JobStoreResult<()> {
        let job_str = format!("{job:#?}");

        debug!(self.logger, "[Job Engine] Deleting job {job_str}");

        let job_key = format!("{}|{}", job.job_group, job.id).into_bytes();
        self.db.delete(job_key)?;

        // Check if it is the last job in the group and delete the metadata for the job  group if so
        if job.is_last_job {
            let job_group_metadata_key = job.job_group.into_bytes();
            let job_group_metadata_cf = self
                .db
                .cf_handle("metadata")
                .ok_or(JobStoreError::UnableToQueryJobGroupCompletion)?;
            self.db
                .delete_cf(job_group_metadata_cf, job_group_metadata_key)?;
        }

        debug!(self.logger, "[Job Engine] Finished deleting job {job_str}");

        Ok(())
    }

    /// Check if the job group metadata exists. If it exists, then the job
    /// group has not finished completion, and if not, it has finished
    /// execution.
    pub async fn is_job_group_complete(&self, job_group: &str) -> JobStoreResult<bool> {
        debug!(
            self.logger,
            "[Job Engine] Checking for completion of job group {job_group:#?}"
        );

        let job_group_metadata_key = job_group.to_string().into_bytes();
        let job_group_metadata_cf = self
            .db
            .cf_handle("metadata")
            .ok_or(JobStoreError::UnableToQueryJobGroupCompletion)?;

        debug!(
            self.logger,
            "[Job Engine] Finished checking for completion of job group {job_group:#?}"
        );

        Ok(self
            .db
            .get_cf(job_group_metadata_cf, job_group_metadata_key)?
            .is_none())
    }

    /// Destroy the `jobs.db` rocksdb store.
    pub(crate) fn destroy(self) -> JobStoreResult<()> {
        let path = self.db.path().to_path_buf();

        drop(self.db);
        rocksdb::TransactionDB::<SingleThreaded>::destroy(&rocksdb::Options::default(), path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::{Deref, DerefMut};

    use slog::Drain;

    fn logger() -> slog::Logger {
        let decorator = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }

    pub(crate) struct TestJobStore(Option<JobStore>);

    impl Default for TestJobStore {
        fn default() -> Self {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!(
                "test-indexer-job-engine-rocksdb-store-{}",
                rand::random::<u32>()
            ));

            Self(Some(JobStore::open(path, logger()).unwrap()))
        }
    }

    impl Drop for TestJobStore {
        fn drop(&mut self) {
            if let Some(store) = self.0.take() {
                store.destroy().unwrap();
            }
        }
    }

    impl Deref for TestJobStore {
        type Target = JobStore;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl DerefMut for TestJobStore {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }

    #[tokio::test]
    async fn test_job_store() {}
}
