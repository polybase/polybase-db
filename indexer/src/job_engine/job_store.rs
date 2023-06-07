use bincode::{deserialize, serialize};
use rocksdb::{
    Options, SingleThreaded, TransactionDB, TransactionDBOptions, WriteBatchWithTransaction,
};
use std::collections::{HashMap, VecDeque};
use std::{convert::AsRef, path::Path, sync::Arc};

use super::Job;

pub type Result<T> = std::result::Result<T, JobStoreError>;

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
}

// todo; see if snapshot, restore, and delete are required for JobStore
impl JobStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let mut txn_db = TransactionDB::<SingleThreaded>::open_cf(
            &options,
            &TransactionDBOptions::default(),
            path,
            vec!["metadata"],
        )?;

        //let mut cf_opts = Options::default();
        //cf_opts.create_if_missing(true);
        //txn_db.create_cf("metadata", &cf_opts)?;

        Ok(Self {
            db: Arc::new(txn_db),
        })
    }

    /// Persist the job in the `jobs.db` rocksdb database.
    pub(crate) async fn save_job(&self, job: Job) -> Result<()> {
        let job_key = format!("{}|{}", job.job_group, job.id).into_bytes();
        let job_bytes = serialize(&job)?;

        // check if there is metadata for this job group. If not, add it.
        let job_group_metadata_key = job.job_group.clone().into_bytes();
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

        Ok(())
    }

    /// Retrieve a map of jobs with `job_group` as the key, and a queue of jobs as the
    /// value.
    pub(crate) async fn get_jobs(&self) -> Result<HashMap<String, VecDeque<Job>>> {
        let mut jobs_map = HashMap::new();

        let mut iter = self.db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            let value = iter
                .value()
                .ok_or(JobStoreError::InvalidOrCorruptedJobValue)?;

            let job: Job = deserialize(&value)?;
            let job_group = job.job_group.clone();

            let jobs_list = jobs_map.entry(job_group).or_insert(VecDeque::new());
            jobs_list.push_back(job);
            iter.next();
        }

        Ok(jobs_map)
    }

    /// Delete the persisted job from the `jobs.db` rocksdb database.
    pub(crate) async fn delete_job(&self, job: Job) -> Result<()> {
        let job_key = format!("{}|{}", job.job_group, job.id).into_bytes();
        self.db.delete(&job_key)?;

        // Check if it is the last job in the group and delete the metadata for the job  group if so
        if job.is_last_job {
            let job_group_metadata_key = job.job_group.clone().into_bytes();
            let job_group_metadata_cf = self
                .db
                .cf_handle("metadata")
                .ok_or(JobStoreError::UnableToQueryJobGroupCompletion)?;
            self.db
                .delete_cf(job_group_metadata_cf, &job_group_metadata_key)?;
        }

        Ok(())
    }

    /// Check if the job group metadata exists. If it exists, then the job
    /// group has not finished completion, and if not, it has finished
    /// execution.
    pub async fn is_job_group_complete(&self, job_group: &str) -> Result<bool> {
        let job_group_metadata_key = job_group.to_string().into_bytes();
        let job_group_metadata_cf = self
            .db
            .cf_handle("metadata")
            .ok_or(JobStoreError::UnableToQueryJobGroupCompletion)?;
        Ok(self
            .db
            .get_cf(job_group_metadata_cf, &job_group_metadata_key)?
            .is_none())
    }

    /// Destroy the `jobs.db` rocksdb store.
    pub(crate) fn destroy(self) -> Result<()> {
        let path = self.db.path().to_path_buf();

        drop(self.db);
        rocksdb::TransactionDB::<SingleThreaded>::destroy(&rocksdb::Options::default(), path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job_engine::jobs::*;
    use std::ops::{Deref, DerefMut};

    pub(crate) struct TestJobStore(Option<JobStore>);

    impl Default for TestJobStore {
        fn default() -> Self {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!(
                "test-indexer-job-engine-rocksdb-store-{}",
                rand::random::<u32>()
            ));

            Self(Some(JobStore::open(path).unwrap()))
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

    // todo - change the tests when the actual Job Types are defined
    #[tokio::test]
    async fn test_job_store() {
        let job_store = TestJobStore::default();

        let job13 = Job::new("Group1", 3, JobState::JobType1 { num: 100 }, true);
        job_store.save_job(job13.clone()).await.unwrap();

        let job12 = Job::new(
            "Group1",
            2,
            JobState::JobType2 {
                string: "Dave".into(),
                num: 42,
            },
            false,
        );
        job_store.save_job(job12.clone()).await.unwrap();

        let job21 = Job::new("Group2", 1, JobState::JobType3 { b: true }, false);
        job_store.save_job(job21.clone()).await.unwrap();

        let job11 = Job::new("Group1", 1, JobState::JobType1 { num: 21 }, false);
        job_store.save_job(job11.clone()).await.unwrap();

        let job22 = Job::new(
            "Group2",
            2,
            JobState::JobType2 {
                string: "Bob".into(),
                num: 99,
            },
            true,
        );
        job_store.save_job(job22.clone()).await.unwrap();

        let mut jobs = job_store.get_jobs().await.unwrap();

        let mut group1_jobs = jobs.remove("Group1").unwrap();
        assert_eq!(group1_jobs.pop_front(), Some(job11.clone()));
        assert_eq!(group1_jobs.pop_front(), Some(job12.clone()));
        assert_eq!(group1_jobs.pop_front(), Some(job13.clone()));
        assert_eq!(group1_jobs.pop_front(), None);

        let mut group2_jobs = jobs.remove("Group2").unwrap();
        assert_eq!(group2_jobs.pop_front(), Some(job21.clone()));
        assert_eq!(group2_jobs.pop_front(), Some(job22.clone()));
        assert_eq!(group2_jobs.pop_front(), None);

        // delete jobs
        job_store.delete_job(job11.clone()).await;
        job_store.delete_job(job22.clone()).await;

        let mut jobs_after_deletion = job_store.get_jobs().await.unwrap();

        let mut group1_jobs = jobs_after_deletion.remove("Group1").unwrap();
        assert_eq!(group1_jobs.pop_front(), Some(job12.clone()));
        assert_eq!(group1_jobs.pop_front(), Some(job13.clone()));
        assert_eq!(group1_jobs.pop_front(), None);

        let mut group2_jobs = jobs_after_deletion.remove("Group2").unwrap();
        assert_eq!(group2_jobs.pop_front(), Some(job21.clone()));
        assert_eq!(group2_jobs.pop_front(), None);

        // delete all jobs
        job_store.delete_job(job12).await;
        job_store.delete_job(job13).await;
        job_store.delete_job(job21).await;

        jobs_after_deletion = job_store.get_jobs().await.unwrap();
        assert!(jobs_after_deletion.is_empty());
    }
}
