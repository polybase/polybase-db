use serde::{Deserialize, Serialize};
use std::fmt;

use crate::RecordRoot;

/// Represents a job that can be enqueued, run, and deleted by the indexer Job Engine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub job_group: String,
    pub id: usize,
    pub job_state: JobState,
    pub is_last_job: bool,
}

impl Job {
    pub fn new(
        job_group: impl AsRef<str>,
        id: usize,
        job_state: JobState,
        is_last_job: bool,
    ) -> Self {
        Self {
            job_group: job_group.as_ref().to_owned(),
            id,
            job_state,
            is_last_job,
        }
    }
}

/// This contains job metadata specific to the task being carried out. This metadata gets stored in
/// the jobs store while the actual job logic is contained within the Job Engine.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum JobState {
    AddIndexes {
        collection_id: String,
        id: String,
        record: RecordRoot,
    },
}

impl fmt::Debug for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobState::AddIndexes {
                ref collection_id,
                ref id,
                ..
            } => {
                write!(
                    f,
                    "AddIndexes {{ collection_id: {collection_id:?}, id: {id:?} }}"
                )
            }
        }
    }
}
