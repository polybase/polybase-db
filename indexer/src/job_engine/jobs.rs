use serde::{Deserialize, Serialize};

use crate::RecordRoot;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub job_group: String,
    pub id: usize,
    pub job_state: JobState,
    pub is_last_job: bool,
}

impl Job {
    pub fn new(
        job_group: impl Into<String>,
        id: usize,
        job_state: JobState,
        is_last_job: bool,
    ) -> Self {
        Self {
            job_group: job_group.into(),
            id,
            job_state,
            is_last_job,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum JobState {
    AddIndexes {
        collection_id: String,
        id: String,
        record: RecordRoot,
    },
}
