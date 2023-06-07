use serde::{Deserialize, Serialize};

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

// todo - change these dummy types to actual job types for the Job Engine
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum JobState {
    JobType1 { num: i32 },
    JobType2 { string: String, num: i32 },
    JobType3 { b: bool },
}
