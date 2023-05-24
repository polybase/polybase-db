use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SolidConfig {
    /// Minimum delay for each proposal
    pub min_proposal_duration: Duration,

    /// Maximum number of confirmed proposals to keep in history
    pub max_proposal_history: usize,

    /// Amount of time to wait before we skip a leader
    pub skip_timeout: Duration,

    /// Amount of time to wait before we send another out of sync message
    pub out_of_sync_timeout: Duration,
}

impl Default for SolidConfig {
    fn default() -> Self {
        SolidConfig {
            min_proposal_duration: Duration::from_secs(1),
            max_proposal_history: 1024,
            skip_timeout: Duration::from_secs(5),
            out_of_sync_timeout: Duration::from_secs(60),
        }
    }
}
