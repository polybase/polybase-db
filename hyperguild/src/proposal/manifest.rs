use super::hash::ProposalHash;
use crate::change::Change;
use crate::peer::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalManifest {
    /// Hash of the last proposal, so we can confirm the last
    /// proposal when we receive this message
    pub last_proposal_hash: ProposalHash,

    // Number of skips of leader that have occured since the last
    // leadership order change, we need a consistent ordering while
    // skips are occurring
    pub skips: usize,

    /// Height of the proposal, for easy checking whether we
    /// are up to date with the network
    pub height: usize,

    /// PeerId of the proposer/leader
    pub peer_id: PeerId,

    /// Changes included in the proposal
    pub changes: Vec<Change>,
}
