use super::hash::ProposalHash;
use super::manifest::ProposalManifest;
use crate::peer::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalEvent {
    /// Proposal register is missing proposals
    OutOfSync {
        /// Height of the node
        local_height: usize,
        max_seen_height: usize,
    },

    /// We are behind the network, but we are comitting
    /// the next proposal
    CatchingUp {
        local_height: usize,
        proposal_height: usize,
        max_seen_height: usize,
    },

    /// Proposal is historic or no longer valid due to
    /// other proposals
    // TODO: should we include skip in this?
    OutOfDate {
        local_height: usize,
        proposal_height: usize,
    },

    /// Send accept to the peer
    SendAccept {
        height: usize,
        skips: usize,
        peer_id: Option<PeerId>,
        proposal_hash: ProposalHash,
    },

    /// Send a new proposal to the network
    Propose {
        last_proposal_hash: ProposalHash,
        height: usize,
    },

    /// Proposal has been confirmed and should be committed
    /// to the data store
    Commit { manifest: ProposalManifest },

    /// Duplicate proposal received
    DuplicateProposal,
}
