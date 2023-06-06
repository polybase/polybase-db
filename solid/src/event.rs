use crate::peer::PeerId;
use crate::proposal::{ProposalAccept, ProposalHash, ProposalManifest};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SolidEvent {
    /// Send a new proposal to the network, as we are the the next leader
    Propose {
        last_proposal_hash: ProposalHash,
        height: usize,
        skips: usize,
    },

    /// Proposal has been confirmed and should be committed
    /// to the data store
    Commit { manifest: ProposalManifest },

    /// Accept a proposal, this is a vote this node
    /// to become the next leader (and create a proposal)
    Accept { accept: ProposalAccept },

    /// Node is missing proposals
    OutOfSync {
        /// Height of the node
        height: usize,
        max_seen_height: usize,
        accepts_sent: usize,
    },

    /// Proposal is historic or no longer valid due to
    /// other proposals
    // TODO: should we include skip in this?
    OutOfDate {
        local_height: usize,
        proposal_height: usize,
        proposal_hash: ProposalHash,
        peer_id: PeerId,
    },

    /// Duplicate proposal received
    DuplicateProposal { proposal_hash: ProposalHash },
}
