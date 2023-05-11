use crate::change::Change;
use crate::peer::PeerId;
use crate::proposal::{ProposalAccept, ProposalHash, ProposalManifest};
use crate::Snapshot;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum SolidEvent {
    /// Propose a commit
    Proposal {
        manifest: ProposalManifest,
        proposal_hash: ProposalHash,
    },

    /// Accept a proposal, this is a vote this node
    /// to become the next leader (and create a proposal)
    Accept { accept: ProposalAccept },

    /// Add a set of pending txns to the queue
    AddPendingChange { changes: Vec<Change> },

    /// Node is missing proposals, send them proposals or snapshot
    OutOfSync {
        /// Height of the node
        height: usize,
        max_seen_height: usize,
        accepts_sent: usize,
    },

    /// Full sync of data
    Snapshot { snapshot: Snapshot },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalEvent {
    /// Proposal register is missing proposals
    OutOfSync {
        /// Height of the node
        local_height: usize,
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

    /// Send accept to the next leader peer, this is sent when we are up to date
    /// and have received a valid proposal, or when we want to skip a leader
    SendAccept { accept: ProposalAccept },

    /// Send a new proposal to the network, as we are the the next leader
    Propose {
        last_proposal_hash: ProposalHash,
        height: usize,
        skips: usize,
    },

    /// Proposal has been confirmed and should be committed
    /// to the data store
    Commit { manifest: ProposalManifest },

    /// Duplicate proposal received
    DuplicateProposal { proposal_hash: ProposalHash },
}
