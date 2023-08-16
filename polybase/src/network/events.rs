use indexer_rocksdb::snapshot::SnapshotChunk;
use serde::{Deserialize, Serialize};
use solid::proposal::ProposalAccept;
use solid::proposal::ProposalManifest;

use crate::txn::CallTxn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkEvent {
    /// A peer is out of sync, and wants us to send them proposals.
    /// They are currently at the given height, and therefore are looking for
    /// proposals at height + 1.
    OutOfSync { height: usize },

    /// Received an accept for a previous proposal
    Accept { accept: ProposalAccept },

    /// Received a proposal
    Proposal { manifest: ProposalManifest },

    /// Received a snapshot request, another peer is out of sync, but they
    /// are too far behind to be able to catch up with proposals.
    SnapshotRequest { id: usize, height: usize },

    /// We have received an offer for a SnapshotRequest we sent to other peers.
    /// We should accept this offer via SnapshotAccept, if we have not already accepted
    /// another offer. Snapshots are very resource intensive, so we should only accept one.
    SnapshotOffer { id: usize },

    /// We have accepted an SnapshotOffer for our SnapshotRequest,
    /// and we can now expect to receive SnapshotChunks. At this point, we should
    /// reset the database.
    SnapshotAccept { id: usize },

    /// A chunk of data for us to load into the database.
    SnapshotChunk {
        id: usize,
        chunk: Option<SnapshotChunk>,
    },

    /// A transaction sent to another peer, which we should add to our Mempool
    Txn { txn: CallTxn },

    /// Used for testing.
    Ping,
}
