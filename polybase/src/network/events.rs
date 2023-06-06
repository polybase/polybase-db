use serde::{Deserialize, Serialize};
use solid::peer::PeerId;
use solid::proposal::ProposalAccept;
use solid::proposal::ProposalManifest;

use crate::txn::CallTxn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkEvent {
    OutOfSync { peer_id: PeerId, height: usize },
    Accept { accept: ProposalAccept },
    Proposal { manifest: ProposalManifest },
    SnapshotRequest { peer_id: PeerId, height: usize },
    Snapshot { snapshot: Vec<u8> },
    Txn { txn: CallTxn },
    Ping,
}
