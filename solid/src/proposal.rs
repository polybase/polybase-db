use crate::key::Key;
use crate::peer::PeerId;
use crate::txn::Txn;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    /// Accepts/votes for this proposal, only received by the leader node
    incoming_accepts: HashMap<usize, HashSet<PeerId>>,

    /// Hash of the proposal state
    hash: Key<ProposalHash>,

    /// State of the proposal that is sent across the network
    pub manifest: ProposalManifest,

    /// Peers (in order based on manifest.last_hash)
    peers: Vec<PeerId>,
}

/// ProposalAccept is sent by all peers to the next leader to indicate
/// they accept a previous proposal. ProposalAccept is also used in the scenario
/// where a leader is skipped because they did not produce a proposal in time.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalAccept {
    /// Peer that sent the accept
    pub leader_id: PeerId,

    /// Hash of the proposal being accepted
    pub proposal_hash: ProposalHash,

    /// Height of the proposal being accepted, allowing to more easily
    /// ignore out of date accepts
    pub height: usize,

    /// If skips > 0, we have skipped over a previous leader because they
    /// did not produce a proposal within the allocated period. This is the
    /// number of skips that have occurred since the last confirmed proposal.
    pub skips: usize,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalManifest {
    /// Hash of the last proposal, so we can confirm the last
    /// proposal when we receive this message
    pub last_proposal_hash: ProposalHash,

    /// Number of skips of leader that have occured since the last
    /// leadership order change. This skips should match the skips
    /// sent to this node in the ProposalAccept messages.
    pub skips: usize,

    /// Height of the proposal, for easy checking whether we
    /// are up to date with the network
    pub height: usize,

    /// PeerId of the proposer/leader
    pub leader_id: PeerId,

    /// Changes included in the proposal
    pub txns: Vec<Txn>,

    /// List of peers on the network
    pub peers: Vec<PeerId>,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub struct ProposalHash(Vec<u8>);

impl Proposal {
    pub fn new(manifest: ProposalManifest) -> Self {
        let hash: ProposalHash = (&manifest).into();
        let hash = Key::new(hash);

        let mut peers = manifest.peers.to_vec();
        peers.sort_by_key(|a| Key::new(a.clone()).distance(&hash));

        Self {
            incoming_accepts: HashMap::new(),
            hash,
            manifest,
            peers,
        }
    }

    /// Generates a genesis proposal, which uses default values except for peers
    pub fn genesis(existing_peers: Vec<PeerId>) -> Self {
        Self::new(ProposalManifest::genesis(existing_peers))
    }

    pub fn hash(&self) -> &ProposalHash {
        self.hash.preimage()
    }

    pub fn last_hash(&self) -> &ProposalHash {
        &self.manifest.last_proposal_hash
    }

    /// Height of this proposal
    pub fn height(&self) -> usize {
        self.manifest.height
    }

    /// Number of skips of leader that have occured since the last leadership order change.
    /// Skips should match the skips sent to this node in the ProposalAccept messages.
    pub fn skips(&self) -> usize {
        self.manifest.skips
    }

    pub fn add_accept(&mut self, skips: &usize, peer_id: PeerId) -> bool {
        let added = self
            .incoming_accepts
            .entry(*skips)
            .or_insert(HashSet::new())
            .insert(peer_id);
        added && self.majority_accept_breached(skips)
    }

    /// Checks that we have just enough accepts for meeting the majority
    /// threshold, allowing us to confirm the proposal when majority threshold met,
    /// but only when the threshold is first breached
    pub fn majority_accept_breached(&self, skips: &usize) -> bool {
        let len = self
            .incoming_accepts
            .get(skips)
            .map(|p| p.len())
            .unwrap_or(0);
        len > (&self.peers.len() / 2) && len - 1 <= (&self.peers.len() / 2)
    }

    pub fn get_next_leader(&self, skip: usize) -> PeerId {
        let len: usize = self.peers.len();
        let pos = skip % len;
        let peer = &self.peers[pos];
        peer.clone()
    }
}

impl ProposalManifest {
    pub fn genesis(peers: Vec<PeerId>) -> Self {
        ProposalManifest {
            last_proposal_hash: ProposalHash::genesis(),
            skips: 0,
            height: 0,
            leader_id: PeerId::genesis(),
            txns: vec![],
            peers,
        }
    }

    pub fn hash(&self) -> ProposalHash {
        (self).into()
    }
}

impl ProposalHash {
    pub fn new(v: Vec<u8>) -> Self {
        ProposalHash(v)
    }

    pub fn genesis() -> Self {
        ProposalHash(vec![0u8])
    }
}

impl Default for ProposalHash {
    fn default() -> Self {
        ProposalHash(Sha256::digest([0u8]).to_vec())
    }
}

impl Display for ProposalHash {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl From<String> for ProposalHash {
    fn from(str: String) -> Self {
        ProposalHash(Sha256::digest(str).to_vec())
    }
}

impl From<&str> for ProposalHash {
    fn from(str: &str) -> Self {
        ProposalHash(Sha256::digest(str).to_vec())
    }
}

impl Borrow<[u8]> for ProposalHash {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl From<&ProposalManifest> for ProposalHash {
    fn from(p: &ProposalManifest) -> Self {
        #[allow(clippy::unwrap_used)]
        let bytes = Sha256::digest(bincode::serialize(p).unwrap());
        ProposalHash(bytes.to_vec())
    }
}

impl From<ProposalHash> for Key<ProposalHash> {
    fn from(p: ProposalHash) -> Self {
        Key::new(p)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_next_leader() {
        let p1 = PeerId::new(vec![1u8]);
        let p2 = PeerId::new(vec![2u8]);
        let p3 = PeerId::new(vec![3u8]);

        let manifest = ProposalManifest {
            last_proposal_hash: ProposalHash::new(vec![0u8]),
            skips: 0,
            height: 0,
            leader_id: p1.clone(),
            txns: vec![],
            peers: vec![p1.clone(), p2.clone(), p3.clone()],
        };

        let proposal = Proposal::new(manifest);

        // Deterministic sort order
        assert_eq!(proposal.peers, vec![p1.clone(), p3.clone(), p2.clone()]);

        // Can loop around the peers if needed
        assert_eq!(proposal.get_next_leader(0), p1);
        assert_eq!(proposal.get_next_leader(1), p3);
        assert_eq!(proposal.get_next_leader(2), p2);
        assert_eq!(proposal.get_next_leader(2), p2);
    }

    #[test]
    fn test_majoirty_accept_breached() {
        let p1 = PeerId::new(vec![1u8]);
        let p2 = PeerId::new(vec![2u8]);
        let p3 = PeerId::new(vec![3u8]);

        let mut proposal = Proposal::new(ProposalManifest {
            last_proposal_hash: ProposalHash::new(vec![0u8]),
            skips: 0,
            height: 0,
            leader_id: p1.clone(),
            txns: vec![],
            peers: vec![p1.clone(), p2.clone(), p3.clone()],
        });

        proposal.add_accept(&0, p1);

        assert!(
            !proposal.majority_accept_breached(&0),
            "Should not be breached"
        );

        proposal.add_accept(&0, p2);

        assert!(proposal.majority_accept_breached(&0), "Should be breached");

        proposal.add_accept(&0, p3);

        assert!(
            !proposal.majority_accept_breached(&0),
            "Should not be breached"
        );
    }
}
