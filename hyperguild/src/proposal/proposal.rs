use super::hash::ProposalHash;
use super::manifest::ProposalManifest;
use crate::key::Key;
use crate::peer::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug)]
pub struct Proposal {
    /// Accepts/votes for this proposal, only received by the leader node
    accepts: HashSet<PeerId>,

    /// Hash of the proposal state
    hash: Key<ProposalHash>,

    /// State of the proposal
    manifest: ProposalManifest,

    /// Peers (in order based on manifest.last_hash)
    peers: Vec<Key<PeerId>>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accept {
    /// Peer that sent the accept
    pub leader_id: PeerId,

    /// Hash of the proposal being accepted
    pub proposal_hash: ProposalHash,

    /// Height of the proposal being accepted, allowing to more easily
    /// skip out of date accepts
    pub height: usize,

    /// If this is a skip accept, i.e. we have skipped over a previous leader
    /// because they did not produce a proposal within the allocated period
    pub skips: usize,
}

impl Proposal {
    pub fn new(manifest: ProposalManifest, existing_peers: &[Key<PeerId>]) -> Self {
        let hash: ProposalHash = (&manifest).into();
        let hash = Key::new(hash);

        // TODO: handle join/leave peers in the manifest

        let mut peers = existing_peers.to_vec();
        peers.sort_by_key(|a| a.distance(&hash));

        Self {
            accepts: HashSet::new(),
            hash,
            manifest,
            peers,
        }
    }

    pub fn genesis(existing_peers: &[Key<PeerId>]) -> Self {
        Self::new(ProposalManifest::genesis(), existing_peers)
    }

    pub fn hash(&self) -> &ProposalHash {
        self.hash.preimage()
    }

    pub fn last_hash(&self) -> &ProposalHash {
        &self.manifest.last_proposal_hash
    }

    pub fn height(&self) -> usize {
        self.manifest.height
    }

    pub fn skips(&self) -> usize {
        self.manifest.skips
    }

    pub fn get_next_leader(&self, skip: &usize) -> PeerId {
        let len: usize = self.peers.len();
        let pos = skip % len;
        let peer = &self.peers[pos];
        peer.preimage().clone()
    }

    pub fn add_accept(&mut self, peer_id: PeerId) {
        self.accepts.insert(peer_id);
    }

    pub fn majority_accept(&self) -> bool {
        self.accepts.len() > (&self.peers.len() / 2)
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
        let peers = vec![
            Key::new(p1.clone()),
            Key::new(p2.clone()),
            Key::new(p3.clone()),
        ];

        let manifest = ProposalManifest {
            last_proposal_hash: ProposalHash::new(vec![0u8]),
            skips: 0,
            height: 0,
            peer_id: p1.clone(),
            changes: vec![],
        };

        let proposal = Proposal::new(manifest, &peers);

        // Deterministic sort order
        assert_eq!(
            proposal.peers,
            vec![
                Key::new(p2.clone()),
                Key::new(p1.clone()),
                Key::new(p3.clone())
            ]
        );

        // Can loop around the peers if needed
        assert_eq!(proposal.get_next_leader(&0), p2);
        assert_eq!(proposal.get_next_leader(&1), p1);
        assert_eq!(proposal.get_next_leader(&2), p3);
        assert_eq!(proposal.get_next_leader(&3), p2);
    }

    #[test]
    fn test_majoirty_accept() {
        let peer_1 = PeerId::new(vec![1u8]);
        let peer_2 = PeerId::new(vec![2u8]);
        let peer_3 = PeerId::new(vec![3u8]);

        let mut proposal = Proposal::new(
            ProposalManifest {
                last_proposal_hash: ProposalHash::new(vec![0u8]),
                skips: 0,
                height: 0,
                peer_id: peer_1.clone(),
                changes: vec![],
            },
            &[
                Key::new(peer_1.clone()),
                Key::new(peer_2.clone()),
                Key::new(peer_3),
            ],
        );

        proposal.add_accept(peer_1);

        assert!(!proposal.majority_accept());

        proposal.add_accept(peer_2);

        assert!(proposal.majority_accept());
    }
}
