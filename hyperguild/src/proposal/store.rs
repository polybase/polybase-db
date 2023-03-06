use super::event::ProposalEvent;
use super::hash::ProposalHash;
use super::manifest::ProposalManifest;
use super::proposal::{Accept, Proposal};
use crate::key::Key;
use crate::peer::PeerId;
use std::collections::{HashMap, VecDeque};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // #[error("Missing proposal for accept")]
    // MissingProposalForAccept {
    //     proposal_hash: ProposalHash,
    //     peer_id: PeerId,
    // },
}

pub struct ProposalStore {
    /// Pending proposals that may or may not end up being confiremd.
    pending_proposals: HashMap<ProposalHash, Proposal>,

    /// List of confirmed proposals, we keep a copy of confirmed proposals to share
    /// with other nodes on the network
    confirmed_proposals: VecDeque<Proposal>,

    /// Next height to considered for processing, proposals must be processed
    /// in order
    next_height: Option<usize>,

    /// Max height seen across all received proposals
    max_height: Option<usize>,

    /// Orphaned accepts are when we receive an accept for a proposal before we
    /// receive the propsal itself. We can then add these as soon as the proposal arrives.
    orphan_accepts: HashMap<ProposalHash, Vec<PeerId>>,
}

impl ProposalStore {
    pub fn new() -> Self {
        Self {
            pending_proposals: HashMap::new(),
            confirmed_proposals: VecDeque::new(),
            max_height: None,
            next_height: None,
            orphan_accepts: HashMap::new(),
        }
    }

    pub fn height(&self) -> Option<usize> {
        self.confirmed_proposals.back().map(|p| p.height())
    }

    pub fn exists(&self, hash: &ProposalHash) -> bool {
        self.pending_proposals.contains_key(hash)
    }

    pub fn add_pending_proposal(&mut self, manifest: ProposalManifest, peers: &[Key<PeerId>]) {
        let hash: ProposalHash = (&manifest).into();
        let mut proposal = Proposal::new(manifest, peers);

        // Check if we have orphaned accepts
        if let Some(accepts) = self.orphan_accepts.remove(&hash) {
            for peer_id in accepts {
                proposal.add_accept(peer_id);
            }
        }

        // Update max height seen
        if self.max_height.is_none() || proposal.height() > self.max_height.unwrap() {
            self.max_height = Some(proposal.height());
        }

        // Insert the proposal to be processed later
        self.pending_proposals.insert(hash, proposal);
    }

    pub fn process_next(&mut self) -> Option<ProposalEvent> {
        let next_height = self.next_height();
        let next_proposal = self.next_pending_proposal(next_height)?;
        let next_proposal_height = next_proposal.height();
        let next_proposal_hash = next_proposal.hash().clone();
        let next_proposal_last_hash = next_proposal.last_hash().clone();

        // We take next leader here - we now know that we are about to confirm another
        // txn, but in order to allow nodes that haven't received this next_proposal yet
        // (and may never receive it)
        let next_leader = self
            .last_confirmed_proposal()
            .map(|p| p.get_next_leader(&next_proposal.skips()));

        // TODO: validate last hash and peer_id is valid

        // Confirm the proposal before this one
        self.confirm(&next_proposal_last_hash);

        // Remove proposal that have now expired
        self.purge_skipped_proposals();

        // Update the next height
        self.next_height = Some(next_proposal_height + 1);

        // If out of sync
        if !self.up_to_date() {
            return Some(ProposalEvent::CatchingUp {
                local_height: self.height().unwrap_or(0),
                proposal_height: next_proposal_height,
                max_seen_height: self.max_height().unwrap_or(0),
            });
        }

        // In sync, so we should send accept to the next leader
        Some(ProposalEvent::SendAccept {
            proposal_hash: next_proposal_hash,
            peer_id: next_leader,
            height: next_proposal_height,
            skips: 0,
        })
    }

    /// Adds an accept to a proposal, we should only be receiving accepts if we are the
    /// designated leader. Returns whether a majority has been reached.
    pub fn add_accept(&mut self, accept: Accept) -> bool {
        let Accept {
            proposal_hash,
            peer_id,
            height,
            skips,
        } = accept;

        // Accept is out of date
        if self.height().unwrap_or(0) >= height {
            return false;
        }

        match self.pending_proposals.get_mut(&proposal_hash) {
            Some(p) => p.add_accept(peer_id),
            None => {
                // Get exisiting orphaned proposal list
                if let Some(p) = self.orphan_accepts.get_mut(&proposal_hash) {
                    p.push(peer_id);
                } else {
                    self.orphan_accepts
                        .insert(proposal_hash.clone(), vec![peer_id]);
                }
                false
            }
        }
    }

    pub fn skip(&self) -> Option<ProposalEvent> {
        // Current active proposal height
        let current_proposal_height = self.height().unwrap_or(0) + 1;

        // Get the next proposal
        let current_proposal = self.next_pending_proposal(current_proposal_height)?;

        let new_skips = current_proposal.skips() + 1;

        let next_leader = self
            .last_confirmed_proposal()
            .map(|p| p.get_next_leader(&new_skips));

        // Send skip
        Some(ProposalEvent::SendAccept {
            height: current_proposal_height,
            skips: new_skips,
            peer_id: next_leader,
            proposal_hash: current_proposal.hash().clone(),
        })
    }

    pub fn next_pending_proposal(&self, height: usize) -> Option<&Proposal> {
        let next_proposal = self
            .pending_proposals
            .values()
            .filter(|proposal| proposal.height() == height)
            .max_by(|a, b| a.skips().cmp(&b.skips()));
        next_proposal
    }

    fn last_confirmed_proposal(&self) -> Option<&Proposal> {
        self.confirmed_proposals.back()
    }

    fn next_height(&self) -> usize {
        self.next_height.unwrap_or(1)
    }

    fn max_height(&self) -> Option<usize> {
        self.max_height
    }

    fn up_to_date(&self) -> bool {
        let max_height = self.max_height().unwrap_or(0);
        let height = self.height().unwrap_or(0);
        height + 1 >= max_height
    }

    fn confirm(&mut self, proposal_hash: &ProposalHash) {
        if let Some(last_proposal) = self.pending_proposals.remove(proposal_hash) {
            // state.pending_proposals.remove(proposal_hash);
            self.confirmed_proposals.push_back(last_proposal)
        }
    }

    /// Purges skipped proposals from the pending proposal state
    fn purge_skipped_proposals(&mut self) {
        let height = self.height();

        if let Some(h) = height {
            self.pending_proposals.retain(|_, p| p.height() > h);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_process_next() {
        let mut store = ProposalStore::new();
        let p1 = PeerId::new(vec![1u8]);
        let p2 = PeerId::new(vec![2u8]);
        let p3 = PeerId::new(vec![3u8]);
        let peers = [Key::from(p1.clone()), Key::from(p2.clone()), Key::from(p3)];

        assert!(store.process_next().is_none());

        let m1 = ProposalManifest {
            last_proposal_hash: "a".into(),
            skips: 0,
            height: 1,
            peer_id: p1.clone(),
            changes: vec![],
        };
        let m1_hash: ProposalHash = (&m1).into();
        store.add_pending_proposal(m1, &peers);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                proposal_hash: m1_hash.clone(),
                height: 1,
                peer_id: None,
                skips: 0,
            })
        );
        assert_eq!(store.confirmed_proposals.len(), 0);

        let m2 = ProposalManifest {
            last_proposal_hash: m1_hash,
            skips: 0,
            height: 2,
            peer_id: p1.clone(),
            changes: vec![],
        };
        let m2_hash: ProposalHash = (&m2).into();
        store.add_pending_proposal(m2, &peers);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                proposal_hash: m2_hash.clone(),
                peer_id: None,
                height: 2,
                skips: 0,
            })
        );
        assert_eq!(store.confirmed_proposals.len(), 1);

        let m3 = ProposalManifest {
            last_proposal_hash: m2_hash,
            skips: 0,
            height: 3,
            peer_id: p1.clone(),
            changes: vec![],
        };
        let m3_hash: ProposalHash = (&m3).into();

        let m4 = ProposalManifest {
            last_proposal_hash: m3_hash,
            skips: 0,
            height: 4,
            peer_id: p1.clone(),
            changes: vec![],
        };
        let m4_hash: ProposalHash = (&m4).into();
        store.add_pending_proposal(m4, &peers);

        assert_eq!(store.process_next(), None);

        store.add_pending_proposal(m3, &peers);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::CatchingUp {
                local_height: 2,
                proposal_height: 3,
                max_seen_height: 4
            })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                proposal_hash: m4_hash,
                peer_id: Some(p2),
                height: 4,
                skips: 0,
            })
        );

        // assert_eq!(store.process_next(), None);
    }

    #[test]
    fn test_skip() {
        let mut store = ProposalStore::new();
        let p1 = PeerId::new(vec![1u8]);
        let p2 = PeerId::new(vec![2u8]);
        let p3 = PeerId::new(vec![3u8]);
        let peers = [Key::from(p1.clone()), Key::from(p2.clone()), Key::from(p3)];

        let m1 = ProposalManifest {
            last_proposal_hash: "a".into(),
            skips: 0,
            height: 1,
            peer_id: p1.clone(),
            changes: vec![],
        };
        let m1_hash: ProposalHash = (&m1).into();
        store.add_pending_proposal(m1, &peers);

        assert_eq!(
            store.skip(),
            Some(ProposalEvent::SendAccept {
                height: 1,
                skips: 1,
                peer_id: None,
                proposal_hash: m1_hash
            })
        )
    }

    #[test]
    fn test_next_pending_propsal() {
        let mut store = ProposalStore::new();
        let peer_id = PeerId::random();
        let peers = [Key::from(peer_id.clone())];

        assert!(store.next_pending_proposal(store.next_height()).is_none());

        let b = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "a".into(),
                skips: 1,
                height: 10,
                peer_id: peer_id.clone(),
                changes: vec![],
            },
            &peers,
        );

        store.confirmed_proposals.push_back(b);

        let m2 = ProposalManifest {
            last_proposal_hash: "a".into(),
            skips: 0,
            height: 10,
            peer_id: peer_id.clone(),
            changes: vec![],
        };
        store.next_height = Some(11);
        store.add_pending_proposal(m2, &peers);

        assert!(store.next_pending_proposal(store.next_height()).is_none());

        let m4 = ProposalManifest {
            last_proposal_hash: "e".into(),
            skips: 0,
            height: 12,
            peer_id: peer_id.clone(),
            changes: vec![],
        };
        store.add_pending_proposal(m4, &peers);

        assert!(store.next_pending_proposal(store.next_height()).is_none());

        let m1 = ProposalManifest {
            last_proposal_hash: "d".into(),
            skips: 0,
            height: 11,
            peer_id: peer_id.clone(),
            changes: vec![],
        };
        store.add_pending_proposal(m1, &peers);

        let m3 = ProposalManifest {
            last_proposal_hash: "d".into(),
            skips: 1,
            height: 11,
            peer_id: peer_id.clone(),
            changes: vec![],
        };
        let m3_hash: ProposalHash = (&m3).into();
        store.add_pending_proposal(m3, &peers);

        assert_eq!(
            store
                .next_pending_proposal(store.next_height())
                .unwrap()
                .hash()
                .clone(),
            m3_hash
        );
    }

    #[test]
    fn test_up_to_date() {
        let mut store = ProposalStore::new();
        let peer_id = PeerId::random();
        let peers = [Key::from(peer_id.clone())];

        // Up to date when store is empty
        assert!(store.up_to_date());

        let b = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "a".into(),
                skips: 0,
                height: 10,
                peer_id: peer_id.clone(),
                changes: vec![],
            },
            &peers,
        );

        store.confirmed_proposals.push_back(b);

        // Up to date when no pending proposals
        assert!(store.up_to_date());

        store.max_height = Some(11);

        // Up to date when max_height == height + 1
        assert!(store.up_to_date());

        store.max_height = Some(12);

        // NOT up to date when max_height > height + 1
        assert!(!store.up_to_date());
    }

    #[test]
    fn test_confirm_proposal() {
        let mut store = ProposalStore::new();
        let peer_id = PeerId::random();
        let peers = [Key::from(peer_id.clone())];

        let b = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "a".into(),
                skips: 1,
                height: 10,
                peer_id: peer_id.clone(),
                changes: vec![],
            },
            &peers,
        );
        let b_hash = b.hash().clone();
        store.pending_proposals.insert(b.hash().clone(), b);

        store.confirm(&b_hash);

        assert_eq!(store.pending_proposals.len(), 0);
        assert_eq!(store.confirmed_proposals[0].hash(), &b_hash);
    }

    #[test]
    fn test_purge_skipped_proposals() {
        let mut store = ProposalStore::new();
        let peer_id = PeerId::random();
        let peers = [Key::from(peer_id.clone())];

        // Purge on empty store
        store.purge_skipped_proposals();

        let b = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "a".into(),
                skips: 1,
                height: 10,
                peer_id: peer_id.clone(),
                changes: vec![],
            },
            &[Key::from(peer_id.clone())],
        );
        store.confirmed_proposals.push_back(b);

        let a = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "x".into(),
                skips: 0,
                height: 10,
                peer_id: peer_id.clone(),
                changes: vec![],
            },
            &peers,
        );
        store.pending_proposals.insert(a.hash().clone(), a);

        let c = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "b".into(),
                skips: 0,
                height: 11,
                peer_id: peer_id.clone(),
                changes: vec![],
            },
            &peers,
        );
        let c_hash = c.hash().clone();
        store.pending_proposals.insert(c.hash().clone(), c);

        store.purge_skipped_proposals();

        assert_eq!(store.pending_proposals.len(), 1);
        assert!(store.pending_proposals.contains_key(&c_hash));
    }
}
