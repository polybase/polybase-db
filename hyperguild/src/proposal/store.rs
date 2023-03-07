use super::event::ProposalEvent;
use super::hash::ProposalHash;
use super::manifest::ProposalManifest;
use super::proposal::{Proposal, ProposalAccept};
use crate::guild;
use crate::key::Key;
use crate::peer::PeerId;
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub struct ProposalStore {
    /// Local peer, required so we can determine if we are the leader
    local_peer_id: PeerId,

    /// All peers on the network, this is used to determine which peer to
    /// send accepts to and the threshold required for
    peers: Vec<Key<PeerId>>,

    /// Pending proposals that may or may not end up being confiremd.
    pending_proposals: HashMap<ProposalHash, Proposal>,

    /// List of confirmed proposals, we keep a copy of confirmed proposals to share
    /// with other nodes on the network
    confirmed_proposals: VecDeque<Proposal>,

    /// Next height to considered for processing, proposals must be processed
    /// in order
    next_height: Option<usize>,

    /// Max height seen across all received proposals
    max_height: usize,

    /// Number of skips for the current height
    skips: usize,

    /// Orphaned accepts are when we receive an accept for a proposal before we
    /// receive the propsal itself. We can then add these as soon as the proposal arrives.
    orphan_accepts: HashMap<ProposalHash, Vec<(usize, PeerId)>>,
}

impl ProposalStore {
    pub fn new(local_peer_id: PeerId, peers: Vec<PeerId>) -> Self {
        let mut peers = peers;

        if !peers.contains(&local_peer_id) {
            peers.push(local_peer_id.clone());
        }

        let peers: Vec<Key<PeerId>> = peers.into_iter().map(Key::new).collect();

        let mut confirmed = VecDeque::new();
        confirmed.push_back(Proposal::genesis(&peers));

        Self {
            local_peer_id,
            peers,
            pending_proposals: HashMap::new(),
            confirmed_proposals: confirmed,
            max_height: 0,
            next_height: None,
            orphan_accepts: HashMap::new(),
            skips: 0,
        }
    }

    pub fn height(&self) -> usize {
        self.confirmed_proposals
            .back()
            .map(|p| p.height())
            .unwrap_or(0)
    }

    pub fn exists(&self, hash: &ProposalHash) -> bool {
        self.pending_proposals.contains_key(hash)
    }

    pub fn add_pending_proposal(&mut self, manifest: ProposalManifest) {
        let hash: ProposalHash = (&manifest).into();
        let mut proposal = Proposal::new(manifest, &self.peers);

        // Check if we have orphaned accepts
        if let Some(accepts) = self.orphan_accepts.remove(&hash) {
            for (skips, peer_id) in accepts {
                proposal.add_accept(&skips, peer_id);
            }
        }

        // Update max height seen
        if proposal.height() > self.max_height {
            self.max_height = proposal.height();
        }

        // Insert the proposal to be processed later
        self.pending_proposals.insert(hash, proposal);
    }

    pub fn process_next(&mut self) -> Option<ProposalEvent> {
        let next_height = self.next_height();
        let next_proposal = match self.next_pending_proposal(next_height) {
            Some(p) => p,
            None => {
                if !self.up_to_date() {
                    return Some(ProposalEvent::OutOfSync {
                        local_height: self.height(),
                        max_seen_height: self.max_height,
                        skips: self.skips,
                    });
                }
                return None;
            }
        };

        let next_proposal_height = next_proposal.height();
        let next_proposal_hash = next_proposal.hash().clone();
        let next_proposal_last_hash = next_proposal.last_hash().clone();

        // Next proposal didn't arrive in time and we're waiting on a later skip
        // proposal, we only accept earlier skipped proposals if the network tells us
        if self.skips > next_proposal.skips() && self.max_height == next_proposal.height() {
            // let next = self.next_pending_proposal(height)
            return None;
        }

        // We take next leader here - we now know that we are about to confirm another
        // txn, but in order to allow nodes that haven't received this next_proposal yet
        // (and may never receive it)
        let next_leader = self
            .last_confirmed_proposal()
            .get_next_leader(&next_proposal.skips());

        // TODO: validate last hash and peer_id is valid

        // Confirm the proposal before this one
        self.confirm(&next_proposal_last_hash);

        // Remove proposal that have now expired
        self.purge_skipped_proposals();

        // Update the next height
        self.next_height = Some(next_proposal_height + 1);

        // Reset skips, as we now have a valid proposal
        self.skips = 0;

        // If out of sync
        if !self.up_to_date() {
            return Some(ProposalEvent::CatchingUp {
                local_height: self.height(),
                proposal_height: next_proposal_height,
                max_seen_height: self.max_height,
            });
        }

        let accept = ProposalAccept {
            proposal_hash: next_proposal_hash,
            leader_id: next_leader,
            height: next_proposal_height,
            skips: 0,
        };

        // In sync, so we should send accept to the next leader
        Some(ProposalEvent::SendAccept { accept })
    }

    /// Adds an accept to a proposal, we should only be receiving accepts if we are the
    /// designated leader. Returns whether a majority has been reached.
    // TODO: this should be a result
    pub fn add_accept(&mut self, accept: &ProposalAccept, from: PeerId) -> Option<ProposalEvent> {
        let ProposalAccept {
            proposal_hash,
            leader_id,
            height,
            skips,
        } = accept;

        // Accept is out of date
        if self.height() >= *height {
            return None;
        }

        match self.pending_proposals.get_mut(proposal_hash) {
            Some(p) => {
                // Skip if skips is not valid
                if p.skips() != *skips {
                    return None;
                }
                p.add_accept(skips, from);
                if p.majority_accept(skips) {
                    return Some(ProposalEvent::Propose {
                        last_proposal_hash: proposal_hash.clone(),
                        height: height + 1,
                    });
                }
                None
            }
            None => {
                // Get exisiting orphaned proposal list
                if let Some(p) = self.orphan_accepts.get_mut(proposal_hash) {
                    p.push((*skips, leader_id.clone()));
                } else {
                    self.orphan_accepts
                        .insert(proposal_hash.clone(), vec![(*skips, leader_id.clone())]);
                }
                None
            }
        }
    }

    // TODO: update to result
    pub fn skip(&mut self) -> Option<ProposalEvent> {
        // Just in case we try to skip when we're still catching up
        if !self.up_to_date() {
            return None;
        }

        // Current active proposal height
        let current_proposal_height = self.height() + 1;

        // Get the next proposal
        let current_proposal = self.next_pending_proposal(current_proposal_height)?;

        // New skip counter
        let new_skips = self.skips + 1;

        let next_leader = self.last_confirmed_proposal().get_next_leader(&new_skips);

        let accept = ProposalAccept {
            height: current_proposal_height,
            skips: new_skips,
            leader_id: next_leader,
            proposal_hash: current_proposal.hash().clone(),
        };

        self.skips = new_skips;

        // Send skip
        Some(ProposalEvent::SendAccept { accept })
    }

    fn next_pending_proposal(&self, height: usize) -> Option<&Proposal> {
        let next_proposal = self
            .pending_proposals
            .values()
            .filter(|proposal| proposal.height() == height)
            .max_by(|a, b| a.skips().cmp(&b.skips()));
        next_proposal
    }

    fn last_confirmed_proposal(&self) -> &Proposal {
        // We can unwrap, because we always ensure that confirmed_proposals has at least one
        // entry (for genesis we add a proxy empty proposal)
        self.confirmed_proposals.back().unwrap()
    }

    fn next_height(&self) -> usize {
        self.next_height.unwrap_or(1)
    }

    fn up_to_date(&self) -> bool {
        self.height() + 1 >= self.max_height
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
        self.pending_proposals.retain(|_, p| p.height() > height);
    }
}

impl guild::Store for ProposalStore {
    fn commit(&self, changes: Vec<crate::change::Change>) -> Vec<u8> {
        todo!()
    }

    fn restore(&self, from: Option<Vec<u8>>) -> guild::SnapshotResp {
        todo!()
    }

    fn snapshot(&self, data: Vec<u8>) {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use super::*;

    fn create_peers() -> (PeerId, PeerId, PeerId) {
        let p1 = PeerId::new(vec![1u8]);
        let p2 = PeerId::new(vec![2u8]);
        let p3 = PeerId::new(vec![3u8]);
        (p1, p2, p3)
    }

    #[test]
    fn test_process_next() {
        let (p1, p2, p3) = create_peers();
        let mut store = ProposalStore::new(p1.clone(), vec![p1.clone(), p2.clone(), p3]);

        assert!(store.process_next().is_none());

        let m1 = ProposalManifest {
            last_proposal_hash: "a".into(),
            skips: 0,
            height: 1,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m1_hash: ProposalHash = (&m1).into();
        store.add_pending_proposal(m1);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    height: 1,
                    leader_id: p2.clone(),
                    skips: 0,
                }
            })
        );
        assert_eq!(store.confirmed_proposals.len(), 1);

        let m2 = ProposalManifest {
            last_proposal_hash: m1_hash,
            skips: 0,
            height: 2,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m2_hash: ProposalHash = (&m2).into();
        store.add_pending_proposal(m2);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    proposal_hash: m2_hash.clone(),
                    leader_id: p2.clone(),
                    height: 2,
                    skips: 0,
                }
            })
        );
        assert_eq!(store.confirmed_proposals.len(), 2);

        let m3 = ProposalManifest {
            last_proposal_hash: m2_hash,
            skips: 0,
            height: 3,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m3_hash: ProposalHash = (&m3).into();

        let m4 = ProposalManifest {
            last_proposal_hash: m3_hash,
            skips: 0,
            height: 4,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m4_hash: ProposalHash = (&m4).into();
        store.add_pending_proposal(m4);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::OutOfSync {
                local_height: 1,
                max_seen_height: 4,
                skips: 0
            })
        );

        store.add_pending_proposal(m3);

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
                accept: ProposalAccept {
                    proposal_hash: m4_hash,
                    leader_id: p2,
                    height: 4,
                    skips: 0,
                }
            })
        );

        // assert_eq!(store.process_next(), None);
    }

    /// Node skips, network skips
    #[test]
    fn test_skip_one_network_skip() {
        let (p1, p2, p3) = create_peers();
        let mut store = ProposalStore::new(p1.clone(), vec![p1.clone(), p2.clone(), p3.clone()]);

        assert!(store.process_next().is_none());

        let m1 = ProposalManifest {
            last_proposal_hash: store.confirmed_proposals[0].hash().clone(),
            skips: 0,
            height: 1,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m1_hash: ProposalHash = (&m1).into();
        store.add_pending_proposal(m1);

        // Send accept for m1
        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    leader_id: p2.clone(),
                    height: 1,
                    skips: 0,
                }
            })
        );

        // Send skip after timeout
        assert_eq!(
            store.skip(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    leader_id: p3.clone(),
                    height: 1,
                    skips: 1,
                }
            })
        );

        // Proposal arrives late (but is now invalid)
        let m2 = ProposalManifest {
            last_proposal_hash: m1_hash.clone(),
            skips: 0,
            height: 2,
            leader_id: p2.clone(),
            changes: vec![],
        };
        store.add_pending_proposal(m2);
        assert_eq!(store.process_next(), None);

        // Proposal (+1 skip) now arrives
        let m3 = ProposalManifest {
            last_proposal_hash: m1_hash,
            skips: 1,
            height: 2,
            leader_id: p2,
            changes: vec![],
        };
        let m3_hash: ProposalHash = (&m3).into();
        store.add_pending_proposal(m3);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    leader_id: p3,
                    proposal_hash: m3_hash,
                    height: 2,
                    skips: 0,
                }
            })
        );

        assert_eq!(store.skips, 0);
    }

    /// Node skips, network no skip
    #[test]
    fn test_skip_one_no_network_skip() {
        let (p1, p2, p3) = create_peers();
        let mut store = ProposalStore::new(p1.clone(), vec![p1.clone(), p2.clone(), p3.clone()]);

        assert!(store.process_next().is_none());

        let m1 = ProposalManifest {
            last_proposal_hash: store.confirmed_proposals[0].hash().clone(),
            skips: 0,
            height: 1,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m1_hash: ProposalHash = (&m1).into();
        store.add_pending_proposal(m1);

        // Send accept for m1
        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    leader_id: p2.clone(),
                    height: 1,
                    skips: 0,
                }
            })
        );

        // Send skip after timeout
        assert_eq!(
            store.skip(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    leader_id: p3.clone(),
                    height: 1,
                    skips: 1,
                }
            })
        );

        let m2 = ProposalManifest {
            last_proposal_hash: m1_hash,
            skips: 0,
            height: 2,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m2_hash: ProposalHash = (&m2).into();
        store.add_pending_proposal(m2);

        // We ignore m2 as we've skipped
        assert_eq!(store.process_next(), None);

        // Next node
        let m3 = ProposalManifest {
            last_proposal_hash: m2_hash.clone(),
            skips: 0,
            height: 3,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m3_hash: ProposalHash = (&m3).into();
        store.add_pending_proposal(m3);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::CatchingUp {
                local_height: 1,
                proposal_height: 2,
                max_seen_height: 3,
            })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    leader_id: p2,
                    height: 3,
                    proposal_hash: m3_hash,
                    skips: 0,
                }
            })
        );
    }

    #[test]
    fn test_skip() {
        let (p1, p2, p3) = create_peers();
        let mut store = ProposalStore::new(p1.clone(), vec![p1.clone(), p2.clone(), p3.clone()]);

        let m1 = ProposalManifest {
            last_proposal_hash: "a".into(),
            skips: 0,
            height: 1,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m1_hash: ProposalHash = (&m1).into();
        store.add_pending_proposal(m1);

        assert_eq!(
            store.skip(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    height: 1,
                    skips: 1,
                    leader_id: p3.clone(),
                    proposal_hash: m1_hash.clone(),
                }
            })
        );

        assert_eq!(
            store.skip(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    height: 1,
                    skips: 2,
                    leader_id: p1,
                    proposal_hash: m1_hash.clone(),
                }
            })
        );

        assert_eq!(
            store.skip(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    height: 1,
                    skips: 3,
                    leader_id: p2,
                    proposal_hash: m1_hash.clone(),
                }
            })
        );

        assert_eq!(
            store.skip(),
            Some(ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    height: 1,
                    skips: 4,
                    leader_id: p3,
                    proposal_hash: m1_hash
                }
            })
        );
    }

    #[test]
    fn test_next_pending_propsal() {
        let (p1, p2, p3) = create_peers();
        let peers = vec![p1.clone(), p2, p3];
        let mut store = ProposalStore::new(p1.clone(), peers.clone());

        assert!(store.next_pending_proposal(store.next_height()).is_none());

        let b = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "a".into(),
                skips: 1,
                height: 10,
                leader_id: p1.clone(),
                changes: vec![],
            },
            &store.peers,
        );

        store.confirmed_proposals.push_back(b);

        let m2 = ProposalManifest {
            last_proposal_hash: "a".into(),
            skips: 0,
            height: 10,
            leader_id: p1.clone(),
            changes: vec![],
        };
        store.next_height = Some(11);
        store.add_pending_proposal(m2);

        assert!(store.next_pending_proposal(store.next_height()).is_none());

        let m4 = ProposalManifest {
            last_proposal_hash: "e".into(),
            skips: 0,
            height: 12,
            leader_id: p1.clone(),
            changes: vec![],
        };
        store.add_pending_proposal(m4);

        assert!(store.next_pending_proposal(store.next_height()).is_none());

        let m1 = ProposalManifest {
            last_proposal_hash: "d".into(),
            skips: 0,
            height: 11,
            leader_id: p1.clone(),
            changes: vec![],
        };
        store.add_pending_proposal(m1);

        let m3 = ProposalManifest {
            last_proposal_hash: "d".into(),
            skips: 1,
            height: 11,
            leader_id: p1.clone(),
            changes: vec![],
        };
        let m3_hash: ProposalHash = (&m3).into();
        store.add_pending_proposal(m3);

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
        let (p1, p2, p3) = create_peers();
        let peers = vec![p1.clone(), p2, p3];
        let mut store = ProposalStore::new(p1.clone(), peers.clone());

        // Up to date when store is empty
        assert!(store.up_to_date());

        let b = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "a".into(),
                skips: 0,
                height: 10,
                leader_id: p1.clone(),
                changes: vec![],
            },
            &store.peers,
        );

        store.confirmed_proposals.push_back(b);

        // Up to date when no pending proposals
        assert!(store.up_to_date());

        store.max_height = 11;

        // Up to date when max_height == height + 1
        assert!(store.up_to_date());

        store.max_height = 12;

        // NOT up to date when max_height > height + 1
        assert!(!store.up_to_date());
    }

    #[test]
    fn test_confirm_proposal() {
        let (p1, p2, p3) = create_peers();
        let peers = vec![p1.clone(), p2, p3];
        let mut store = ProposalStore::new(p1.clone(), peers.clone());

        let b = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "a".into(),
                skips: 1,
                height: 10,
                leader_id: p1.clone(),
                changes: vec![],
            },
            &store.peers,
        );
        let b_hash = b.hash().clone();
        store.pending_proposals.insert(b.hash().clone(), b);

        store.confirm(&b_hash);

        assert_eq!(store.pending_proposals.len(), 0);
        assert_eq!(store.confirmed_proposals[1].hash(), &b_hash);
    }

    #[test]
    fn test_purge_skipped_proposals() {
        let (p1, p2, p3) = create_peers();
        let peers = vec![p1.clone(), p2, p3];
        let mut store = ProposalStore::new(p1.clone(), peers.clone());

        // Purge on empty store
        store.purge_skipped_proposals();

        let b = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "a".into(),
                skips: 1,
                height: 10,
                leader_id: p1.clone(),
                changes: vec![],
            },
            &[Key::from(p1.clone())],
        );
        store.confirmed_proposals.push_back(b);

        let a = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "x".into(),
                skips: 0,
                height: 10,
                leader_id: p1.clone(),
                changes: vec![],
            },
            &store.peers,
        );
        store.pending_proposals.insert(a.hash().clone(), a);

        let c = Proposal::new(
            ProposalManifest {
                last_proposal_hash: "b".into(),
                skips: 0,
                height: 11,
                leader_id: p1.clone(),
                changes: vec![],
            },
            &store.peers,
        );
        let c_hash = c.hash().clone();
        store.pending_proposals.insert(c.hash().clone(), c);

        store.purge_skipped_proposals();

        assert_eq!(store.pending_proposals.len(), 1);
        assert!(store.pending_proposals.contains_key(&c_hash));
    }
}
