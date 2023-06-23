use super::event::SolidEvent;
use super::proposal::{Proposal, ProposalAccept, ProposalHash, ProposalManifest};
use crate::cache::ProposalCache;
use crate::peer::PeerId;
use std::collections::HashMap;

/// ProposalStore is responsible for handling new proposals and accepts.
#[derive(Debug)]
pub struct ProposalStore {
    /// peer_id of the local node
    local_peer_id: PeerId,

    /// Pending proposals that may or may not end up being confiremd.
    proposals: ProposalCache,

    /// Max confirmed height seen across all received proposals
    // max_height: usize,

    /// Number of skips sent for the accepts_sent_height
    accepts_sent: usize,

    /// We reset accepts_sent to 0 when we receive a proposal with a new height on first_accept
    accepts_sent_height: usize,

    /// Orphaned accepts are when we receive an accept for a proposal before we
    /// receive the proposal itself. We can then add these as soon as the proposal arrives.
    orphan_accepts: HashMap<ProposalHash, Vec<(usize, PeerId)>>,
}

#[derive(Debug)]
pub struct ProposeNextState {
    pub last_proposal_hash: ProposalHash,
    pub height: usize,
}

impl ProposalStore {
    pub fn with_last_confirmed(
        local_peer_id: PeerId,
        last_confirmed_proposal: ProposalManifest,
        cache_size: usize,
    ) -> Self {
        let max_height = last_confirmed_proposal.height;

        Self {
            local_peer_id,
            proposals: ProposalCache::new(Proposal::new(last_confirmed_proposal), cache_size),
            // max_height,
            accepts_sent: 0,
            accepts_sent_height: max_height,
            orphan_accepts: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn genesis(local_peer_id: PeerId, peers: Vec<PeerId>, cache_size: usize) -> Self {
        Self::with_last_confirmed(local_peer_id, ProposalManifest::genesis(peers), cache_size)
    }

    /// Height of the proposal that was last confirmed
    pub fn height(&self) -> usize {
        self.proposals.height()
    }

    /// Checks if the proposal hash exists, only checks pending proposals
    /// as confirmed proposals can be checked via height.
    pub fn exists(&self, hash: &ProposalHash) -> bool {
        self.proposals.contains(hash)
    }

    pub fn proposals_from(&self, i: usize) -> Vec<ProposalManifest> {
        self.proposals
            .proposals_from(i)
            .iter()
            .map(|p| p.manifest.clone())
            .collect()
    }

    pub fn confirmed_proposals_from(&self, i: usize) -> Vec<ProposalManifest> {
        self.proposals
            .confirmed_proposals_from(i)
            .iter()
            .map(|p| p.manifest.clone())
            .collect()
    }

    pub fn min_proposal_height(&self) -> usize {
        self.proposals.min_proposal_height()
    }

    /// Add a pending proposal to the store
    pub fn add_pending_proposal(&mut self, manifest: ProposalManifest) {
        let hash: ProposalHash = (&manifest).into();
        let mut proposal = Proposal::new(manifest);

        // Check if we have orphaned accepts
        if let Some(accepts) = self.orphan_accepts.remove(&hash) {
            for (skips, peer_id) in accepts {
                proposal.add_accept(&skips, peer_id);
            }
        }

        // Insert the proposal to be processed later
        self.proposals.insert(proposal);
    }

    /// Processes the next event in the store, this will either return a commit or accept
    /// event, or None if no more events are ready
    pub fn process_next(&mut self) -> Option<SolidEvent> {
        // Gets the next valid proposal (confirmed height + 1)
        let proposal = match self.proposals.next_pending_proposal(0) {
            Some(p) => p,
            None => {
                // Indicates a gap in proposals, as we have no next pending commit, but we
                // do have some pending commits
                if self.has_pending_commits() {
                    return Some(SolidEvent::OutOfSync {
                        height: self.height(),
                        max_seen_height: self.proposals.max_height(),
                        accepts_sent: self.accepts_sent,
                    });
                }

                // This occurs when we have no pending proposals, and we've sent no previous
                // accepts (this usually occurs on startup)
                if self.accepts_sent == 0 && self.height() == self.accepts_sent_height {
                    return Some(self.get_next_accept_event());
                }

                // We are up to date, we need to wait for new incoming
                // proposals
                return None;
            }
        };

        let proposal_hash = proposal.hash().clone();

        // Send commit if we have uncommitted proposals that can be committed
        if self.has_next_commit() {
            let manifest = proposal.manifest.clone();

            // Add proposal to confirmed list
            self.proposals.confirm(proposal_hash);

            // Reset accepts sent, as we have a new commit
            self.accepts_sent = 0;

            // Send commit
            return Some(SolidEvent::Commit { manifest });
        }

        // Only send first accept using the process_next, otherwise we'll infinitely send
        // accepts. Additional accepts (aka skips) will be sent when the timeout expires
        // and store.skip() is called.
        if self.accepts_sent > 0 && proposal.height() == self.accepts_sent_height {
            return None;
        }

        // In sync, so we should send accept to the next leader
        Some(self.get_next_accept_event())
    }

    /// Skip should be called when we have not received a proposal from the next leader
    /// within the timeout period. Skip will send an accept to the next leader.
    pub fn skip(&mut self) -> Option<SolidEvent> {
        // Just in case we try to skip when we're still catching up
        if self.has_network_commits() {
            return None;
        }

        // Get the next accept
        Some(self.get_next_accept_event())
    }

    /// Gets the next accept to send, where no pending proposal is available,
    /// last confirmed will be used.
    fn get_next_accept_event(&mut self) -> SolidEvent {
        let last_confirmed = self.proposals.last_confirmed_proposal();
        let current_proposal = self
            .proposals
            .next_pending_proposal(0)
            .unwrap_or(last_confirmed);

        let skips = if current_proposal.height() == self.accepts_sent_height {
            self.accepts_sent
        } else {
            0
        };

        let accept = ProposalAccept {
            proposal_hash: current_proposal.hash().clone(),
            leader_id: last_confirmed.get_next_leader(skips),
            height: current_proposal.height(),
            skips,
        };

        self.accepts_sent_height = current_proposal.height();
        self.accepts_sent = skips + 1;

        // Copy the local_peer_id to avoid borrowing issues
        let local_peer_id = self.local_peer_id.clone();

        // If the next accept is self, then we should add the accept
        if self.is_peer(&accept.leader_id) {
            if let Some(event) = self.add_accept(&accept, &local_peer_id) {
                return event;
            }
        }

        SolidEvent::Accept { accept }
    }

    // Is peer the current node
    pub fn is_peer(&self, other_peer: &PeerId) -> bool {
        &self.local_peer_id == other_peer
    }

    /// Adds an accept to a proposal, we should only be receiving accepts if we are the
    /// next designated leader. Returns ProposalNextState if we have hit the majority and the
    /// accept is still valid, otherwise returns None.
    pub fn add_accept(&mut self, accept: &ProposalAccept, from: &PeerId) -> Option<SolidEvent> {
        let ProposalAccept {
            proposal_hash: last_proposal_hash,
            leader_id,
            height: accept_height,
            skips,
        } = accept;

        // Check if accept is out of date, normally accept_height must be greater than confirmed height,
        // but if there are no pending proposals we may need to accept a proposal with
        // accept height == confirmed height (e.g. during start up)
        if self.height() > *accept_height {
            return None;
        }

        // Update accepts sent if we have received a higher skip, so the network eventually
        // converges on the same skip
        if self.accepts_sent_height == *accept_height && *skips > self.accepts_sent {
            self.accepts_sent = *skips;
        }

        // Add accept to proposal (or to orphaned hash map if proposal is not found/received yet).
        // We always store accepts for any future proposal, as we may need them later
        return match self.proposals.get_mut(last_proposal_hash) {
            Some(p) => {
                // Skip if skips is not valid
                if p.add_accept(skips, from.clone()) {
                    return Some(SolidEvent::Propose {
                        last_proposal_hash: last_proposal_hash.clone(),
                        height: p.height() + 1,
                        skips: *skips,
                    });
                }
                None
            }
            None => {
                // Get exisiting orphaned proposal list (or create it if it doesn't exist yet)
                if let Some(p) = self.orphan_accepts.get_mut(last_proposal_hash) {
                    p.push((*skips, leader_id.clone()));
                } else {
                    self.orphan_accepts.insert(
                        last_proposal_hash.clone(),
                        vec![(*skips, leader_id.clone())],
                    );
                }

                // We're the designated leader, and yet we don't have the proposal being
                // accepted. We should request this proposals ASAP, so we can become
                // build on it once we have enough accept votes.
                return Some(SolidEvent::OutOfSync {
                    height: self.height(),
                    // Accept is always one ahead of a confirmed proposal, so we subtract 1
                    // to get the highest confirmed proposal that must exist
                    max_seen_height: *accept_height,
                    accepts_sent: 0,
                });
            }
        };
    }

    /// Do we have any pending proposals that match the skip we are looking for? If so,
    /// this means we can commit the next proposal (height + 1) and send an accept for the next one
    fn has_next_commit(&self) -> bool {
        // Always commit if we have valid network commits
        if self.has_network_commits() {
            return true;
        }

        // Check if we have a valid next proposal (confirmed height + 2)
        if let Some(next_proposal) = self.proposals.next_pending_proposal(1) {
            return next_proposal.skips() + 1 >= self.accepts_sent;
        }

        false
    }

    /// Pending commits are proposals that MAY enable a commit to occur (i.e. if the skips match)
    fn has_pending_commits(&self) -> bool {
        self.proposals.max_height() > self.height() + 1
    }

    /// Do we have have commits from the network that must be accepted, as majority of network
    /// is following that chain. This means we are behind, so we should not contribute to
    /// consensus until we are caught up.
    fn has_network_commits(&self) -> bool {
        self.proposals.max_height() > self.height() + 2
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use super::*;

    fn create_peers() -> [PeerId; 3] {
        let p1 = PeerId::new(vec![1u8]);
        let p2 = PeerId::new(vec![2u8]);
        let p3 = PeerId::new(vec![3u8]);
        [p1, p2, p3]
    }

    fn create_manifest(
        height: usize,
        skips: usize,
        leader: u8,
        last_proposal_hash: ProposalHash,
    ) -> (ProposalManifest, ProposalHash) {
        let m = ProposalManifest {
            last_proposal_hash,
            height,
            skips,
            leader_id: peer(leader),
            txns: vec![],
            peers: create_peers().to_vec(),
        };
        let m_hash = m.hash();
        (m, m_hash)
    }

    fn peer(id: u8) -> PeerId {
        PeerId::new(vec![id])
    }

    #[test]
    fn test_process_next_genesis() {
        let [p1, _, _] = create_peers();
        let mut store: ProposalStore = ProposalStore::genesis(p1, create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    leader_id: peer(2),
                    skips: 0,
                }
            })
        );
        assert_eq!(store.process_next(), None);

        let (m1, m1_hash) = create_manifest(1, 0, 1, genesis_hash);
        store.add_pending_proposal(m1.clone());

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    height: 1,
                    leader_id: peer(2),
                    skips: 0,
                }
            })
        );
        assert_eq!(store.process_next(), None);
        assert_eq!(store.proposals.len(), 2);

        let (m2, m2_hash) = create_manifest(2, 0, 1, m1_hash);
        store.add_pending_proposal(m2.clone());

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Commit { manifest: m1 })
        );

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m2_hash.clone(),
                    leader_id: peer(3),
                    height: 2,
                    skips: 0,
                }
            })
        );
        assert_eq!(store.process_next(), None);
        assert_eq!(store.proposals.confirmed_proposals_from(0).len(), 2);

        let (m3, m3_hash) = create_manifest(3, 0, 2, m2_hash);
        let (m4, m4_hash) = create_manifest(4, 0, 1, m3_hash);

        store.add_pending_proposal(m4);

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::OutOfSync {
                height: 1,
                max_seen_height: 4,
                accepts_sent: 1
            })
        );
        store.add_pending_proposal(m3.clone());

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Commit { manifest: m2 })
        );

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Commit { manifest: m3 })
        );

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m4_hash,
                    leader_id: peer(1),
                    height: 4,
                    skips: 0,
                }
            })
        );

        assert_eq!(store.process_next(), None);
    }

    #[test]
    fn test_process_next_restore() {
        let [p1, _, _] = create_peers();
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        let (m10, m10_hash) = create_manifest(10, 0, 1, genesis_hash);
        let mut store = ProposalStore::with_last_confirmed(p1, m10, 100);

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m10_hash.clone(),
                    height: 10,
                    leader_id: peer(1),
                    skips: 0,
                }
            })
        );

        let (m11, m11_hash) = create_manifest(11, 0, 1, m10_hash);
        store.add_pending_proposal(m11.clone());

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m11_hash.clone(),
                    height: 11,
                    leader_id: peer(1),
                    skips: 0,
                }
            })
        );

        assert_eq!(store.process_next(), None);

        let (m12, m12_hash) = create_manifest(12, 0, 1, m11_hash);
        store.add_pending_proposal(m12);

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Commit { manifest: m11 })
        );

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m12_hash,
                    leader_id: peer(1),
                    height: 12,
                    skips: 0,
                }
            })
        );
        assert_eq!(store.proposals.confirmed_proposals_from(0).len(), 2);
    }

    /// Node skips, network skips
    #[test]
    fn test_skip_one_network_skip() {
        let [p1, _, _] = create_peers();
        let mut store = ProposalStore::genesis(p1, create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    leader_id: peer(2),
                    skips: 0,
                }
            })
        );

        // First pending proposal
        let (m1, m1_hash) = create_manifest(1, 0, 1, genesis_hash);
        store.add_pending_proposal(m1.clone());

        // Send accept for m1
        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    leader_id: peer(2),
                    height: 1,
                    skips: 0,
                }
            })
        );

        assert_eq!(store.process_next(), None);

        // Send skip for m1 after timeout
        assert_eq!(
            store.skip(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    leader_id: peer(1),
                    height: 1,
                    skips: 1,
                }
            })
        );

        // Proposal arrives late (but is now invalid)
        let (m2a, _) = create_manifest(2, 0, 2, m1_hash.clone());
        store.add_pending_proposal(m2a);

        assert_eq!(store.process_next(), None);

        // Proposal (+1 skip) now arrives
        let (m2b, m2b_hash) = create_manifest(2, 1, 2, m1_hash);
        store.add_pending_proposal(m2b);

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Commit { manifest: m1 })
        );

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    leader_id: peer(3),
                    proposal_hash: m2b_hash,
                    height: 2,
                    skips: 0,
                }
            })
        );

        assert_eq!(store.accepts_sent, 1);
    }

    #[test]
    fn test_out_of_sync() {
        let [p1, _, _] = create_peers();
        let (m3, m3_hash) = create_manifest(3, 0, 1, ProposalHash::default());
        let mut store = ProposalStore::with_last_confirmed(p1, m3, 100);

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m3_hash,
                    height: 3,
                    leader_id: peer(2),
                    skips: 0,
                }
            })
        );

        let (m5, _) = create_manifest(5, 0, 1, ProposalHash::default());
        store.add_pending_proposal(m5);

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::OutOfSync {
                height: 3,
                max_seen_height: 5,
                accepts_sent: 1
            })
        );

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::OutOfSync {
                height: 3,
                max_seen_height: 5,
                accepts_sent: 1
            })
        );
    }

    /// Node skips, network no skips
    #[test]
    fn test_skip_one_no_network_skip() {
        let [p1, _, _] = create_peers();
        let mut store = ProposalStore::genesis(p1, create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    leader_id: peer(2),
                    skips: 0,
                }
            })
        );

        let (m1, m1_hash) = create_manifest(1, 0, 2, genesis_hash);
        store.add_pending_proposal(m1.clone());

        // Send accept for m1
        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    leader_id: peer(2),
                    height: 1,
                    skips: 0,
                }
            })
        );
        assert!(store.process_next().is_none());

        // Send skip=1 for m1 (simulating timeout)
        assert_eq!(
            store.skip(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: m1_hash.clone(),
                    leader_id: peer(1),
                    height: 1,
                    skips: 1,
                }
            })
        );

        assert_eq!(store.process_next(), None);

        // Proposal m2 received (but with skips=0), we can no longer
        // accept this proposal as we sent a skip message
        let (m2, m2_hash) = create_manifest(2, 0, 3, m1_hash);
        store.add_pending_proposal(m2.clone());

        assert_eq!(store.process_next(), None);

        // Proposal m3 receivied which references m2, so we know network
        // approved m2
        let (m3, m3_hash) = create_manifest(3, 0, 2, m2_hash);
        store.add_pending_proposal(m3);

        // We now need to catch up to network by applying m2
        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Commit { manifest: m1 })
        );

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Commit { manifest: m2 })
        );

        // We can now send accept for m3, as we are up to date with the network
        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    leader_id: peer(2),
                    height: 3,
                    proposal_hash: m3_hash,
                    skips: 0,
                }
            })
        );
    }

    #[test]
    fn test_skip_one_proposal() {
        let [p1, p2, p3] = create_peers();
        let mut store = ProposalStore::genesis(p1.clone(), create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    leader_id: p2.clone(),
                    skips: 0,
                }
            })
        );

        let (m1, m1_hash) = create_manifest(1, 0, 1, genesis_hash);
        store.add_pending_proposal(m1);

        assert_eq!(
            store.process_next(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    leader_id: p2.clone(),
                    proposal_hash: m1_hash.clone(),
                    height: 1,
                    skips: 0
                }
            })
        );

        assert_eq!(
            store.skip(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    height: 1,
                    skips: 1,
                    leader_id: p1.clone(),
                    proposal_hash: m1_hash.clone(),
                }
            })
        );

        assert_eq!(
            store.skip(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    height: 1,
                    skips: 2,
                    leader_id: p3.clone(),
                    proposal_hash: m1_hash.clone(),
                }
            })
        );

        assert_eq!(
            store.skip(),
            Some(SolidEvent::Accept {
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
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    height: 1,
                    skips: 4,
                    leader_id: p1,
                    proposal_hash: m1_hash.clone()
                }
            })
        );

        assert_eq!(
            store.skip(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    height: 1,
                    skips: 5,
                    leader_id: p3,
                    proposal_hash: m1_hash,
                }
            })
        );
    }

    #[test]
    fn test_multi_accept_proposal() {
        let [p1, p2, p3] = create_peers();
        let mut store = ProposalStore::genesis(p1.clone(), create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        // First accept, no majority
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: p1.clone(),
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    skips: 0
                },
                &p1,
            ),
            None,
        );

        // Ignores duplicate accept from the same peer
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: p1.clone(),
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    skips: 0
                },
                &p1,
            ),
            None,
        );

        // Second accept, threshold met, send proposal
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: p1.clone(),
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    skips: 0
                },
                &p2,
            ),
            Some(SolidEvent::Propose {
                last_proposal_hash: genesis_hash.clone(),
                height: 1,
                skips: 0,
            }),
        );

        // Final proposal not needed as already met
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: p1.clone(),
                    proposal_hash: genesis_hash,
                    height: 0,
                    skips: 0
                },
                &p3,
            ),
            None,
        );
    }

    #[test]
    fn test_higher_skip_accept_received() {
        let [p1, _, _] = create_peers();
        let mut store = ProposalStore::genesis(p1, create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        // First accept, no majority
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: peer(1),
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    skips: 10
                },
                &peer(1),
            ),
            None,
        );

        // Skip should now start from 10, so skips will be 10 + 1
        assert_eq!(
            store.skip(),
            Some(SolidEvent::Accept {
                accept: ProposalAccept {
                    leader_id: peer(1),
                    proposal_hash: genesis_hash,
                    height: 0,
                    skips: 10
                }
            })
        );
    }

    #[test]
    fn test_add_accept_out_of_sync() {
        let [p1, _, _] = create_peers();
        let mut store = ProposalStore::genesis(p1, create_peers().to_vec(), 100);

        // Receive accept of higher value
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: peer(1),
                    proposal_hash: ProposalHash::new(vec![2u8]),
                    height: 1,
                    skips: 0,
                },
                &peer(1),
            ),
            Some(SolidEvent::OutOfSync {
                height: 0,
                max_seen_height: 1,
                accepts_sent: 0
            }),
        );
    }

    #[test]
    fn test_duplicate_accepts_received() {
        let [p1, _, _] = create_peers();
        let mut store = ProposalStore::genesis(p1, create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        // First accept, no majority
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: peer(1),
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    skips: 0
                },
                &peer(1),
            ),
            None,
        );

        // Duplicate accept
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: peer(1),
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    skips: 0
                },
                &peer(1),
            ),
            None,
        );

        // New accept
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: peer(1),
                    proposal_hash: genesis_hash.clone(),
                    height: 0,
                    skips: 0
                },
                &peer(2),
            ),
            Some(SolidEvent::Propose {
                last_proposal_hash: genesis_hash.clone(),
                height: 1,
                skips: 0,
            }),
        );

        // Duplicate accept
        assert_eq!(
            store.add_accept(
                &ProposalAccept {
                    leader_id: peer(1),
                    proposal_hash: genesis_hash,
                    height: 0,
                    skips: 0
                },
                &peer(2),
            ),
            None,
        );
    }

    #[test]
    fn test_next_pending_propsal() {
        let [p1, _, _] = create_peers();
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        // Create store with init genesis proposal
        let mut store = ProposalStore::genesis(p1, create_peers().to_vec(), 100);

        assert_eq!(store.proposals.next_pending_proposal(0), None);

        let (m1, m1_hash) = create_manifest(1, 0, 1, genesis_hash);

        let (m2a, _) = create_manifest(2, 0, 1, m1_hash.clone());
        let (m2b, _) = create_manifest(2, 1, 1, m1_hash.clone());
        let (m3, _) = create_manifest(3, 0, 1, ProposalHash::default());

        // Add m1 to pending proposals
        store.add_pending_proposal(m1);

        assert_eq!(
            store
                .proposals
                .next_pending_proposal(0)
                .unwrap()
                .hash()
                .clone(),
            m1_hash,
        );

        // Add next proposal height: 2
        store.add_pending_proposal(m2a);

        assert_eq!(
            store
                .proposals
                .next_pending_proposal(0)
                .unwrap()
                .hash()
                .clone(),
            m1_hash,
        );

        // Add additional proposal for height: 2
        store.add_pending_proposal(m2b);

        assert_eq!(
            store
                .proposals
                .next_pending_proposal(0)
                .unwrap()
                .hash()
                .clone(),
            m1_hash,
        );

        // Add proposal with gap
        store.add_pending_proposal(m3);

        assert!(store.proposals.next_pending_proposal(0).is_none());
    }

    #[test]
    fn test_has_next_commt() {
        let [p1, _, _] = create_peers();
        let mut store = ProposalStore::genesis(p1, create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();
        assert!(!store.has_next_commit());

        let (m1, _) = create_manifest(1, 0, 1, genesis_hash.clone());
        let (m2, _) = create_manifest(2, 0, 2, genesis_hash);

        store.add_pending_proposal(m1);

        assert!(!store.has_next_commit());

        store.add_pending_proposal(m2);

        assert!(store.has_next_commit());
    }
}
