use super::event::ProposalEvent;
use super::proposal::{Proposal, ProposalAccept, ProposalHash, ProposalManifest};
use crate::cache::ProposalCache;
use crate::peer::PeerId;
use std::collections::HashMap;

/// ProposalStore is responsible for handling new proposals and accepts.
#[derive(Debug)]
pub struct ProposalStore {
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
        last_confirmed_proposal: ProposalManifest,
        cache_size: usize,
    ) -> Self {
        let max_height = last_confirmed_proposal.height;

        Self {
            proposals: ProposalCache::new(Proposal::new(last_confirmed_proposal), cache_size),
            // max_height,
            accepts_sent: 0,
            accepts_sent_height: max_height,
            orphan_accepts: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn genesis(peers: Vec<PeerId>, cache_size: usize) -> Self {
        Self::with_last_confirmed(ProposalManifest::genesis(peers), cache_size)
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

    pub fn confirmed_proposals_from(&self, i: usize) -> Vec<ProposalManifest> {
        self.proposals
            .confirmed_proposals_from(i)
            .iter()
            .map(|p| p.manifest.clone())
            .collect()
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

    pub fn process_next(&mut self) -> Option<ProposalEvent> {
        let proposal = match self.proposals.next_pending_proposal(0) {
            Some(p) => p,
            None => {
                if self.has_pending_commits() {
                    return Some(ProposalEvent::OutOfSync {
                        local_height: self.height(),
                        max_seen_height: self.proposals.max_height(),
                        accepts_sent: self.accepts_sent,
                    });
                }
                return None;
            }
        };

        let proposal_hash = proposal.hash().clone();

        // Send commit if we have uncommitted proposals that can be committed
        if self.has_network_commits() || self.has_next_commit() {
            let manifest = proposal.manifest.clone();

            // Add proposal to confirmed list
            self.proposals.confirm(proposal_hash);

            // Reset accepts sent, as we have a new commit
            self.accepts_sent = 0;

            // Send commit
            return Some(ProposalEvent::Commit { manifest });
        }

        // Only send initial accept using the process_next, otherwise accept is sent
        // when skip is called
        if self.accepts_sent > 0 && proposal.height() == self.accepts_sent_height {
            return None;
        }

        let accept = self.get_next_accept();

        // In sync, so we should send accept to the next leader
        Some(ProposalEvent::SendAccept { accept })
    }

    /// Skip should be called when we have not received a proposal from the next leader
    /// within the timeout period. Skip will send an accept to the next leader.
    pub fn skip(&mut self) -> Option<ProposalEvent> {
        // Just in case we try to skip when we're still catching up
        if self.has_network_commits() {
            return None;
        }

        // Get the next accept
        let accept = self.get_next_accept();

        // Send skip
        Some(ProposalEvent::SendAccept { accept })
    }

    /// Gets the next accept to send, where no pending proposal is available, last confirmed will be used.
    fn get_next_accept(&mut self) -> ProposalAccept {
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

        accept
    }

    /// Adds an accept to a proposal, we should only be receiving accepts if we are the
    /// next designated leader. Returns ProposalNextState if we have hit the majority and the
    /// accept is still valid, otherwise returns None.
    pub fn add_accept(&mut self, accept: &ProposalAccept, from: &PeerId) -> Option<ProposalEvent> {
        let ProposalAccept {
            proposal_hash: last_proposal_hash,
            leader_id,
            height: accept_height,
            skips,
        } = accept;

        // Accept is out of date
        if self.height() > *accept_height {
            return None;
        }

        // Update accepts sent if we have received a higher skip
        if self.accepts_sent_height == *accept_height && *skips > self.accepts_sent {
            self.accepts_sent = *skips;
        }

        // Add accept to proposal (or to orphaned hash map if proposal is not found/received yet)
        let res = match self.proposals.get_mut(last_proposal_hash) {
            Some(p) => {
                // Skip if skips is not valid
                if p.add_accept(skips, from.clone()) {
                    return Some(ProposalEvent::Propose {
                        last_proposal_hash: last_proposal_hash.clone(),
                        height: p.height() + 1,
                        skips: *skips,
                    });
                }
                None
            }
            None => {
                // Get exisiting orphaned proposal list
                if let Some(p) = self.orphan_accepts.get_mut(last_proposal_hash) {
                    p.push((*skips, leader_id.clone()));
                } else {
                    self.orphan_accepts.insert(
                        last_proposal_hash.clone(),
                        vec![(*skips, leader_id.clone())],
                    );
                }
                None
            }
        };

        // Accept is indicating that we are behind the network in terms of proposals
        if res.is_none() && *accept_height > self.height() + 2 {
            return Some(ProposalEvent::OutOfSync {
                local_height: self.height(),
                max_seen_height: accept_height - 1,
                accepts_sent: 0,
            });
        }

        res
    }

    /// Do we have any pending proposals that match the skip we are looking for? If so,
    /// this means we can commit the last proposal (and send an accept for the next one)
    fn has_next_commit(&self) -> bool {
        // If we don't have more than one commit, then we can exit early
        if !self.has_pending_commits() {
            return false;
        }

        // Check if we want to accept the next pending commit (skips match)
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
            changes: vec![],
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
        let mut store: ProposalStore = ProposalStore::genesis(create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::Commit { manifest: m1 })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::OutOfSync {
                local_height: 1,
                max_seen_height: 4,
                accepts_sent: 1
            })
        );
        store.add_pending_proposal(m3.clone());

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::Commit { manifest: m2 })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::Commit { manifest: m3 })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        let (m10, m10_hash) = create_manifest(10, 0, 1, genesis_hash);
        let mut store = ProposalStore::with_last_confirmed(m10, 100);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::Commit { manifest: m11 })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
        let mut store = ProposalStore::genesis(create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::Commit { manifest: m1 })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
        let (m3, m3_hash) = create_manifest(3, 0, 1, ProposalHash::default());
        let mut store = ProposalStore::with_last_confirmed(m3, 100);

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::OutOfSync {
                local_height: 3,
                max_seen_height: 5,
                accepts_sent: 1
            })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::OutOfSync {
                local_height: 3,
                max_seen_height: 5,
                accepts_sent: 1
            })
        );
    }

    /// Node skips, network no skips
    #[test]
    fn test_skip_one_no_network_skip() {
        let mut store = ProposalStore::genesis(create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::Commit { manifest: m1 })
        );

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::Commit { manifest: m2 })
        );

        // We can now send accept for m3, as we are up to date with the network
        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
        let mut store = ProposalStore::genesis(create_peers().to_vec(), 100);
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        assert_eq!(
            store.process_next(),
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
            Some(ProposalEvent::SendAccept {
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
                    leader_id: p1,
                    proposal_hash: m1_hash.clone()
                }
            })
        );

        assert_eq!(
            store.skip(),
            Some(ProposalEvent::SendAccept {
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
        let mut store = ProposalStore::genesis(create_peers().to_vec(), 100);
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
            Some(ProposalEvent::Propose {
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
        let mut store = ProposalStore::genesis(create_peers().to_vec(), 100);
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
            Some(ProposalEvent::SendAccept {
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
    fn test_duplicate_accepts_received() {
        let mut store = ProposalStore::genesis(create_peers().to_vec(), 100);
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
            Some(ProposalEvent::Propose {
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
        let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

        // Create store with init genesis proposal
        let mut store = ProposalStore::genesis(create_peers().to_vec(), 100);

        assert_eq!(
            store
                .proposals
                .next_pending_proposal(0)
                .unwrap()
                .hash()
                .clone(),
            genesis_hash,
        );

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

    // #[test]
    // fn test_has_pending_commits() {
    //     let [p1, _, _] = create_peers();
    //     let mut store = ProposalStore::genesis(create_peers().to_vec());
    //     let genesis_hash = ProposalManifest::genesis(create_peers().to_vec()).hash();

    //     // Up to date when store is empty
    //     assert!(!store.has_pending_commits());

    //     let b = Proposal::new(ProposalManifest {
    //         last_proposal_hash: genesis_hash,
    //         skips: 0,
    //         height: 10,
    //         leader_id: p1,
    //         changes: vec![],
    //         peers: create_peers().to_vec(),
    //     });
    //     let b_hash = b.hash().clone();
    //     // store.proposals.last_confirmed_proposal_hash = b.hash().clone();
    //     store.proposals.insert(b);
    //     store.proposals.confirm(b_hash);

    //     // Up to date when no pending proposals
    //     assert!(!store.has_pending_commits());

    //     store.max_height = 11;

    //     // Up to date when max_height == height + 1
    //     assert!(!store.has_pending_commits());

    //     store.max_height = 12;

    //     // NOT up to date when max_height > height + 1
    //     assert!(store.has_pending_commits());
    // }

    #[test]
    fn test_has_next_commt() {
        let mut store = ProposalStore::genesis(create_peers().to_vec(), 100);
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
