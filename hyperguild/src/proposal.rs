use crate::change::Change;
use crate::key::Key;
use crate::peer::PeerId;
use futures::future::Ready;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::Read;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{broadcast, Notify};
use tokio::time::{sleep_until, Instant};
use tokio_stream::Stream;
use tokio_stream::StreamExt;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub struct ProposalHash(Vec<u8>);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalEvent {
    /// Proposal register is missing proposals
    OutOfSync {
        /// Height of the node
        local_height: u64,
        max_seen_height: u64,
    },

    /// Proposal is historic or no longer valid due to
    /// other proposals
    // TODO: should we include skip in this?
    OutOfDate {
        local_height: u64,
        proposal_height: u64,
    },

    /// Send accept to the peer
    Accept {
        proposal_hash: ProposalHash,
        peer_id: PeerId,
        skips: u16,
    },

    /// Send a new proposal to the network
    Propose {
        last_proposal_hash: ProposalHash,
        height: u64,
    },

    /// Proposal has been confirmed and should be committed
    /// to the data store
    Commit { manifest: ProposalManifest },

    /// Duplicate proposal received
    DuplicateProposal,
}

#[derive(Debug)]
pub struct ProposalRegister {
    shared: Arc<ProposalRegisterShared>,
}

#[derive(Debug)]
pub struct ProposalRegisterShared {
    background_worker: Notify,
    state: Mutex<ProposalRegisterState>,
}

#[derive(Debug)]
struct ProposalRegisterState {
    // Events to be streamed
    events: VecDeque<ProposalEvent>,

    /// Local peer, required so we can determine if
    /// we are the leader
    local_peer_id: PeerId,

    /// All peers on the network, this is used to determine
    /// which peer to send accepts to and the threshold required
    /// for
    peers: Vec<Key<PeerId>>,

    /// When a peer is unresponsive when they should have provided
    /// a proposal, we need to skip the peer and send an accept to
    /// the next peer in line. Skips ensure we have a predicatble
    /// order for next peer until the consensus has stabalised.
    skips: u16,

    /// List of confirmed proposals, these cannot be undone
    confirmed_proposals: VecDeque<Proposal>,

    current_proposal: Option<ProposalHash>,

    /// Primary proposal being confirmed
    proposals: HashMap<ProposalHash, Proposal>,

    timeout: Option<Instant>,

    shutdown: bool,
}

impl Drop for ProposalRegister {
    fn drop(&mut self) {
        // Signal the 'Db' instance to shut down the task that purges expired keys
        self.shutdown_purge_task();
    }
}

#[derive(Debug)]
pub struct Proposal {
    /// Accepts/votes for this proposal, only received by the leader node
    accepts: HashSet<PeerId>,

    /// Hash of the proposal state
    hash: Key<ProposalHash>,

    /// State of the proposal
    manifest: ProposalManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalManifest {
    /// Hash of the last proposal, so we can confirm the last
    /// proposal when we receive this message
    last_proposal_hash: ProposalHash,

    // Number of skips of leader that have occured since the last
    // leadership order change, we need a consistent ordering while
    // skips are occurring
    skips: u16,

    /// Height of the proposal, for easy checking whether we
    /// are up to date with the network
    height: u64,

    /// PeerId of the proposer/leader
    peer_id: PeerId,

    /// Changes included in the proposal
    changes: Vec<Change>,
}

impl ProposalRegister {
    pub(crate) fn new(local_peer_id: PeerId, peers: Vec<PeerId>) -> Self {
        let mut peers = peers;

        if !peers.contains(&local_peer_id) {
            peers.push(local_peer_id.clone());
        }

        let shared = Arc::new(ProposalRegisterShared {
            state: Mutex::new(ProposalRegisterState {
                events: VecDeque::new(),
                local_peer_id,
                peers: peers.into_iter().map(Key::new).collect(),
                confirmed_proposals: VecDeque::new(),
                current_proposal: None,
                proposals: HashMap::new(),
                skips: 0,
                timeout: None,
                shutdown: false,
            }),
            background_worker: Notify::new(),
        });

        let register = Self {
            shared: shared.clone(),
        };

        // Create background worker, this is mostly responsible for sending skips
        // when a new proposal has not been created by the next responsible leader
        tokio::spawn(background_worker(shared));

        register
    }

    /// Gets the highest confirmed height for this reigster
    pub fn height(&self) -> Option<u64> {
        let state = self.shared.state.lock().unwrap();
        Some(state.confirmed_proposals.back()?.manifest.height)
    }

    pub fn exists(&self, hash: &ProposalHash) -> bool {
        let state = self.shared.state.lock().unwrap();
        state.proposals.contains_key(hash)
    }

    fn next_leader(&self) -> PeerId {
        let hash;
        let state = self.shared.state.lock().unwrap();

        // Get the last hash of the last confirmed proposals
        if let Some(proposal) = state.confirmed_proposals.back() {
            hash = proposal.manifest.last_proposal_hash.clone();
        } else {
            hash = ProposalHash::default();
        }

        let hash = Key::new(hash);

        let leader = state
            .peers
            .iter()
            .min_by(|a, b| a.distance(&hash).cmp(&b.distance(&hash)))
            .unwrap();

        leader.preimage().clone()
    }

    fn reset_timeout(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.timeout = Some(Instant::now() + Duration::from_secs(1));
        self.shared.background_worker.notify_one();
    }

    fn find_next_closest_height(&self, height: u64) -> Option<u64> {
        let state = self.shared.state.lock().unwrap();
        state
            .proposals
            .values()
            .map(|p| p.manifest.height)
            .filter(|p_height| *p_height > height)
            .min_by(|x, y| x.cmp(y))
    }

    // Add proposal to list of proposals we are storing
    fn add_proposal(&self, manifest: ProposalManifest) {
        let hash: ProposalHash = (&manifest).into();
        let proposal = Proposal::new(manifest);

        let mut state = self.shared.state.lock().unwrap();
        state.proposals.insert(hash, proposal);
    }

    // Process the next proposal in the chain, this should move the proposal
    // If we don't have the next proposal in the chain, request it from the network
    fn process_next_round(&self, proposal_hash: ProposalHash) {
        let state = self.shared.state.lock().unwrap();
        let last = state.confirmed_proposals.back();

        // Height of last confirmed in registery
        let height = last.map(|p| p.manifest.height).unwrap_or(0);

        // Get the height of the last
    }

    /// Receive a new proposal from an external source. We need to validate
    /// before processing this further
    fn receive_proposal(&mut self, manifest: ProposalManifest) {
        let hash: ProposalHash = (&manifest).into();

        // Proposal already exists, don't re-create
        if self.exists(&hash) {
            self.shared.send_event(ProposalEvent::DuplicateProposal);
            return;
        }

        // If we have existing height, check for out of date proposal
        if let Some(height) = self.height() {
            let manifest_height = manifest.height;

            // Check that the height of this proposal > confirmed
            if height >= manifest_height {
                self.shared.send_event(ProposalEvent::OutOfDate {
                    local_height: height,
                    proposal_height: manifest.height,
                });
                return;
            }

            // Proposal is too new, we are probably behind
            if manifest_height > height + 1 {
                self.add_proposal(manifest);

                // Find the next closest set of proposals to the current height.
                // If we have more gaps, then these will be picked up later.
                // TODO: we should probably most this to check for gaps elsewhere
                self.shared.send_event(ProposalEvent::OutOfSync {
                    max_seen_height: self
                        .find_next_closest_height(height)
                        .unwrap_or(manifest_height),
                    local_height: height,
                });
                return;
            }
        }

        // Check proposal from correct peer
        self.add_proposal(manifest);

        // Confirm the last_proposal_id matches and if so move proposal to confirmed

        // Reset timout timer, as we've received a proposal we ideally need to wait for
        // accepts.
        self.reset_timeout();

        // Find leader we should send accept to, this needs to be
        let next_leader = self.next_leader();

        // Move previous current proposal to committed state

        // TODO: Confirm existing proposal if this is a child
        // TODO: Does this proposal complete a chain of proposals?

        // Send accept to next leader
        // TODO: only send accept if we are up to date
        self.shared.send_event(ProposalEvent::Accept {
            proposal_hash: hash,
            peer_id: next_leader,
            skips: 0,
        })
    }

    pub fn receive_accept(&mut self, proposal_hash: ProposalHash, peer_id: PeerId) {}

    fn shutdown_purge_task(&self) {
        // The background task must be signaled to shut down. This is done by
        // setting `State::shutdown` to `true` and signalling the task.
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // Drop the lock before signalling the background task. This helps
        // reduce lock contention by ensuring the background task doesn't
        // wake up only to be unable to acquire the mutex.
        drop(state);
        self.shared.background_worker.notify_one();
    }
}

impl Stream for ProposalRegister {
    type Item = ProposalEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<ProposalEvent>> {
        let mut state = self.shared.state.lock().unwrap();
        // self.shared = cx.waker().wake_by_ref();
        if let Some(event) = state.events.pop_front() {
            return Poll::Ready(Some(event));
        }
        //  cx.waker().wake_by_ref()
        Poll::Pending
    }
}

impl ProposalRegisterShared {
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }

    fn send_event(&self, event: ProposalEvent) {
        let mut state = self.state.lock().unwrap();
        state.events.push_back(event)
    }

    fn timeout(&self) -> Option<Instant> {
        let state = self.state.lock().unwrap();

        // Check if we have an expiry time
        if state.timeout.is_none() {
            return None;
        }

        // self.send_event();

        state.timeout
    }
}

impl Proposal {
    pub fn new(manifest: ProposalManifest) -> Self {
        let hash: ProposalHash = (&manifest).into();

        Self {
            accepts: HashSet::new(),
            hash: Key::new(hash),
            manifest,
        }
    }

    fn add_accept(&mut self, peer_id: PeerId) {
        self.accepts.insert(peer_id);
    }
}

impl Default for ProposalHash {
    fn default() -> Self {
        ProposalHash(Sha256::digest([0u8]).to_vec())
    }
}

impl Borrow<[u8]> for ProposalHash {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl From<&ProposalManifest> for ProposalHash {
    fn from(p: &ProposalManifest) -> Self {
        let bytes = Sha256::digest(bincode::serialize(p).unwrap());
        ProposalHash(bytes.to_vec())
    }
}

impl From<ProposalHash> for Key<ProposalHash> {
    fn from(p: ProposalHash) -> Self {
        Key::new(p)
    }
}

async fn background_worker(shared: Arc<ProposalRegisterShared>) {
    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        // Check timeout
        if let Some(when) = shared.timeout() {
            tokio::select! {
                _ = sleep_until(when) => {}
                _ = shared.background_worker.notified() => {}
            }
        } else {
            // No expiry set, so wait to be notified
            shared.background_worker.notified().await;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // TODO: test with no peers

    #[tokio::test]
    async fn ignores_duplicate_proposal() {
        let peer_1 = PeerId::random();
        let mut register = ProposalRegister::new(peer_1.clone(), vec![]);
        let manifest = ProposalManifest {
            last_proposal_hash: ProposalHash(vec![]),
            height: 0,
            skips: 0,
            peer_id: peer_1,
            changes: vec![],
        };

        // Send proposal twice
        // let mut state = register.state.lock().unwrap();
        register.receive_proposal(manifest.clone());
        register.receive_proposal(manifest.clone());

        register.next().await.unwrap();
        let next = register.next().await.unwrap();

        assert_eq!(next, ProposalEvent::DuplicateProposal)
    }

    #[tokio::test]
    async fn first_proposal() {
        let peer_1 = PeerId::random();

        let mut register = ProposalRegister::new(peer_1.clone(), vec![]);
        let manifest = ProposalManifest {
            last_proposal_hash: ProposalHash(vec![]),
            height: 0,
            skips: 0,
            peer_id: peer_1.clone(),
            changes: vec![],
        };
        let hash: ProposalHash = (&manifest).into();

        register.receive_proposal(manifest.clone());

        let next = register.next().await.unwrap();

        assert_eq!(
            next,
            ProposalEvent::Accept {
                proposal_hash: hash,
                skips: 0,
                peer_id: peer_1
            }
        )
    }
}
