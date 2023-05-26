use super::config::SolidConfig;
use super::event::SolidEvent;
use super::proposal::{ProposalAccept, ProposalHash, ProposalManifest};
use super::store::ProposalStore;
use crate::peer::PeerId;
#[allow(unused_imports)]
use futures::stream::StreamExt;
use futures::task::Waker;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
#[cfg(test)]
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{sleep_until, Instant};
use tokio_stream::Stream;

/**
 * Solid is responsible creating an event stream of SolidEvent events, and
 * triggering skip messages if interval expires.
 */

// TODO: Logic for adding/removing peers

#[derive(Debug)]
pub struct Solid {
    shared: Arc<SolidShared>,
}

#[derive(Debug)]
pub struct SolidShared {
    local_peer_id: PeerId,

    /// Notifies the background worker to wake up
    background_worker: Notify,

    /// Events to be streamed
    events: Mutex<VecDeque<SolidEvent>>,

    /// Timeout used for skips (aka expired accepts, no new proposal
    /// received within timeout period)
    skip_timeout: Mutex<Option<Instant>>,

    /// Timeout used when an out of sync message has been sent to the network,
    /// we need to wait a configurable amount of time before sending another
    out_of_sync_timeout: Mutex<Option<Instant>>,

    /// Shared proposal state, must be updated together
    state: Mutex<SolidState>,

    store: Mutex<ProposalStore>,

    /// Configuration for the proposal register
    config: SolidConfig,
}

#[derive(Debug)]
struct SolidState {
    shutdown: bool,
    waker: Option<Waker>,
}

impl Drop for Solid {
    fn drop(&mut self) {
        // Signal that we want to shutdown (i.e. remove the timeout timer)
        self.shutdown_background_worker();
    }
}

impl Solid {
    pub fn with_last_confirmed(
        local_peer_id: PeerId,
        manifest: ProposalManifest,
        config: SolidConfig,
    ) -> Self {
        let shared = Arc::new(SolidShared {
            local_peer_id: local_peer_id.clone(),
            events: Mutex::new(VecDeque::new()),
            store: Mutex::new(ProposalStore::with_last_confirmed(
                local_peer_id,
                manifest,
                config.max_proposal_history,
            )),
            skip_timeout: Mutex::new(None),
            out_of_sync_timeout: Mutex::new(None),
            state: Mutex::new(SolidState {
                shutdown: false,
                waker: None,
            }),
            config,
            background_worker: Notify::new(),
        });

        Self { shared }
    }

    pub fn genesis(local_peer_id: PeerId, peers: Vec<PeerId>, config: SolidConfig) -> Self {
        Self::with_last_confirmed(local_peer_id, ProposalManifest::genesis(peers), config)
    }

    pub fn run(&self) -> tokio::task::JoinHandle<()> {
        // Create background worker, this is mostly responsible for sending skips
        // when a new proposal has not been created by the next responsible leader
        // *self.shared.skip_timeout.lock().unwrap() = Some(Instant::now());
        self.process_next();
        tokio::spawn(background_worker(Arc::clone(&self.shared)))
    }

    /// Gets the highest confirmed height for this reigster
    pub fn height(&self) -> usize {
        self.shared.store.lock().height()
    }

    /// Whether a proposal hash exists in the data
    pub fn exists(&self, hash: &ProposalHash) -> bool {
        self.shared.store.lock().exists(hash)
    }

    /// Get a list of confirmed proposals from a given height
    pub fn confirmed_proposals_from(&self, i: usize) -> Vec<ProposalManifest> {
        self.shared
            .store
            .lock()
            .confirmed_proposals_from(i)
            .to_vec()
    }

    /// Receive a new proposal from an external source, we do some basic validation
    /// to make sure this is a valid proposal that could be confirmed.
    pub fn receive_proposal(&mut self, manifest: ProposalManifest) {
        let hash: ProposalHash = (&manifest).into();

        // Proposal already exists, don't recreate
        if self.exists(&hash) {
            self.shared.send_event(SolidEvent::DuplicateProposal {
                proposal_hash: hash,
            });
            return;
        }

        // If we have existing height, check for out of date proposal
        let manifest_height = manifest.height;

        // Check that the height of this proposal > confirmed
        let height = self.height();
        if height >= manifest_height {
            self.shared.send_event(SolidEvent::OutOfDate {
                local_height: height,
                proposal_height: manifest_height,
                proposal_hash: manifest.hash(),
                peer_id: manifest.leader_id,
            });
            return;
        }

        // Add proposal to the store
        {
            self.shared.store.lock().add_pending_proposal(manifest);
        }

        // Process next rounds with the newly added proposal state, and keep
        // processing until nothing is left
        self.process_next()
    }

    /// Receive a new accept from an external source, we should only really receive accepts
    /// if we are to to be the next leader, store will determine if this is valid and send
    pub fn receive_accept(&self, accept: &ProposalAccept, from: &PeerId) {
        let mut store = self.shared.store.lock();
        if let Some(event) = store.add_accept(accept, from) {
            self.shared.send_event(event)
        }
    }

    /// Reset skip timeout as we received the next proposal in time
    fn reset_skip_timeout(&self) {
        let mut skip_timeout = self.shared.skip_timeout.lock();
        *skip_timeout = Some(Instant::now() + self.shared.config.skip_timeout);
        self.shared.background_worker.notify_one();
    }

    /// Clear skip timeout when we are the one proposing a new proposal, or when we are
    /// behind/out of sync with the network
    fn clear_skip_timeout(&self) {
        let mut skip_timeout = self.shared.skip_timeout.lock();
        *skip_timeout = None;
        self.shared.background_worker.notify_one();
    }

    // Process the next proposal in the chain, this should move the proposal
    // If we don't have the next proposal in the chain, request it from the network
    fn process_next(&self) {
        let mut store = self.shared.store.lock();

        // Keep telling the store to process proposals until it returns None, signalling it cannot
        // make further process until another proposal or accept is received
        while let Some(event) = store.process_next() {
            match &event {
                // We are catching up or out of sync, so there is no need for the timeout to be active,
                // we will wake up the timer after we have caught up (i.e. when we see the next SendAccept)
                SolidEvent::OutOfSync { .. } => {
                    self.clear_skip_timeout();

                    let mut timeout = self.shared.out_of_sync_timeout.lock();
                    if let Some(timeout) = &mut *timeout {
                        if *timeout > Instant::now() {
                            return;
                        }
                    }

                    // Set the out of sync timeout
                    *timeout = Some(Instant::now() + self.shared.config.out_of_sync_timeout);

                    // Send the event, but cancel looping
                    self.shared.send_event(event);
                    return;
                }
                SolidEvent::Commit { .. } => {
                    self.reset_skip_timeout();
                }

                SolidEvent::Propose { .. } => {
                    // We don't want to send a skip for our own proposals, so clear the timeout
                    self.clear_skip_timeout();
                }

                // We received a new valid proposal, that we are willing to accept. This is the best
                // indication that we think we are in sync with the network. We now want to track
                // whether the proposal for this accept is also accepted by the network (in time), indicated
                // by receiving a subsequent proposal at the next height (and subsequently the store tells us
                // to send the next SendAccept)
                SolidEvent::Accept { accept } => {
                    // Cancel out of sync, if we are sending an accept
                    *self.shared.out_of_sync_timeout.lock() = None;
                    // Restart the timer for the current proposal to be accepted
                    self.reset_skip_timeout();

                    // Skip sending accept for our own proposal
                    if accept.leader_id == self.shared.local_peer_id {
                        return;
                    }
                }

                _ => {}
            }

            // Send the event to the event stream
            self.shared.send_event(event);
        }
    }

    fn shutdown_background_worker(&self) {
        // The background task must be signaled to shut down. This is done by
        // setting `State::shutdown` to `true` and signalling the task.
        let mut state = self.shared.state.lock();
        state.shutdown = true;

        // Drop the lock before signalling the background task. This helps
        // reduce lock contention by ensuring the background task doesn't
        // wake up only to be unable to acquire the mutex.
        drop(state);
        self.shared.background_worker.notify_one();
    }
}

impl Stream for Solid {
    type Item = SolidEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut events = self.shared.events.lock();
        if let Some(event) = events.pop_front() {
            return Poll::Ready(Some(event));
        }
        let mut state = self.shared.state.lock();
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl SolidShared {
    fn is_shutdown(&self) -> bool {
        self.state.lock().shutdown
    }

    fn send_event(&self, event: SolidEvent) {
        {
            let mut events = self.events.lock();
            events.push_back(event);
        }
        let mut state = self.state.lock();
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// Checks for either: next_proposal or a skip timeout
    fn tick(&self) -> Option<Instant> {
        let mut skip_timeout_guard = self.skip_timeout.lock();

        if (*skip_timeout_guard)? > Instant::now() {
            return *skip_timeout_guard;
        }

        if let Some(event) = self.store.lock().skip() {
            self.send_event(event);
            *skip_timeout_guard = Some(Instant::now() + self.config.skip_timeout);
        }

        *skip_timeout_guard
    }
}

/// Background worker that calls `tick` whenever its scheduled to run the
/// next task or worken up by the `background_worker` channel.
async fn background_worker(shared: Arc<SolidShared>) {
    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        // Check timeout
        if let Some(when) = shared.tick() {
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
    use crate::proposal::ProposalAccept;

    use super::*;

    fn create_peers() -> [PeerId; 3] {
        let p1 = PeerId::new(vec![1u8]);
        let p2 = PeerId::new(vec![2u8]);
        let p3 = PeerId::new(vec![3u8]);
        [p1, p2, p3]
    }

    #[tokio::test]
    async fn ignores_duplicate_proposal() {
        let [p1, _, _] = create_peers();
        let config = SolidConfig::default();
        let mut register = Solid::genesis(p1.clone(), vec![p1.clone()], config);
        let manifest = ProposalManifest {
            last_proposal_hash: ProposalHash::default(),
            height: 1,
            skips: 0,
            leader_id: p1,
            txns: vec![],
            peers: vec![],
        };

        // Send proposal twice
        // let mut state = register.state.lock().unwrap();
        register.receive_proposal(manifest.clone());
        register.receive_proposal(manifest.clone());

        register.next().await.unwrap();
        let next = register.next().await.unwrap();

        assert_eq!(
            next,
            SolidEvent::DuplicateProposal {
                proposal_hash: manifest.hash()
            }
        )
    }

    #[tokio::test]
    async fn first_proposal_single_peer() {
        let [p1, _, _] = create_peers();
        let config = SolidConfig::default();

        let mut register = Solid::genesis(p1.clone(), vec![p1.clone()], config);
        let manifest: ProposalManifest = ProposalManifest {
            last_proposal_hash: ProposalHash::default(),
            height: 1,
            skips: 0,
            leader_id: p1.clone(),
            txns: vec![],
            peers: vec![],
        };
        let hash: ProposalHash = (&manifest).into();

        register.receive_proposal(manifest.clone());

        let next = register.next().await.unwrap();

        assert_eq!(
            next,
            SolidEvent::Propose {
                last_proposal_hash: hash,
                height: 2,
                skips: 0
            }
        )
    }

    #[tokio::test]
    async fn first_proposal_multi_peer() {
        let [p1, p2, _] = create_peers();
        let config = SolidConfig::default();

        let mut register = Solid::genesis(p1.clone(), create_peers().to_vec(), config);
        let manifest: ProposalManifest = ProposalManifest {
            last_proposal_hash: ProposalHash::default(),
            height: 1,
            skips: 0,
            leader_id: p1.clone(),
            txns: vec![],
            peers: vec![],
        };
        let hash: ProposalHash = (&manifest).into();

        register.receive_proposal(manifest.clone());

        let next = register.next().await.unwrap();

        assert_eq!(
            next,
            SolidEvent::Accept {
                accept: ProposalAccept {
                    proposal_hash: hash,
                    leader_id: p2,
                    height: 1,
                    skips: 0,
                }
            }
        )
    }

    #[tokio::test]
    async fn test_tick_no_action() {
        let [p1, _, _] = create_peers();
        let config = SolidConfig::default();
        let register = Solid::genesis(p1, create_peers().to_vec(), config);
        assert_eq!(register.shared.tick(), None);
    }

    #[test]
    fn test_tick_send_skip() {
        let [p1, _, _] = create_peers();
        let config = SolidConfig::default();
        let register = Solid::genesis(p1, create_peers().to_vec(), config);

        // Add an expired skip_timeout instant
        {
            register.shared.skip_timeout.lock().replace(Instant::now());
        }

        let next_tick = register.shared.tick();

        // Should return the next tick
        assert!(
            next_tick > Some(Instant::now() + Duration::from_secs(3)),
            "next tick {:?} should be more than 3 seconds away",
            next_tick
        );

        // Should add skip event
        assert_eq!(register.shared.events.lock().len(), 1);
    }

    #[test]
    fn test_tick_not_ready() {
        let [p1, _, _] = create_peers();
        let config = SolidConfig::default();
        let register = Solid::genesis(p1, create_peers().to_vec(), config);

        let time = Instant::now() + Duration::from_secs(10);

        // Add time which is not ready
        {
            register.shared.skip_timeout.lock().replace(time);
        }

        // Returns time to wake up tick
        assert_eq!(register.shared.tick(), Some(time));

        // No events added
        assert_eq!(register.shared.events.lock().len(), 0);
    }
}
