use super::event::ProposalEvent;
use super::hash::ProposalHash;
use super::manifest::ProposalManifest;
use super::proposal::ProposalAccept;
use super::store::ProposalStore;
use crate::peer::PeerId;
use futures::stream::{Next, StreamExt};
use futures::task::Waker;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{sleep_until, Instant};
use tokio_stream::Stream;

#[derive(Debug)]
pub struct ProposalRegister {
    shared: Arc<ProposalRegisterShared>,
}

#[derive(Debug)]
pub struct ProposalRegisterConfig {
    timeout: Duration,
    interval: Duration,
}

impl Default for ProposalRegisterConfig {
    fn default() -> Self {
        ProposalRegisterConfig {
            interval: Duration::from_secs(1),
            timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug)]
pub struct ProposalRegisterShared {
    /// Local peer, required so we can determine if we are the leader
    local_peer_id: PeerId,

    /// Notifies the background worker to wake up
    background_worker: Notify,

    /// Events to be streamed
    events: Mutex<VecDeque<ProposalEvent>>,

    /// Timeout used for expired accepts (no new proposal received within timeout period)
    timeout: Mutex<Option<Instant>>,

    /// Shared proposal state, must be updated together
    state: Mutex<ProposalRegisterState>,

    config: ProposalRegisterConfig,
}

#[derive(Debug)]
struct ProposalRegisterState {
    shutdown: bool,

    next_proposal: Option<NextProposal>,

    store: ProposalStore,

    waker: Option<Waker>,
}

#[derive(Debug)]
struct NextProposal {
    timer: Instant,
    height: usize,
    last_proposal_hash: ProposalHash,
}

impl Drop for ProposalRegister {
    fn drop(&mut self) {
        // Signal that we want to shutdown (i.e. remove the timeout timer)
        self.shutdown_background_worker();
    }
}

impl ProposalRegister {
    pub fn new(local_peer_id: PeerId, peers: Vec<PeerId>) -> Self {
        let shared = Arc::new(ProposalRegisterShared {
            local_peer_id: local_peer_id.clone(),
            events: Mutex::new(VecDeque::new()),
            timeout: Mutex::new(None),
            state: Mutex::new(ProposalRegisterState {
                shutdown: false,
                store: ProposalStore::new(local_peer_id, peers),
                waker: None,
                next_proposal: None,
            }),
            config: ProposalRegisterConfig::default(),
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
    pub fn height(&self) -> usize {
        self.shared.state.lock().unwrap().store.height()
    }

    /// Whether a proposal hash exists in the data
    pub fn exists(&self, hash: &ProposalHash) -> bool {
        self.shared.state.lock().unwrap().store.exists(hash)
    }

    /// Receive a new proposal from an external source, we do some basic validation
    /// to make sure this is a valid proposal that could be confirmed.
    pub fn receive_proposal(&mut self, manifest: ProposalManifest) {
        let hash: ProposalHash = (&manifest).into();

        // Proposal already exists, don't recreate
        if self.exists(&hash) {
            self.shared.send_event(ProposalEvent::DuplicateProposal);
            return;
        }

        // If we have existing height, check for out of date proposal
        let manifest_height = manifest.height;

        // Check that the height of this proposal > confirmed
        let height = self.height();
        if height >= manifest_height {
            self.shared.send_event(ProposalEvent::OutOfDate {
                local_height: height,
                proposal_height: manifest.height,
            });
            return;
        }

        // Add proposal to the store
        {
            self.shared
                .state
                .lock()
                .unwrap()
                .store
                .add_pending_proposal(manifest);
        }

        // Process next rounds with the newly added proposal state, and keep
        // processing until nothing is left
        self.process_next()

        // TODO: wait 1 second before next update?
    }

    pub fn receive_accept(
        &mut self,
        accept: &ProposalAccept,
        from: PeerId,
    ) -> Option<ProposalEvent> {
        let mut state = self.shared.state.lock().unwrap();
        state.store.add_accept(accept, from)
    }

    fn reset_timeout(&self) {
        let mut timeout = self.shared.timeout.lock().unwrap();
        *timeout = Some(Instant::now() + self.shared.config.timeout);
        self.shared.background_worker.notify_one();
    }

    fn clear_timeout(&self) {
        let mut timeout = self.shared.timeout.lock().unwrap();
        *timeout = None;
        self.shared.background_worker.notify_one();
    }

    // Process the next proposal in the chain, this should move the proposal
    // If we don't have the next proposal in the chain, request it from the network
    fn process_next(&self) {
        let mut state = self.shared.state.lock().unwrap();
        while let Some(event) = state.store.process_next() {
            match event {
                ProposalEvent::OutOfSync { .. } => {
                    // Clear the timeout, as we're out of date, we don't want to send
                    // any skip messages to the network until we are.
                    self.clear_timeout();
                    self.shared.send_event(event);
                    return;
                }
                ProposalEvent::Propose {
                    height,
                    last_proposal_hash,
                } => {
                    // Don't send proposal immedietely, as we only want one proposal
                    // per interval period
                    state.next_proposal = Some(NextProposal {
                        last_proposal_hash,
                        height,
                        timer: Instant::now() + self.shared.config.interval,
                    });

                    // So we wait on the right moment
                    self.shared.background_worker.notify_one();

                    return;
                }
                ProposalEvent::CatchingUp { .. } => {
                    self.clear_timeout();
                }
                _ => {}
            }

            // For accept, we should pass it back in
            if let ProposalEvent::SendAccept { accept } = &event {
                self.reset_timeout();
                // Accept our own proposals
                if let Some(event) = state
                    .store
                    .add_accept(accept, self.shared.local_peer_id.clone())
                {
                    self.shared.send_event(event);
                }
            }

            self.shared.send_event(event);
        }
    }

    fn shutdown_background_worker(&self) {
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
        let mut events = self.shared.events.lock().unwrap();
        if let Some(event) = events.pop_front() {
            return Poll::Ready(Some(event));
        }
        let mut state = self.shared.state.lock().unwrap();
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl ProposalRegisterShared {
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }

    fn send_event(&self, event: ProposalEvent) {
        let mut events = self.events.lock().unwrap();
        events.push_back(event);
        drop(events);
        let mut state = self.state.lock().unwrap();
        if let Some(waker) = state.waker.take() {
            waker.wake()
        }
    }

    fn tick(&self) -> Option<Instant> {
        // Do we have a proposal to send?
        let mut state = self.state.lock().unwrap();
        if let Some(next) = &state.next_proposal {
            if next.timer > Instant::now() {
                return Some(next.timer);
            }
            let NextProposal {
                height,
                last_proposal_hash,
                ..
            } = state.next_proposal.take().unwrap();
            self.send_event(ProposalEvent::Propose {
                last_proposal_hash,
                height,
            });

            return None;
        }

        let mut timeout = self.timeout.lock().unwrap();

        // Check if we have an expiry time
        if timeout.is_none() {
            return None;
        }

        if let Some(event) = self.state.lock().unwrap().store.skip() {
            self.events.lock().unwrap().push_back(event);
            *timeout = Some(Instant::now() + self.config.timeout);
        }

        *timeout
    }
}

async fn background_worker(shared: Arc<ProposalRegisterShared>) {
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
    use crate::proposal::proposal::ProposalAccept;

    use super::*;

    // TODO: test with no peers

    #[tokio::test]
    async fn ignores_duplicate_proposal() {
        let peer_1 = PeerId::random();
        let mut register = ProposalRegister::new(peer_1.clone(), vec![]);
        let manifest = ProposalManifest {
            last_proposal_hash: ProposalHash::default(),
            height: 1,
            skips: 0,
            leader_id: peer_1,
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
            last_proposal_hash: ProposalHash::default(),
            height: 1,
            skips: 0,
            leader_id: peer_1.clone(),
            changes: vec![],
        };
        let hash: ProposalHash = (&manifest).into();

        register.receive_proposal(manifest.clone());

        let next = register.next().await.unwrap();

        assert_eq!(
            next,
            ProposalEvent::SendAccept {
                accept: ProposalAccept {
                    proposal_hash: hash,
                    leader_id: peer_1,
                    height: 1,
                    skips: 0,
                }
            }
        )
    }
}
