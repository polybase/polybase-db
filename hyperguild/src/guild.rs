use crate::change::Change;
use crate::event::GuildEvent;
use crate::key::Key;
use crate::peer::PeerId;
use crate::proposal::event::ProposalEvent;
use crate::proposal::manifest::ProposalManifest;
use crate::proposal::register::ProposalRegister;
use bincode::{deserialize, serialize};
use futures::{Future, Stream, StreamExt};
use slog::{crit, debug, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::select;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // #[error(transparent)]
    // PendingCache(#[from] pending::Error),
}

pub trait Store {
    // Store should apply changes and return the new root hash
    // for the rollup/store
    fn commit(&self, changes: Vec<Change>) -> Vec<u8>;

    // Restore the database from a snapshot
    fn restore(&self, from: Option<Vec<u8>>) -> SnapshotResp;

    // Take a snapshot of the database, so it can be sent to another
    // node that needs to catch up
    fn snapshot(&self, data: Vec<u8>);
}

pub struct SnapshotResp {
    data: Vec<u8>,
}

impl SnapshotResp {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

pub trait Network: NetworkSender {
    type EventStream: Stream<
            Item = (
                PeerId,
                std::result::Result<crate::service::EventResponse, tonic::Status>,
            ),
        > + Unpin;

    fn events(&mut self) -> &mut Self::EventStream;
    fn snapshot(
        &mut self,
        peer_id: PeerId,
        from: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<SnapshotResp>> + '_>;
}

pub trait NetworkSender {
    fn send(&self, peer_id: PeerId, data: Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + '_>>;
}

// Purpose of Guild is to create consensus among members
// Guild is concerned with maintaining consistent state across members
// rather than for determining what the state should be. It cares
// about the how not the what.

pub struct Guild<TStore, TNetwork> {
    local_peer_id: PeerId,

    /// Connected members
    connected_members: Vec<Key<PeerId>>,

    /// Proposal Register
    register: ProposalRegister,

    /// Pending changes (aka txn pool)
    pending_changes: HashMap<Vec<u8>, Change>,

    /// Store to set and get state
    store: TStore,

    /// Network for sending and receiving events
    network: TNetwork,

    /// Logger
    logger: slog::Logger,

    /// Root hash
    root_hash: Option<Vec<u8>>,
}

impl<TStore, TNetwork> Guild<TStore, TNetwork>
where
    TStore: Store + Send,
    TNetwork: Network,
{
    pub fn new(
        local_peer_id: PeerId,
        store: TStore,
        network: TNetwork,
        logger: slog::Logger,
    ) -> Self {
        Self {
            local_peer_id: local_peer_id.clone(),
            connected_members: Vec::new(),
            // TODO: add peers
            register: ProposalRegister::new(local_peer_id, vec![]),
            pending_changes: HashMap::new(),
            store,
            network,
            logger,
            root_hash: None,
        }
    }

    pub async fn add_pending_changes(&mut self, changes: Vec<Change>) {
        for change in changes.iter() {
            self.pending_changes
                .entry(change.id.clone())
                .or_insert_with(|| change.clone());
        }

        // Send new txns to other members
        self.send_all(&GuildEvent::AddPendingChange { changes })
            .await;
    }

    pub async fn send_pending_change(&mut self, changes: Vec<Change>) {
        // Send new txns to other members
        self.send_all(&GuildEvent::AddPendingChange { changes })
            .await;
    }

    pub async fn run(&mut self) {
        debug!(self.logger, "init guild");
        loop {
            select! {
                network_event =  self.network.events().next() => {
                    if let Some((peer_id, Ok(event))) = network_event {
                        let event: GuildEvent = deserialize(event.data.as_slice()).unwrap();
                        debug!(self.logger, "network_event {:?}", event);
                        self.on_network_event(event, peer_id).await;
                    }
                }
                proposal_event = self.register.next() => {
                    if let Some(event) = proposal_event {
                        debug!(self.logger, "proposal event: {:?}", event);
                        self.on_proposal_event(event).await;
                    }
                }
            };
        }
    }

    async fn send(&self, peer_id: &PeerId, event: &GuildEvent) {
        debug!(
            self.logger,
            "sending event to peer: {:?} {:?}", peer_id, event
        );

        // Serialize the data
        let data = serialize(&event).unwrap();

        // Send event to all peers
        self.network.send(peer_id.clone(), data).await;
    }

    async fn send_all(&self, event: &GuildEvent) {
        for peer in self.connected_members.iter() {
            self.send(peer.preimage(), event).await;
        }
    }

    /// Events from the proposal state machine, which notify of new
    /// actions that should be taken
    async fn on_proposal_event(&mut self, event: ProposalEvent) {
        match event {
            // Node should send accept for an active proposal
            // to another peer
            ProposalEvent::SendAccept { accept } => {
                // TODO: send proposal hash and peer_id
                let leader = &accept.leader_id.clone();
                info!(self.logger, "Send accept"; "height" => &accept.height, "skips" => &accept.skips);
                self.send(
                    // TODO: Accept should not have optional peer
                    leader,
                    &GuildEvent::Accept { accept },
                )
                .await;
            }

            // Node should create and send a new proposal
            ProposalEvent::Propose {
                last_proposal_hash,
                height,
            } => {
                // Get changes from the pending changes cache
                let changes = self.pending_changes.values().cloned().collect();

                // Create the proposl manfiest
                let manifest = ProposalManifest {
                    last_proposal_hash,
                    skips: 0,
                    height,
                    leader_id: self.local_peer_id.clone(),
                    changes,
                };

                self.register.receive_proposal(manifest);
            }

            // Commit a confirmed proposal changes
            ProposalEvent::Commit { manifest } => {
                // Remove commits from pending changes store
                for change in &manifest.changes {
                    self.pending_changes.remove(&change.id);
                }

                self.root_hash = Some(self.store.commit(manifest.changes));
            }

            ProposalEvent::OutOfSync {
                local_height,
                max_seen_height,
                skips,
            } => {
                // TODO: send request to other nodes for missing proposals
            }

            ProposalEvent::OutOfDate {
                local_height,
                proposal_height,
            } => {
                debug!(self.logger, "Out of date proposal"; "local_height" => local_height, "proposal_height" => proposal_height);
            }

            ProposalEvent::DuplicateProposal => {
                info!(self.logger, "Duplicate proposal");
            }

            ProposalEvent::CatchingUp {
                local_height,
                proposal_height,
                max_seen_height,
            } => {
                info!(self.logger, "Catching up"; "local_height" => local_height, "proposal_height" => proposal_height, "max_seen_height" => max_seen_height);
            }
        }
    }

    /// Events sent by other peers to this node
    async fn on_network_event(&mut self, event: GuildEvent, peer_id: PeerId) {
        match event {
            // Incoming proposal from another peer
            GuildEvent::Proposal { manifest, .. } => {
                self.register.receive_proposal(manifest);
            }

            // Incoming accept from another peer
            GuildEvent::Accept { accept } => {
                if let Some(event) = self.register.receive_accept(&accept, peer_id) {
                    self.on_proposal_event(event).await;
                }
            }

            // Incoming changes from another peer
            GuildEvent::AddPendingChange { changes } => {
                for change in changes {
                    self.pending_changes
                        .entry(change.id.clone())
                        .or_insert_with(|| change);
                }
            }

            // TODO: catch up from state
            GuildEvent::Status { height, max_height } => {
                // TODO: update status
            }

            _ => {
                crit!(self.logger, "Received unknown event: {event:?}")
            }
        }
    }
}
