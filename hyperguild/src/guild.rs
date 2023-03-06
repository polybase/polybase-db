use crate::change::Change;
use crate::event::GuildEvent;
use crate::key::Key;
use crate::peer::PeerId;
use crate::proposal::event::ProposalEvent;
use crate::proposal::manifest::{self, ProposalManifest};
use crate::proposal::proposal::Accept;
use crate::proposal::register::ProposalRegister;
use bincode::{deserialize, serialize};

use slog::{crit, debug, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

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

pub trait Network {
    fn send(&self, peerId: &PeerId, data: Vec<u8>);
    fn recv(&self) -> Vec<u8>;
}

// Purpose of Guild is to create consensus among members
// Guild is concerned with maintaining consistent state across members
// rather than for determining what the state should be. It cares
// about the how not the what.

pub struct Guild<TStore, TNetwork> {
    local_peer_id: PeerId,

    /// List of other peers sockets (for connecting)
    members: Vec<SocketAddr>,

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

    /// Flag to indicate if this node is up to date with the leader
    up_to_date: bool,

    /// Logger
    logger: slog::Logger,

    /// Root hash
    root_hash: Option<Vec<u8>>,
}

impl<TStore, TNetwork> Guild<TStore, TNetwork>
where
    TStore: Store + Send,
    TNetwork: Network + Send,
{
    pub fn new(
        local_peer_id: PeerId,
        store: TStore,
        network: TNetwork,
        logger: slog::Logger,
    ) -> Self {
        Self {
            local_peer_id: local_peer_id.clone(),
            members: Vec::new(),
            connected_members: Vec::new(),
            // TODO: add peers
            register: ProposalRegister::new(local_peer_id, vec![]),
            pending_changes: HashMap::new(),
            store,
            network,
            up_to_date: false,
            logger,
            root_hash: None,
        }
    }

    // TODO: have a promise that can be used
    pub fn add_pending_changes(&mut self, changes: Vec<Change>) {
        for change in changes.iter() {
            self.pending_changes
                .entry(change.id.clone())
                .or_insert_with(|| change.clone());
        }

        // Send new txns to other members
        self.send_all(&GuildEvent::AddPendingChange { changes });
    }

    pub fn send_pending_change(&mut self, changes: Vec<Change>) {
        // Send new txns to other members
        self.send_all(&GuildEvent::AddPendingChange { changes });
    }

    async fn run() {
        loop {
            sleep(Duration::from_secs(1)).await
        }
    }

    fn send(&self, peer_id: &PeerId, event: &GuildEvent) {
        // Serialize the data
        let data = serialize(&event).unwrap();

        // Send event to all peers
        self.network.send(&peer_id, data);
    }

    fn send_all(&self, event: &GuildEvent) {
        for peer in self.connected_members.iter() {
            self.send(peer.preimage(), &event);
        }
    }

    fn join() {
        // Attempt to dial other peers
        // Send state to other peers on
    }

    /// Events from the proposal state machine, which notify of new
    /// actions that should be taken
    fn on_proposal_event(&mut self, event: ProposalEvent) {
        match event {
            // Node should send accept for an active proposal
            // to another peer
            ProposalEvent::SendAccept {
                leader_id,
                height,
                proposal_hash,
                skips,
            } => {
                // TODO: send proposal hash and peer_id
                info!(self.logger, "Send accept"; "height" => height, "skips" => skips);
                self.send(
                    // TODO: Accept should not have optional peer
                    &leader_id,
                    &GuildEvent::Accept {
                        accept: Accept {
                            leader_id: self.local_peer_id.clone(),
                            proposal_hash,
                            height,
                            skips,
                        },
                    },
                );
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
                    peer_id: self.local_peer_id.clone(),
                    changes,
                };

                //
                self.register.receive_proposal(manifest)
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
    fn on_incoming_event(&mut self, event: GuildEvent, peer_id: PeerId) {
        match event {
            // Incoming proposal from another peer
            GuildEvent::Proposal { manifest, .. } => self.register.receive_proposal(manifest),

            // Incoming accept from another peer
            GuildEvent::Accept { accept } => {
                if let Some(event) = self.register.receive_accept(accept) {
                    self.on_proposal_event(event);
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
            _ => {
                crit!(self.logger, "Received unknown event: {event:?}")
            }
        }
    }
}
