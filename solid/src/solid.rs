use crate::change::Change;
use crate::event::ProposalEvent;
use crate::event::SolidEvent;
use crate::network::Network;
use crate::peer::PeerId;
use crate::proposal::ProposalManifest;
use crate::register::ProposalRegister;
use bincode::{deserialize, serialize};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use slog::{debug, info};
use std::collections::HashMap;
use tokio::select;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // #[error(transparent)]
    // PendingCache(#[from] pending::Error),
}

/// Trait that defines the interface for a persistent store
pub trait Store {
    /// Store should apply changes and return the new root hash
    /// for the rollup/store. Store should also persist the stored proposal manifests, as
    /// these are required for determining the next leader in case of a restart.
    fn commit(&mut self, manifest: ProposalManifest) -> Vec<u8>;

    /// Restore the database from a snapshot, this will be called when we have received a snapshot
    /// from another node on the network and we want to rebuild the database
    fn restore(&mut self, snapshot: Snapshot);

    /// Take a snapshot of the database, so it can be sent to another
    /// node that needs to catch up
    fn snapshot(&self) -> std::result::Result<Snapshot, Box<dyn std::error::Error>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub proposal: ProposalManifest,
    pub data: Vec<u8>,
}

// Purpose of Solid is to create consensus among members
// Solid is concerned with maintaining consistent state across members
// rather than for determining what the state should be. It cares
// about the how not the what.

pub struct Solid<TStore, TNetwork> {
    local_peer_id: PeerId,

    /// Connected members
    peers: Vec<PeerId>,

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

impl<TStore, TNetwork> Solid<TStore, TNetwork>
where
    TStore: Store + Send,
    TNetwork: Network,
{
    pub fn new(
        local_peer_id: PeerId,
        peers: Vec<PeerId>,
        store: TStore,
        network: TNetwork,
        logger: slog::Logger,
    ) -> Self {
        let register = ProposalRegister::genesis(peers.to_vec());

        Self {
            local_peer_id,
            register,
            peers,
            // TODO: we should be able to pass in existing proposals
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
        self.send_all(&SolidEvent::AddPendingChange { changes })
            .await;
    }

    pub async fn send_pending_change(&mut self, changes: Vec<Change>) {
        // Send new txns to other members
        self.send_all(&SolidEvent::AddPendingChange { changes })
            .await;
    }

    pub async fn run(&mut self) {
        debug!(self.logger, "init solid");
        select! {
            _ = self.register.run() => {
                debug!(self.logger, "register closed");
            },
            _ = self.stream_events() => {
                debug!(self.logger, "stream events closed");
            }
        }
    }

    pub async fn stream_events(&mut self) {
        loop {
            select! {
                network_event =  self.network.events().next() => {
                    if let Some((peer_id, data)) = network_event {
                        let event: SolidEvent = deserialize(data.as_slice()).unwrap();
                        // debug!(self.logger, "network event"; "from" => format!("{:?}", peer_id.prefix()));
                        self.on_network_event(event, peer_id).await;
                    }
                }
                proposal_event = self.register.next() => {
                    if let Some(event) = proposal_event {
                        // debug!(self.logger, "proposal event"; "event" => format!("{:?}", event));
                        self.on_proposal_event(event).await;
                    }
                }
            };
        }
    }

    async fn send(&self, peer_id: &PeerId, event: &SolidEvent) {
        // Serialize the data
        let data = serialize(&event).unwrap();

        // Send event to all peers
        self.network.send(peer_id.clone(), data).await;
    }

    async fn send_all(&self, event: &SolidEvent) {
        for peer in self.peers.iter() {
            if peer != &self.local_peer_id {
                self.send(peer, event).await;
            }
        }
    }

    /// Events from the proposal state machine, which notify of new
    /// actions that should be taken
    async fn on_proposal_event(&mut self, event: ProposalEvent) {
        match event {
            // Node should send accept for an active proposal
            // to another peer
            ProposalEvent::SendAccept { accept } => {
                info!(self.logger, "Send accept"; "height" => &accept.height, "skips" => &accept.skips, "to" => &accept.leader_id.prefix(), "hash" => accept.proposal_hash.to_string());

                // If we are the leader, then immediately accept our own proposal
                if accept.leader_id == self.local_peer_id {
                    self.register.receive_accept(&accept, &self.local_peer_id);

                    // Don't send accept to self
                    return;
                }

                let leader = &accept.leader_id.clone();

                self.send(
                    // TODO: Accept should not have optional peer
                    leader,
                    &SolidEvent::Accept { accept },
                )
                .await;
            }

            // Node should create and send a new proposal
            ProposalEvent::Propose {
                last_proposal_hash,
                height,
                skips,
            } => {
                // Get changes from the pending changes cache
                let changes = self.pending_changes.values().cloned().collect();

                // Simulate delay
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

                // Create the proposl manfiest
                let manifest = ProposalManifest {
                    last_proposal_hash,
                    skips,
                    height,
                    leader_id: self.local_peer_id.clone(),
                    changes,
                    peers: self.peers.clone(),
                };
                let proposal_hash = manifest.hash();

                info!(self.logger, "Propose"; "hash" => proposal_hash.to_string(), "height" => height, "skips" => skips);

                // Add proposal to own register, this will trigger an accept
                self.register.receive_proposal(manifest.clone());

                // Send proposal to all other nodes
                self.send_all(
                    // TODO: Accept should not have optional peer
                    &SolidEvent::Proposal {
                        manifest: manifest.clone(),
                        proposal_hash,
                    },
                )
                .await;
            }

            // Commit a confirmed proposal changes
            ProposalEvent::Commit { manifest } => {
                info!(self.logger, "Commit"; "hash" => manifest.hash().to_string(), "height" => manifest.height, "skips" => manifest.skips);
                // Remove commits from pending changes store
                for change in &manifest.changes {
                    self.pending_changes.remove(&change.id);
                }

                self.root_hash = Some(self.store.commit(manifest));
            }

            ProposalEvent::OutOfSync {
                local_height,
                max_seen_height,
                accepts_sent,
            } => {
                info!(self.logger, "Out of sync"; "local_height" => local_height, "accepts_sent" => accepts_sent, "max_seen_height" => max_seen_height);

                // Send to all peers, so they can send us proposals (we may receive some duplicates!)
                self.send_all(&SolidEvent::OutOfSync {
                    height: local_height,
                    max_seen_height,
                    accepts_sent,
                })
                .await
            }

            ProposalEvent::OutOfDate {
                local_height,
                proposal_height,
                proposal_hash,
                peer_id,
            } => {
                debug!(self.logger, "Out of date proposal"; "local_height" => local_height, "proposal_height" => proposal_height, "proposal_hash" => proposal_hash.to_string(), "from" => peer_id.prefix());
            }

            ProposalEvent::DuplicateProposal { proposal_hash } => {
                debug!(self.logger, "Duplicate proposal"; "hash" => proposal_hash.to_string());
            }
        }
    }

    /// Events sent by other peers to this node
    async fn on_network_event(&mut self, event: SolidEvent, peer_id: PeerId) {
        match event {
            // Incoming proposal from another peer
            SolidEvent::Proposal { manifest, .. } => {
                debug!(self.logger, "Receive proposal"; "hash" => manifest.hash().to_string(), "height" => manifest.height, "from" => peer_id.prefix());
                self.register.receive_proposal(manifest);
            }

            // Incoming accept from another peer
            SolidEvent::Accept { accept } => {
                debug!(self.logger, "Receive accept"; 
                    "hash" => accept.proposal_hash.to_string(), 
                    "from" => peer_id.prefix(), 
                    "height" => accept.height, 
                    "skips" => accept.skips);
                // In case we haven't accepted ourselves yet
                self.register.receive_accept(&accept, &self.local_peer_id);
                self.register.receive_accept(&accept, &peer_id);
            }

            // Incoming changes from another peer
            SolidEvent::AddPendingChange { changes } => {
                for change in changes {
                    self.pending_changes
                        .entry(change.id.clone())
                        .or_insert_with(|| change);
                }
            }

            // Receive a snapshot from another peer
            SolidEvent::Snapshot { snapshot } => {
                debug!(self.logger, "Received snapshot"; "height" => snapshot.proposal.height);

                let proposal: ProposalManifest = snapshot.proposal.clone();

                // Restore the store from the snapshot
                self.store.restore(snapshot);

                // Reset the register
                let register = ProposalRegister::with_last_confirmed(proposal);

                self.register = register;
            }

            // Incoming backfill request from another peer
            SolidEvent::OutOfSync {
                height,
                max_seen_height,
                accepts_sent,
            } => {
                debug!(self.logger, "Received out of sync"; "height" => height, "accepts_sent" => accepts_sent, "max_seen_height" => max_seen_height);
                if height + 1024 < self.register.height() {
                    let snapshot = self.store.snapshot().unwrap();
                    self.send(&peer_id, &SolidEvent::Snapshot { snapshot })
                        .await;
                } else {
                    for proposal in self.register.confirmed_proposals_from(height) {
                        self.send(
                            &peer_id,
                            &SolidEvent::Proposal {
                                manifest: proposal.clone(),
                                proposal_hash: proposal.hash(),
                            },
                        )
                        .await;
                    }
                }
            }
        }
    }
}
