use crate::change::Change;
use crate::event::GuildEvent;
use crate::key::Key;
use crate::pending::{self, PendingQueue};
use crate::proposal::hash::ProposalHash;
use crate::proposal::manifest::ProposalManifest;
use crate::proposal::proposal::Proposal;
use bincode::{deserialize, serialize};
use libp2p_core::PeerId;

use slog::{crit, warn};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    PendingCache(#[from] pending::Error),
}

pub trait Store {
    // Store should apply changes and return the new root hash
    // for the rollup/store
    fn commit(&self, changes: Change) -> Vec<u8>;

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

    /// PeerId for the current leader
    last_proposal: Option<Proposal>,

    /// List of proposals that have been accepted
    proposals: HashMap<Vec<u8>, Proposal>,

    /// Pending txns that this node will include it if becomes the leader
    pending: PendingQueue<Vec<u8>, Change>,

    /// Store to set and get state
    store: TStore,

    /// Network for sending and receiving events
    network: TNetwork,

    /// Flag to indicate if this node is up to date with the leader
    up_to_date: bool,

    /// Logger
    logger: slog::Logger,
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
            local_peer_id,
            members: Vec::new(),
            connected_members: Vec::new(),
            last_proposal: None,
            proposals: HashMap::new(),
            pending: PendingQueue::new(),
            store,
            network,
            up_to_date: false,
            logger,
        }
    }

    pub fn add(&mut self, changes: Vec<Change>) {
        // Send new txns to other members
        self.send_all(GuildEvent::Pending { changes });

        // Add pending changes locally
        // self.add_pending_changes(changes);
    }

    async fn run() {
        loop {
            sleep(Duration::from_secs(1)).await
        }
    }

    fn send(&self, peerId: &PeerId, event: &GuildEvent) {
        // Serialize the data
        let data = serialize(&event).unwrap();

        // Send event to all peers
        self.network.send(&peerId, data);
    }

    fn send_all(&self, event: GuildEvent) {
        for peer in self.connected_members.iter() {
            self.send(peer.preimage(), &event);
        }
    }

    fn handle_proposal(&self) {
        // Check if node is valid leader
        // Check if up to date
        // Check if txn is valid
    }

    fn is_proposal_valid(&self) -> bool {
        // Check if node is valid leader
        // Check if up to date
        // Check if txn is valid
        true
    }

    fn join() {
        // Attempt to dial other peers

        // Send state to other peers on
    }

    fn snapshot() {
        // Take a snapshot of proposals??
        // Take a snapshot of database state
    }

    fn add_pending_changes(&self, changes: Vec<Change>) -> Result<()> {
        let kvp = changes
            .into_iter()
            .map(|change| (change.id.clone(), change))
            .collect();

        Ok(self.pending.append(kvp)?)
    }

    fn receive_proposal(&self, proposal_manifest: ProposalManifest) {
        // let proposal = Proposal::new(proposal_manifest);
    }

    fn receive_accept(&self, proposal_hash: ProposalHash) {}

    fn on_event(&self, event: GuildEvent, peerId: PeerId) {
        match event {
            GuildEvent::Proposal {
                proposal_manifest, ..
            } => self.receive_proposal(proposal_manifest),
            GuildEvent::Accept { proposal_hash } => {}
            GuildEvent::Pending { changes } => {
                match self.add_pending_changes(changes) {
                    Ok(_) => {}
                    Err(e) => match e {
                        Error::PendingCache(pending::Error::KeyExists) => {
                            // In this case, we won't add the txn to the queue and so these
                            // txns won't be included if this node becomes the leader. However,
                            // the txn is likely included by another node
                            warn!(self.logger, "Duplicate changes detected, dropping changes");
                        }
                    },
                }
            }
            _ => {
                crit!(self.logger, "Received unknown event: {event:?}")
            }
        }
    }
}
