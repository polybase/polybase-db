use behaviour::{Behaviour, BehaviourEvent};
use events::NetworkEvent;
use futures_util::StreamExt;
use libp2p::{
    identity::Keypair,
    request_response,
    swarm::{keep_alive, SwarmBuilder, SwarmEvent},
    Multiaddr, PeerId,
};
use parking_lot::Mutex;
use protocol::PolyProtocol;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::{select, sync::mpsc, sync::oneshot, sync::Mutex as AsyncMutex};
use tracing::{debug, error, info};
use transport::create_transport;

mod behaviour;
pub mod events;
mod protocol;
mod transport;

type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to dial peer: {0}")]
    DialPeer(#[from] libp2p::swarm::DialError),

    #[error("Tansport error: {0}")]
    Transport(#[from] libp2p::TransportError<std::io::Error>),

    #[error("Channel error")]
    Send(#[from] tokio::sync::oneshot::error::RecvError),
}

pub struct Network {
    netin_rx: AsyncMutex<mpsc::UnboundedReceiver<(NetworkPeerId, NetworkEvent)>>,
    netout_tx: mpsc::UnboundedSender<(PeerId, NetworkEvent, oneshot::Sender<()>)>,
    local_peer_id: PeerId,
    shared: Arc<NetworkShared>,
}

impl Network {
    pub fn new(
        keypair: &Keypair,
        listenaddrs: impl Iterator<Item = Multiaddr>,
        dialaddrs: impl Iterator<Item = Multiaddr>,
    ) -> Result<Network> {
        let local_peer_id = PeerId::from(keypair.public());
        let transport = create_transport(keypair);
        let protocols = vec![(PolyProtocol(), request_response::ProtocolSupport::Full)];
        let config = request_response::Config::default();
        let mut swarm = {
            let behaviour = Behaviour {
                rr: request_response::Behaviour::new(PolyProtocol(), protocols, config),
                keep_alive: keep_alive::Behaviour,
            };
            SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build()
        };

        // Listen on given addresses
        for addr in listenaddrs {
            swarm.listen_on(addr)?;
        }

        // Connect to peers
        for addr in dialaddrs {
            info!(addr = ?addr, "Dialing peer");
            swarm.dial(addr)?;
        }

        // Channel to receive NetworkEvents from the network
        let (netin_tx, netin_rx) = mpsc::unbounded_channel::<(NetworkPeerId, NetworkEvent)>();
        let (netout_tx, mut netout_rx) =
            mpsc::unbounded_channel::<(PeerId, NetworkEvent, oneshot::Sender<()>)>();

        // Shared state between the network and the spawned network behaviour event loop
        let shared: Arc<NetworkShared> = Arc::new(NetworkShared::new());
        let shared_clone = Arc::clone(&shared);

        tokio::spawn(async move {
            let shared = shared_clone;
            let mut requests = HashMap::new();
            loop {
                select! {
                    Some((peer_id, event, tx)) = netout_rx.recv() => {
                        let request_id = swarm.behaviour_mut().rr.send_request(&peer_id, protocol::Request { event });
                        requests.insert(request_id, tx);
                    }
                    event = swarm.select_next_some() => match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!(addr = ?address, "Listening on");
                        }
                        SwarmEvent::Dialing(peer_id) => {
                            info!(peer_id = ?peer_id, "Dialing peer");
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, established_in, .. } => {
                            info!(peer_id = ?peer_id, established_in = ?established_in, "Connection established");
                            shared.add_peer(peer_id);
                        }
                        SwarmEvent::ConnectionClosed { peer_id, endpoint, num_established, cause } => {
                            info!(peer_id = ?peer_id, num_established = num_established, endpoint = ?endpoint, cause = ?cause, "Connection closed");
                            shared.remove_peer(&peer_id);
                        }
                        SwarmEvent::IncomingConnection { local_addr, send_back_addr } => {
                            info!(local_addr = ?local_addr, send_back_addr = ?send_back_addr, "Incoming connection");
                        }
                        SwarmEvent::IncomingConnectionError { local_addr, send_back_addr, error } => {
                            error!(local_addr = ?local_addr, send_back_addr = ?send_back_addr, error = ?error, "Incoming connection error");
                        }
                        SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                            error!(peer_id = ?peer_id, error = ?error, "Outgoing connection error");
                        }
                        SwarmEvent::ListenerClosed { listener_id, addresses, reason } => {
                            error!(listener_id = ?listener_id, addresses = ?addresses, reason = ?reason, "Listener closed");
                        }
                        SwarmEvent::ListenerError { listener_id, error } => {
                            error!(listener_id = ?listener_id, error = ?error, "Listener error");
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Rr(request_response::Event::Message { peer, message })) => {
                            match message {
                                request_response::Message::Response{ request_id, .. } => {
                                    // Notify sender that request/response process is complete
                                    if let Some(tx) = requests.remove(&request_id) {
                                        tx.send(()).ok();
                                    }
                                },
                                request_response::Message::Request{ request, channel, .. } => {
                                        match netin_tx.send((peer.into(), request.event)) {
                                            Ok(_) => {},
                                            Err(_) => {
                                                error!(peer_id = ?peer, "Failed to send, dropping event");
                                            }
                                        }
                                        match swarm.behaviour_mut().rr.send_response(channel, protocol::Response) {
                                            Ok(_) => {},
                                            Err(err) => {
                                                error!(peer_id = ?peer,  "Failed to send response: {:?}", err);
                                            }
                                        }
                                }
                           }
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Rr(request_response::Event::ResponseSent { .. })) => {}
                        event => {
                            debug!(event = ?event, "Swarm event");
                        }
                    }
                }
            }
        });

        Ok(Network {
            netin_rx: AsyncMutex::new(netin_rx),
            netout_tx,
            local_peer_id,
            shared,
        })
    }

    // pub fn dial(&self, addr: Multiaddr) -> Result<()> {
    //     Ok(self.swarm.lock().dial(addr)?)
    // }

    pub async fn send(
        &self,
        peer: &NetworkPeerId,
        event: NetworkEvent,
    ) -> Option<oneshot::Receiver<()>> {
        self._send(&peer.0, event).await
    }

    pub async fn send_all(&self, event: NetworkEvent) {
        let peers = self.shared.state.lock().connected_peers.clone();
        let mut futures = vec![];

        for peer in peers.iter() {
            futures.push(self._send(peer, event.clone()));
        }

        futures::future::join_all(futures).await;
    }

    async fn _send(&self, peer: &PeerId, event: NetworkEvent) -> Option<oneshot::Receiver<()>> {
        // Don't send messages to self
        if self.local_peer_id == *peer {
            return None;
        }

        // if !self.shared.state.lock().connected_peers.contains(peer) {
        //     debug!(self.logger, "Attempt to send to disconnected peer"; "peer_id" => format!("{:?}", peer));
        //     return None;
        // }

        let (tx, rx) = oneshot::channel();

        match self.netout_tx.send((*peer, event, tx)) {
            Ok(_) => {}
            Err(_) => {
                error!(peer_id = ?peer, "Failed to send, dropping event");
            }
        }

        Some(rx)
    }

    pub async fn next(&self) -> Option<(NetworkPeerId, NetworkEvent)> {
        self.netin_rx.lock().await.recv().await
    }
}

struct NetworkShared {
    state: Mutex<NetworkSharedState>,
}

impl NetworkShared {
    fn new() -> NetworkShared {
        NetworkShared {
            state: Mutex::new(NetworkSharedState {
                connected_peers: HashSet::new(),
            }),
        }
    }

    fn add_peer(&self, peer_id: PeerId) {
        self.state.lock().connected_peers.insert(peer_id);
    }

    fn remove_peer(&self, peer_id: &PeerId) {
        self.state.lock().connected_peers.remove(peer_id);
    }
}

struct NetworkSharedState {
    connected_peers: HashSet<PeerId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkPeerId(pub PeerId);

impl From<solid::peer::PeerId> for NetworkPeerId {
    fn from(peer_id: solid::peer::PeerId) -> Self {
        #[allow(clippy::unwrap_used)]
        NetworkPeerId(PeerId::from_bytes(&peer_id.to_bytes()[..]).unwrap())
    }
}

impl From<PeerId> for NetworkPeerId {
    fn from(peer_id: PeerId) -> Self {
        NetworkPeerId(peer_id)
    }
}

impl From<NetworkPeerId> for solid::peer::PeerId {
    fn from(peer_id: NetworkPeerId) -> Self {
        solid::peer::PeerId::new(peer_id.0.to_bytes())
    }
}

// pub fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
//     let components: Vec<_> = addr.iter().collect();
//     if let Some(libp2p::multiaddr::Protocol::P2p(hash)) = components.last() {
//         let peer_id = PeerId::from_multihash(*hash).ok();
//         return peer_id;
//     }
//     None
// }
