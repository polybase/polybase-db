use behaviour::{Behaviour, BehaviourEvent};
use events::NetworkEvent;
use futures_util::StreamExt;
use libp2p::{
    identity::Keypair,
    request_response,
    swarm::{keep_alive, Swarm, SwarmBuilder, SwarmEvent},
    Multiaddr, PeerId,
};
use parking_lot::Mutex;
use protocol::PolyProtocol;
use slog::{debug, info};
use std::sync::Arc;
use stream::SwarmStream;
use tokio::{select, sync::mpsc};
use transport::create_transport;

mod behaviour;
pub mod events;
mod protocol;
mod stream;
mod transport;

type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to dial peer: {0}")]
    DialPeer(#[from] libp2p::swarm::DialError),

    #[error("Tansport error: {0}")]
    Transport(#[from] libp2p::TransportError<std::io::Error>),
}

pub struct Network {
    swarm: Arc<Mutex<Swarm<Behaviour>>>,
    // peers: HashMap<PeerId, Multiaddr>,
    receiver: mpsc::UnboundedReceiver<(NetworkPeerId, NetworkEvent)>,
    // logger: slog::Logger,
    shared: Arc<NetworkShared>,
}

impl Network {
    pub fn new(
        keypair: &Keypair,
        listenaddrs: impl Iterator<Item = Multiaddr>,
        dialaddrs: impl Iterator<Item = Multiaddr>,
        logger: slog::Logger,
    ) -> Result<Network> {
        let local_peer_id = PeerId::from(keypair.public());
        let transport = create_transport(keypair);
        let protocols = vec![(PolyProtocol(), request_response::ProtocolSupport::Full)];
        let config = request_response::Config::default();
        let mut swarm = {
            let behaviour = Behaviour {
                rr: request_response::Behaviour::new(PolyProtocol(), protocols, config),
                keep_alive: keep_alive::Behaviour::default(),
            };
            SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build()
        };

        // Listen on given addresses
        for addr in listenaddrs {
            swarm.listen_on(addr)?;
        }

        // Connect to peers
        for addr in dialaddrs {
            swarm.dial(addr)?;
        }

        let swarm = Arc::new(Mutex::new(swarm));

        // SwarmStream helps us to create a mutable stream from a Arc'd Mutex'd Swarm
        let mut swarm_stream = SwarmStream::new(swarm.clone(), logger.clone());

        // Channel to receive NetworkEvents from the network
        let (sender, receiver) = mpsc::unbounded_channel::<(NetworkPeerId, NetworkEvent)>();
        let cloned_logger = logger.clone();

        // Shared state between the network and the spawned network behaviour event loop
        let shared: Arc<NetworkShared> = Arc::new(NetworkShared::new());
        let shared_clone = Arc::clone(&shared);

        tokio::spawn(async move {
            let shared = shared_clone;
            let logger = cloned_logger;
            loop {
                select! {
                    event = swarm_stream.select_next_some() => match event {
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            debug!(logger, "Connection established"; "peer_id" => format!("{:?}", peer_id));
                        }
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            debug!(logger, "Connection closed"; "peer_id" => format!("{:?}", peer_id));
                            shared.add_peer(peer_id);
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Rr(request_response::Event::Message { peer, message })) => {
                            match message {
                               request_response::Message::Response{ .. } => {},
                               request_response::Message::Request{ request, channel, .. } => {
                                    match sender.send((peer.into(), request.event)) {
                                        Ok(_) => {},
                                        Err(_) => {
                                            error!(logger, "Failed to send, dropping event"; "peer_id" => format!("{:?}", peer));
                                        }
                                    }
                                    swarm_stream.send_response(channel);
                               }
                           }
                        }
                        SwarmEvent::Behaviour(_) => {},
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!(logger, "Listening on"; "addr" => format!("{:?}", address));
                        }
                        event => {
                            debug!(logger, "Swarm event"; "event" => format!("{:?}", event));
                        }
                    }
                }
            }
        });

        Ok(Network {
            // peers: HashMap::new(),
            swarm,
            receiver,
            // logger,
            shared,
        })
    }

    // pub fn dial(&self, addr: Multiaddr) -> Result<()> {
    //     Ok(self.swarm.lock().dial(addr)?)
    // }

    pub async fn send(&self, peer: &NetworkPeerId, event: NetworkEvent) {
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

    async fn _send(&self, peer: &PeerId, event: NetworkEvent) {
        self.swarm
            .lock()
            .behaviour_mut()
            .rr
            .send_request(peer, protocol::Request { event });
    }

    pub async fn next(&mut self) -> Option<(NetworkPeerId, NetworkEvent)> {
        self.receiver.recv().await
    }
}

struct NetworkShared {
    state: Mutex<NetworkSharedState>,
}

impl NetworkShared {
    fn new() -> NetworkShared {
        NetworkShared {
            state: Mutex::new(NetworkSharedState {
                connected_peers: vec![],
            }),
        }
    }

    fn add_peer(&self, peer_id: PeerId) {
        let mut state = self.state.lock();
        state.connected_peers.push(peer_id);
    }
}

struct NetworkSharedState {
    connected_peers: Vec<PeerId>,
}

#[derive(Debug)]
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
