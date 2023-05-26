use super::protocol::PolyProtocol;
use libp2p::{
    request_response,
    swarm::{keep_alive, NetworkBehaviour},
};

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub rr: request_response::Behaviour<PolyProtocol>,
    pub keep_alive: keep_alive::Behaviour,
}
