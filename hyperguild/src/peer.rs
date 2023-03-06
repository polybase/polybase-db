use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
// use libp2p_core::PeerId
// use multihash::{Code, Multihash};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PeerId(Vec<u8>);

impl PeerId {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn genesis() -> Self {
        Self(vec![0u8])
    }

    pub fn random() -> PeerId {
        let peer_id = rand::thread_rng().gen::<[u8; 32]>();
        PeerId(peer_id.to_vec())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }
}

impl Borrow<[u8]> for PeerId {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}
