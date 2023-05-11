use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt::Display;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
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

    pub fn prefix(&self) -> String {
        self.to_string()[..4].to_string()
    }
}

impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl Borrow<[u8]> for PeerId {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}
