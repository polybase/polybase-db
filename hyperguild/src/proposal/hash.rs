use super::manifest::ProposalManifest;
use crate::key::Key;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Borrow;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub struct ProposalHash(Vec<u8>);

impl ProposalHash {
    pub fn new(v: Vec<u8>) -> Self {
        ProposalHash(v)
    }
}

impl Default for ProposalHash {
    fn default() -> Self {
        ProposalHash(Sha256::digest([0u8]).to_vec())
    }
}

impl From<String> for ProposalHash {
    fn from(str: String) -> Self {
        ProposalHash(Sha256::digest(str).to_vec())
    }
}

impl From<&str> for ProposalHash {
    fn from(str: &str) -> Self {
        ProposalHash(Sha256::digest(str).to_vec())
    }
}

impl Borrow<[u8]> for ProposalHash {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl From<&ProposalManifest> for ProposalHash {
    fn from(p: &ProposalManifest) -> Self {
        let bytes = Sha256::digest(bincode::serialize(p).unwrap());
        ProposalHash(bytes.to_vec())
    }
}

impl From<ProposalHash> for Key<ProposalHash> {
    fn from(p: ProposalHash) -> Self {
        Key::new(p)
    }
}
