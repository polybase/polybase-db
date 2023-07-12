//! This module provides various utilities for use by the indexer:
//!   - hashing using Rescue-Prime (with proper truncation)
//!   - encoding using base64.

pub(crate) mod rpo {
    use base64::Engine;
    use miden_crypto::hash::rpo::Rpo256;

    pub fn hash_and_encode(s: &str) -> String {
        base64::engine::general_purpose::URL_SAFE
            .encode(compute_miden256_hash(s.as_bytes()))
            .replace('+', "_") // todo - do this or simply use a custom alphabet?
            .replace('-', "_")
            .replace('=', "_")
    }

    fn compute_miden256_hash(data: &[u8]) -> Vec<u8> {
        Rpo256::hash(data)
            .as_bytes()
            .into_iter()
            .take(20) // truncate to 20 bytes
            .collect::<Vec<_>>()
    }
}
