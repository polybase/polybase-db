use crate::errors::Result;
use libp2p::identity;
use rand::RngCore;
use std::path::PathBuf;

pub(crate) fn get_key_path(dir: &str) -> Option<PathBuf> {
    let mut path_buf = get_base_dir(dir)?;
    path_buf.push("config/secret_key");
    Some(path_buf)
}

pub(crate) fn get_indexer_dir(dir: &str) -> Option<PathBuf> {
    let mut path_buf = get_base_dir(dir)?;
    path_buf.push("data/indexer.db");
    Some(path_buf)
}

pub(crate) fn get_base_dir(dir: &str) -> Option<PathBuf> {
    let mut path_buf = PathBuf::new();
    if dir.starts_with("~/") {
        if let Some(home_dir) = dirs::home_dir() {
            path_buf.push(home_dir);
            path_buf.push(dir.strip_prefix("~/")?);
        }
    } else {
        path_buf.push(dir);
    }
    Some(path_buf)
}

pub(crate) fn to_peer_id(base58_string: &String) -> Result<solid::peer::PeerId> {
    let decoded = bs58::decode(base58_string).into_vec()?;
    Ok(solid::peer::PeerId::new(decoded))
}

pub(crate) fn generate_key() -> (identity::Keypair, [u8; 32]) {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    #[allow(clippy::unwrap_used)]
    let keypair = identity::Keypair::ed25519_from_bytes(bytes).unwrap();
    (keypair, bytes)
}
