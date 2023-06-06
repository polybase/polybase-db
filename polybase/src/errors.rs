use super::db;
use super::network;
use indexer::IndexerError;
use libp2p::{identity, multiaddr};

mod code;
pub mod http;
pub mod logger;
pub(crate) mod metrics;
pub mod reason;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("failed to initialize indexer")]
    Indexer(#[from] IndexerError),

    #[error("failed to join task")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("server failed unexpectedly")]
    HttpServer(#[from] actix_web::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("network error")]
    Network(#[from] network::Error),
    // #[error("")]
    #[error("multiaddr error")]
    Multiaddr(#[from] multiaddr::Error),

    #[error("decoding error")]
    Decoding(#[from] identity::DecodingError),

    #[error("decode hex error")]
    FromHex(#[from] hex::FromHexError),

    #[error("db error")]
    Db(#[from] db::Error),

    #[error("invalid request")]
    B58(#[from] bs58::decode::Error),

    #[error("public key not included in allowed whitelist")]
    Whitelist,
}
