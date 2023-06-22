use super::db;
use super::network;
use indexer::IndexerError;
use libp2p::{identity, multiaddr};

mod code;
pub mod http;
pub mod logger;
pub(crate) mod metrics;
pub mod reason;

pub type Result<T> = std::result::Result<T, AppError>;

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

    #[error("anonymous namespaces are not allowed, sign your request")]
    AnonNamespace,

    #[error("public key not included in allowed whitelist")]
    Whitelist,

    #[error(
        "namespace is invalid, must be in format pk/<public_key_hex>/<CollectionName> got {0}"
    )]
    InvalidNamespace(String),

    #[error("tracing parse error")]
    TracingParse(#[from] tracing_subscriber::filter::ParseError),

    #[error("error setting tracing global subscriber")]
    TracingSetGlobalDefault(#[from] tracing::subscriber::SetGlobalDefaultError),

    #[error("error extract the workspace members list")]
    CargoMetadata(#[from] cargo_metadata::Error),
}
