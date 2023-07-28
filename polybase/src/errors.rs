use super::db;
use super::network;
use indexer_db_adaptor::indexer::IndexerError;
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

    #[error("namespace is invalid, must be in format pk/<public_key_hex>/<namespace> got {0}")]
    InvalidNamespace(String),

    #[error("namespace public key is invalid, expected {0} got {1}")]
    InvalidNamespacePublicKey(String, String),

    #[error("failed to compile Miden program")]
    MidenCompile(Box<dyn std::error::Error>),

    #[error("ABI is missing `this` type")]
    ABIIsMissingThisType,

    #[error("prover error")]
    ProveError(Box<dyn std::error::Error>),

    #[error("abi error")]
    ABIError(Box<dyn std::error::Error>),

    #[error("tracing parse error")]
    TracingParse(#[from] tracing_subscriber::filter::ParseError),

    #[error("error setting tracing global subscriber")]
    TracingSetGlobalDefault(#[from] tracing::subscriber::SetGlobalDefaultError),
}
