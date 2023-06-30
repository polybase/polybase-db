use clap::{Parser, Subcommand, ValueEnum};
use std::fmt;

/// Polybase is a p2p decentralized database
#[derive(Parser, Debug)]
#[command(name = "Polybase")]
#[command(author = "Polybase <hello@polybase.xyz>")]
#[command(author, version, about = "The p2p decentralized database", long_about = None)]
#[command(propagate_version = true)]
pub struct Config {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// ID of the node
    #[arg(long, env = "ID")]
    pub id: Option<u64>,

    /// Root directory where application data is stored
    #[arg(short, long, env = "ROOT_DIR", default_value = "~/.polybase")]
    pub root_dir: String,

    /// Log level
    #[arg(value_enum, long, env = "LOG_LEVEL", default_value = "INFO")]
    pub log_level: LogLevel,

    /// Log format
    #[arg(value_enum, long, env = "LOG_FORMAT", default_value = "PRETTY")]
    pub log_format: LogFormat,

    /// RPC listen address
    #[arg(long, env = "RPC_LADDR", default_value = "0.0.0.0:8080")]
    pub rpc_laddr: String,

    /// Secret key encoded as hex
    #[arg(long, env = "SECRET_KEY")]
    pub secret_key: Option<String>,

    /// Peer listen address
    #[arg(
        long,
        env = "NETWORK_LADDR",
        value_parser,
        value_delimiter = ',',
        default_value = "/ip4/0.0.0.0/tcp/0"
    )]
    pub network_laddr: Vec<String>,

    /// Peers to dial
    #[arg(
        long,
        env = "DIAL_ADDR",
        default_value = "",
        value_parser,
        value_delimiter = ','
    )]
    pub dial_addr: Vec<String>,

    /// Validator peers
    #[arg(
        long,
        env = "PEERS",
        default_value = "",
        value_parser,
        value_delimiter = ','
    )]
    pub peers: Vec<String>,

    // Maximum history of blocks to keep in memory
    #[arg(long, env = "BLOCK_CACHE_SIZE", default_value = "1024")]
    pub block_cache_count: usize,

    /// Maximum number of txns to include in a block
    #[arg(long, env = "BLOCK_TXN_COUNT", default_value = "1024")]
    pub block_txns_count: usize,

    /// Size of the chunks of data sent during snapshot load
    #[arg(long, env = "SNAPSHOT_CHUNK_SIZE", default_value = "4194304")]
    pub snapshot_chunk_size: usize,

    /// Size of the chunks of data sent during snapshot load
    #[arg(long, env = "MIN_BLOCK_DURATION", default_value = "500")]
    pub min_block_duration: u64,

    /// Sentry DSN
    #[arg(long, env = "SENTRY_DSN", default_value = "")]
    pub sentry_dsn: Option<String>,

    /// Public key whitelist
    #[arg(long, env = "WHITELIST", value_parser, value_delimiter = ',')]
    pub whitelist: Option<Vec<String>>,

    /// Restrict namespaces to pk/<pk>/<collection_name>
    #[arg(long, env = "RESTRICT_NAMESPACES", default_value = "false")]
    pub restrict_namespaces: bool,

    /// Restrict namespaces to pk/<pk>/<collection_name>
    #[arg(long, env = "MIGRATION_BATCH_SIZE", default_value = "1000")]
    pub migration_batch_size: usize,
}

#[derive(Subcommand, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
#[clap(rename_all = "SNAKE_CASE")]
pub enum Command {
    /// Start the server
    Start,
    /// Generate a new secret key
    GenerateKey,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                LogLevel::Trace => "trace",
                LogLevel::Debug => "debug",
                LogLevel::Info => "info",
                LogLevel::Warn => "warn",
                LogLevel::Error => "error",
            }
        )
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogFormat {
    Pretty,
    Json,
    StackDriver,
}
