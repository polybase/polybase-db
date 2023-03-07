use clap::{Parser, ValueEnum};

/// Polybase is a p2p decentralized database
#[derive(Parser, Debug)]
#[command(name = "Polybase")]
#[command(author = "Polybase <hello@polybase.xyz>")]
#[command(author, version, about = "The p2p decentralized database", long_about = None)]
#[command(propagate_version = true)]
pub struct Config {
    /// ID of the node
    #[arg(long, env = "ID")]
    pub id: Option<u64>,

    /// Root directory where application data is stored
    #[arg(short, long, env = "ROOT_DIR", default_value = "~/.polybase")]
    pub root_dir: String,

    /// Log level
    #[arg(value_enum, long, env = "LOG_LEVEL", default_value = "INFO")]
    pub log_level: LogLevel,

    /// RPC listen address
    #[arg(long, env = "RPC_LADDR", default_value = "0.0.0.0:8080")]
    pub rpc_laddr: String,

    /// Peer listen address
    #[arg(long, env = "NETWORK_LADDR", default_value = "0.0.0.0:6000")]
    pub network_laddr: String,

    /// Peer listen address
    #[arg(long, env = "PEERS", default_value = "")]
    pub peers: String,

    /// RAFT listen address
    #[arg(long, env = "RAFT_LADDR", default_value = "0.0.0.0:5001")]
    pub raft_laddr: String,

    /// RAFT peer addresses
    #[arg(long, env = "RAFT_PEERS", default_value = "")]
    pub raft_peers: String,

    /// Sentry DSN
    #[arg(long, env = "SENTRY_DSN", default_value = "")]
    pub sentry_dsn: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogLevel {
    Debug,
    Info,
    Error,
}
