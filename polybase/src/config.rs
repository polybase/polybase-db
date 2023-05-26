use clap::{Parser, ValueEnum};
// use std::str::FromStr;

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

    /// Log format
    #[arg(value_enum, long, env = "LOG_FORMAT", default_value = "PRETTY")]
    pub log_format: LogFormat,

    /// RPC listen address
    #[arg(long, env = "RPC_LADDR", default_value = "0.0.0.0:8080")]
    pub rpc_laddr: String,

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
        env = "PEERS",
        default_value = "",
        value_parser,
        value_delimiter = ','
    )]
    pub peers: Vec<String>,

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

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogFormat {
    Pretty,
    Json,
}
