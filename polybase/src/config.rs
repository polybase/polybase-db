use clap::{Parser, Subcommand, ValueEnum};

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
    #[arg(value_enum, long, env = "LOG_LEVEL", default_value = "DEBUG")]
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

    /// Sentry DSN
    #[arg(long, env = "SENTRY_DSN", default_value = "")]
    pub sentry_dsn: Option<String>,

    /// Public key whitelist
    #[arg(long, env = "WHITELIST", value_parser, value_delimiter = ',')]
    pub whitelist: Option<Vec<String>>,
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
