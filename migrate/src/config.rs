use clap::{Parser, ValueEnum};

/// Polybase is a p2p decentralized database
#[derive(Parser, Debug)]
#[command(name = "Polybase")]
#[command(author = "Polybase <hello@polybase.xyz>")]
#[command(author, version, about = "The p2p decentralized database", long_about = None)]
#[command(propagate_version = true)]
pub struct Config {
    /// Root directory where application data is stored
    #[arg(short, long, env = "ROOT_DIR", default_value = "~/.polybase")]
    pub root_dir: String,

    /// Root directory where application data is stored
    #[arg(
        long,
        env = "MIGRATION_URL",
        default_value = "https://testnet.polybase.xyz"
    )]
    pub migration_url: String,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogLevel {
    Debug,
    Info,
    Error,
}
