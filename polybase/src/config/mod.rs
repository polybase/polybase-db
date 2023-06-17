//! Configuration for Polybase - using the CLI (clap), env (clap), and configuration file (toml).

mod clap_config;
mod toml_config;

use clap::{parser::ValueSource, ArgMatches, ValueEnum};
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("toml file read error")]
    TomlRead(#[from] std::io::Error),

    #[error("toml deserialization error")]
    TomlDeserialization(#[from] toml::de::Error),
}

pub type ConfigResult<T> = std::result::Result<T, ConfigError>;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub command: Option<PolybaseCommand>,

    /// ID of the node
    pub id: Option<u64>,

    /// Root directory where application data is stored
    pub root_dir: String,

    /// Log level
    pub log_level: LogLevel,

    /// Log format
    pub log_format: LogFormat,

    /// RPC listen address
    pub rpc_laddr: String,

    /// Secret key encoded as hex
    pub secret_key: Option<String>,

    /// Peer listen address
    pub network_laddr: Vec<String>,

    /// Peers to dial
    pub dial_addr: Vec<String>,

    /// Validator peers
    pub peers: Vec<String>,

    /// Maximum history of blocks to keep in memory
    pub block_cache_count: usize,

    /// Maximum number of txns to include in a block
    pub block_txns_count: usize,

    /// Size of the chunks of data sent during snapshot load
    pub snapshot_chunk_size: usize,

    /// Size of the chunks of data sent during snapshot load
    pub min_block_duration: u64,

    /// Sentry DSN
    pub sentry_dsn: Option<String>,

    /// Public key whitelist
    pub whitelist: Option<Vec<String>>,

    /// Restrict namespaces to pk/<pk>/<collection_name>
    pub restrict_namespaces: bool,
}

impl Config {
    pub fn new() -> ConfigResult<Self> {
        let clap_matches = clap_config::get_matches();

        let mut config: Config = clap_matches.clone().into();
        Self::merge_toml_core_config(&mut config, clap_matches)?;

        Ok(config)
    }

    fn was_supplied_by_user(key: &str, matches: &ArgMatches) -> bool {
        !matches!(matches.value_source(key), Some(ValueSource::DefaultValue))
    }

    /// The order of priority is (in decreasing order):
    /// cli -> env -> toml -> default
    ///
    /// As such, here we will check if a field with a default value Was
    /// supplied by the user. If so, do nothing. If not, if the TOML config
    /// has a value for the same field, use that instead.
    ///
    /// Secondly, if a value for an optional type has not been set, and the TOML config again has a
    /// value for it, then set it.
    fn merge_toml_core_config(&mut self, matches: ArgMatches) -> ConfigResult<()> {
        if let Some(mut toml_config) = toml_config::read_config()? {
            if self.id.is_none() && toml_config.core.id.is_some() {
                self.id = toml_config.core.id.take();
            }

            if !Self::was_supplied_by_user("root-dir", &matches) {
                if let Some(root_dir) = toml_config.core.root_dir {
                    self.root_dir = root_dir;
                }
            }

            if !Self::was_supplied_by_user("log-level", &matches) {
                if let Some(log_level) = toml_config.core.log_level {
                    self.log_level = log_level;
                }
            }

            if !Self::was_supplied_by_user("log-format", &matches) {
                if let Some(log_format) = toml_config.core.log_format {
                    self.log_format = log_format;
                }
            }

            if !Self::was_supplied_by_user("rpc-laddr", &matches) {
                if let Some(rpc_laddr) = toml_config.core.rpc_laddr {
                    self.rpc_laddr = rpc_laddr;
                }
            }

            if self.secret_key.is_none() && toml_config.core.secret_key.is_some() {
                self.secret_key = toml_config.core.secret_key.take();
            }

            if !Self::was_supplied_by_user("network-laddr", &matches) {
                if let Some(network_laddr) = toml_config.core.network_laddr {
                    self.network_laddr = network_laddr;
                }
            }

            if !Self::was_supplied_by_user("dial-addr", &matches) {
                if let Some(dial_addr) = toml_config.core.dial_addr {
                    self.dial_addr = dial_addr;
                }
            }

            if !Self::was_supplied_by_user("peers", &matches) {
                if let Some(peers) = toml_config.core.peers {
                    self.peers = peers;
                }
            }

            if !Self::was_supplied_by_user("block-cache-count", &matches) {
                if let Some(block_cache_count) = toml_config.core.block_cache_count {
                    self.block_cache_count = block_cache_count;
                }
            }

            if !Self::was_supplied_by_user("block-txns-count", &matches) {
                if let Some(block_txns_count) = toml_config.core.block_txns_count {
                    self.block_txns_count = block_txns_count;
                }
            }

            if !Self::was_supplied_by_user("snapshot-chunk-size", &matches) {
                if let Some(snapshot_chunk_size) = toml_config.core.snapshot_chunk_size {
                    self.snapshot_chunk_size = snapshot_chunk_size;
                }
            }

            if !Self::was_supplied_by_user("min-block-duration", &matches) {
                if let Some(min_block_duration) = toml_config.core.min_block_duration {
                    self.min_block_duration = min_block_duration;
                }
            }

            if !Self::was_supplied_by_user("sentry-dsn", &matches)
                && toml_config.core.sentry_dsn.is_some()
            {
                self.sentry_dsn = toml_config.core.sentry_dsn.take();
            }

            if self.whitelist.is_none() && toml_config.core.whitelist.is_some() {
                self.whitelist = toml_config.core.whitelist.take();
            }

            if !Self::was_supplied_by_user("restrict-namespaces", &matches) {
                if let Some(restrict_namespaces) = toml_config.core.restrict_namespaces {
                    self.restrict_namespaces = restrict_namespaces;
                }
            }
        }

        Ok(())
    }
}

// To convert from an ArgMatches into the main `Config` enity used by Polybase main.
// `clap` does not provide an automated way to do so in builder mode.
#[allow(clippy::unwrap_used)]
impl From<ArgMatches> for Config {
    fn from(am: ArgMatches) -> Self {
        Config {
            command: {
                match am.subcommand() {
                    Some(("start", _)) => Some(PolybaseCommand::Start),
                    Some(("generate_key", _)) => Some(PolybaseCommand::GenerateKey),
                    _ => None,
                }
            },

            id: am.get_one::<u64>("id").copied(),
            root_dir: am.get_one::<String>("root-dir").unwrap().clone(),
            log_level: *am.get_one::<LogLevel>("log-level").unwrap(),
            log_format: *am.get_one::<LogFormat>("log-format").unwrap(),
            rpc_laddr: am.get_one::<String>("rpc-laddr").unwrap().clone(),
            secret_key: am
                .get_one::<Option<String>>("secret-key")
                .unwrap_or(&None)
                .clone(),
            network_laddr: am
                .get_many::<String>("network-laddr")
                .unwrap()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            dial_addr: am
                .get_many::<String>("dial-addr")
                .unwrap()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            peers: am
                .get_many::<String>("peers")
                .unwrap()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            block_cache_count: *am.get_one::<usize>("block-cache-count").unwrap(),
            block_txns_count: *am.get_one::<usize>("block-txns-count").unwrap(),
            snapshot_chunk_size: *am.get_one::<usize>("snapshot-chunk-size").unwrap(),
            min_block_duration: *am.get_one::<u64>("min-block-duration").unwrap(),
            sentry_dsn: Some(am.get_one::<String>("sentry-dsn").unwrap().clone()),

            whitelist: am
                .get_many::<String>("whitelist")
                .map(|values| values.into_iter().map(String::from).collect::<Vec<_>>()),

            restrict_namespaces: *am.get_one::<bool>("restrict-namespaces").unwrap(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub enum PolybaseCommand {
    /// Start the server
    #[serde(rename = "start")]
    Start,
    /// Generate a new secret key
    #[serde(rename = "generate_key")]
    GenerateKey,
}

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, ValueEnum)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogLevel {
    #[serde(rename = "DEBUG")]
    Debug,
    #[serde(rename = "INFO")]
    Info,
    #[serde(rename = "ERROR")]
    Error,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug, Deserialize)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogFormat {
    #[serde(rename = "PRETTY")]
    Pretty,
    #[serde(rename = "JSON")]
    Json,
}
