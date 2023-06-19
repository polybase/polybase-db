//! module for handling file-based (TOML) configuration for Polybase.

use crate::util;
use std::fs;

use super::{ConfigResult, Deserialize, LogFormat, LogLevel};

#[derive(Debug, Deserialize)]
pub(crate) struct TomlConfig {
    pub core: CoreConfig,
    #[allow(dead_code)]
    pub extra: ExtraConfig,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CoreConfig {
    pub id: Option<u64>,
    pub log_level: Option<LogLevel>,
    pub log_format: Option<LogFormat>,
    pub rpc_laddr: Option<String>,
    pub secret_key: Option<String>,
    pub network_laddr: Option<Vec<String>>,
    pub dial_addr: Option<Vec<String>>,
    pub peers: Option<Vec<String>>,
    pub block_cache_count: Option<usize>,
    pub block_txns_count: Option<usize>,
    pub snapshot_chunk_size: Option<usize>,
    pub min_block_duration: Option<u64>,
    pub sentry_dsn: Option<String>,
    pub whitelist: Option<Vec<String>>,
    pub restrict_namespaces: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ExtraConfig {}

/// Read the TOML configuration file, if present in the `config` sub-directory under the
/// root Polybase directory.
pub(super) fn read_config(root_dir: &str) -> ConfigResult<Option<TomlConfig>> {
    util::get_toml_config_file(root_dir, "config").map_or(Ok(None), |config_file| {
        if !config_file.exists() {
            return Ok(None);
        }

        Ok(Some(toml::from_str::<TomlConfig>(
            fs::read_to_string(config_file)?.as_str(),
        )?))
    })
}
