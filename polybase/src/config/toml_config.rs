//! module for handling file-based (TOML) configuration for Polybase.

const TOML_CONFIG_DIR: &'static str = "config";
const TOML_CONFIG_FILE_PATH: &'static str = "config.toml";

use std::{fs, path::PathBuf};

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
    pub root_dir: Option<String>,
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
pub(super) fn read_config() -> ConfigResult<Option<TomlConfig>> {
    let toml_config_file = [TOML_CONFIG_DIR, TOML_CONFIG_FILE_PATH]
        .iter()
        .collect::<PathBuf>();

    if toml_config_file.exists() {
        let toml_config: TomlConfig =
            toml::from_str::<TomlConfig>(fs::read_to_string(toml_config_file)?.as_str())?;

        Ok(Some(toml_config))
    } else {
        Ok(None)
    }
}
