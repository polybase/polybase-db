//! module for handling file-based (TOML) configuration for Polybase.

use crate::util;
use std::{fmt, fs};
use toml::{self, Value};

use super::{
    Config, ConfigError, ConfigResult, Deserialize, ExtraConfig, LogFormat, LogLevel,
    PolybaseCommand,
};

#[derive(thiserror::Error)]
pub enum TomlConfigError {
    #[error("toml file read error")]
    Read(#[from] std::io::Error),

    #[error("toml deserialization error")]
    Deserialization(#[from] toml::de::Error),

    #[error("invalid command '{0}': command must be one of 'start' or 'generate_key'")]
    InvalidCommand(String),

    #[error(
        "`command` must be a string with one of the following values: 'start', 'generate_key'"
    )]
    InvalidCommandType,

    #[error("`id' must be an unsigned 64-bit value")]
    InvalidIdType,

    #[error("invalid log_level: '{0}': log_level must be one of 'DEBUG', 'INFO', or 'ERROR")]
    InvalidLogLevel(String),

    #[error(
        "`log_level` must be a string with one of the following values: 'DEBUG', 'INFO', 'ERROR'"
    )]
    InvalidLogLevelType,

    #[error("invalid log_format: '{0}': log_format must be one of 'PRETTY' or 'JSON'")]
    InvalidLogFormat(String),

    #[error("`log_format` must be a string with one of the following values: 'PRETTY', 'JSON'")]
    InvalidLogFormatType,

    #[error("`rpc_laddr` must be a string")]
    InvalidRpcLaddrType,

    #[error("`secret_key` must be a hex string")]
    InvalidSecretKeyType,

    #[error("`network_laddr` must be a list of strings delimited by commas")]
    InvalidNetworkLaddrType,

    #[error("`dial_addr` must be a list of strings delimited by commas")]
    InvalidDialAddrType,

    #[error("`peers` must be a list of strings delimited by commas")]
    InvalidPeersType,

    #[error("`block_cache_count` must be an unsigned integer")]
    InvalidBlockCacheCountType,

    #[error("`block_txns_count` must be an unsigned integer")]
    InvalidBlockTxnsCountType,

    #[error("`snapshot_chunk_size` must be an unsigned integer")]
    InvalidSnapshotChunkSizeType,

    #[error("`min_block_duration` must be an unsigned 64-bit value")]
    InvalidMinBlockDurationType,

    #[error("`sentry_dsn` must be a string")]
    InvalidSentryDsnType,

    #[error("`whitelist` must be a list of strings delimited by commas")]
    InvalidWhiteListType,

    #[error("`restrict_namespaces` must be a boolean with value either 'false' or 'true")]
    InvalidRestrictNamespacesType,
}

impl fmt::Debug for TomlConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TomlConfigError::*;

        write!(
                f,
                "{}",
                match self {
                    Read(ref e) => format!("TOML file read error. {e:?}"),
                    Deserialization(ref e) => format!("TOML deserialization error: {e:?}"),
                    InvalidCommand(ref cmd) => format!(
                        "invalid command '{cmd}': command must be one of 'start' or 'generate_key'"
                    ),
                    InvalidCommandType => "`command` must be a string with one of the following values: 'start', 'generate_key'".into(),
                    InvalidIdType => "`id` must be an unsigned 64-bit value".into(),
                    InvalidLogLevel(ref log_level) => format!("invalid log_level: '{log_level}': log_level must be one of 'DEBUG', 'INFO', or 'ERROR"),
                    InvalidLogLevelType => "`log_level` must be a string with one of the following values: 'DEBUG', 'INFO', 'ERROR'".into(),
                    InvalidLogFormat(ref log_format) => format!("invalid log_format: '{log_format}': log_format must be one of 'PRETTY' or 'JSON'"),
                    InvalidLogFormatType => "`log_format` must be a string with one of the following values: 'PRETTY', 'JSON'".into(),
                    InvalidRpcLaddrType => "`rpc_laddr` must be a string".into(),
                    InvalidSecretKeyType => "`secret_key` must be a hex string".into(),
                    InvalidNetworkLaddrType => "`network_laddr` must be a list of strings delimited by commas".into(),
                    InvalidDialAddrType => "`dial_addr` must be a list of strings delimited by commas".into(),
                    InvalidPeersType => "`peers` must be a list of strings delimited by commas".into(),
                    InvalidBlockCacheCountType => "`block_cache_count` must be an unsigned integer".into(),
                    InvalidBlockTxnsCountType => "`block_txns_count` must be an unsigned integer".into(),
                    InvalidSnapshotChunkSizeType => "`snapshot_chunk_size` must be an unsigned integer".into(),
                    InvalidMinBlockDurationType => "`min_block_duration` must be an unsigned 64-bit value".into(),
                    InvalidSentryDsnType => "`sentry_dsn` must be a string".into(),
                    InvalidWhiteListType => "`whitelist` must be a list of strings delimited by commas".into(),
                    InvalidRestrictNamespacesType => "`restrict_namespaces` must be a boolean with value either 'false' or 'true".into(),
                }
            )
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct TomlConfig {
    pub core: Config,
    pub extra: Option<ExtraConfig>,
}

/// Read the TOML configuration file, if present in the `config` sub-directory under the
/// root Polybase directory.
pub(super) fn read_config(root_dir: &str) -> ConfigResult<Option<TomlConfig>> {
    util::get_toml_config_file(root_dir, "config").map_or(Ok(None), |config_file| {
        if !config_file.exists() {
            return Ok(None);
        }

        // read the core configuration into the`Config` struct.
        let toml_value = toml::from_str::<toml::Value>(
            fs::read_to_string(config_file)
                .map_err(TomlConfigError::from)?
                .as_str(),
        )
        .map_err(TomlConfigError::from)?;

        let core = read_core_config(&toml_value)?;
        let extra = read_extra_config(&toml_value)?;

        Ok(Some(TomlConfig { core, extra }))
    })
}

/// Read the TOML configuration file and populate two fields:
///   - `core` for the core configurations common to the cli and env, and
///   - `extra` for extra configurations peculiar to the TOML config file.
fn read_core_config(toml_value: &Value) -> ConfigResult<Config> {
    // default and optional values - separate from the default values
    // read in by `clap`
    let mut command = None;
    let mut id = None;
    let root_dir = "~/.polybase".into();
    let mut log_level = LogLevel::Info;
    let mut log_format = LogFormat::Pretty;
    let mut rpc_laddr = "0.0.0.0:8080".into();
    let mut secret_key = None;
    let mut network_laddr = vec!["/ip4/0.0.0.0/tcp/0".into()];
    let mut dial_addr = vec!["".into()];
    let mut peers = vec!["".into()];
    let mut block_cache_count = 1024;
    let mut block_txns_count = 1024;
    let mut snapshot_chunk_size = 4194304;
    let mut min_block_duration = 500;
    let mut sentry_dsn = None;
    let mut whitelist = None;
    let mut restrict_namespaces = false;

    if let Some(core) = toml_value.get("core").and_then(|core| core.as_table()) {
        if let Some(toml_cmd) = core.get("command") {
            if let Value::String(toml_cmd) = toml_cmd {
                command = match toml_cmd.as_str() {
                    "start" => Some(PolybaseCommand::Start),
                    "generate_key" => Some(PolybaseCommand::GenerateKey),
                    _ => {
                        return Err(ConfigError::from(TomlConfigError::InvalidCommand(
                            toml_cmd.clone(),
                        )))
                    }
                }
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidCommandType));
            }
        }

        if let Some(toml_id) = core.get("id") {
            if let Value::Integer(toml_id) = toml_id {
                if *toml_id < 0 {
                    return Err(ConfigError::from(TomlConfigError::InvalidIdType));
                }
                id = Some(*toml_id as u64);
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidIdType));
            }
        }

        if let Some(toml_log_level) = core.get("log_level") {
            if let Value::String(toml_log_level) = toml_log_level {
                log_level = match toml_log_level.as_str() {
                    "DEBUG" => LogLevel::Debug,
                    "INFO" => LogLevel::Info,
                    "ERROR" => LogLevel::Error,
                    _ => {
                        return Err(ConfigError::from(TomlConfigError::InvalidLogLevel(
                            toml_log_level.clone(),
                        )))
                    }
                }
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidLogLevelType));
            }
        }

        if let Some(toml_log_format) = core.get("log_format") {
            if let Value::String(toml_log_format) = toml_log_format {
                log_format = match toml_log_format.as_str() {
                    "PRETTY" => LogFormat::Pretty,
                    "JSON" => LogFormat::Json,
                    _ => {
                        return Err(ConfigError::from(TomlConfigError::InvalidLogFormat(
                            toml_log_format.clone(),
                        )))
                    }
                }
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidLogFormatType));
            }
        }

        if let Some(toml_rpc_laddr) = core.get("rpc_laddr") {
            if let Value::String(toml_rpc_laddr) = toml_rpc_laddr {
                rpc_laddr = toml_rpc_laddr.clone();
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidRpcLaddrType));
            }
        }

        if let Some(toml_secret_key) = core.get("secret_key") {
            if let Value::String(toml_secret_key) = toml_secret_key {
                secret_key = Some(toml_secret_key.clone());
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidSecretKeyType));
            }
        }

        if let Some(toml_network_laddr) = core.get("network_laddr") {
            if let Value::Array(toml_network_laddr) = toml_network_laddr {
                if toml_network_laddr.is_empty()
                    || !toml_network_laddr.iter().all(|laddr| laddr.is_str())
                {
                    return Err(ConfigError::from(TomlConfigError::InvalidNetworkLaddrType));
                }

                network_laddr = toml_network_laddr
                    .iter()
                    .map(|laddr| laddr.to_string())
                    .collect::<Vec<_>>();
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidNetworkLaddrType));
            }
        }

        if let Some(toml_dial_addr) = core.get("dial_addr") {
            if let Value::Array(toml_dial_addr) = toml_dial_addr {
                if toml_dial_addr.is_empty() || !toml_dial_addr.iter().all(|addr| addr.is_str()) {
                    return Err(ConfigError::from(TomlConfigError::InvalidDialAddrType));
                }

                dial_addr = toml_dial_addr
                    .iter()
                    .map(|addr| addr.to_string())
                    .collect::<Vec<_>>();
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidDialAddrType));
            }
        }

        if let Some(toml_peers) = core.get("peers") {
            if let Value::Array(toml_peers) = toml_peers {
                if toml_peers.is_empty() || !toml_peers.iter().all(|peer| peer.is_str()) {
                    return Err(ConfigError::from(TomlConfigError::InvalidPeersType));
                }

                peers = toml_peers
                    .iter()
                    .map(|peer| peer.to_string())
                    .collect::<Vec<_>>();
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidPeersType));
            }
        }

        if let Some(toml_block_cache_count) = core.get("block_cache_count") {
            if let Value::Integer(toml_block_cache_count) = toml_block_cache_count {
                if *toml_block_cache_count < 0 {
                    return Err(ConfigError::from(
                        TomlConfigError::InvalidBlockCacheCountType,
                    ));
                }
                block_cache_count = *toml_block_cache_count as usize;
            } else {
                return Err(ConfigError::from(
                    TomlConfigError::InvalidBlockCacheCountType,
                ));
            }
        }

        if let Some(toml_block_txns_count) = core.get("block_txns_count") {
            if let Value::Integer(toml_block_txns_count) = toml_block_txns_count {
                if *toml_block_txns_count < 0 {
                    return Err(ConfigError::from(
                        TomlConfigError::InvalidBlockTxnsCountType,
                    ));
                }
                block_txns_count = *toml_block_txns_count as usize;
            } else {
                return Err(ConfigError::from(
                    TomlConfigError::InvalidBlockTxnsCountType,
                ));
            }
        }

        if let Some(toml_snapshot_chunk_size) = core.get("snapshot_chunk_size") {
            if let Value::Integer(toml_snapshot_chunk_size) = toml_snapshot_chunk_size {
                if *toml_snapshot_chunk_size < 0 {
                    return Err(ConfigError::from(
                        TomlConfigError::InvalidSnapshotChunkSizeType,
                    ));
                }
                snapshot_chunk_size = *toml_snapshot_chunk_size as usize;
            } else {
                return Err(ConfigError::from(
                    TomlConfigError::InvalidSnapshotChunkSizeType,
                ));
            }
        }

        if let Some(toml_min_block_duration) = core.get("min_block_duration") {
            if let Value::Integer(toml_min_block_duration) = toml_min_block_duration {
                if *toml_min_block_duration < 0 {
                    return Err(ConfigError::from(
                        TomlConfigError::InvalidMinBlockDurationType,
                    ));
                }

                min_block_duration = *toml_min_block_duration as u64;
            } else {
                return Err(ConfigError::from(
                    TomlConfigError::InvalidMinBlockDurationType,
                ));
            }
        }

        if let Some(toml_sentry_dsn) = core.get("sentry_dsn") {
            if let Value::String(toml_sentry_dsn) = toml_sentry_dsn {
                sentry_dsn = Some(toml_sentry_dsn.to_string());
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidSentryDsnType));
            }
        }

        if let Some(toml_whitelist) = core.get("whitelist") {
            if let Value::Array(toml_whitelist) = toml_whitelist {
                if toml_whitelist.is_empty() || !toml_whitelist.iter().all(|wl| wl.is_str()) {
                    return Err(ConfigError::from(TomlConfigError::InvalidWhiteListType));
                }

                whitelist = Some(
                    toml_whitelist
                        .iter()
                        .map(|wl| wl.to_string())
                        .collect::<Vec<_>>(),
                );
            } else {
                return Err(ConfigError::from(TomlConfigError::InvalidWhiteListType));
            }
        }

        if let Some(toml_restrict_namespaces) = core.get("restrict_namespaces") {
            if let Value::Boolean(toml_restrict_namespaces) = toml_restrict_namespaces {
                restrict_namespaces = *toml_restrict_namespaces;
            } else {
                return Err(ConfigError::from(
                    TomlConfigError::InvalidRestrictNamespacesType,
                ));
            }
        }
    }

    Ok(Config {
        command,
        id,
        root_dir,
        log_level,
        log_format,
        rpc_laddr,
        secret_key,
        network_laddr,
        dial_addr,
        peers,
        block_cache_count,
        block_txns_count,
        snapshot_chunk_size,
        min_block_duration,
        sentry_dsn,
        whitelist,
        restrict_namespaces,
        extra_config: None,
    })
}

fn read_extra_config(toml_value: &Value) -> ConfigResult<Option<ExtraConfig>> {
    let extra = toml_value.get("extra").and_then(|extra| extra.as_table());

    if let Some(_extra) = extra {
        Ok(Some(ExtraConfig {}))
    } else {
        Ok(None)
    }
}
