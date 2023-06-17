use clap::{crate_version, Arg, ArgAction, ArgMatches, Command};

use super::{LogFormat, LogLevel};

/// Low-level `clap` object which provides with `value_source` which
/// indicates whether an option was set by the user (cli/env) or by the
/// default value.
///
/// This also encapsulates the core configuation that is supported for the cli, env,
/// and TOML (file-based) configuration.
pub(super) fn get_matches() -> ArgMatches {
    Command::new("Polybase")
        .author("Polybase <hello@polybase.xyz>")
        .about("The p2p decentralized database")
        .version(crate_version!()) // pick the version from `Cargo.toml`
        .propagate_version(true)
        .subcommand(Command::new("start").about("Start the server"))
        .subcommand(Command::new("generate_key").about("Generate a new secret key"))
        .arg(
            Arg::new("id")
                .help("ID of the node")
                .long("id")
                .value_name("ID")
                .env("ID")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("root-dir")
                .help("Root directory where application data is stored")
                .short('r')
                .long("root-dir")
                .value_name("ROOT_DIR")
                .env("ROOT_DIR")
                .value_parser(clap::value_parser!(String))
                .default_value("~/.polybase"),
        )
        .arg(
            Arg::new("log-level")
                .help("Log level")
                .long("log-level")
                .value_name("LOG_LEVEL")
                .env("LOG_LEVEL")
                .value_parser(clap::builder::EnumValueParser::<LogLevel>::new())
                .default_value("INFO"),
        )
        .arg(
            Arg::new("log-format")
                .help("Log format")
                .long("log-format")
                .value_name("LOG_FORMAT")
                .env("LOG_FORMAT")
                .value_parser(clap::builder::EnumValueParser::<LogFormat>::new())
                .default_value("PRETTY"),
        )
        .arg(
            Arg::new("rpc-laddr")
                .help("RPC listen address")
                .long("rpc-laddr")
                .value_name("RPC_LADDR")
                .env("RPC_LADDR")
                .value_parser(clap::value_parser!(String))
                .default_value("0.0.0.0:8080"),
        )
        .arg(
            Arg::new("secret-key")
                .help("Secret key encoded as hex")
                .long("secret-key")
                .value_name("SECRET_KEY")
                .env("SECRET_KEY")
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("network-laddr")
                .help("Peer listen address")
                .long("network-laddr")
                .value_name("NETWORK_LADDR")
                .env("NETWORK_LADDR")
                .value_parser(clap::value_parser!(String))
                .value_delimiter(',')
                .default_value("/ip4/0.0.0.0/tcp/0"),
        )
        .arg(
            Arg::new("dial-addr")
                .help("Peers to dial")
                .long("dial-addr")
                .value_name("DIAL_ADDR")
                .env("DIAL_ADDR")
                .value_parser(clap::value_parser!(String))
                .value_delimiter(',')
                .default_value(""),
        )
        .arg(
            Arg::new("peers")
                .help("Validator peers")
                .long("peers")
                .value_name("PEERS")
                .env("PEERS")
                .value_parser(clap::value_parser!(String))
                .value_delimiter(',')
                .default_value(""),
        )
        .arg(
            Arg::new("block-cache-count")
                .help("Maximum history of blocks to keep in memory")
                .long("block-cache-count")
                .value_name("BLOCK_CACHE_COUNT")
                .env("BLOCK_CACHE_COUNT")
                .value_parser(clap::value_parser!(usize))
                .default_value("1024"),
        )
        .arg(
            Arg::new("block-txns-count")
                .help("Maximum number of txns to include in a block")
                .long("block-txns-count")
                .value_name("BLOCK_TXNS_COUNT")
                .env("BLOCK_TXNS_COUNT")
                .value_parser(clap::value_parser!(usize))
                .default_value("1024"),
        )
        .arg(
            Arg::new("snapshot-chunk-size")
                .help("Size of the chunks of data sent during snapshot load")
                .long("snapshot-chunk-size")
                .value_name("SNAPSHOT_CHUNK_SIZE")
                .env("SNAPSHOT_CHUNK_SIZE")
                .value_parser(clap::value_parser!(usize))
                .default_value("4194304"),
        )
        .arg(
            Arg::new("min-block-duration")
                .help("Size of the chunks of data sent during snapshot load")
                .long("min-block-duration")
                .value_name("MIN_BLOCK_DURATION")
                .env("MIN_BLOCK_DURATION")
                .value_parser(clap::value_parser!(u64))
                .default_value("500"),
        )
        .arg(
            Arg::new("sentry-dsn")
                .help("Sentry DSN")
                .long("sentry-dsn")
                .value_name("SENTRY_DSN")
                .env("SENTRY_DSN")
                .value_parser(clap::value_parser!(String))
                .default_value(""),
        )
        .arg(
            Arg::new("whitelist")
                .help("Public key whitelist")
                .long("whitelist")
                .value_name("WHITELIST")
                .env("WHITELIST")
                .value_parser(clap::value_parser!(String))
                .value_delimiter(','),
        )
        .arg(
            Arg::new("restrict-namespaces")
                .help("Restrict namespaces to pk/<pk>/<collection_name>")
                .long("restrict-namespaces")
                .env("RESTRICT_NAMESPACES")
                .action(ArgAction::SetTrue),
        )
        .get_matches()
}
