#![warn(clippy::unwrap_used, clippy::expect_used)]

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_json;
extern crate slog_term;

mod auth;
mod config;
mod db;
mod errors;
mod hash;
mod mempool;
mod network;
mod rollup;
mod rpc;
mod txn;

use crate::config::{Command, Config, LogFormat};
use crate::db::Db;
use crate::errors::AppError;
use crate::rpc::create_rpc_server;
use chrono::Utc;
use clap::Parser;
use ed25519_dalek::{self as ed25519};
use futures::StreamExt;
use indexer::{Indexer, IndexerError};
use libp2p::PeerId;
use libp2p::{identity, Multiaddr};
use network::{events::NetworkEvent, Network, NetworkPeerId};
use rand::RngCore;
use slog::Drain;
use solid::config::SolidConfig;
use solid::event::SolidEvent;
use solid::proposal::ProposalManifest;
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

type Result<T> = std::result::Result<T, AppError>;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    if let Some(Command::GenerateKey) = config.command {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        #[allow(clippy::unwrap_used)]
        let key = ed25519::SecretKey::from_bytes(&bytes).unwrap();
        let public: ed25519::PublicKey = (&key).into();
        #[allow(clippy::unwrap_used)]
        let keypair = identity::Keypair::ed25519_from_bytes(bytes).unwrap();
        let local_peer_id = PeerId::from(keypair.public());
        println!(" ");
        println!("  Public Key: 0x{}", hex::encode(public.to_bytes()));
        println!("  Secret Key: 0x{}", hex::encode(bytes));
        println!("  PeerId: {}", local_peer_id);
        println!(" ");
        return Ok(());
    }

    // Setup Sentry logging
    let _guard;
    if let Some(dsn) = config.sentry_dsn {
        _guard = sentry::init((
            dsn,
            sentry::ClientOptions {
                release: sentry::release_name!(),
                environment: Some(
                    std::env::var("ENV_NAME")
                        .unwrap_or("dev".to_string())
                        .into(),
                ),
                ..Default::default()
            },
        ));
    }

    // Parse log level
    let log_level = match &config.log_level {
        config::LogLevel::Debug => slog::Level::Debug,
        config::LogLevel::Info => slog::Level::Info,
        config::LogLevel::Error => slog::Level::Error,
    };

    // Create logger drain (json/pretty)
    let drain: Box<dyn Drain<Ok = (), Err = slog::Never> + Send + Sync> =
        if config.log_format == LogFormat::Json {
            // JSON output
            let json_drain = slog_json::Json::new(std::io::stdout())
                .add_key_value(o!(
                    // Add the required Cloud Logging fields
                    "severity" => slog::PushFnValue(move |record : &slog::Record, ser| {
                        ser.emit(record.level().as_str().to_uppercase())
                    }),
                    "timestamp" => slog::PushFnValue(move |_, ser| {
                        ser.emit(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
                    }),
                    "message" => slog::PushFnValue(move |record : &slog::Record, ser| {
                        ser.emit(record.msg())
                    }),
                ))
                .build()
                .fuse();
            Box::new(slog_async::Async::new(json_drain).build().fuse())
        } else {
            // Terminal output
            let decorator = slog_term::TermDecorator::new().build();
            let term_drain = slog_term::FullFormat::new(decorator).build().fuse();
            Box::new(slog_async::Async::new(term_drain).build().fuse())
        };

    // Create logger with log level filter
    let drain = slog::LevelFilter::new(drain, log_level).fuse();
    let logger = slog::Logger::root(
        slog_async::Async::new(drain).build().fuse(),
        slog_o!("version" => env!("CARGO_PKG_VERSION")),
    );

    // Indexer is responsible for indexing db data
    #[allow(clippy::unwrap_used)]
    let indexer_dir = get_indexer_dir(&config.root_dir).unwrap();
    let indexer = Arc::new(Indexer::new(logger.clone(), indexer_dir).map_err(IndexerError::from)?);

    // Database combines various components into a single interface
    // that is thread safe
    let db: Arc<Db> = Arc::new(Db::new(Arc::clone(&indexer), logger.clone()));

    // Get the keypair (provided or auto-generated)
    let keypair = match config.secret_key {
        Some(key) => {
            let key = match key.strip_prefix("0x") {
                Some(key) => key,
                None => &key,
            };
            let key_bytes = hex::decode(key)?;
            identity::Keypair::ed25519_from_bytes(key_bytes)?
        }
        None => identity::Keypair::generate_ed25519(),
    };
    let local_peer_id = PeerId::from(keypair.public());

    // Log the peer id
    info!(
        logger,
        "Peer starting";
        "peer_id" => local_peer_id.to_string()
    );

    // Interface for sending messages to peers, runs in its own thread
    // and can be polled for events
    let network_laddr: Vec<Multiaddr> = config
        .network_laddr
        .iter()
        .filter(|p| !p.is_empty())
        .map(|p| Ok(p.to_owned().parse()?))
        .collect::<Result<Vec<_>>>()?;

    let mut solid_peers = network_laddr
        .iter()
        .filter_map(extract_peer_id)
        .collect::<Vec<solid::peer::PeerId>>();

    let peers_addr: Vec<Multiaddr> = config
        .peers
        .iter()
        .filter(|p| !p.is_empty())
        .map(|p| Ok(p.to_owned().parse()?))
        .collect::<Result<Vec<_>>>()?;

    let mut network = Network::new(
        &keypair,
        network_laddr.into_iter(),
        peers_addr.into_iter(),
        logger.clone(),
    )?;

    let local_peer_solid = solid::peer::PeerId(local_peer_id.to_bytes());
    solid_peers.push(local_peer_solid.clone());

    // TODO: load solid state from disk state
    let mut solid = match db.get_manifest().await? {
        Some(manifest) => {
            solid::Solid::with_last_confirmed(local_peer_solid, manifest, SolidConfig::default())
        }
        None => solid::Solid::genesis(local_peer_solid, solid_peers, SolidConfig::default()),
    };

    // Run the RPC server
    let server = create_rpc_server(
        config.rpc_laddr,
        Arc::clone(&indexer),
        Arc::clone(&db),
        Arc::new(config.whitelist.clone()),
        logger.clone(),
    )?;

    let solid_handle = solid.run();

    let logger_clone = logger.clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    let main_handle = tokio::spawn(async move {
        let logger = logger_clone;
        let shutdown = shutdown_clone;
        while !shutdown.load(Ordering::Relaxed) {
            tokio::select! {
                // Db only produces CallTxn events, that should be propogated
                // to other nodes
                Some(txn) = db.next() => {
                    network.send_all(NetworkEvent::Txn { txn }).await;
                },

                Some((network_peer_id, event)) = network.next() => {
                    match event {
                        NetworkEvent::OutOfSync { peer_id, height } => {
                            info!(logger, "Peer is out of sync"; "peer_id" => peer_id.prefix(), "height" => height);
                            if height + 1024 < solid.height() {
                                let snapshot = match db.snapshot() {
                                    Ok(snapshot) => snapshot,
                                    Err(err) => {
                                        error!(logger, "Error creating snapshot"; "for" => peer_id.prefix(), "err" => format!("{:?}", err));
                                        continue;
                                    }
                                };
                                network.send(&peer_id.into(), NetworkEvent::Snapshot { snapshot }).await;
                            } else {
                                for proposal in solid.confirmed_proposals_from(height) {
                                    network.send(
                                        &network_peer_id,
                                        NetworkEvent::Proposal {
                                            manifest: proposal.clone(),
                                        },
                                    )
                                    .await;
                                }
                            }
                        }

                        NetworkEvent::Snapshot { snapshot } => {
                            info!(logger, "Restoring db from snapshot");

                            // We should panic if we are unable to restore
                            #[allow(clippy::unwrap_used)]
                            db.restore(&snapshot).unwrap();

                            // TODO: reset solid state after db restore

                            // This will close the server, for now that's fine during
                            // snapshot reload (as we have auth-restarts)
                            return;
                        }

                        NetworkEvent::Accept { accept } => {
                            info!(logger, "Received accept"; "height" => &accept.height, "skips" => &accept.skips, "from" => &accept.leader_id.prefix(), "hash" => accept.proposal_hash.to_string());
                            solid.receive_accept(&accept, &network_peer_id.into());
                        }

                        NetworkEvent::Proposal { manifest } => {
                            info!(logger, "Received proposal"; "height" => &manifest.height, "skips" => &manifest.skips, "from" => &manifest.leader_id.prefix(), "hash" => &manifest.hash().to_string());
                            solid.receive_proposal(manifest);
                        }

                        NetworkEvent::Txn { txn } => {
                            info!(logger, "Received txn"; "collection" => &txn.collection_id);
                            match db.add_txn(txn).await {
                                Ok(_) => (),
                                Err(err) => {
                                    error!(logger, "Error adding txn"; "err" => format!("{:?}", err));
                                }
                            }
                        }
                    }
                },

                Some(event) = solid.next() => {
                    match event {
                        // Node should send accept for an active proposal
                        // to another peer
                        SolidEvent::Accept { accept } => {
                            info!(logger, "Send accept"; "height" => &accept.height, "skips" => &accept.skips, "to" => &accept.leader_id.prefix(), "hash" => accept.proposal_hash.to_string());
                            // let leader = &accept.leader_id;

                            network.send(
                                &NetworkPeerId::from(accept.leader_id.clone()),
                                NetworkEvent::Accept { accept },
                            )
                            .await;
                        }

                        // Node should create and send a new proposal
                        SolidEvent::Propose {
                            last_proposal_hash,
                            height,
                            skips,
                        } => {
                            // Get changes from the pending changes cache, if we have an error
                            // skip being the leader and just continue
                            let txns = match db.propose_txns(height) {
                                Ok(txns) => txns,
                                Err(err) => {
                                    error!(logger, "Error getting pending changes"; "err" => format!("{:?}", err));
                                    continue;
                                }
                            };

                            // Simulate delay
                            tokio::time::sleep(Duration::from_secs(1)).await;

                            // Create the proposl manfiest
                            let manifest = ProposalManifest {
                                last_proposal_hash,
                                skips,
                                height,
                                leader_id: NetworkPeerId(local_peer_id).into(),
                                txns,

                                // TODO: get peers from start
                                peers: vec![NetworkPeerId(local_peer_id).into()]
                            };
                            let proposal_hash = manifest.hash();

                            info!(logger, "Propose"; "leader_id" => manifest.leader_id.prefix(), "hash" => proposal_hash.to_string(), "height" => height, "skips" => skips);

                            // Add proposal to own register, this will trigger an accept
                            solid.receive_proposal(manifest.clone());

                            // // Send proposal to all other nodes
                            network.send_all(
                                NetworkEvent::Proposal { manifest: manifest.clone() }
                            )
                            .await;
                        }

                        // Commit a confirmed proposal changes
                        SolidEvent::Commit { manifest } => {
                            info!(logger, "Commit"; "hash" => manifest.hash().to_string(), "height" => manifest.height, "skips" => manifest.skips);

                            // We should panic here, because there is really no way to recover from
                            // an error once a value is committed
                            #[allow(clippy::expect_used)]
                            db.commit(manifest).await.expect("Error committing proposal");
                        }

                        SolidEvent::OutOfSync {
                            height,
                            max_seen_height,
                            accepts_sent,
                        } => {
                            info!(logger, "Out of sync"; "local_height" => height, "accepts_sent" => accepts_sent, "max_seen_height" => max_seen_height);
                            network.send_all(NetworkEvent::OutOfSync { peer_id: NetworkPeerId(local_peer_id).into(), height })
                            .await
                        }

                        SolidEvent::OutOfDate {
                            local_height,
                            proposal_height,
                            proposal_hash,
                            peer_id,
                        } => {
                            info!(logger, "Out of date proposal"; "local_height" => local_height, "proposal_height" => proposal_height, "proposal_hash" => proposal_hash.to_string(), "from" => peer_id.prefix());
                        }

                        SolidEvent::DuplicateProposal { proposal_hash } => {
                            info!(logger, "Duplicate proposal"; "hash" => proposal_hash.to_string());
                        }
                    }
                }
            }
        }
    });

    tokio::select!(
        res = server => { // TODO: check if err
            error!(logger, "HTTP server exited unexpectedly {res:#?}");
            res?
        }
        res = solid_handle => {
            error!(logger, "Db handle exited unexpectedly {res:#?}");
            res?
        },
        res = main_handle => {
            error!(logger, "Db handle exited unexpectedly {res:#?}");
            res?
        },
        _ = tokio::signal::ctrl_c() => {
            shutdown.store(true, Ordering::Relaxed);
        },
    );

    Ok(())
}

fn get_indexer_dir(dir: &str) -> Option<PathBuf> {
    let mut path_buf = PathBuf::new();
    if dir.starts_with("~/") {
        if let Some(home_dir) = dirs::home_dir() {
            path_buf.push(home_dir);
            path_buf.push(dir.strip_prefix("~/")?);
        }
    } else {
        path_buf.push(dir);
    }
    path_buf.push("data/indexer.db");
    Some(path_buf)
}

fn extract_peer_id(addr: &Multiaddr) -> Option<solid::peer::PeerId> {
    let components: Vec<_> = addr.iter().collect();
    if let Some(libp2p::multiaddr::Protocol::P2p(hash)) = components.last() {
        let peer_id = PeerId::from_multihash(*hash).ok();
        return Some(solid::peer::PeerId(peer_id.map(|p| p.to_bytes())?));
    }
    None
}
