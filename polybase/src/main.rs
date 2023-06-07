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
mod util;

use crate::config::{Command, Config, LogFormat};
use crate::db::{Db, DbConfig};
use crate::errors::AppError;
use crate::rpc::create_rpc_server;
use chrono::Utc;
use clap::Parser;
use ed25519_dalek::{self as ed25519};
use futures::StreamExt;
use libp2p::PeerId;
use libp2p::{identity, Multiaddr};
use network::{events::NetworkEvent, Network, NetworkPeerId};
use slog::Drain;
use solid::config::SolidConfig;
use solid::event::SolidEvent;
use solid::proposal::ProposalManifest;
use std::time::Duration;
use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{Read, Write},
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
        let (keypair, bytes) = util::generate_key();
        #[allow(clippy::unwrap_used)]
        let key = ed25519::SecretKey::from_bytes(&bytes).unwrap();
        let public: ed25519::PublicKey = (&key).into();
        #[allow(clippy::unwrap_used)]
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

    // Database combines various components into a single interface
    // that is thread safe
    #[allow(clippy::unwrap_used)]
    let db: Arc<Db> =
        Arc::new(Db::new(config.root_dir.clone(), logger.clone(), DbConfig::default()).unwrap());

    // Get the keypair (provided or auto-generated)
    // TODO: store keypair if auto-generated
    let keypair = match config.secret_key {
        Some(key) => {
            let key = match key.strip_prefix("0x") {
                Some(key) => key,
                None => &key,
            };
            let key_bytes = hex::decode(key)?;
            identity::Keypair::ed25519_from_bytes(key_bytes)?
        }
        None => {
            #[allow(clippy::expect_used)]
            let key_path: std::path::PathBuf =
                util::get_key_path(&config.root_dir).expect("failed to get key path");
            if key_path.exists() {
                let mut file = File::open(key_path)?;
                let mut key = String::new();
                file.read_to_string(&mut key)?;
                let key = match key.trim().strip_prefix("0x") {
                    Some(key) => key,
                    None => &key,
                };
                let key_bytes = hex::decode(key)?;
                identity::Keypair::ed25519_from_bytes(key_bytes)?
            } else {
                warn!(logger, "Automatically generating keypair, keep this secret"; "path" => key_path.to_str());
                if let Some(dir) = key_path.parent() {
                    if !dir.exists() {
                        // Create the directory and all its parent directories if they do not exist
                        create_dir_all(dir)?;
                    }
                }
                let (keypair, bytes) = util::generate_key();
                let mut file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&key_path)?;
                file.write_all(hex::encode(bytes).as_bytes())?;
                keypair
            }
        }
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

    let peers_addr: Vec<Multiaddr> = config
        .dial_addr
        .iter()
        .filter(|p| !p.is_empty())
        .map(|p| Ok(p.to_owned().parse()?))
        .collect::<Result<Vec<_>>>()?;

    let mut solid_peers = config
        .peers
        .iter()
        .filter(|p| !p.is_empty())
        .map(util::to_peer_id)
        .collect::<Result<Vec<solid::peer::PeerId>>>()?;

    let mut network = Network::new(
        &keypair,
        network_laddr.into_iter(),
        peers_addr.into_iter(),
        logger.clone(),
    )?;

    let local_peer_solid = solid::peer::PeerId(local_peer_id.to_bytes());
    solid_peers.push(local_peer_solid.clone());
    solid_peers.sort_unstable();
    solid_peers.dedup();

    let mut solid = match db.get_manifest().await? {
        Some(manifest) => solid::Solid::with_last_confirmed(
            local_peer_solid,
            manifest,
            SolidConfig {
                max_proposal_history: config.block_cache_count,
                ..SolidConfig::default()
            },
        ),
        None => solid::Solid::genesis(
            local_peer_solid,
            solid_peers.clone(),
            SolidConfig {
                max_proposal_history: config.block_cache_count,
                ..SolidConfig::default()
            },
        ),
    };

    // Run the RPC server
    let server = create_rpc_server(
        config.rpc_laddr,
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
        let mut restore_height = solid.height();

        // For migration only, check if DB is empty
        if db.is_empty().await.unwrap_or(true) {
            network
                .send_all(NetworkEvent::SnapshotRequest {
                    peer_id: NetworkPeerId(local_peer_id).into(),
                    height: 0,
                })
                .await;
            db.out_of_sync(1);
        }

        while !shutdown.load(Ordering::Relaxed) {
            tokio::select! {
                // Db only produces CallTxn events, that should be propogated
                // to other nodes
                Some(txn) = db.next() => {
                    network.send_all(NetworkEvent::Txn { txn }).await;
                },

                Some((network_peer_id, event)) = network.next() => {
                    match event {
                        NetworkEvent::Ping => {
                            info!(logger, "Ping received");
                        },
                        NetworkEvent::OutOfSync { peer_id, height } => {
                            info!(logger, "Peer is out of sync"; "peer_id" => peer_id.prefix(), "height" => height);
                            if height + config.block_cache_count < solid.height() {
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

                        NetworkEvent::SnapshotRequest{ peer_id, ..  } => {
                            let snapshot = match db.snapshot() {
                                Ok(snapshot) => snapshot,
                                Err(err) => {
                                    error!(logger, "Error creating snapshot"; "for" => peer_id.prefix(), "err" => format!("{:?}", err));
                                    continue;
                                }
                            };
                            network.send(&peer_id.into(), NetworkEvent::Snapshot { snapshot }).await;
                        }

                        NetworkEvent::Snapshot { snapshot } => {
                            // Check if we have already advanced since our request
                            if solid.height() > restore_height  {
                                debug!(logger, "Skipping restore, already advanced"; "restore_height" => restore_height, "current_height" => solid.height());
                                continue;
                            }

                            info!(logger, "Restoring db from snapshot");

                            // We should panic if we are unable to restore
                            #[allow(clippy::unwrap_used)]
                            db.restore(&snapshot).unwrap();

                            // Reset solid with the new proposal state from the snapshot
                            #[allow(clippy::unwrap_used)]
                            let manifest = db.get_manifest().await.unwrap().unwrap();
                            solid.reset(manifest);

                            info!(logger, "Restore db from snapshot complete");
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
                            tokio::time::sleep(Duration::from_millis(300)).await;

                            // Create the proposl manfiest
                            let manifest = ProposalManifest {
                                last_proposal_hash,
                                skips,
                                height,
                                leader_id: NetworkPeerId(local_peer_id).into(),
                                txns,

                                // TODO: get peers from start
                                peers: solid_peers.clone(),
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
                            restore_height = height;
                            db.out_of_sync(height);
                            if solid.height() == 0 {
                                network.send_all(NetworkEvent::SnapshotRequest { peer_id: NetworkPeerId(local_peer_id).into(), height }).await;
                            } else {
                                network.send_all(NetworkEvent::OutOfSync { peer_id: NetworkPeerId(local_peer_id).into(), height }).await;
                            }
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

    // Check for deadlocks
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(10));
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        println!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            println!("Deadlock #{}", i);
            for t in threads {
                println!("Thread Id {:#?}", t.thread_id());
                println!("{:#?}", t.backtrace());
            }
        }
    });

    tokio::select!(
        res = server => { // TODO: check if err
            error!(logger, "HTTP server exited unexpectedly {res:#?}");
            res?
        }
        res = solid_handle => {
            error!(logger, "Solid handle exited unexpectedly {res:#?}");
            res?
        },
        res = main_handle => {
            error!(logger, "Main handle exited unexpectedly {res:#?}");
            res?
        },
        _ = tokio::signal::ctrl_c() => {
            shutdown.store(true, Ordering::Relaxed);
        },
    );

    Ok(())
}
