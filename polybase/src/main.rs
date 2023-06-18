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
use std::time::{Duration, Instant};
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

    let network = Arc::new(Network::new(
        &keypair,
        network_laddr.into_iter(),
        peers_addr.into_iter(),
        logger.clone(),
    )?);

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
        Arc::new(config.restrict_namespaces),
        logger.clone(),
    )?;

    let solid_handle = solid.run();

    let logger_clone = logger.clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let network_clone = Arc::clone(&network);

    let main_handle = tokio::spawn(async move {
        let network = network_clone;

        let shutdown = shutdown_clone;
        let mut snapshot_from = None;
        let mut last_commit = Instant::now();

        while !shutdown.load(Ordering::Relaxed) {
            let network = Arc::clone(&network);
            let logger = logger_clone.clone();

            tokio::select! {
                // Db only produces CallTxn events, that should be propogated
                // to other nodes
                Some(txn) = db.next() => {
                    network.send_all(NetworkEvent::Txn { txn }).await;
                },

                Some((network_peer_id, event)) = network.next() => {
                    let from_peer_id: solid::peer::PeerId = network_peer_id.clone().into();
                    match event {
                        NetworkEvent::Ping => {
                            info!(logger, "Ping received");
                        },
                        NetworkEvent::OutOfSync { height } => {
                            // Don't help other nodes if we're unhealthy ourselves
                            if !db.is_healthy() {
                                continue;
                            }

                            if height + config.block_cache_count > solid.height() {
                                info!(logger, "Peer is out of sync, sending proposals"; "peer_id" => from_peer_id.prefix(), "height" => height);
                                if solid.min_proposal_height() > height {
                                    // We don't have all the proposals needed for this peer, send offer to peer
                                    network.send(
                                        &network_peer_id,
                                        NetworkEvent::SnapshotOffer {
                                            id: util::unix_now(),
                                        },
                                    ).await;
                                }

                                for proposal in solid.confirmed_proposals_from(height) {
                                    network.send(
                                        &network_peer_id,
                                        NetworkEvent::Proposal {
                                            manifest: proposal.clone(),
                                        },
                                    )
                                    .await;
                                }
                            } else {
                                error!(logger, "Peer is out of sync, peer should request full snapshot"; "peer_id" => from_peer_id.prefix(), "height" => height);
                            }
                        }

                        // We've received a request for a snapshot from another peer, if we are healthy we should offer
                        // to provide them with a snapshot
                        NetworkEvent::SnapshotRequest{ id, height } => {
                            if db.is_healthy() {
                                info!(logger, "Peer requested snapshot, sending offer"; "to" => from_peer_id.prefix(), "height" => height, "id" => id);
                                network.send(
                                    &network_peer_id,
                                    NetworkEvent::SnapshotOffer {
                                        id,
                                    },
                                ).await;
                            } else {
                                info!(logger, "Peer requested snapshot, unable to provide snapshot due to unhealty state"; "peer_id" => from_peer_id.prefix(), "height" => height);
                            }
                        },

                        // We've been offered a snapshot from another peer, we should accept this offer if we
                        // don't already have an ongoing snapshot in progress
                        NetworkEvent::SnapshotOffer{ id } => {
                            if snapshot_from.is_some() {
                                debug!(logger, "Already have snapshot in progress, ignoring offer"; "peer_id" => from_peer_id.prefix(),  "id" => id);
                                continue;
                            }

                            if db.is_healthy() {
                                debug!(logger, "Peer offered snapshot, ignoring as already healthy"; "peer_id" => from_peer_id.prefix(), "id" => id);
                                continue;
                            }

                            info!(logger, "Peer offered snapshot, sending accept"; "peer_id" => from_peer_id.prefix(), "id" => id);

                            // Save who the snapshot is from
                            snapshot_from = Some((network_peer_id.clone(), id));

                            // Reset the database
                            #[allow(clippy::expect_used)]
                            db.reset().expect("Failed to reset database");

                            info!(logger, "Db reset ready for snapshot");

                            network.send(
                                &network_peer_id,
                                NetworkEvent::SnapshotAccept {
                                    id,
                                },
                            ).await;
                        },

                        // A node has accepted our offer to provide a snapshot, therefore we should start sending them
                        // chunks of the snapshot
                        NetworkEvent::SnapshotAccept{ id  } => {
                            let db = Arc::clone(&db);

                            info!(logger, "Peer accepted snapshot offer, sending chunks"; "peer_id" => from_peer_id.prefix(), "id" => id);

                            // Spawn a task, as we don't want to block the thread while we send network events,
                            // and this snapshot may take a while to complete
                            tokio::spawn(async move {
                                // 100MB chunks
                                let snapshot_iter = db.snapshot_iter(config.snapshot_chunk_size);
                                for chunk in snapshot_iter {
                                    let peer_id = from_peer_id.clone();
                                    match chunk {
                                        Ok(chunk) => {
                                            debug!(logger, "Sending snapshot chunk"; "for" => peer_id.prefix(), "chunk_size" => chunk.len());
                                            if let Some(tx) = network.send(
                                                &peer_id.into(),
                                                NetworkEvent::SnapshotChunk { id, chunk: Some(chunk) },
                                            ).await {
                                                // Wait for the send to complete
                                                tx.await.ok();
                                            }
                                        },
                                        Err(err) => {
                                            error!(logger, "Error creating snapshot"; "for" => peer_id.prefix(), "err" => format!("{:?}", err));
                                            return;
                                        }
                                    }
                                }

                                info!(logger, "Snapshot complete"; "peer_id" => from_peer_id.prefix(), "id" => id);

                                // Send end of snapshot event
                                network.send(
                                    &from_peer_id.into(),
                                    NetworkEvent::SnapshotChunk { id, chunk: None },
                                ).await;
                            });
                        },

                        // We've received a chunk of a snapshot from another peer, we should load this into
                        // our db
                        NetworkEvent::SnapshotChunk { id, chunk } => {
                            info!(logger, "Received snapshot chunk"; "peer_id" => from_peer_id.prefix(), "id" => id, "chunk_size" => chunk.as_ref().map(|c| c.len()).unwrap_or(0));
                            if let Some((peer_id, snapshot_id)) = &snapshot_from {
                                if peer_id != &network_peer_id || snapshot_id != &id  {
                                    error!(logger, "Received invalid snapshot chunk");
                                    continue;
                                }
                            } else {
                                // We're not expecting a snapshot
                                error!(logger, "Received invalid snapshot chunk");
                                continue;
                            }

                            if let Some(chunk) = chunk {
                                // We should panic if we are unable to restore
                                #[allow(clippy::unwrap_used)]
                                db.restore_chunk(chunk).unwrap();
                            } else {
                                // We are finished, reset solid with the new proposal state from the snapshot
                                #[allow(clippy::unwrap_used)]
                                let manifest = db.get_manifest().await.unwrap().unwrap();
                                let height = manifest.height;
                                solid.reset(manifest);

                                // Reset snapshot from
                                snapshot_from = None;

                                info!(logger, "Restore db from snapshot complete"; "height" => height);
                            }
                        }

                        NetworkEvent::Accept { accept } => {
                            info!(logger, "Received accept"; "height" => &accept.height, "skips" => &accept.skips, "from" => &from_peer_id.prefix(), "leader" => &accept.leader_id.prefix(), "hash" => accept.proposal_hash.to_string(), "local_height" => solid.height());
                            solid.receive_accept(&accept, &from_peer_id);
                        }

                        NetworkEvent::Proposal { manifest } => {
                            info!(logger, "Received proposal"; "height" => &manifest.height, "skips" => &manifest.skips, "from" => &from_peer_id.prefix(), "leader" => &manifest.leader_id.prefix(), "hash" => &manifest.hash().to_string());

                            // Lease the proposal changes
                            // #[allow(clippy::expect_used)]
                            // TODO: handle the error better
                            db.lease(&manifest).await.ok();

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
                            // Wait minimum period
                            if last_commit + Duration::from_millis(config.min_block_duration) > Instant::now() {
                                let delay = last_commit + Duration::from_millis(config.min_block_duration) - Instant::now();
                                tokio::time::sleep(delay).await;
                            }

                            // Get changes from the pending changes cache, if we have an error
                            // skip being the leader and just continue
                            let txns = match db.propose_txns(height) {
                                Ok(txns) => txns,
                                Err(err) => {
                                    error!(logger, "Error getting pending changes"; "err" => format!("{:?}", err));
                                    continue;
                                }
                            };

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

                            last_commit = Instant::now();

                            // We should panic here, because there is really no way to recover from
                            // an error once a value is committed
                            if let Err(err) = db.commit(manifest).await {
                                error!(logger, "Error committing proposal"; "err" => format!("{:?}", err));
                                return;
                            }
                        }

                        SolidEvent::OutOfSync {
                            height,
                            max_seen_height,
                            accepts_sent,
                        } => {
                            info!(logger, "Out of sync"; "local_height" => height, "accepts_sent" => accepts_sent, "max_seen_height" => max_seen_height);

                            // Set as out of sync, so we mark the node as unhealthy immediately
                            db.out_of_sync(height);

                            if snapshot_from.is_some() {
                                // We are already restoring from a snapshot, so we don't need to request another
                                continue;
                            }

                            // Check how far behind we are, to determine if we request proposals or a full snapshot
                            if max_seen_height > solid.height() + config.block_cache_count {
                                network.send_all(NetworkEvent::SnapshotRequest { height, id: util::unix_now() }).await;
                            } else {
                                network.send_all(NetworkEvent::OutOfSync { height }).await;
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
