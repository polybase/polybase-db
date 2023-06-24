#![warn(clippy::unwrap_used, clippy::expect_used)]

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

use crate::config::{Command, Config, LogFormat, LogLevel};
use crate::db::{Db, DbConfig};
use crate::errors::AppError;
use crate::rpc::create_rpc_server;
use clap::Parser;
use ed25519_dalek::{self as ed25519};
use futures::StreamExt;
use libp2p::PeerId;
use libp2p::{identity, Multiaddr};
use network::{events::NetworkEvent, Network, NetworkPeerId};
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

use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;

type Result<T> = std::result::Result<T, AppError>;

/// Set up tracing support for Polybase for:
///   - logging
///   - createing stack driver traces, and
///   - for profiling
async fn setup_tracing(log_level: &LogLevel, log_format: &LogFormat) -> Result<()> {
    // common filter - show only `warn` and above for external crates.
    let mut filter = tracing_subscriber::EnvFilter::try_new("warn")?;

    for proj_crate in ["polybase", "indexer", "gateway", "solid"] {
        filter = filter.add_directive(format!("{proj_crate}={}", log_level).parse()?);
    }

    // TODO - see if the different tracing layers can be resolved into a common type.
    // Format<Pretty> is not compatible with Format<Json> (for instance).
    match log_format {
        LogFormat::Pretty => {
            let stdout_trace_layer = tracing_subscriber::fmt::layer();

            let subscriber = tracing_subscriber::registry()
                .with(stdout_trace_layer)
                .with(filter);

            tracing::subscriber::set_global_default(subscriber)?;
        }

        LogFormat::Json => {
            let stdout_trace_layer = tracing_subscriber::fmt::layer().json();

            let subscriber = tracing_subscriber::registry()
                .with(stdout_trace_layer)
                .with(filter);

            tracing::subscriber::set_global_default(subscriber)?;
        }

        LogFormat::StackDriver => {
            // This outputs to stdout for now, but integration with Google Cloud's logging suite will need to be done.
            // Also potentially use OpenTelemetry instead with an exporter for StackDriver traces.
            let stackdriver_layer = tracing_stackdriver::layer();

            let subscriber = tracing_subscriber::registry()
                .with(stackdriver_layer)
                .with(filter);

            tracing::subscriber::set_global_default(subscriber)?;
        }
    }

    Ok(())
}

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

    // setup tracing for the whole project
    setup_tracing(&config.log_level, &config.log_format).await?;

    // Database combines various components into a single interface
    // that is thread safe
    #[allow(clippy::unwrap_used)]
    let db: Arc<Db> = Arc::new(Db::new(config.root_dir.clone(), DbConfig::default()).unwrap());

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
                warn!(
                    path = key_path.to_str(),
                    "Automatically generating keypair, keep this secret"
                );
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
    info!(peer_id = local_peer_id.to_string(), "Peer starting");

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
    )?;

    let solid_handle = solid.run();

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
                            info!("Ping received");
                        },
                        NetworkEvent::OutOfSync { height } => {
                            // Don't help other nodes if we're unhealthy ourselves
                            if !db.is_healthy() {
                                continue;
                            }

                            if height + config.block_cache_count > solid.height() {
                                // height + 1 as, peer already has block at height
                                if solid.min_proposal_height() > height + 1 {
                                    // We don't have all the proposals needed for this peer, send Snapshot offer to peer
                                    info!(peer_id = from_peer_id.prefix(), height = height, "Peer is out of sync, sending snapshot offer");
                                    network.send(
                                        &network_peer_id,
                                        NetworkEvent::SnapshotOffer {
                                            id: util::unix_now(),
                                        },
                                    ).await;
                                } else {
                                    info!(peer_id = from_peer_id.prefix(), height = height, "Peer is out of sync, sending proposals");
                                    for proposal in solid.proposals_from(height) {
                                        network.send(
                                            &network_peer_id,
                                            NetworkEvent::Proposal {
                                                manifest: proposal.clone(),
                                            },
                                        )
                                        .await;
                                    }
                                }

                            } else {
                                error!(peer_id = from_peer_id.prefix(), height = height, "Peer is out of sync, peer should request full snapshot");
                            }
                        }

                        // We've received a request for a snapshot from another peer, if we are healthy we should offer
                        // to provide them with a snapshot
                        NetworkEvent::SnapshotRequest{ id, height } => {
                            if db.is_healthy() {
                                info!(to = from_peer_id.prefix(), height = height, id = id, "Peer requested snapshot, sending offer");
                                network.send(
                                    &network_peer_id,
                                    NetworkEvent::SnapshotOffer {
                                        id,
                                    },
                                ).await;
                            } else {
                                info!(peer_id = from_peer_id.prefix(), height = height, "Peer requested snapshot, unable to provide snapshot due to unhealty state");
                            }
                        },

                        // We've been offered a snapshot from another peer, we should accept this offer if we
                        // don't already have an ongoing snapshot in progress
                        // TODO: we should check if we've received new proposals since the original out of sync request (i.e. another node
                        // has the proposals therefore we don't need to use a snapshot)
                        NetworkEvent::SnapshotOffer{ id } => {
                            if snapshot_from.is_some() {
                                info!(peer_id = from_peer_id.prefix(),  id = id, "Already have snapshot in progress, ignoring offer");
                                continue;
                            }

                            if db.is_healthy() {
                                info!(peer_id = from_peer_id.prefix(), id = id, "Peer offered snapshot, ignoring as already healthy");
                                continue;
                            }

                            info!(peer_id = from_peer_id.prefix(), id = id, "Peer offered snapshot, resetting db");

                            // Save who the snapshot is from
                            snapshot_from = Some((network_peer_id.clone(), id));

                            // Reset the database
                            #[allow(clippy::expect_used)]
                            db.reset().expect("Failed to reset database");

                            info!("Db reset ready for snapshot, sending accept");

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

                            info!(peer_id = from_peer_id.prefix(), id = id, "Peer accepted snapshot offer, sending chunks");

                            // Spawn a task, as we don't want to block the thread while we send network events,
                            // and this snapshot may take a while to complete
                            tokio::spawn(async move {
                                // 100MB chunks
                                let snapshot_iter = db.snapshot_iter(config.snapshot_chunk_size);
                                for chunk in snapshot_iter {
                                    let peer_id = from_peer_id.clone();
                                    match chunk {
                                        Ok(chunk) => {
                                            tracing::event!(tracing::Level::DEBUG, "for" = peer_id.prefix(), chunk_size = chunk.len(), "Sending snapshot chunk");
                                            if let Some(tx) = network.send(
                                                &peer_id.into(),
                                                NetworkEvent::SnapshotChunk { id, chunk: Some(chunk) },
                                            ).await {
                                                // Wait for the send to complete
                                                tx.await.ok();
                                            }
                                        },
                                        Err(err) => {
                                            tracing::event!(tracing::Level::ERROR, "for" = peer_id.prefix(), err = ?err, "Error creating snapshot");
                                            return;
                                        }
                                    }
                                }

                                info!(peer_id = from_peer_id.prefix(), id = id, "Snapshot complete");

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
                            info!(peer_id = from_peer_id.prefix(), id = id, chunk_size = chunk.as_ref().map(|c| c.len()).unwrap_or(0),  "Received snapshot chunk");
                            if let Some((peer_id, snapshot_id)) = &snapshot_from {
                                if peer_id != &network_peer_id || snapshot_id != &id  {
                                    error!("Received invalid snapshot chunk");
                                    continue;
                                }
                            } else {
                                // We're not expecting a snapshot
                                error!("Received invalid snapshot chunk");
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

                                info!(height = height, "Restore db from snapshot complete");
                            }
                        }

                        NetworkEvent::Accept { accept } => {
                            info!(height = &accept.height, skips = &accept.skips, from = &from_peer_id.prefix(), leader = &accept.leader_id.prefix(), hash = accept.proposal_hash.to_string(), local_height = solid.height(), "Received accept");
                            solid.receive_accept(&accept, &from_peer_id);
                        }

                        NetworkEvent::Proposal { manifest } => {
                            info!(height = &manifest.height, skips = &manifest.skips, from = &from_peer_id.prefix(), leader = &manifest.leader_id.prefix(), hash = &manifest.hash().to_string(), "Received proposal");

                            // Lease the proposal changes
                            // #[allow(clippy::expect_used)]
                            // TODO: handle the error better
                            db.lease(&manifest).await.ok();

                            solid.receive_proposal(manifest);
                        }

                        NetworkEvent::Txn { txn } => {
                            info!(collection = &txn.collection_id, "Received txn");
                            match db.add_txn(txn).await {
                                Ok(_) => (),
                                Err(err) => {
                                    error!(err = ?err,  "Error adding txn");
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
                            info!(height = &accept.height, skips = &accept.skips, to = &accept.leader_id.prefix(), hash = accept.proposal_hash.to_string(), local_height = solid.height(), "Send accept");
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
                                    error!(err = ?err, "Error getting pending changes");
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

                            info!(leader_id = manifest.leader_id.prefix(), hash = proposal_hash.to_string(), height = height, "skips" = skips, "Propose");

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
                            info!(hash = manifest.hash().to_string(), height = manifest.height, skips = manifest.skips, "Commit");

                            last_commit = Instant::now();

                            // We should panic here, because there is really no way to recover from
                            // an error once a value is committed
                            if let Err(err) = db.commit(manifest).await {
                                error!(err = ?err, "Error committing proposal");
                                return;
                            }
                        }

                        SolidEvent::OutOfSync {
                            height,
                            max_seen_height,
                            accepts_sent,
                        } => {
                            info!(local_height = height, accepts_sent = accepts_sent, max_seen_height = max_seen_height, "Out of sync");

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
                            info!(local_height = local_height, proposal_height = proposal_height, proposal_hash = proposal_hash.to_string(), from = peer_id.prefix(), "Out of date proposal");
                        }

                        SolidEvent::DuplicateProposal { proposal_hash } => {
                            info!(hash = proposal_hash.to_string(),  "Duplicate proposal");
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
            error!("HTTP server exited unexpectedly {res:#?}");
            res?
        }
        res = solid_handle => {
            error!("Solid handle exited unexpectedly {res:#?}");
            res?
        },
        res = main_handle => {
            error!("Main handle exited unexpectedly {res:#?}");
            res?
        },
        _ = tokio::signal::ctrl_c() => {
            shutdown.store(true, Ordering::Relaxed);
        },
    );

    Ok(())
}
