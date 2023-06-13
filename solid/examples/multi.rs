#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use bincode::{deserialize, serialize};
use futures::channel::mpsc::{self, Sender, TrySendError};
use futures::stream::Stream;
use futures::StreamExt;
use parking_lot::deadlock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use slog::{Drain, Level};
use solid::config::SolidConfig;
use solid::event::SolidEvent;
use solid::peer::PeerId;
use solid::proposal::ProposalAccept;
use solid::proposal::ProposalManifest;
use solid::Solid;
use std::collections::HashMap;
// use std::mem;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize)]
struct Store {
    data: HashMap<String, String>,
    proposal: Option<ProposalManifest>,
    pending: Vec<solid::txn::Txn>,
}

impl Store {
    // fn propose(&mut self) -> Vec<solid::txn::Txn> {
    //     mem::take(&mut self.pending)
    // }

    // fn add_pending_txn(&mut self, txn: solid::txn::Txn) {
    //     self.pending.push(txn);
    // }

    fn commit(&mut self, manifest: ProposalManifest) -> Vec<u8> {
        self.proposal = Some(manifest);
        vec![]
    }

    // fn restore(&mut self, snapshot: Snapshot) {
    //     let Snapshot { data, proposal } = snapshot;
    //     let data: HashMap<String, String> = deserialize(&data).unwrap();
    //     self.data = data;
    //     self.proposal = Some(proposal)
    // }

    fn snapshot(&self) -> std::result::Result<Snapshot, Box<dyn std::error::Error>> {
        Ok(Snapshot {
            data: serialize(&self.data)?,
            proposal: self.proposal.as_ref().unwrap().clone(),
        })
    }
}

pub type SenderMap = HashMap<PeerId, Sender<(PeerId, Vec<u8>)>>;

#[derive(Clone)]
pub struct MyNetworkConfig {
    min_latency: u64,
    max_latency: u64,
    drop_probability: f64,
    partition_duration: u64,
    partition_frequency: u64,
}

pub struct MyNetwork {
    local_peer_id: PeerId,
    event_stream: Option<Box<dyn Stream<Item = (PeerId, Vec<u8>)> + Unpin + Sync + Send>>,
    senders: Arc<Mutex<SenderMap>>,
    logger: slog::Logger,
    partition: Arc<AtomicBool>,
    config: MyNetworkConfig,
}

impl MyNetwork {
    pub fn new(
        local_peer_id: PeerId,
        senders: Arc<Mutex<SenderMap>>,
        logger: slog::Logger,
        config: MyNetworkConfig,
    ) -> Self {
        // A small buffer, so we can simulate network partition
        let (sender, receiver) = mpsc::channel(20);

        // Insert self into senders!
        {
            let mut senders = senders.lock().unwrap();
            senders.insert(local_peer_id.clone(), sender);
        }

        let partition = Arc::new(AtomicBool::new(false));

        let drop_probability = config.drop_probability;
        let dropping_stream = DroppingStream::new(
            receiver,
            drop_probability,
            logger.clone(),
            partition.clone(),
        );

        let event_stream = Some(Box::new(dropping_stream)
            as Box<dyn Stream<Item = (PeerId, Vec<u8>)> + Unpin + Send + Sync>);

        let prefix = local_peer_id.prefix();
        let shared_partition = partition.clone();
        tokio::spawn(async move {
            loop {
                if rand::thread_rng().gen_range(0..=config.partition_frequency) == 1 {
                    println!(
                        "----- START: simulating {}s network partition for {} -----",
                        config.partition_duration / 1000,
                        prefix,
                    );
                    {
                        shared_partition.swap(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    sleep(std::time::Duration::from_millis(config.partition_duration)).await;
                    println!(
                        "----- END: simulating network partition for {prefix} -----",
                    );
                    {
                        shared_partition.swap(false, std::sync::atomic::Ordering::Relaxed);
                    }
                }

                // Only check for partition every 1 seconds
                sleep(std::time::Duration::from_millis(1000)).await;
            }
        });

        Self {
            local_peer_id,
            event_stream,
            senders,
            logger,
            config,
            partition,
        }
    }

    async fn send(&self, peer_id: &PeerId, event: &NetworkEvent) {
        let data = serialize(event).unwrap();

        let senders = self.senders.clone();
        let local_peer_id = self.local_peer_id.clone();
        let MyNetworkConfig {
            min_latency,
            max_latency,
            ..
        } = self.config;
        let sleep_duration =
            rand::thread_rng().gen_range(0..=max_latency - min_latency) + min_latency;

        // If we're simulating a network partition ignore requests
        {
            if self.partition.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
        }

        // Randomly sleep for a minute (simulate network partition)
        sleep(std::time::Duration::from_millis(sleep_duration)).await;

        let mut sender_opt = None;
        {
            let senders = senders.lock().unwrap();
            if let Some(sender) = senders.get(peer_id) {
                sender_opt = Some(sender.clone());
            }
        }
        if let Some(mut sender) = sender_opt {
            match sender.try_send((local_peer_id, data)) {
                Ok(_) => {
                    // println!("Sending from {}", self.local_peer_id.prefix());
                }
                Err(TrySendError { .. }) => {
                    info!(
                        self.logger,
                        "Failed to send message to {} from {}",
                        peer_id.prefix(),
                        self.local_peer_id.prefix()
                    )
                }
            }
        }
    }

    async fn send_all(&self, event: &NetworkEvent) {
        let senders = self.senders.lock().unwrap().clone();
        let local_peer_id = self.local_peer_id.clone();
        let tasks: Vec<_> = senders
            .keys()
            .filter(|peer_id| **peer_id != local_peer_id) // Exclude local_peer_id from broadcasting
            .map(|peer_id| self.send(peer_id, event))
            .collect();
        futures::future::join_all(tasks).await;
    }
}

impl Stream for MyNetwork {
    type Item = (PeerId, Vec<u8>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.event_stream.as_mut().unwrap();
        Pin::new(stream).poll_next(cx)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum NetworkEvent {
    OutOfSync { peer_id: PeerId, height: usize },
    Accept { accept: ProposalAccept },
    Proposal { manifest: ProposalManifest },
    Snapshot { snapshot: Snapshot },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub proposal: ProposalManifest,
    pub data: Vec<u8>,
}

#[tokio::main]
async fn main() {
    let num_of_nodes = 3;
    let senders = Arc::new(Mutex::new(HashMap::new()));
    let peers: Vec<PeerId> = (0..num_of_nodes).map(|_| PeerId::random()).collect();

    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = slog::LevelFilter::new(drain, Level::Debug).fuse();
    let root_log = slog::Logger::root(drain, o!());

    // Check for deadlocks
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(10));
        let deadlocks = deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        println!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            println!("Deadlock #{i}");
            for t in threads {
                println!("Thread Id {:#?}", t.thread_id());
                println!("{:#?}", t.backtrace());
            }
        }
    });

    let mut handles = Vec::new();

    for i in 0..num_of_nodes {
        let local_peer_id = peers[i].clone();
        let peers = peers.clone();

        let mut store = Store {
            data: HashMap::new(),
            proposal: None,
            pending: vec![],
        };

        info!(root_log, "Starting node {}", local_peer_id.prefix());

        let logger: slog::Logger =
            root_log.new(o!("local_peer_id" => format!("{}", local_peer_id.prefix())));

        let config = MyNetworkConfig {
            min_latency: 200,
            max_latency: 600,
            drop_probability: 0.1,
            partition_duration: 80_000,
            partition_frequency: 600,
        };

        let mut network = MyNetwork::new(
            local_peer_id.clone(),
            senders.clone(),
            logger.clone(),
            config,
        );

        let mut solid = Solid::genesis(
            local_peer_id.clone(),
            peers.clone(),
            SolidConfig {
                min_proposal_duration: Duration::from_secs(1),
                max_proposal_history: 20,
                skip_timeout: Duration::from_secs(5),
                out_of_sync_timeout: Duration::from_secs(60),
            },
        );

        handles.push(tokio::spawn(async move {
            solid.run();
            loop {
                tokio::select! {
                    Some((peer_id, data)) = network.next() => {
                        let event = deserialize::<NetworkEvent>(&data).unwrap();
                        match event {
                            NetworkEvent::OutOfSync { peer_id, height } => {
                                info!(logger, "Peer is out of sync"; "peer_id" => peer_id.prefix(), "height" => height);
                                if height + 1024 < solid.height() {
                                    let snapshot = match store.snapshot() {
                                        Ok(snapshot) => snapshot,
                                        Err(err) => {
                                            error!(logger, "Error creating snapshot"; "for" => peer_id.prefix(), "err" => format!("{err:?}"));
                                            return;
                                        }
                                    };
                                    network.send(&peer_id, &NetworkEvent::Snapshot { snapshot }).await;
                                } else {
                                    for proposal in solid.confirmed_proposals_from(height) {
                                        network.send(
                                            &peer_id,
                                            &NetworkEvent::Proposal {
                                                manifest: proposal.clone(),
                                            },
                                        )
                                        .await;
                                    }
                                }
                            }

                            NetworkEvent::Snapshot { .. } => {
                                info!(logger, "Received snapshot");
                                // solid.receive_snapshot(snapshot);
                            }

                            NetworkEvent::Accept { accept } => {
                                info!(logger, "Received accept"; "height" => &accept.height, "skips" => &accept.skips, "from" => &accept.leader_id.prefix(), "hash" => accept.proposal_hash.to_string());
                                solid.receive_accept(&accept, &peer_id);
                            }

                            NetworkEvent::Proposal { manifest } => {
                                info!(logger, "Received proposal"; "height" => &manifest.height, "skips" => &manifest.skips, "from" => &manifest.leader_id.prefix(), "hash" => &manifest.hash().to_string());
                                solid.receive_proposal(manifest);
                            }
                        }
                    },

                    Some(event) = solid.next() => {
                        match event {
                            // Node should send accept for an active proposal
                            // to another peer
                            SolidEvent::Accept { accept } => {
                                info!(logger, "Send accept"; "height" => &accept.height, "skips" => &accept.skips, "to" => &accept.leader_id.prefix(), "hash" => accept.proposal_hash.to_string());
                                let leader = &accept.leader_id.clone();

                                network.send(
                                    leader,
                                    &NetworkEvent::Accept { accept },
                                )
                                .await;
                            }

                            // Node should create and send a new proposal
                            SolidEvent::Propose {
                                last_proposal_hash,
                                height,
                                skips,
                            } => {
                                // Get changes from the pending changes cache
                                let txns = vec![];

                                // Simulate delay
                                tokio::time::sleep(Duration::from_secs(1)).await;

                                // Create the proposl manfiest
                                let manifest = ProposalManifest {
                                    last_proposal_hash,
                                    skips,
                                    height,
                                    leader_id: local_peer_id.clone(),
                                    txns,
                                    peers: peers.clone(),
                                };
                                let proposal_hash = manifest.hash();

                                info!(logger, "Propose"; "hash" => proposal_hash.to_string(), "height" => height, "skips" => skips);

                                // Add proposal to own register, this will trigger an accept
                                solid.receive_proposal(manifest.clone());

                                // // Send proposal to all other nodes
                                network.send_all(
                                    &NetworkEvent::Proposal { manifest: manifest.clone() }
                                )
                                .await;
                            }

                            // Commit a confirmed proposal changes
                            SolidEvent::Commit { manifest } => {
                                info!(logger, "Commit"; "hash" => manifest.hash().to_string(), "height" => manifest.height, "skips" => manifest.skips);
                                store.commit(manifest);
                            }

                            SolidEvent::OutOfSync {
                                height,
                                max_seen_height,
                                accepts_sent,
                            } => {
                                info!(logger, "Out of sync"; "local_height" => height, "accepts_sent" => accepts_sent, "max_seen_height" => max_seen_height);
                                network.send_all(&NetworkEvent::OutOfSync { peer_id: local_peer_id.clone(), height })
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
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Util for simulating dropping of messages
pub struct DroppingStream<S> {
    inner: S,
    drop_probability: f64,
    partition: Arc<AtomicBool>,
    logger: slog::Logger,
}

impl<S> DroppingStream<S> {
    pub fn new(
        inner: S,
        drop_probability: f64,
        logger: slog::Logger,
        partition: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner,
            drop_probability,
            logger,
            partition,
        }
    }
}

impl<S: Stream + Unpin> Stream for DroppingStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let res = self.inner.poll_next_unpin(cx);
            match res {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(item)) => {
                    if rand::thread_rng().gen_range(0.0..=1.0) < self.drop_probability
                        || self.partition.load(std::sync::atomic::Ordering::Relaxed)
                    {
                        info!(self.logger, "Dropping message");
                        continue;
                    }
                    return Poll::Ready(Some(item));
                }
                Poll::Ready(None) => return Poll::Ready(None),
            }
        }
    }
}
