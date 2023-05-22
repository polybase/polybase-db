#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use bincode::{deserialize, serialize};
use futures::channel::mpsc;
use futures::future;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use slog::{Drain, Level};
use solid::config::SolidConfig;
use solid::peer::PeerId;
use solid::proposal::ProposalManifest;
use solid::{Snapshot, Solid};
use std::collections::HashMap;
use std::future::Future;
// use std::hash::Hash;
use std::mem;
use std::pin::Pin;

#[derive(Debug, Serialize, Deserialize)]
struct Store {
    data: HashMap<String, String>,
    proposal: Option<ProposalManifest>,
    pending: Vec<solid::txn::Txn>,
}

impl solid::Store for Store {
    fn propose(&mut self) -> Vec<solid::txn::Txn> {
        mem::take(&mut self.pending)
    }

    // fn txn(&mut self, txn: solid::txn::Txn) {
    //     self.pending.push(txn);
    // }

    fn commit(&mut self, manifest: ProposalManifest) -> Vec<u8> {
        self.proposal = Some(manifest);
        vec![]
    }

    fn restore(&mut self, snapshot: Snapshot) {
        let Snapshot { data, proposal } = snapshot;
        let data: HashMap<String, String> = deserialize(&data).unwrap();
        self.data = data;
        self.proposal = Some(proposal)
    }

    fn snapshot(&self) -> std::result::Result<Snapshot, Box<dyn std::error::Error>> {
        Ok(Snapshot {
            data: serialize(&self.data)?,
            proposal: self.proposal.as_ref().unwrap().clone(),
        })
    }
}

pub struct MyNetwork {
    event_stream: Box<dyn Stream<Item = (PeerId, Vec<u8>)> + Unpin + Send + Sync>,
}

impl MyNetwork {
    pub fn new() -> Self {
        let (_, receiver) = mpsc::channel(100);
        let event_stream =
            Box::new(receiver) as Box<dyn Stream<Item = (PeerId, Vec<u8>)> + Unpin + Send + Sync>;

        Self { event_stream }
    }
}

impl Default for MyNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl solid::network::NetworkSender for MyNetwork {
    fn send(&self, _: PeerId, _: Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>> {
        // Implement your sending logic here
        Box::pin(future::ready(()))
    }

    fn send_all(&self, _: Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>> {
        // Implement your sending logic here
        Box::pin(future::ready(()))
    }
}

impl solid::network::Network for MyNetwork {
    type EventStream = Box<dyn Stream<Item = (PeerId, Vec<u8>)> + Unpin + Sync + Send>;

    fn events(&mut self) -> &mut Self::EventStream {
        &mut self.event_stream
    }
}

#[tokio::main]
async fn main() {
    let local_peer_id = PeerId::random();
    let network = MyNetwork::new();
    let store = Store {
        data: HashMap::new(),
        proposal: None,
        pending: vec![],
    };

    // Logging
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = slog::LevelFilter::new(drain, Level::Info).fuse();
    let log = slog::Logger::root(drain, o!());

    let mut solid = Solid::new(
        local_peer_id.clone(),
        vec![local_peer_id],
        store,
        network,
        log.clone(),
        SolidConfig::default(),
    );

    // Start
    solid.run().await;
}
