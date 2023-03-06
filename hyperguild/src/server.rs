use futures::{Future, TryStreamExt};
use peer::PeerId;
use std::{
    collections::{HashMap, HashSet},
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::{
    wrappers::{BroadcastStream, ReceiverStream},
    Stream, StreamExt,
};
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    guild::NetworkSender,
    peer,
    proposal::register::ProposalRegister,
    service::{EventResponse, RegisterStream, SnapshotRequest, SnapshotResponse},
};
use crate::{
    proposal,
    service::guild_service_server::{GuildService, GuildServiceServer},
};

type TonicResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<EventResponse, Status>> + Send>>;

#[derive(Debug)]
pub struct GuildServer {
    timeout: Duration,
    addr: SocketAddr,
    peers: Arc<Mutex<HashSet<PeerId>>>,
    peer_to_sender: Arc<RwLock<HashMap<PeerId, mpsc::Sender<EventResponse>>>>,
    peer_to_receiver: Arc<RwLock<HashMap<PeerId, mpsc::Receiver<EventResponse>>>>,
}

const BROADCAST_EVENTS_CAPACITY: usize = 128;

impl GuildServer {
    pub fn new<A: ToSocketAddrs>(timeout: Duration, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let peer_id = peer::PeerId::random();

        Self {
            timeout,
            addr,
            peers: Arc::new(Mutex::new(HashSet::new())),
            peer_to_sender: Arc::new(RwLock::new(HashMap::new())),
            peer_to_receiver: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn sender(&self) -> Sender {
        Sender {
            peer_to_sender: Arc::clone(&self.peer_to_sender),
        }
    }

    pub async fn run(self) {
        let addr = self.addr;
        let service = GuildServiceServer::new(self);

        let server = Server::builder().add_service(service).serve(addr);

        server.await.unwrap()
    }
}

#[tonic::async_trait]
impl GuildService for GuildServer {
    type EventStreamStream = ResponseStream;

    async fn event_stream(
        &self,
        req: Request<RegisterStream>,
    ) -> TonicResult<Self::EventStreamStream> {
        let peer_id = PeerId::new(req.into_inner().peer_id);

        {
            let mut peers = self.peers.lock().unwrap();
            if !peers.insert(peer_id.clone()) {
                return Err(Status::already_exists("Peer already listening to events"));
            }
        }

        let rx = match self.peer_to_receiver.write().unwrap().remove(&peer_id) {
            Some(rx) => rx,
            None => {
                let (tx, rx) = mpsc::channel(BROADCAST_EVENTS_CAPACITY);
                self.peer_to_sender.write().unwrap().insert(peer_id, tx);
                rx
            }
        };

        let rx = ReceiverStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(rx) as ResponseStream))
    }

    async fn snapshot(&self, request: Request<SnapshotRequest>) -> TonicResult<SnapshotResponse> {
        println!("Got a request: {:?}", request);

        let reply = SnapshotResponse { data: Vec::new() };

        Ok(Response::new(reply))
    }
}

pub struct Sender {
    peer_to_sender: Arc<RwLock<HashMap<PeerId, mpsc::Sender<EventResponse>>>>,
}

impl NetworkSender for Sender {
    fn send(&self, peer_id: PeerId, data: Vec<u8>) -> Box<dyn Future<Output = ()> + '_> {
        Box::new(async move {
            let peers = self.peer_to_sender.read().unwrap();
            let sender = peers.get(&peer_id).unwrap();
            sender.send(EventResponse { data }).await.unwrap();
        })
    }
}
