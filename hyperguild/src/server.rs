use futures::TryStreamExt;
use libp2p_core::PeerId;
use std::{
    collections::{HashMap, HashSet},
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
    time::Duration,
};
use tokio::sync::{
    broadcast::{self, Receiver},
    mpsc,
};
use tokio_stream::{
    wrappers::{BroadcastStream, ReceiverStream},
    Stream, StreamExt,
};
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    peer,
    service::{EventResponse, RegisterStream, SnapshotRequest, SnapshotResponse},
};
use crate::{
    proposal,
    service::guild_service_server::{GuildService, GuildServiceServer},
};

type TonicResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<EventResponse, Status>> + Send>>;

#[derive(Debug)]
struct GuildServer {
    timeout: Duration,
    addr: SocketAddr,
    peers: Arc<Mutex<HashSet<PeerId>>>,
    proposal_register: Arc<proposal::ProposalRegister>,
    event_sender: Arc<broadcast::Sender<EventResponse>>,
    peer_to_receiver: Arc<RwLock<HashMap<PeerId, Receiver<EventResponse>>>>,
}

const BROADCAST_EVENTS_CAPACITY: usize = 128;

impl GuildServer {
    pub fn new<A: ToSocketAddrs>(timeout: Duration, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let peer_id = peer::PeerId::random();
        let proposal_register = proposal::ProposalRegister::new(peer_id, vec![]);

        let (event_sender, _) = broadcast::channel(BROADCAST_EVENTS_CAPACITY);

        Self {
            timeout,
            addr,
            peers: Arc::new(Mutex::new(HashSet::new())),
            proposal_register: Arc::new(proposal_register),
            event_sender: Arc::new(event_sender),
            peer_to_receiver: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn broadcaster(
        proposal_register: Arc<proposal::ProposalRegister>,
        event_sender: Arc<broadcast::Sender<EventResponse>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            interval.tick().await;
            let proposal = match proposal_register.poll() {
                Poll::Ready(p) => p,
                Poll::Pending => continue,
            };

            let proposal_serialized = bincode::serialize(&proposal).unwrap();

            let event = EventResponse {
                data: proposal_serialized,
            };

            #[allow(clippy::single_match)]
            match event_sender.send(event) {
                Ok(_) => {}
                // Err means that there are no receivers, which is fine
                Err(_) => {}
            }
        }
    }

    pub async fn run(self) {
        let broadcaster = Self::broadcaster(
            Arc::clone(&self.proposal_register),
            Arc::clone(&self.event_sender),
        );

        let addr = self.addr;
        let service = GuildServiceServer::new(self);

        let server = Server::builder().add_service(service).serve(addr);

        tokio::select! {
            _ = server => {
                panic!("Server stopped");
            }
            _ = broadcaster => {
                panic!("Broadcaster stopped");
            }
        }
    }
}

#[tonic::async_trait]
impl GuildService for GuildServer {
    type EventStreamStream = ResponseStream;

    async fn event_stream(
        &self,
        req: Request<RegisterStream>,
    ) -> TonicResult<Self::EventStreamStream> {
        let peer_id = PeerId::from_bytes(&req.into_inner().peer_id).unwrap();

        {
            let mut peers = self.peers.lock().unwrap();
            if !peers.insert(peer_id) {
                return Err(Status::already_exists("Peer already listening to events"));
            }
        }

        let mut events = match self.peer_to_receiver.write().unwrap().remove(&peer_id) {
            // If events.len() was more than BROADCAST_EVENTS_CAPACITY, calling recv would return an error `Lagged`
            Some(events) if events.len() < BROADCAST_EVENTS_CAPACITY => events,
            _ => self.event_sender.subscribe(),
        };

        let (tx, rx) = mpsc::channel(128);
        let peer_to_receiver = Arc::clone(&self.peer_to_receiver);
        let peers = Arc::clone(&self.peers);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = events.recv() => {
                        match tx
                            .send(event.map_err(|e| {
                                Status::internal(format!("Broadcast Stream Recv Error: {:#?}", e))
                            }))
                            .await
                        {
                            Ok(_) => {
                                // item (server response) was queued to be send to client
                            }
                            Err(_item) => {
                                // TODO: how to handle this?
                                // The events stream doesn't have the event anymore,
                                // so we can't save the stream for reconnect
                                break;
                            }
                        }
                    }
                    _ = tx.closed() => {
                        // We store their receiver in case they reconnect
                        peer_to_receiver.write().unwrap().insert(peer_id, events);
                        break;
                    }
                };

                peers.lock().unwrap().remove(&peer_id);
                eprintln!("\tclient disconnected");
            }
        });

        let rx = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(rx) as ResponseStream))
    }

    async fn snapshot(&self, request: Request<SnapshotRequest>) -> TonicResult<SnapshotResponse> {
        println!("Got a request: {:?}", request);

        let reply = SnapshotResponse { data: Vec::new() };

        Ok(Response::new(reply))
    }
}
