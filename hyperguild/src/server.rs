use futures::{Future, StreamExt, TryStreamExt};
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
    Stream,
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
type ResponseStream = Pin<Box<dyn Stream<Item = Result<EventResponse, Status>> + Send + Sync>>;

pub struct GuildServer {
    timeout: Duration,
    addr: SocketAddr,
    peers: Arc<Mutex<HashSet<PeerId>>>,
    peer_to_sender: Arc<RwLock<HashMap<PeerId, mpsc::Sender<EventResponse>>>>,
    peer_to_stream: Arc<RwLock<HashMap<PeerId, ResponseStream>>>,
}

const EVENTS_CAPACITY: usize = 128;

impl GuildServer {
    pub fn new<A: ToSocketAddrs>(timeout: Duration, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let peer_id = peer::PeerId::random();

        Self {
            timeout,
            addr,
            peers: Arc::new(Mutex::new(HashSet::new())),
            peer_to_sender: Arc::new(RwLock::new(HashMap::new())),
            peer_to_stream: Arc::new(RwLock::new(HashMap::new())),
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

        let stream = match self.peer_to_stream.write().unwrap().remove(&peer_id) {
            Some(s) => s,
            None => {
                let (tx, rx) = mpsc::channel(EVENTS_CAPACITY);
                self.peer_to_sender
                    .write()
                    .unwrap()
                    .insert(peer_id.clone(), tx);

                Box::pin(ReceiverStream::new(rx).map(Ok))
            }
        };

        let stream = RecoverableResponseStream {
            peer_id,
            stream,
            peer_to_stream: Arc::clone(&self.peer_to_stream),
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn snapshot(&self, request: Request<SnapshotRequest>) -> TonicResult<SnapshotResponse> {
        println!("Got a request: {:?}", request);

        let reply = SnapshotResponse { data: Vec::new() };

        Ok(Response::new(reply))
    }
}

struct RecoverableResponseStream {
    peer_id: PeerId,
    stream: ResponseStream,
    peer_to_stream: Arc<RwLock<HashMap<PeerId, ResponseStream>>>,
}

impl Stream for RecoverableResponseStream {
    type Item = Result<EventResponse, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl Drop for RecoverableResponseStream {
    fn drop(&mut self) {
        let stream = core::mem::replace(&mut self.stream, Box::pin(futures::stream::empty()));

        self.peer_to_stream
            .write()
            .unwrap()
            .insert(self.peer_id.clone(), stream);
    }
}

pub struct Sender {
    peer_to_sender: Arc<RwLock<HashMap<PeerId, mpsc::Sender<EventResponse>>>>,
}

impl NetworkSender for Sender {
    fn send(&self, peer_id: PeerId, data: Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(async move {
            let out_of_capacity = {
                let peers = self.peer_to_sender.read().unwrap();
                let sender = peers.get(&peer_id).unwrap();

                match sender.send(EventResponse { data }).await {
                    Ok(_) => false,
                    Err(_) => {
                        // Channel is out of capacity, we cannot buffer any more events for this peer.
                        true
                    }
                }
            };

            if out_of_capacity {
                self.peer_to_sender.write().unwrap().remove(&peer_id);
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_recoverable_stream() {
        let (tx, rx) = mpsc::channel(1);

        let stream = Box::pin(ReceiverStream::new(rx).map(Ok));

        let peer_to_stream = Arc::new(RwLock::new(HashMap::new()));
        let mut recoverable_stream = RecoverableResponseStream {
            peer_id: PeerId::new(vec![]),
            stream,
            peer_to_stream: Arc::clone(&peer_to_stream),
        };

        tx.send(EventResponse { data: vec![0] }).await.unwrap();
        assert_eq!(
            recoverable_stream.next().await.unwrap().unwrap().data,
            vec![0]
        );

        assert!(peer_to_stream.read().unwrap().is_empty());
        drop(recoverable_stream);
        tx.send(EventResponse { data: vec![1] }).await.unwrap();

        let mut stream = peer_to_stream
            .write()
            .unwrap()
            .remove(&PeerId::new(vec![]))
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().data, vec![1]);
    }
}
