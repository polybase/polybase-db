use super::service::network_service_server::{NetworkService, NetworkServiceServer};
use super::service::{EventResponse, RegisterStream};
use futures::{Future, StreamExt};
use peer::PeerId;
use solid::{network::NetworkSender, peer};
use std::{
    collections::{HashMap, HashSet},
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{transport::Server, Request, Response, Status};

type TonicResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<EventResponse, Status>> + Send + Sync>>;

pub struct NetworkServer {
    timeout: Duration,
    addr: SocketAddr,
    peers: Arc<Mutex<HashSet<PeerId>>>,
    peer_to_sender: Arc<RwLock<HashMap<PeerId, mpsc::Sender<EventResponse>>>>,
    peer_to_stream: Arc<RwLock<HashMap<PeerId, ResponseStream>>>,
}

const EVENTS_CAPACITY: usize = 128;

impl NetworkServer {
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
        let service = NetworkServiceServer::new(self);

        let server = Server::builder().add_service(service).serve(addr);

        server.await.unwrap()
    }
}

#[tonic::async_trait]
impl NetworkService for NetworkServer {
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
    fn send(
        &self,
        peer_id: PeerId,
        data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>> {
        Box::pin(async move {
            let sender = {
                let peers = self.peer_to_sender.read().unwrap();
                peers.get(&peer_id).unwrap().clone()
            };

            let out_of_capacity = match sender.send(EventResponse { data }).await {
                Ok(_) => false,
                Err(_) => {
                    // Channel is out of capacity, we cannot buffer any more events for this peer.
                    true
                }
            };

            if out_of_capacity {
                self.peer_to_sender.write().unwrap().remove(&peer_id);
            }
        })
    }

    fn send_all(&self, data: Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>> {
        let peer_to_sender = self.peer_to_sender.read().unwrap().clone();
        let tasks: Vec<_> = peer_to_sender
            .keys()
            .map(|peer_id| self.send(peer_id.clone(), data.clone()))
            .collect();
        Box::pin(async move {
            futures::future::join_all(tasks).await;
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_recoverable_stream() {
        let peer = PeerId::random();

        let sender = Sender {
            peer_to_sender: Arc::new(RwLock::new(HashMap::new())),
        };
        let (tx, rx) = mpsc::channel(1);

        sender
            .peer_to_sender
            .write()
            .unwrap()
            .insert(peer.clone(), tx);

        let stream = Box::pin(ReceiverStream::new(rx).map(Ok));

        let peer_to_stream = Arc::new(RwLock::new(HashMap::new()));
        let mut recoverable_stream = RecoverableResponseStream {
            peer_id: peer.clone(),
            stream,
            peer_to_stream: Arc::clone(&peer_to_stream),
        };

        sender.send(peer.clone(), vec![0]).await;
        assert_eq!(
            recoverable_stream.next().await.unwrap().unwrap().data,
            vec![0]
        );

        assert!(peer_to_stream.read().unwrap().is_empty());
        drop(recoverable_stream);
        sender.send(peer.clone(), vec![1]).await;

        let mut stream = peer_to_stream.write().unwrap().remove(&peer).unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().data, vec![1]);
    }
}
