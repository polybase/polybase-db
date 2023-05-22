use futures::{Future, Stream, StreamExt};
use parking_lot::Mutex;
use solid::{self, peer};
use std::sync::Arc;
use std::{collections::HashMap, net::ToSocketAddrs, pin::Pin, time::Duration};

mod client;
mod server;
mod service;

pub struct Network {
    clients: HashMap<peer::PeerId, client::Client>,
    listener: Listener,
    sender: server::Sender,
}

impl Network {
    pub async fn init(
        server_addr: impl ToSocketAddrs,
        nodes: Vec<(peer::PeerId, tonic::transport::Endpoint)>,
    ) -> Result<Self, tonic::transport::Error> {
        let server = server::NetworkServer::new(Duration::from_secs(5), server_addr);
        let sender = server.sender();

        tokio::spawn(server.run());

        let mut clients = HashMap::new();
        let mut client_streams = HashMap::new();
        for (peer_id, endpoint) in nodes {
            let mut client = client::Client::connect(endpoint, peer_id.clone()).await?;
            let stream = client
                .event_stream()
                .await
                .expect("failed to get event stream");

            clients.insert(peer_id.clone(), client);
            client_streams.insert(peer_id, Arc::new(Mutex::new(Box::pin(stream) as _)));
        }

        Ok(Self {
            clients,
            listener: Listener { client_streams },
            sender,
        })
    }
}

pub struct Listener {
    client_streams: HashMap<peer::PeerId, Arc<Mutex<Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>>>>,
}

impl Stream for Listener {
    type Item = (peer::PeerId, Vec<u8>);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        for (peer, stream) in self.client_streams.iter_mut() {
            let mut locked_stream = stream.lock();
            if let std::task::Poll::Ready(Some(event)) = locked_stream.poll_next_unpin(cx) {
                return std::task::Poll::Ready(Some((peer.clone(), event)));
            }
        }

        std::task::Poll::Pending
    }
}

impl solid::network::NetworkSender for Network {
    fn send(
        &self,
        peer_id: peer::PeerId,
        data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>> {
        self.sender.send(peer_id, data)
    }

    fn send_all(&self, data: Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>> {
        self.sender.send_all(data)
    }
}

impl solid::network::Network for Network {
    type EventStream = Listener;

    fn events(&mut self) -> &mut Self::EventStream {
        &mut self.listener
    }
}
