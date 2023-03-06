use std::{collections::HashMap, net::ToSocketAddrs, pin::Pin, sync::Arc, time::Duration};

use futures::{Future, Stream, StreamExt};

use crate::{guild, peer, server};

pub struct Network {
    clients: HashMap<peer::PeerId, crate::client::Client>,
    listener: Arc<Listener>,
    sender: server::Sender,
}

impl Network {
    pub async fn init(
        server_addr: impl ToSocketAddrs,
        nodes: Vec<(peer::PeerId, tonic::transport::Endpoint)>,
    ) -> Result<Self, tonic::transport::Error> {
        let server = crate::server::GuildServer::new(Duration::from_secs(5), server_addr);
        let sender = server.sender();

        tokio::spawn(server.run());

        let mut clients = HashMap::new();
        let mut client_streams = HashMap::new();
        for (peer_id, endpoint) in nodes {
            let mut client = crate::client::Client::connect(endpoint, peer_id.clone()).await?;
            let stream = client
                .event_stream()
                .await
                .expect("failed to get event stream");

            clients.insert(peer_id.clone(), client);
            client_streams.insert(peer_id, Box::pin(stream) as _);
        }

        Ok(Self {
            clients,
            listener: Arc::new(Listener { client_streams }),
            sender,
        })
    }
}

pub struct Listener {
    client_streams: HashMap<
        peer::PeerId,
        Pin<Box<dyn Stream<Item = Result<crate::service::EventResponse, tonic::Status>> + Send>>,
    >,
}

impl Stream for Listener {
    type Item = (
        peer::PeerId,
        Result<crate::service::EventResponse, tonic::Status>,
    );

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        for (peer, stream) in self.client_streams.iter_mut() {
            if let std::task::Poll::Ready(Some(event)) = stream.poll_next_unpin(cx) {
                return std::task::Poll::Ready(Some((peer.clone(), event)));
            }
        }

        std::task::Poll::Pending
    }
}

impl guild::NetworkSender for Network {
    fn send(&self, peer_id: peer::PeerId, data: Vec<u8>) -> Box<dyn Future<Output = ()> + '_> {
        self.sender.send(peer_id, data)
    }
}

impl guild::Network for Network {
    type EventStream = Listener;

    fn events(&self) -> Arc<Self::EventStream> {
        Arc::clone(&self.listener)
    }

    fn snapshot(
        &mut self,
        peer_id: peer::PeerId,
        from: Vec<u8>,
    ) -> Box<dyn Future<Output = guild::Result<guild::SnapshotResp>> + '_> {
        Box::new(async move {
            let resp = self
                .clients
                .get_mut(&peer_id)
                .unwrap()
                .snapshot(from)
                .await
                .unwrap();

            Ok(guild::SnapshotResp::new(resp.data))
        })
    }
}
