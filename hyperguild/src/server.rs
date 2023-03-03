use futures::TryStreamExt;
use libp2p_core::PeerId;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, RwLock},
    task::Poll,
    time::Duration,
};
use tokio::sync::{broadcast, mpsc};
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
    peers: Vec<PeerId>,
    addr: SocketAddr,
    proposal_register: Arc<proposal::ProposalRegister>,
    event_sender: Arc<broadcast::Sender<EventResponse>>,
}

impl GuildServer {
    pub fn new<A: ToSocketAddrs>(timeout: Duration, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let peer_id = peer::PeerId::random();
        let proposal_register = proposal::ProposalRegister::new(peer_id, vec![]);

        let (event_sender, _) = broadcast::channel(128);

        Self {
            timeout,
            peers: vec![],
            addr,
            proposal_register: Arc::new(proposal_register),
            event_sender: Arc::new(event_sender),
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
        let mut events = BroadcastStream::new(self.event_sender.subscribe());

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = events.next().await {
                match tx
                    .send(item.map_err(|e| Status::internal(format!("{}", e))))
                    .await
                {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            eprintln!("\tclient disconnected");
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
