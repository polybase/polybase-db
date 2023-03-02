use futures::Stream;
use libp2p_core::PeerId;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
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
    proposal_register: Arc<RwLock<proposal::ProposalRegister>>,
    event_listeners: Arc<RwLock<Vec<mpsc::Sender<Result<EventResponse, tonic::Status>>>>>,
}

impl GuildServer {
    pub fn new<A: ToSocketAddrs>(timeout: Duration, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let peer_id = peer::PeerId::random();
        let proposal_register = proposal::ProposalRegister::new(peer_id, vec![]);

        Self {
            timeout,
            peers: vec![],
            addr,
            proposal_register: Arc::new(RwLock::new(proposal_register)),
            event_listeners: Arc::new(RwLock::new(vec![])),
        }
    }

    pub async fn broadcaster(
        proposal_register: Arc<RwLock<proposal::ProposalRegister>>,
        event_listeners: Arc<RwLock<Vec<mpsc::Sender<Result<EventResponse, tonic::Status>>>>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            interval.tick().await;
            let Some(proposal) = proposal_register.write().unwrap().next().await else {
                continue;
            };

            let proposal_serialized = bincode::serialize(&proposal).unwrap();

            let event = EventResponse {
                data: proposal_serialized,
            };

            let mut closed_listeners = vec![];
            for (i, listener) in event_listeners.read().unwrap().iter().enumerate() {
                match listener.try_send(Ok(event.clone())) {
                    Ok(()) => {}
                    Err(e) => match e {
                        mpsc::error::TrySendError::Full(_) => todo!(),
                        mpsc::error::TrySendError::Closed(_) => {
                            closed_listeners.push(i);
                        }
                    },
                }
            }

            if !closed_listeners.is_empty() {
                // Remove from last to first to avoid index shifting
                closed_listeners.reverse();
                let mut event_listeners = event_listeners.write().unwrap();
                for listener in closed_listeners {
                    event_listeners.remove(listener);
                }
            }
        }
    }

    pub async fn run(self) {
        let broadcaster = Self::broadcaster(
            Arc::clone(&self.proposal_register),
            Arc::clone(&self.event_listeners),
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
        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        self.event_listeners.write().unwrap().push(tx);

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as ResponseStream))
    }

    async fn snapshot(&self, request: Request<SnapshotRequest>) -> TonicResult<SnapshotResponse> {
        println!("Got a request: {:?}", request);

        let reply = SnapshotResponse { data: Vec::new() };

        Ok(Response::new(reply))
    }
}
