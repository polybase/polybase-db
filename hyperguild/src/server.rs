use futures::Stream;
use libp2p_core::PeerId;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

use crate::service::guild_service_server::{GuildService, GuildServiceServer};
use crate::service::{EventResponse, RegisterStream, SnapshotRequest, SnapshotResponse};

type TonicResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<EventResponse, Status>> + Send>>;

#[derive(Debug)]
struct GuildServer {
    timeout: Duration,
    peers: Vec<PeerId>,
    addr: SocketAddr,
}

impl GuildServer {
    pub fn new<A: ToSocketAddrs>(timeout: Duration, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        Self {
            timeout,
            peers: vec![],
            addr,
        }
    }

    pub async fn run<A: ToSocketAddrs>(self) {
        let addr = self.addr;
        let service = GuildServiceServer::new(self);
        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .unwrap();
    }
}

#[tonic::async_trait]
impl GuildService for GuildServer {
    type EventStreamStream = ResponseStream;

    async fn event_stream(
        &self,
        req: Request<RegisterStream>,
    ) -> TonicResult<Self::EventStreamStream> {
        let repeat = std::iter::repeat(EventResponse { data: vec![] });

        let mut stream = Box::pin(tokio_stream::iter(repeat));

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            println!("\tclient disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as ResponseStream))
    }

    async fn snapshot(&self, request: Request<SnapshotRequest>) -> TonicResult<SnapshotResponse> {
        println!("Got a request: {:?}", request);

        let reply = SnapshotResponse { data: Vec::new() };

        Ok(Response::new(reply))
    }
}
