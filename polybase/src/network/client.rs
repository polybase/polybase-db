use super::service::{self, network_service_client::NetworkServiceClient};
use futures::{Stream, StreamExt};
use futures_util::TryStreamExt;
use solid::peer;
use tonic::{
    transport::{self, Channel},
    Request,
};

pub struct Client {
    peer_id: peer::PeerId,
    grpc_client: NetworkServiceClient<Channel>,
}

impl Client {
    pub async fn connect(
        endpoint: transport::Endpoint,
        peer_id: peer::PeerId,
    ) -> Result<Self, transport::Error> {
        let grpc_client = NetworkServiceClient::connect(endpoint).await?;

        Ok(Self {
            peer_id,
            grpc_client,
        })
    }

    pub async fn event_stream(&mut self) -> Result<impl Stream<Item = Vec<u8>>, tonic::Status> {
        let response = self
            .grpc_client
            .event_stream(Request::new(service::RegisterStream {
                peer_id: self.peer_id.to_bytes(),
            }))
            .await?;

        let byte_stream = response
            .into_inner()
            .map_ok(|event_response| event_response.data)
            .filter_map(|result| async { result.ok() });

        Ok(byte_stream)
    }
}