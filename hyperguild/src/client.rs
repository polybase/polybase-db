use futures::Stream;
use tonic::{
    transport::{self, Channel},
    Request,
};

use crate::{
    peer,
    service::{guild_service_client::GuildServiceClient, EventResponse},
};

pub struct Client {
    peer_id: peer::PeerId,
    grpc_client: GuildServiceClient<Channel>,
}

impl Client {
    pub async fn connect(
        endpoint: transport::Endpoint,
        peer_id: peer::PeerId,
    ) -> Result<Self, transport::Error> {
        let grpc_client = GuildServiceClient::connect(endpoint).await?;

        Ok(Self {
            peer_id,
            grpc_client,
        })
    }

    pub async fn event_stream(
        &mut self,
    ) -> Result<impl Stream<Item = Result<EventResponse, tonic::Status>>, tonic::Status> {
        let response = self
            .grpc_client
            .event_stream(Request::new(crate::service::RegisterStream {
                peer_id: self.peer_id.to_bytes(),
            }))
            .await?;

        Ok(response.into_inner())
    }
}
