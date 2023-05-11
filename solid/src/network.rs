use crate::peer::PeerId;
// use async_trait::async_trait;
use futures::{Future, Stream};
use std::pin::Pin;

pub trait Network: NetworkSender {
    type EventStream: Stream<Item = (PeerId, Vec<u8>)> + Unpin + Sync + Send;

    fn events(&mut self) -> &mut Self::EventStream;
}

// #[async_trait]
pub trait NetworkSender: Send {
    fn send(
        &self,
        peer_id: PeerId,
        data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + '_>>;
}
