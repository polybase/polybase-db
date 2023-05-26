use super::protocol;
use crate::network::behaviour::{Behaviour, BehaviourEvent};
use either::Either;
use futures::stream::FusedStream;
use futures::task::{Context, Poll};
use futures_util::{Stream, StreamExt};
use libp2p::{
    request_response,
    swarm::{handler::ConnectionHandlerUpgrErr, Swarm, SwarmEvent},
};
use parking_lot::Mutex;
use slog::error;
use std::pin::Pin;
use std::sync::Arc;
use void::Void;

pub struct SwarmStream {
    swarm: Arc<Mutex<Swarm<Behaviour>>>,
    logger: slog::Logger,
}

impl SwarmStream {
    pub fn new(swarm: Arc<Mutex<Swarm<Behaviour>>>, logger: slog::Logger) -> SwarmStream {
        SwarmStream { swarm, logger }
    }

    pub fn send_response(&self, channel: request_response::ResponseChannel<protocol::Response>) {
        match self
            .swarm
            .lock()
            .behaviour_mut()
            .rr
            .send_response(channel, protocol::Response)
        {
            Ok(_) => {}
            Err(e) => {
                error!(self.logger, "Failed to send response: {:?}", e);
            }
        }
    }
}

impl Stream for SwarmStream {
    type Item = SwarmEvent<BehaviourEvent, Either<ConnectionHandlerUpgrErr<std::io::Error>, Void>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.swarm.lock().poll_next_unpin(cx)
    }
}

impl FusedStream for SwarmStream {
    fn is_terminated(&self) -> bool {
        // This method should return `true` if the stream is terminated.
        // You will have to provide a correct implementation based on how `SwarmStream` works.
        self.swarm.lock().is_terminated()
    }
}
