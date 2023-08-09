use futures::stream::{Stream, StreamExt};
use std::task::{Context, Poll};
use std::{boxed::Box, pin::Pin};

struct ResultWrapper<S: Unpin, E> {
    inner: S,
    error: Option<Box<E>>,
}

impl<I, S, E> Stream for ResultWrapper<S, E>
where
    S: Stream<Item = Result<I, E>> + Unpin,
{
    type Item = I;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(err) = &self.error {
            return Poll::Ready(None);
        }

        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(val))) => Poll::Ready(Some(val)),
            Poll::Ready(Some(Err(err))) => {
                self.get_mut().error = Some(Box::new(err));
                Poll::Ready(None) // Stream ends
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub fn convert_stream<I, S, E>(stream: S) -> Result<impl Stream<Item = I>, E>
where
    S: Stream<Item = Result<I, E>> + Unpin,
{
    let wrapper = ResultWrapper {
        inner: stream,
        error: None,
    };

    if let Some(err) = wrapper.error {
        Err(*err)
    } else {
        Ok(wrapper)
    }
}
