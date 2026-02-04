use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use apalis_core::backend::codec::Codec;
use async_nats::jetstream::{self};
use futures::{FutureExt, Sink, future::BoxFuture};

use crate::{Config, JetStreamTask, NatsJetStream, error::JetStreamError};

pub struct JetStreamSink<T, C> {
    context: jetstream::Context,
    config: Config,
    items: VecDeque<JetStreamTask<T>>,
    pending_sends: VecDeque<PendingSend>,
    _codec: std::marker::PhantomData<C>,
}

impl<T, C> JetStreamSink<T, C> {
    pub fn new(context: jetstream::Context, config: Config) -> Self {
        Self {
            context,
            config,
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: PhantomData,
        }
    }
}

impl<T, C> Clone for JetStreamSink<T, C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            config: self.config.clone(),
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: PhantomData,
        }
    }
}

struct PendingSend {
    future: BoxFuture<'static, Result<(), JetStreamError>>,
}

impl<T, C> Sink<JetStreamTask<T>> for NatsJetStream<T, C>
where
    T: Send + 'static + Unpin,
    C: Codec<T, Compact = Vec<u8>> + Unpin,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = JetStreamError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = &mut self.get_mut().sink;

        // Poll pending sends
        while let Some(pending) = this.pending_sends.front_mut() {
            match pending.future.as_mut().poll(cx) {
                Poll::Ready(Ok(_msg_ids)) => {
                    this.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    this.pending_sends.pop_front();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: JetStreamTask<T>) -> Result<(), Self::Error> {
        let this = &mut self.get_mut().sink;

        this.items.push_back(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = &mut self.get_mut().sink;

        let mut messages = Vec::new();

        while let Some(item) = this.items.pop_front() {
            let queue_name = this.config.stream.name.clone();
            let bytes =
                C::encode(&item.args).map_err(|e| JetStreamError::ParseError(Box::new(e)))?;
            let headers = item.parts.ctx.headers.unwrap_or_default();
            let conn = this.context.clone();
            let fut = async move {
                let _ = conn
                    .publish_with_headers(queue_name, headers, bytes.into())
                    .await
                    .map_err(|e| JetStreamError::PublishError(e))?;
                Ok(())
            }
            .boxed();
            messages.push(fut);
        }

        // Create a single pending send for all messages
        if !messages.is_empty() {
            let future = async move {
                futures::future::try_join_all(messages).await?;
                Ok(())
            }
            .boxed();

            this.pending_sends.push_back(PendingSend { future });
        }

        // Now poll all pending sends
        while let Some(pending) = this.pending_sends.front_mut() {
            match pending.future.as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => {
                    this.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    this.pending_sends.pop_front();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
