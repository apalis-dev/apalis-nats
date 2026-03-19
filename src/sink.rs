use apalis_core::{backend::codec::Codec, error::BoxDynError};
use async_nats::jetstream::{
    self,
    consumer::{Consumer, IntoConsumerConfig},
    message::PublishMessage,
};
use futures::{
    FutureExt, Sink,
    future::{BoxFuture, Shared},
};
use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    Config, JetStreamTask, NatsJetStream, consumer::IntoMessageStream, error::JetStreamError,
};

pub struct JetStreamSink<T, Encode, C> {
    context: jetstream::Context,
    config: Config<C>,
    items: VecDeque<JetStreamTask<T>>,
    pending_sends: VecDeque<PendingSend>,
    _codec: std::marker::PhantomData<Encode>,
}

impl<T, Encode, C> JetStreamSink<T, Encode, C> {
    pub fn new(context: jetstream::Context, config: Config<C>) -> Self {
        Self {
            context,
            config,
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: PhantomData,
        }
    }
}

impl<T, Encode, C: Clone> Clone for JetStreamSink<T, Encode, C> {
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
    future: Shared<BoxFuture<'static, Result<(), Arc<BoxDynError>>>>,
}

impl<T, Encode, C, PollErr> Sink<JetStreamTask<T>> for NatsJetStream<T, Encode, C>
where
    T: Send + 'static + Unpin,
    Encode: Codec<T, Compact = Vec<u8>> + Unpin,
    Encode::Error: std::error::Error + Send + Sync + 'static,
    C: IntoConsumerConfig + Unpin,
    Consumer<C>: IntoMessageStream<Error = PollErr>,
    PollErr: Send + 'static,
{
    type Error = JetStreamError<PollErr>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = &mut self.get_mut().sink;

        // Poll pending sends
        while let Some(pending) = this.pending_sends.front_mut() {
            match pending.future.poll_unpin(cx) {
                Poll::Ready(Ok(_msg_ids)) => {
                    this.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    this.pending_sends.pop_front();
                    return Poll::Ready(Err(JetStreamError::SinkError(
                        Arc::into_inner(e).unwrap(),
                    )));
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
                Encode::encode(&item.args).map_err(|e| JetStreamError::ParseError(Box::new(e)))?;
            let headers = item.parts.ctx.headers.unwrap_or_default();
            let mut publish = PublishMessage::build()
                .payload(bytes.into())
                .headers(headers);
            if let Some(task_id) = item.parts.task_id {
                publish = publish.message_id(task_id.inner().to_string());
            }
            let conn = this.context.clone();

            let fut: BoxFuture<'static, Result<(), BoxDynError>> = async move {
                let _ = conn
                    .send_publish(queue_name, publish)
                    .await
                    .map_err(BoxDynError::from)?;
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
            .boxed()
            .shared();

            this.pending_sends.push_back(PendingSend { future });
        }

        // Now poll all pending sends
        while let Some(pending) = this.pending_sends.front_mut() {
            match pending.future.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => {
                    this.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    this.pending_sends.pop_front();
                    return Poll::Ready(Err(JetStreamError::SinkError(
                        Arc::into_inner(e).unwrap(),
                    )));
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
