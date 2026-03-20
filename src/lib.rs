#![doc = include_str!("../README.md")]
use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue},
    task::{Task, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use async_nats::{
    Client, HeaderMap, StatusCode, Subject, header,
    jetstream::{
        self,
        consumer::{Consumer, FromConsumer, IntoConsumerConfig},
    },
};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream, once},
};
use std::marker::PhantomData;
use ulid::Ulid;

pub use crate::{
    config::Config, consumer::IntoMessageStream, error::JetStreamError, sink::JetStreamSink,
};

mod ack;
mod config;
mod consumer;
mod error;
mod sink;

pub type JetStreamTask<Args> = Task<Args, JetStreamContext, ulid::Ulid>;

#[derive(Debug, Default, Clone)]
pub struct JetStreamContext {
    pub subject: Option<Subject>,
    pub reply: Option<Subject>,
    pub headers: Option<HeaderMap>,
    pub status: Option<StatusCode>,
    pub description: Option<String>,
}

pub struct NatsJetStream<Args, Codec, C> {
    _args: PhantomData<(Args, Codec)>,
    client: Client,
    config: Config<C>,
    sink: JetStreamSink<Args, Codec, C>,
}

impl<Args, C: Clone> NatsJetStream<Args, JsonCodec<Vec<u8>>, C> {
    pub async fn new(client: Client, config: Config<C>) -> Self {
        let context = jetstream::new(client.clone());
        context
            .get_or_create_stream(config.clone().stream)
            .await
            .expect("Could not create stream");
        Self {
            sink: JetStreamSink::new(context, config.clone()),
            _args: PhantomData,
            client,
            config,
        }
    }
}

impl<Args, Codec, C: Clone> Clone for NatsJetStream<Args, Codec, C> {
    fn clone(&self) -> Self {
        Self {
            _args: PhantomData,
            client: self.client.clone(),
            config: self.config.clone(),
            sink: self.sink.clone(),
        }
    }
}

impl<Args, Decode, C, PollErr> Backend for NatsJetStream<Args, Decode, C>
where
    Args: Send + Sync + 'static + Unpin,
    Decode: Codec<Args, Compact = Vec<u8>> + Send + Sync + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    C: Clone + IntoConsumerConfig + FromConsumer + Send + 'static,
    Consumer<C>: IntoMessageStream<Error = PollErr>,
    PollErr: Send + 'static,
{
    type Args = Args;

    type Context = JetStreamContext;

    type Beat = BoxStream<'static, Result<(), JetStreamError<PollErr>>>;

    type Error = JetStreamError<PollErr>;

    type IdType = ulid::Ulid;

    type Layer = AcknowledgeLayer<Self>;

    type Stream = TaskStream<JetStreamTask<Args>, JetStreamError<PollErr>>;

    fn heartbeat(&self, _worker: &WorkerContext) -> Self::Beat {
        let heartbeat = self.config.heartbeat;
        stream::unfold(self.client.clone(), move |client| async move {
            apalis_core::timer::sleep(heartbeat).await;
            let res = client
                .flush()
                .await
                .map_err(|e| JetStreamError::FlushError(e));
            Some((res, client))
        })
        .boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(self.clone())
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        self.poll_general()
            .map(|t| match t {
                Ok(Some(task)) => Ok(Some(task.try_map(|task| {
                    Decode::decode(&task).map_err(|e| JetStreamError::ParseError(e.into()))
                })?)),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode, C, PollErr> BackendExt for NatsJetStream<Args, Decode, C>
where
    Args: Send + Sync + 'static + Unpin,
    Decode: Codec<Args, Compact = Vec<u8>> + Send + Sync + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    C: Clone + IntoConsumerConfig + FromConsumer + Send + 'static,
    Consumer<C>: IntoMessageStream<Error = PollErr>,
    PollErr: Send + 'static,
{
    type Codec = Decode;
    type Compact = Vec<u8>;

    type CompactStream = TaskStream<JetStreamTask<Self::Compact>, JetStreamError<PollErr>>;

    fn get_queue(&self) -> Queue {
        Queue::from(self.config.stream.name.as_str())
    }

    fn poll_compact(self, _worker: &WorkerContext) -> Self::CompactStream {
        self.poll_general()
    }
}

impl<Args, Decode, C, PollErr> NatsJetStream<Args, Decode, C>
where
    Args: Send + Sync + 'static + Unpin,
    Decode: Codec<Args, Compact = Vec<u8>> + Send + Sync + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    C: Clone + IntoConsumerConfig + FromConsumer + Send + 'static,
    Consumer<C>: IntoMessageStream<Error = PollErr>,
    PollErr: Send + 'static,
{
    pub fn poll_general(self) -> TaskStream<JetStreamTask<Vec<u8>>, JetStreamError<PollErr>> {
        let config = self.config;
        once(async move {
            let jetstream = jetstream::new(self.client);
            let consumer = jetstream
                .create_stream(config.stream)
                .await
                .map_err(|e| JetStreamError::CreateStreamError(e))?
                // Then, on that `Stream` use method to create Consumer and bind to it too.
                .create_consumer(config.consumer)
                .await
                .map_err(|e| JetStreamError::ConsumerError(e))?;
            let stream = consumer
                .into_messages()
                .await
                .map_err(|e| JetStreamError::StreamError(e))?
                .map_err(|e| JetStreamError::PollError(e));
            Ok::<_, JetStreamError<PollErr>>(stream)
        })
        .try_flatten()
        .map(|message| match message {
            Ok(msg) => {
                let args = msg.payload[..].to_vec();
                let mut task = Task::builder(args);
                if let Some(headers) = &msg.headers {
                    let task_id = headers
                        .get(header::NATS_MESSAGE_ID)
                        .and_then(|s| Ulid::from_string(s.as_str()).ok());
                    if let Some(task_id) = task_id {
                        task = task.with_task_id(TaskId::new(task_id));
                    }
                }
                let ctx = JetStreamContext {
                    subject: Some(msg.subject.clone()),
                    reply: msg.reply.clone(),
                    status: msg.status,
                    headers: msg.headers.clone(),
                    description: msg.description.clone(),
                };
                task = task.with_ctx(ctx);
                Ok(Some(task.build()))
            }
            Err(e) => Err(e),
        })
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env, time::Duration};

    use apalis_core::{error::BoxDynError, worker::builder::WorkerBuilder};
    use futures::SinkExt;

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

        // Create an unauthenticated connection to NATS.
        let client = async_nats::connect(nats_url).await.unwrap();

        let config = Config::new("push_messages")
            .with_pull_consumer()
            .durable()
            .with_max_ack_pending(1);

        let mut backend = NatsJetStream::new(client.clone(), config).await;

        backend.send(Task::new(HashMap::new())).await.unwrap();

        async fn send_reminder(
            _: HashMap<String, String>,
            wrk: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(5)).await;
            wrk.stop().unwrap();
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();

        // This ensures all pending messages are delivered
        client.flush().await.unwrap();
    }
}
