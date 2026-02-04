use std::marker::PhantomData;

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, TaskStream, codec::Codec},
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use async_nats::{
    Client, HeaderMap, StatusCode, Subject,
    jetstream::{self, consumer::PullConsumer, stream},
};
use futures::{
    StreamExt, TryStreamExt,
    stream::{BoxStream, once},
};

use crate::{error::JetStreamError, sink::JetStreamSink};

mod ack;
mod error;
mod sink;

pub type JetStreamTask<Args> = Task<Args, JetStreamContext, i64>;

#[derive(Debug, Clone, Default)]
pub struct Config {
    stream: stream::Config,
    consumer: jetstream::consumer::pull::Config,
}

#[derive(Debug, Default, Clone)]
pub struct JetStreamContext {
    pub subject: Option<Subject>,
    pub reply: Option<Subject>,
    pub headers: Option<HeaderMap>,
    pub status: Option<StatusCode>,
    pub description: Option<String>,
}

pub struct NatsJetStream<Args, Codec> {
    _args: PhantomData<(Args, Codec)>,
    client: Client,
    config: Config,
    sink: JetStreamSink<Args, Codec>,
}

impl<Args> NatsJetStream<Args, JsonCodec<Vec<u8>>> {
    pub async fn new(client: Client, config: Config) -> Self {
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

impl<Args, Codec> Clone for NatsJetStream<Args, Codec> {
    fn clone(&self) -> Self {
        Self {
            _args: PhantomData,
            client: self.client.clone(),
            config: self.config.clone(),
            sink: self.sink.clone(),
        }
    }
}

impl<Args, C> Backend for NatsJetStream<Args, C>
where
    Args: Send + Sync + 'static + Unpin,
    C: Codec<Args, Compact = Vec<u8>> + Send + Sync + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;

    type Context = JetStreamContext;

    type Beat = BoxStream<'static, Result<(), JetStreamError>>;

    type Error = JetStreamError;

    type IdType = i64;

    type Layer = AcknowledgeLayer<Self>;

    type Stream = TaskStream<JetStreamTask<Args>, JetStreamError>;

    fn heartbeat(&self, _worker: &WorkerContext) -> Self::Beat {
        Box::pin(futures::stream::pending())
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(self.clone())
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let worker = worker.clone();
        let config = self.config;
        once(async move {
            let jetstream = jetstream::new(self.client);
            let consumer: PullConsumer = jetstream
                .create_stream(config.stream)
                .await
                .map_err(|e| JetStreamError::CreateStreamError(e))?
                // Then, on that `Stream` use method to create Consumer and bind to it too.
                .create_consumer(jetstream::consumer::pull::Config {
                    durable_name: Some(worker.name().clone()),
                    ..config.consumer
                })
                .await
                .map_err(|e| JetStreamError::ConsumerError(e))?;
            let stream = consumer
                .messages()
                .await
                .map_err(|e| JetStreamError::StreamError(e))?
                .map_err(|e| JetStreamError::PollError(e));
            Ok::<_, JetStreamError>(stream)
        })
        .try_flatten()
        .map(|message| match message {
            Ok(msg) => {
                let args = C::decode(&(&msg.payload[..].to_vec()))
                    .map_err(|e| JetStreamError::ParseError(e.into()))?;
                let ctx = JetStreamContext {
                    subject: Some(msg.subject.clone()),
                    reply: msg.reply.clone(),
                    status: msg.status.clone(),
                    headers: msg.headers.clone(),
                    description: msg.description.clone(),
                };
                let task = Task::builder(args).with_ctx(ctx).build();
                Ok(Some(task))
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

        let mut config = Config::default();
        config.stream.name = "messages".to_owned();
        let mut backend = NatsJetStream::new(client, config).await;

        backend.send(Task::new(HashMap::new())).await.unwrap();

        async fn send_reminder(
            _: HashMap<String, String>,
            wrk: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            wrk.stop().unwrap();
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
    }
}
