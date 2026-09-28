#![doc = include_str!("../README.md")]
use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{
        Backend, BackendConfig, WireFormatBackend, finalize::Durable, future::BoxSyncFuture,
    },
    task::{Task, builder::TaskBuilder, metadata::MetadataStore, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use async_nats::{
    Client, HeaderMap, StatusCode, Subject, header,
    jetstream::{
        self, Context,
        consumer::{Consumer, FromConsumer, IntoConsumerConfig, push},
        stream,
    },
};
use futures::{FutureExt, Stream};
use std::fmt::{self, Debug};
use std::{pin::Pin, str::FromStr, task::Poll};
use ulid::Ulid;

use crate::{ack::NatsJetAck, sink::JetStreamSink};
pub use crate::{config::Config, consumer::IntoMessageStream, error::Error};

mod ack;
mod config;
mod consumer;
mod error;
mod sink;

/// A task as handled by the JetStream backend.
///
/// This is an alias for [`Task`] whose argument type defaults to `Vec<u8>`,
/// the raw message payload, so `JetStreamTask` on its own means a task whose
/// arguments have not been decoded yet. Use `JetStreamTask<MyArgs>` for a
/// task whose payload has been decoded (with the backend's JSON codec) into
/// `MyArgs`.
///
/// The task's context is the [`NatsTaskContext`] captured from the received
/// message, which holds the subject and the reply subject used to
/// acknowledge it.
pub type JetStreamTask<Args = Vec<u8>> = Task<Args>;

/// A task backend that stores and consumes jobs through NATS JetStream.
///
/// The backend publishes tasks to a JetStream stream and reads them back
/// through a consumer. `Args` is the type of the task payload, and `C` is
/// the consumer configuration type (for example `pull::Config` or
/// `push::Config`), which selects whether messages are pulled or pushed.
///
/// A backend is created from a connected client and a [`Config`], and is
/// then handed to a worker. The stream and consumer are created lazily
/// when the backend first starts consuming.
///
/// # Type parameters
///
/// * `Args`: the task argument type, serialized to bytes when published.
/// * `C`: the consumer config type. It must be convertible into a
///   consumer config ([`IntoConsumerConfig`]) and clonable, and its
///   consumer must be able to produce a message stream
///   ([`IntoMessageStream`]).
pub struct NatsJetStream<Args, C>
where
    Consumer<C>: IntoMessageStream,
    C: IntoConsumerConfig,
{
    context: Context,
    config: Config<C>,
    sink: JetStreamSink<Args>,
    codec: JsonCodec,
    state: State<<Consumer<C> as IntoMessageStream>::Stream>,
    messages: Option<<Consumer<C> as IntoMessageStream>::Stream>,
}

impl<Args, C: Debug> Debug for NatsJetStream<Args, C>
where
    Consumer<C>: IntoMessageStream,
    C: IntoConsumerConfig,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsJetStream")
            .field("context", &self.context)
            .field("config", &self.config)
            .field("sink", &self.sink)
            .field("codec", &self.codec)
            .field("state", &self.state)
            .field("messages", &self.messages.as_ref().map(|_| ".."))
            .finish()
    }
}

impl<Args> NatsJetStream<Args, push::Config> {
    /// Creates a backend for the given NATS client, using a push consumer
    /// with default settings.
    ///
    /// The queue name, which is also the stream name and its subject, is
    /// taken from the type name of `Args` (via [`std::any::type_name`]), so a
    /// task type `Email` is published to a queue named after `Email`'s full
    /// type path. To use a different name or a pull consumer, follow up with
    /// [`with_config`](Self::with_config).
    ///
    /// No connection to the server is made beyond wrapping the client in a
    /// JetStream context. The stream and consumer are created later, when the
    /// backend starts consuming.
    #[must_use]
    pub fn new(client: Client) -> Self {
        let context = jetstream::new(client);
        let config = Config::new(std::any::type_name::<Args>()).with_push_consumer();
        Self {
            sink: JetStreamSink::new(),
            context,
            config,
            codec: JsonCodec::default(),
            state: State::Init,
            messages: None,
        }
    }

    /// Replaces the configuration, which can also change the consumer type.
    ///
    /// Use this to set a custom queue name, switch from a push to a pull
    /// consumer, or adjust stream and consumer settings such as scheduling,
    /// acknowledgement wait, or delivery limits. The returned backend has
    /// type `NatsJetStream<Args, C>` where `C` is the consumer type of the
    /// new config.
    ///
    /// The JetStream context, the sink, and the codec are kept. The
    /// consuming state is reset to its initial value and any message stream
    /// already opened is dropped, so call this before the backend starts
    /// consuming.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let config = Config::new("cron::minute")
    ///     .with_pull_consumer()
    ///     .durable()
    ///     .enable_scheduling();
    /// let backend = NatsJetStream::new(client).with_config(config);
    /// ```
    pub fn with_config<C>(self, config: Config<C>) -> NatsJetStream<Args, C>
    where
        Consumer<C>: IntoMessageStream,
        C: IntoConsumerConfig,
    {
        NatsJetStream {
            sink: self.sink,
            context: self.context,
            config,
            codec: self.codec,
            state: State::Init,
            messages: None,
        }
    }
}

impl<Args, C> Clone for NatsJetStream<Args, C>
where
    Consumer<C>: IntoMessageStream,
    C: IntoConsumerConfig + Clone,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            sink: self.sink.clone(),
            codec: self.codec.clone(),
            state: State::Init,
            context: self.context.clone(),
            messages: None,
        }
    }
}

enum State<Stm> {
    Init,
    CreateStream(BoxSyncFuture<Result<stream::Stream, Error>>),
    CreateConsumer(BoxSyncFuture<Result<Stm, Error>>),
    Running,
    CleanUp(BoxSyncFuture<Result<(), Error>>),
}

impl<Stm> fmt::Debug for State<Stm> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => f.write_str("Init"),
            Self::CreateStream(_) => f.write_str("CreateStream(..)"),
            Self::CreateConsumer(_) => f.write_str("CreateConsumer(..)"),
            Self::Running => f.write_str("Running"),
            Self::CleanUp(_) => f.write_str("CleanUp(..)"),
        }
    }
}

impl<Args, C, E> Backend for NatsJetStream<Args, C>
where
    Args: Send + Sync + 'static + Unpin,
    C: IntoConsumerConfig + Clone + FromConsumer + Send + 'static,
    Consumer<C>: IntoMessageStream<Error = E>,
    E: Into<Error>,
{
    type Task = JetStreamTask;
    type Error = Error;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        loop {
            match &mut self.state {
                State::Init => {
                    let ctx = self.context.clone();
                    let cfg = self.config.clone();
                    self.state = State::CreateStream(
                        async move { Self::create_stream(&ctx, cfg).await }
                            .boxed()
                            .into(),
                    );
                }

                State::CreateStream(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => {
                        self.state = State::Init; // allow retry on next poll
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(stream)) => {
                        let cfg = self.config.clone();
                        self.state = State::CreateConsumer(
                            async move { Self::start_consume(&stream, cfg).await }
                                .boxed()
                                .into(),
                        );
                    }
                },

                State::CreateConsumer(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => {
                        self.state = State::Init;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(stm)) => {
                        self.messages = Some(stm);
                        self.state = State::Running;
                    }
                },

                State::Running => return Poll::Ready(Ok(())),
                State::CleanUp(_) => unreachable!(),
            }
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let Some(messages) = self.messages.as_mut() else {
            return Poll::Ready(None);
        };

        match Pin::new(messages).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(Some(Ok(msg))) => {
                let args = msg.payload[..].to_vec();
                let mut task = TaskBuilder::new(args);
                if let Some(headers) = &msg.headers {
                    let task_id = headers
                        .get(header::NATS_MESSAGE_ID)
                        .map(|s| s.as_str().to_owned())
                        .or_else(|| {
                            headers
                                .get(header::NATS_SCHEDULER)
                                .and_then(|s| s.as_str().rsplit('.').next().map(str::to_owned))
                        })
                        .and_then(|s| Ulid::from_str(&s).ok());
                    if let Some(task_id) = task_id {
                        task = task.task_id(TaskId::Ulid(task_id));
                    }
                }

                let metadata = MetadataStore::from_map(
                    msg.headers
                        .as_ref()
                        .unwrap_or(&HeaderMap::default())
                        .iter()
                        .map(|(k, v)| (k.to_string(), v[0].as_str().to_owned()))
                        .collect(),
                );
                let ctx = NatsTaskContext {
                    subject: Some(msg.subject.clone()),
                    reply: msg.reply.clone(),
                    status: msg.status,
                    description: msg.description.clone(),
                };
                task = task.with_metadata(metadata).data(ctx);

                Poll::Ready(Some(Ok(task.build())))
            }
        }
    }

    fn poll_close(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        loop {
            match &mut self.state {
                State::Running
                | State::CreateConsumer(_)
                | State::CreateStream(_)
                | State::Init => {
                    self.messages = None;
                    let ctx = self.context.clone();
                    let cfg = self.config.clone();
                    self.state = State::CleanUp(
                        async move { Self::cleanup(&ctx, cfg).await }.boxed().into(),
                    );
                }

                State::CleanUp(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(res) => {
                        self.state = State::Init;
                        return Poll::Ready(res);
                    }
                },
            }
        }
    }
}

impl<Args, C> BackendConfig for NatsJetStream<Args, C>
where
    Consumer<C>: IntoMessageStream,
    C: IntoConsumerConfig,
{
    type Args = Args;

    type Id = Ulid;

    type Kind = Durable;

    type Config = Config<C>;

    type Layer = AcknowledgeLayer<NatsJetAck>;

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn middleware(&mut self, _worker: &mut WorkerContext) -> Self::Layer {
        AcknowledgeLayer::new(NatsJetAck {
            context: self.context.clone(),
        })
    }
}

impl<Args, C> WireFormatBackend for NatsJetStream<Args, C>
where
    Consumer<C>: IntoMessageStream,
    C: IntoConsumerConfig,
{
    type Codec = JsonCodec;
    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<Args, C, Stm> NatsJetStream<Args, C>
where
    Consumer<C>: IntoMessageStream<Stream = Stm>,
    C: IntoConsumerConfig + FromConsumer,
{
    pub(crate) async fn create_stream(
        context: &Context,
        mut config: Config<C>,
    ) -> Result<stream::Stream, Error> {
        let name = config.stream.name.clone();
        if config.can_schedule() {
            for subject in [name, config.schedules()] {
                if !config.stream.subjects.contains(&subject) {
                    config.stream.subjects.push(subject);
                }
            }
        }

        let stream = context
            .get_or_create_stream(config.stream)
            .await
            .map_err(Error::CreateStreamError)?;
        Ok(stream)
    }

    async fn start_consume(stream: &stream::Stream, config: Config<C>) -> Result<Stm, Error> {
        let consumer = stream
            .create_consumer(config.consumer)
            .await
            .map_err(Error::ConsumerError)?;
        let stream = consumer.into_messages().await.map_err(Error::StreamError)?;
        Ok(stream)
    }
    async fn cleanup(context: &Context, config: Config<C>) -> Result<(), Error> {
        let stream_name = config.stream.name.clone();
        let consumer_cfg = config.consumer.into_consumer_config();
        let Some(name) = consumer_cfg
            .name
            .as_ref()
            .or(consumer_cfg.durable_name.as_ref())
        else {
            return Ok(());
        };

        if consumer_cfg.durable_name.is_some() {
            return Ok(());
        }

        context
            .delete_consumer_from_stream(name, stream_name)
            .await?;

        Ok(())
    }
}

/// Per-message context captured from a received NATS message.
///
/// This carries the parts of the incoming message that are needed after the
/// payload has been decoded, most importantly the reply subject used to
/// acknowledge the message. It is attached to the task's execution context
/// so that handlers and the backend's `ack` implementation can read it.
///
/// All fields are optional because they are copied from the message as it
/// arrived: a message may have no reply subject (for example a plain NATS
/// message that was not delivered through JetStream), and most messages
/// carry no status.
#[derive(Debug, Default, Clone)]
pub struct NatsTaskContext {
    /// The subject the message was published to.
    ///
    /// For a scheduled message this is the target subject it was delivered
    /// to (for example `workflow`), not the schedule subject
    /// (`workflow.schedules.<id>`) it was originally published on.
    pub subject: Option<Subject>,

    /// The reply subject used to acknowledge the message.
    ///
    /// JetStream sets this on every delivered message. Acknowledgements are
    /// published to it: `+ACK` for success, `-NAK` to retry (optionally with
    /// a delay), and `+TERM` to stop redelivery. The subject also encodes
    /// delivery metadata such as the delivery count and stream sequence.
    ///
    /// It is `None` for messages without a reply subject, and there is
    /// nothing to acknowledge in that case.
    pub reply: Option<Subject>,

    /// The status code of the message, if the server attached one.
    ///
    /// Regular delivered messages have no status. It is set on control
    /// messages from the server, such as idle heartbeats, flow control
    /// requests, or errors returned to a pull request (for example "no
    /// messages" or "request timeout").
    pub status: Option<StatusCode>,

    /// A human-readable description that accompanies [`status`](Self::status).
    ///
    /// Only present when the server sent a status message, and usually
    /// explains why (for example "Idle Heartbeat"). It is `None` for normal
    /// messages.
    pub description: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env, time::Duration};

    use apalis_core::{backend::TaskSink, error::BoxDynError, worker::builder::WorkerBuilder};

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

        let mut backend = NatsJetStream::new(client).with_config(config);

        backend.push(HashMap::new()).await.unwrap();

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
    }
}
