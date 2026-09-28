use async_nats::{
    error::Error,
    jetstream::{
        Message,
        consumer::{
            OrderedPullConsumer, OrderedPushConsumer, PullConsumer, PushConsumer, StreamError,
            pull, push,
        },
    },
};

pub(crate) type PullOrdered = async_nats::jetstream::consumer::pull::Ordered;
pub(crate) type PullOrderedError = async_nats::jetstream::consumer::pull::OrderedError;

pub(crate) type PushOrdered = async_nats::jetstream::consumer::push::Ordered;
pub(crate) type PushOrderedError = async_nats::jetstream::consumer::push::OrderedError;

/// Converts a JetStream consumer into a stream of messages.
///
/// This trait gives pull, push and ordered consumers a common interface, so
/// the backend can read messages without knowing which kind of consumer it
/// holds. Each implementation decides how messages are fetched (for example
/// batched pull requests versus a push subscription), but always yields the
/// same [`Message`] items.
///
/// The method takes `self` by value: once a consumer is turned into a
/// stream, the stream owns it and drives all further fetching itself.
pub trait IntoMessageStream {
    /// The error type yielded by the message stream while it is being read.
    ///
    /// This covers failures that happen mid-stream, such as a missed
    /// heartbeat or a broken connection. Failures while setting the stream
    /// up are reported separately as [`StreamError`] by
    /// [`into_messages`](Self::into_messages).
    type Error;

    /// The stream of messages produced by this consumer.
    ///
    /// Each item is either a [`Message`] or an error. The stream must be
    /// [`Send`] so it can be polled from a multi-threaded runtime, and
    /// [`Unpin`] so callers can use it with combinators like `next()`
    /// without pinning it first.
    type Stream: futures::Stream<Item = Result<Message, Self::Error>> + Send + Unpin;

    /// Starts consuming and returns the message stream.
    ///
    /// # Errors
    ///
    /// Returns a [`StreamError`] if the stream cannot be started, for
    /// example when the subscription or the first pull request fails. Errors
    /// that occur after startup are yielded as items of
    /// [`Self::Stream`](Self::Stream) instead.
    ///
    /// The returned future is [`Send`], so it can be awaited inside spawned
    /// tasks.
    fn into_messages(self) -> impl Future<Output = Result<Self::Stream, StreamError>> + Send;
}

impl IntoMessageStream for OrderedPullConsumer {
    type Error = PullOrderedError;

    type Stream = PullOrdered;

    async fn into_messages(self) -> Result<Self::Stream, StreamError> {
        self.messages().await
    }
}

impl IntoMessageStream for PullConsumer {
    type Error = Error<pull::MessagesErrorKind>;

    type Stream = pull::Stream;

    async fn into_messages(self) -> Result<Self::Stream, StreamError> {
        self.messages().await
    }
}

impl IntoMessageStream for OrderedPushConsumer {
    type Error = PushOrderedError;

    type Stream = PushOrdered;

    async fn into_messages(self) -> Result<Self::Stream, StreamError> {
        self.messages().await
    }
}

impl IntoMessageStream for PushConsumer {
    type Error = Error<push::MessagesErrorKind>;

    type Stream = push::Messages;

    async fn into_messages(self) -> Result<Self::Stream, StreamError> {
        self.messages().await
    }
}
