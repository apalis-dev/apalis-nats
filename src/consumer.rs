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

pub type PullOrdered = async_nats::jetstream::consumer::pull::Ordered;
pub type PullOrderedError = async_nats::jetstream::consumer::pull::OrderedError;

pub type PushOrdered = async_nats::jetstream::consumer::push::Ordered;
pub type PushOrderedError = async_nats::jetstream::consumer::push::OrderedError;

pub trait IntoMessageStream {
    type Error;
    type Stream: futures::Stream<Item = Result<Message, Self::Error>> + Send;

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
