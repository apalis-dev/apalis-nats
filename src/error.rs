use apalis_core::error::BoxDynError;
use async_nats::{
    client::PublishErrorKind,
    error::Error,
    jetstream::{
        self,
        consumer::{StreamError, pull::MessagesErrorKind},
        context::CreateStreamError,
        stream::ConsumerError,
    },
};

pub type JetStreamMessageError = Error<MessagesErrorKind>;

pub type AckError = Error<PublishErrorKind>;

pub type PublishError = Error<jetstream::context::PublishErrorKind>;

#[derive(Debug, thiserror::Error)]
pub enum JetStreamError {
    #[error("PollError: {0}")]
    PollError(JetStreamMessageError),

    #[error("ConsumerError: {0}")]
    ConsumerError(ConsumerError),

    #[error("StreamError: {0}")]
    StreamError(StreamError),

    #[error("CreateStreamError: {0}")]
    CreateStreamError(CreateStreamError),

    #[error("ParseError: {0}")]
    ParseError(BoxDynError),

    #[error("AckError: {0}")]
    AckError(AckError),

    #[error("PublishError: {0}")]
    PublishError(PublishError),
}
