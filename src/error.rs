use apalis_core::error::BoxDynError;
use async_nats::{
    client::{FlushErrorKind, PublishErrorKind},
    error::Error,
    jetstream::{consumer::StreamError, context::CreateStreamError, stream::ConsumerError},
};

pub type AckError = Error<PublishErrorKind>;

#[derive(Debug, thiserror::Error)]
pub enum JetStreamError<PollErr> {
    #[error("PollError: {0}")]
    PollError(PollErr),

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

    #[error("SinkError: {0}")]
    SinkError(BoxDynError),

    #[error("FlushError: {0}")]
    FlushError(Error<FlushErrorKind>),
}
