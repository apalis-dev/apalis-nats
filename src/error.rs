use async_nats::jetstream::{
    consumer::{StreamError, pull, push},
    context::{CreateStreamError, PublishError},
    stream::ConsumerError,
};

use crate::consumer::{PullOrderedError, PushOrderedError};
/// Errors produced by the NATS JetStream backend.
///
/// Each variant wraps the underlying `async_nats` error for one stage of the
/// backend's lifecycle (creating the stream, creating the consumer, reading
/// messages, publishing), so callers can tell where a failure occurred.
/// All variants implement `From`, so `?` converts the underlying error
/// automatically.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed while receiving messages from a push-based consumer.
    ///
    /// Raised when the message stream of a push consumer errors, for example
    /// on a missed heartbeat or a broken subscription.
    #[error("PushError: {0}")]
    PushError(#[from] async_nats::error::Error<push::MessagesErrorKind>),

    /// Failed while receiving messages from an ordered push consumer.
    ///
    /// Ordered consumers are ephemeral and recreate themselves on gaps or
    /// missed heartbeats, so this usually means recovery itself failed.
    #[error("PushOrderedError: {0}")]
    PushOrderedError(#[from] PushOrderedError),

    /// Failed while receiving messages from a pull-based consumer.
    ///
    /// Raised when fetching or streaming a batch of messages fails, for
    /// example on a pull request error, a missed heartbeat, or a connection
    /// problem.
    #[error("PullError: {0}")]
    PullError(#[from] async_nats::error::Error<pull::MessagesErrorKind>),

    /// Failed while receiving messages from an ordered pull consumer.
    ///
    /// Ordered consumers are ephemeral and recreate themselves on gaps or
    /// missed heartbeats, so this usually means recovery itself failed.
    #[error("PullOrderedError: {0}")]
    PullOrderedError(#[from] PullOrderedError),

    /// Failed to create or look up the consumer.
    ///
    /// Common causes: the stream doesn't exist, the consumer config conflicts
    /// with an existing durable consumer of the same name, or the config
    /// contains invalid values (for example a bad `filter_subject`).
    #[error("ConsumerError: {0}")]
    ConsumerError(#[from] ConsumerError),

    /// Failed to turn a consumer into a message stream.
    ///
    /// Raised when opening the subscription or starting the pull loop fails
    /// after the consumer has already been created.
    #[error("StreamError: {0}")]
    StreamError(#[from] StreamError),

    /// Failed to create or fetch the JetStream stream.
    ///
    /// Note that creating a stream that already exists is only accepted when
    /// the existing config matches. If it doesn't (for example the subjects
    /// changed or scheduling was enabled later), the stream must be updated
    /// or recreated manually.
    #[error("CreateStreamError: {0}")]
    CreateStreamError(#[from] CreateStreamError),

    /// Failed to publish a task to the stream.
    ///
    /// Covers both the send itself (no responders, no stream matching the
    /// subject, connection lost) and the JetStream acknowledgement. This
    /// includes scheduled tasks published to `<queue>.schedules.<id>`, which
    /// fail here if the stream doesn't capture that subject or has scheduling
    /// disabled.
    #[error("PublishError: {0}")]
    PublishError(#[from] PublishError),
}
