use std::time::Duration;

use async_nats::jetstream::{
    consumer::{AckPolicy, ReplayPolicy, pull, push},
    stream,
};

/// Configuration for a [`NatsJetStream`] backend.
///
/// Bundles the settings for the JetStream stream that stores tasks, the
/// consumer that reads them, and how often the consumer checks that the
/// connection is alive. A `Config` is built with [`Config::new`] and refined
/// with the builder methods (for example
/// [`enable_scheduling`](Self::enable_scheduling) or
/// [`durable`](Self::durable)), then passed to
/// [`NatsJetStream::with_config`].
///
/// `C` is the consumer config type, either `pull::Config` or `push::Config`.
/// It selects how messages are delivered, and decides which builder methods
/// are available.
///
/// The builder methods take `self` by value and return it, so the config is
/// marked `#[must_use]`: a call whose result is dropped has no effect.
///
/// # Examples
///
/// ```ignore
/// let config = Config::new("cron::minute")
///     .with_pull_consumer()
///     .durable()
///     .enable_scheduling()
///     .with_max_ack_pending(1);
/// ```
///
/// [`NatsJetStream`]: crate::NatsJetStream
/// [`NatsJetStream::with_config`]: crate::NatsJetStream::with_config
#[must_use]
#[derive(Debug, Clone)]
pub struct Config<C> {
    /// Configuration of the JetStream stream that stores the tasks.
    ///
    /// Includes the stream name, its subjects, and stream-level options such
    /// as `allow_message_schedules`. The stream is created from this config
    /// if it does not exist. An existing stream is not modified, so changes
    /// made here (for example enabling scheduling or adding subjects) only
    /// take effect on a new stream, or after updating the existing one
    /// manually.
    pub stream: stream::Config,

    /// Configuration of the consumer that reads tasks from the stream.
    ///
    /// Holds consumer-level options such as the durable name, the
    /// acknowledgement policy and wait time, the delivery limit
    /// (`max_deliver`), and the filter subject.
    pub consumer: C,

    /// Interval between heartbeats while consuming.
    ///
    /// A heartbeat lets the client notice a stalled or disconnected consumer
    /// instead of waiting silently. Shorter intervals detect problems sooner
    /// at the cost of a little extra traffic. Note that
    /// [`durable`](Self::durable) resets this to 30 seconds, so set it
    /// after calling `durable`.
    pub heartbeat: Duration,
}

impl Config<()> {
    /// Create a new JetStream configuration scoped to a namespace.
    ///
    /// This initializes the underlying stream with the given `namespace` as its name.
    /// No consumer type is selected at this stage — you must choose one of
    /// [`Config::with_pull_consumer`], [`Config::with_push_consumer`].
    ///
    /// # Arguments
    /// - `namespace`: Logical name used for the stream.
    pub fn new(namespace: &str) -> Self {
        let mut stream = stream::Config {
            name: namespace.to_owned(),
            ..Default::default()
        };
        stream.name = namespace.to_owned();
        Self {
            stream,
            consumer: (),
            heartbeat: Duration::from_secs(30),
        }
    }

    /// Configure a **pull-based consumer**.
    ///
    /// In this mode, the client explicitly requests messages using APIs like
    /// `fetch` or `next`. This provides strong control over throughput and
    /// natural backpressure.
    ///
    /// # Characteristics
    /// - Client-driven (you decide when/how many messages to receive)
    /// - Supports acknowledgements and redelivery
    /// - Durable and fault-tolerant
    ///
    /// # Use cases
    /// - Job queues
    /// - Worker systems
    /// - Batch processing
    ///
    /// # Notes
    /// - Does **not** support features like `idle_heartbeat`
    /// - Recommended default for most backend processing systems
    pub fn with_pull_consumer(self) -> Config<pull::Config> {
        Config {
            stream: self.stream,
            consumer: Default::default(),
            heartbeat: Duration::from_secs(30),
        }
    }

    /// Configure an **ordered pull-based consumer**.
    ///
    /// This is a simplified pull consumer that guarantees strict message ordering,
    /// but disables reliability features such as acknowledgements and redelivery.
    ///
    /// # Characteristics
    /// - Strict ordering guaranteed
    /// - No acknowledgements
    /// - No redelivery on failure
    /// - Ephemeral (non-durable)
    ///
    /// # Behavior
    /// If a message is missed or a sequence gap is detected, the consumer is
    /// transparently reset to a new position.
    ///
    /// # Use cases
    /// - Stream inspection
    /// - Debugging
    /// - Replay / analytics pipelines where occasional loss is acceptable
    ///
    /// # ⚠️ Warning
    /// Do **not** use for job processing or systems requiring reliability.
    pub fn with_ordered_pull_consumer(self) -> Config<pull::OrderedConfig> {
        Config {
            stream: self.stream,
            consumer: Default::default(),
            heartbeat: Duration::from_secs(30),
        }
    }

    /// Configure a **push-based consumer**.
    ///
    /// In this mode, JetStream delivers messages to a subject (`deliver_subject`)
    /// and the client subscribes to that subject.
    ///
    /// # Characteristics
    /// - Server-driven (messages are pushed to the client)
    /// - Supports acknowledgements and redelivery
    /// - Can be combined with queue groups for load balancing
    ///
    /// # Use cases
    /// - Event-driven systems
    /// - Real-time pipelines
    /// - Reactive services
    ///
    /// # Notes
    /// - Supports features like `idle_heartbeat` and flow control
    /// - Requires configuring a `deliver_subject` defaults to `apalis-worker-group`
    pub fn with_push_consumer(self) -> Config<push::Config> {
        let consumer: push::Config = push::Config {
            deliver_subject: "apalis-worker-group".to_owned(),
            ..Default::default()
        };
        Config {
            stream: self.stream,
            consumer,
            heartbeat: Duration::from_secs(30),
        }
    }

    /// Configure an **ordered push-based consumer**.
    ///
    /// This is a push consumer that guarantees strict ordering, but removes
    /// reliability guarantees such as acknowledgements and redelivery.
    ///
    /// # Characteristics
    /// - Strict ordering guaranteed
    /// - No acknowledgements
    /// - No redelivery
    /// - Ephemeral (non-durable)
    ///
    /// # Behavior
    /// If message delivery order is disrupted, the consumer is automatically
    /// recreated and resumes from a new position.
    ///
    /// # Use cases
    /// - Observability pipelines
    /// - Real-time stream inspection
    /// - Monitoring and debugging
    ///
    /// # ⚠️ Warning
    /// Not suitable for production job processing or any system that requires
    /// guaranteed delivery.
    pub fn with_ordered_push_consumer(self) -> Config<push::OrderedConfig> {
        let consumer: push::OrderedConfig = push::OrderedConfig {
            deliver_subject: "apalis-worker-ordered-group".to_owned(),
            ..Default::default()
        };
        Config {
            stream: self.stream,
            consumer,
            heartbeat: Duration::from_secs(30),
        }
    }
}
impl<C> Config<C> {
    /// Enables message scheduling on the stream.
    ///
    /// Sets `allow_message_schedules` on the stream config, which requires
    /// NATS Server 2.12 or newer. With scheduling on, tasks that carry a
    /// `run_at` time or a `Nats-Schedule` header are published to
    /// [`schedules`](Self::schedules) instead of the plain queue subject, and
    /// the server delivers them to the queue subject when they are due.
    ///
    /// This only affects streams created by this config. A stream that
    /// already exists is not modified by `get_or_create_stream`, so enable
    /// scheduling on it manually (for example
    /// `nats stream edit <stream> --allow-schedules`).
    pub fn enable_scheduling(mut self) -> Self {
        self.stream.allow_message_schedules = true;
        self
    }

    /// Sets the heartbeat interval used when consuming messages.
    ///
    /// The heartbeat lets the client detect a stalled or disconnected
    /// consumer. Shorter intervals detect problems faster at the cost of
    /// slightly more traffic.
    pub fn heartbeat(mut self, interval: Duration) -> Self {
        self.heartbeat = interval;
        self
    }

    /// Returns `true` if message scheduling is enabled for this stream.
    ///
    /// The publisher uses this to decide whether a task with a `run_at`
    /// time can be scheduled or has to fall back to immediate delivery.
    pub fn can_schedule(&self) -> bool {
        self.stream.allow_message_schedules
    }

    /// Returns the subject pattern that captures scheduled messages.
    ///
    /// The result is `<stream name>.schedules.>`, for example
    /// `workflow.schedules.>`. Add it to the stream's subjects so that
    /// scheduled messages, which are published to
    /// `<stream name>.schedules.<task id>`, are accepted by the stream.
    ///
    /// Note that this is the wildcard pattern, not a concrete subject. NATS
    /// keeps a single schedule per subject, so each task publishes to its
    /// own `<task id>` suffix.
    pub fn schedules(&self) -> String {
        format!("{}.schedules.>", self.stream.name)
    }
}

impl Config<pull::Config> {
    /// Makes the consumer durable and names it after the stream.
    ///
    /// Sets both the durable name and the consumer name to
    /// `<stream name>-queue`. A durable consumer keeps its position and
    /// pending messages across restarts, and workers using the same name
    /// share the work instead of each receiving every message.
    ///
    /// This also resets the heartbeat to 30 seconds, so call
    /// [`heartbeat`](Self::heartbeat) after this method if you want a
    /// different value.
    pub fn durable(mut self) -> Self {
        self.consumer.durable_name = Some(format!("{}-queue", self.stream.name));
        self.consumer.name = Some(format!("{}-queue", self.stream.name));
        Self {
            stream: self.stream,
            consumer: self.consumer,
            heartbeat: Duration::from_secs(30),
        }
    }

    /// Sets a human-readable description for the consumer.
    ///
    /// Shown in tools such as `nats consumer info`. It has no effect on
    /// behavior.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.consumer.description = Some(desc.into());
        self
    }

    /// Sets how messages must be acknowledged.
    ///
    /// See [`AckPolicy`]. The backend sends explicit acknowledgements
    /// (`+ACK`, `-NAK`, `+TERM`), so this should normally stay `Explicit`.
    pub fn with_ack_policy(mut self, policy: AckPolicy) -> Self {
        self.consumer.ack_policy = policy;
        self
    }

    /// Sets how long the server waits for an acknowledgement before
    /// redelivering a message.
    ///
    /// Set this longer than your slowest task, or the server will redeliver
    /// a message that is still being processed.
    pub fn with_ack_wait(mut self, wait: Duration) -> Self {
        self.consumer.ack_wait = wait;
        self
    }

    /// Sets the maximum number of delivery attempts per message.
    ///
    /// Once a message has been delivered this many times without an
    /// acknowledgement, the server stops redelivering it. `-1` means
    /// unlimited. This is a consumer-wide limit and cannot be set per task.
    /// Delayed retries (`-NAK` with a delay) count as deliveries too.
    pub fn with_max_deliver(mut self, max: i64) -> Self {
        self.consumer.max_deliver = max;
        self
    }

    /// Restricts the consumer to messages published to the given subject.
    ///
    /// For a queue with scheduling enabled, set this to the plain queue
    /// subject so the consumer does not receive the raw schedule messages
    /// on `<queue>.schedules.*`.
    pub fn with_filter_subject(mut self, subject: impl Into<String>) -> Self {
        self.consumer.filter_subject = subject.into();
        self
    }

    /// Sets the replay policy for messages already in the stream.
    ///
    /// See [`ReplayPolicy`]. `Instant` delivers stored messages as fast as
    /// possible, while `Original` replays them at the pace they were
    /// originally published.
    pub fn with_replay_policy(mut self, policy: ReplayPolicy) -> Self {
        self.consumer.replay_policy = policy;
        self
    }

    /// Limits the rate at which messages are delivered, in bits per second.
    ///
    /// `0` means no limit.
    pub fn with_rate_limit(mut self, rate: u64) -> Self {
        self.consumer.rate_limit = rate;
        self
    }

    /// Sets the maximum number of messages that can be in flight without an
    /// acknowledgement.
    ///
    /// Once the limit is reached, the server stops delivering until some
    /// messages are acknowledged. `1` gives strictly one task at a time
    /// across all workers sharing the consumer.
    pub fn with_max_ack_pending(mut self, max: i64) -> Self {
        self.consumer.max_ack_pending = max;
        self
    }

    /// Sets the redelivery backoff schedule.
    ///
    /// Each entry is the delay before the corresponding redelivery attempt.
    /// When set, it overrides `ack_wait` for redeliveries, and `max_deliver`
    /// must be greater than the number of entries.
    pub fn with_backoff(mut self, backoff: Vec<Duration>) -> Self {
        self.consumer.backoff = backoff;
        self
    }

    /// Sets how long an ephemeral consumer may sit idle before the server
    /// deletes it.
    ///
    /// This applies to consumers without a durable name.
    pub fn with_inactive_threshold(mut self, threshold: Duration) -> Self {
        self.consumer.inactive_threshold = threshold;
        self
    }
}

impl Config<push::Config> {
    /// Makes the consumer durable and names it after the stream.
    ///
    /// Sets both the durable name and the consumer name to
    /// `<stream name>-queue`. A durable consumer keeps its position and
    /// pending messages across restarts.
    ///
    /// This also resets the heartbeat to 30 seconds, so call
    /// [`heartbeat`](Self::heartbeat) after this method if you want a
    /// different value.
    pub fn durable(mut self) -> Self {
        self.consumer.durable_name = Some(format!("{}-queue", self.stream.name));
        self.consumer.name = Some(format!("{}-queue", self.stream.name));
        Self {
            stream: self.stream,
            consumer: self.consumer,
            heartbeat: Duration::from_secs(30),
        }
    }

    /// Sets a human-readable description for the consumer.
    ///
    /// Shown in tools such as `nats consumer info`. It has no effect on
    /// behavior.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.consumer.description = Some(desc.into());
        self
    }

    /// Sets the deliver group (queue group) for the consumer.
    ///
    /// Subscribers that share a deliver group split the messages between
    /// them instead of each receiving every message. Use this to run several
    /// workers against one push consumer.
    pub fn with_deliver_group(mut self, group: impl Into<String>) -> Self {
        self.consumer.deliver_group = Some(group.into());
        self
    }

    /// Sets how messages must be acknowledged.
    ///
    /// See [`AckPolicy`]. The backend sends explicit acknowledgements
    /// (`+ACK`, `-NAK`, `+TERM`), so this should normally stay `Explicit`.
    pub fn with_ack_policy(mut self, policy: AckPolicy) -> Self {
        self.consumer.ack_policy = policy;
        self
    }

    /// Sets how long the server waits for an acknowledgement before
    /// redelivering a message.
    ///
    /// Set this longer than your slowest task, or the server will redeliver
    /// a message that is still being processed.
    pub fn with_ack_wait(mut self, wait: Duration) -> Self {
        self.consumer.ack_wait = wait;
        self
    }

    /// Sets the maximum number of delivery attempts per message.
    ///
    /// Once a message has been delivered this many times without an
    /// acknowledgement, the server stops redelivering it. `-1` means
    /// unlimited. This is a consumer-wide limit and cannot be set per task.
    /// Delayed retries (`-NAK` with a delay) count as deliveries too.
    pub fn with_max_deliver(mut self, max: i64) -> Self {
        self.consumer.max_deliver = max;
        self
    }

    /// Restricts the consumer to messages published to the given subject.
    ///
    /// For a queue with scheduling enabled, set this to the plain queue
    /// subject so the consumer does not receive the raw schedule messages
    /// on `<queue>.schedules.*`.
    pub fn with_filter_subject(mut self, subject: impl Into<String>) -> Self {
        self.consumer.filter_subject = subject.into();
        self
    }

    /// Sets the replay policy for messages already in the stream.
    ///
    /// See [`ReplayPolicy`]. `Instant` delivers stored messages as fast as
    /// possible, while `Original` replays them at the pace they were
    /// originally published.
    pub fn with_replay_policy(mut self, policy: ReplayPolicy) -> Self {
        self.consumer.replay_policy = policy;
        self
    }

    /// Limits the rate at which messages are delivered, in bits per second.
    ///
    /// `0` means no limit.
    pub fn with_rate_limit(mut self, rate: u64) -> Self {
        self.consumer.rate_limit = rate;
        self
    }

    /// Sets the maximum number of messages that can be in flight without an
    /// acknowledgement.
    ///
    /// Once the limit is reached, the server stops delivering until some
    /// messages are acknowledged.
    pub fn with_max_ack_pending(mut self, max: i64) -> Self {
        self.consumer.max_ack_pending = max;
        self
    }

    /// Enables or disables flow control.
    ///
    /// With flow control on, the server pauses delivery when the subscriber
    /// falls behind, which protects slow consumers from being overwhelmed.
    /// It requires an idle heartbeat to be set, see
    /// [`with_idle_heartbeat`](Self::with_idle_heartbeat).
    pub fn with_flow_control(mut self, enabled: bool) -> Self {
        self.consumer.flow_control = enabled;
        self
    }

    /// Sets the idle heartbeat interval sent by the server.
    ///
    /// When no messages are flowing, the server sends a heartbeat at this
    /// interval so the client can tell an idle consumer from a broken
    /// connection. Required when flow control is enabled.
    pub fn with_idle_heartbeat(mut self, hb: Duration) -> Self {
        self.consumer.idle_heartbeat = hb;
        self
    }

    /// Sets the redelivery backoff schedule.
    ///
    /// Each entry is the delay before the corresponding redelivery attempt.
    /// When set, it overrides `ack_wait` for redeliveries, and `max_deliver`
    /// must be greater than the number of entries.
    pub fn with_backoff(mut self, backoff: Vec<Duration>) -> Self {
        self.consumer.backoff = backoff;
        self
    }

    /// Sets how long an ephemeral consumer may sit idle before the server
    /// deletes it.
    ///
    /// This applies to consumers without a durable name.
    pub fn with_inactive_threshold(mut self, threshold: Duration) -> Self {
        self.consumer.inactive_threshold = threshold;
        self
    }
}
