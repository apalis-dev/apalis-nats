use std::time::Duration;

use async_nats::jetstream::{
    consumer::{AckPolicy, ReplayPolicy, pull, push},
    stream,
};

#[derive(Debug, Clone)]
pub struct Config<C> {
    pub stream: stream::Config,
    pub consumer: C,
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

impl Config<pull::Config> {
    pub fn durable(mut self) -> Self {
        self.consumer.durable_name = Some(format!("{}-queue", &self.stream.name));
        self.consumer.name = Some(format!("{}-queue", &self.stream.name));
        Self {
            stream: self.stream,
            consumer: self.consumer,
            heartbeat: Duration::from_secs(30),
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.consumer.description = Some(desc.into());
        self
    }

    pub fn with_ack_policy(mut self, policy: AckPolicy) -> Self {
        self.consumer.ack_policy = policy;
        self
    }

    pub fn with_ack_wait(mut self, wait: Duration) -> Self {
        self.consumer.ack_wait = wait;
        self
    }

    pub fn with_max_deliver(mut self, max: i64) -> Self {
        self.consumer.max_deliver = max;
        self
    }

    pub fn with_filter_subject(mut self, subject: impl Into<String>) -> Self {
        self.consumer.filter_subject = subject.into();
        self
    }

    pub fn with_replay_policy(mut self, policy: ReplayPolicy) -> Self {
        self.consumer.replay_policy = policy;
        self
    }

    pub fn with_rate_limit(mut self, rate: u64) -> Self {
        self.consumer.rate_limit = rate;
        self
    }

    pub fn with_max_ack_pending(mut self, max: i64) -> Self {
        self.consumer.max_ack_pending = max;
        self
    }

    pub fn with_backoff(mut self, backoff: Vec<Duration>) -> Self {
        self.consumer.backoff = backoff;
        self
    }

    pub fn with_inactive_threshold(mut self, threshold: Duration) -> Self {
        self.consumer.inactive_threshold = threshold;
        self
    }
}

impl Config<push::Config> {
    pub fn durable(mut self) -> Self {
        self.consumer.durable_name = Some(format!("{}-queue", &self.stream.name));
        self.consumer.name = Some(format!("{}-queue", &self.stream.name));
        Self {
            stream: self.stream,
            consumer: self.consumer,
            heartbeat: Duration::from_secs(30),
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.consumer.description = Some(desc.into());
        self
    }

    pub fn with_deliver_group(mut self, group: impl Into<String>) -> Self {
        self.consumer.deliver_group = Some(group.into());
        self
    }

    pub fn with_ack_policy(mut self, policy: AckPolicy) -> Self {
        self.consumer.ack_policy = policy;
        self
    }

    pub fn with_ack_wait(mut self, wait: Duration) -> Self {
        self.consumer.ack_wait = wait;
        self
    }

    pub fn with_max_deliver(mut self, max: i64) -> Self {
        self.consumer.max_deliver = max;
        self
    }

    pub fn with_filter_subject(mut self, subject: impl Into<String>) -> Self {
        self.consumer.filter_subject = subject.into();
        self
    }

    pub fn with_replay_policy(mut self, policy: ReplayPolicy) -> Self {
        self.consumer.replay_policy = policy;
        self
    }

    pub fn with_rate_limit(mut self, rate: u64) -> Self {
        self.consumer.rate_limit = rate;
        self
    }

    pub fn with_max_ack_pending(mut self, max: i64) -> Self {
        self.consumer.max_ack_pending = max;
        self
    }

    pub fn with_flow_control(mut self, enabled: bool) -> Self {
        self.consumer.flow_control = enabled;
        self
    }

    pub fn with_idle_heartbeat(mut self, hb: Duration) -> Self {
        self.consumer.idle_heartbeat = hb;
        self
    }

    pub fn with_backoff(mut self, backoff: Vec<Duration>) -> Self {
        self.consumer.backoff = backoff;
        self
    }

    pub fn with_inactive_threshold(mut self, threshold: Duration) -> Self {
        self.consumer.inactive_threshold = threshold;
        self
    }
}
