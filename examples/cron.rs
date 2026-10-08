#![allow(missing_docs)]
use apalis::prelude::*;
use apalis_nats::*;
use std::{env, str::FromStr};

const QUEUE: &str = "cron::minute";

const STABLE_ID: &str = "01M3KVR63AVEHZCFGJB0MKZPZ4"; // Nats needs a stable ULID

#[tokio::main]
async fn main() {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let client = async_nats::connect(nats_url).await.unwrap();

    let config = Config::new(QUEUE)
        .with_pull_consumer()
        .durable()
        .enable_scheduling()
        .with_max_ack_pending(1);
    let mut backend = NatsJetStream::new(client).with_config(config);

    let mut metadata = MetadataStore::default();
    metadata.insert("Nats-Schedule", "@every 1m").unwrap();
    metadata.insert("Nats-Schedule-Target", QUEUE).unwrap();

    let task = TaskBuilder::new(42u32)
        .task_id(TaskId::from_str(STABLE_ID).unwrap())
        .with_metadata(metadata)
        .build();

    backend.push_task(task).await.unwrap();

    async fn send_reminder(_: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
        wrk.stop().unwrap();
        Ok(())
    }

    let worker = WorkerBuilder::new("rango-tango-1")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
