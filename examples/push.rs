use apalis::prelude::*;
use apalis_nats::*;
use futures::{self, SinkExt};
use std::collections::HashMap;
use std::env;

#[tokio::main]
async fn main() {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let client = async_nats::connect(nats_url).await.unwrap();

    let config = Config::new("push::messages")
        .with_pull_consumer()
        .durable()
        .with_max_ack_pending(1);
    let mut backend = NatsJetStream::new(client, config).await;

    backend.send(Task::new(HashMap::new())).await.unwrap();

    async fn send_reminder(
        _: HashMap<String, String>,
        wrk: WorkerContext,
    ) -> Result<(), BoxDynError> {
        wrk.stop().unwrap();
        Ok(())
    }

    let worker = WorkerBuilder::new("rango-tango-1")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
