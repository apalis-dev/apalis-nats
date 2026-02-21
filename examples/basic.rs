use apalis::prelude::*;
use apalis_nats::*;
use futures::{self, SinkExt};
use std::collections::HashMap;
use std::env;

#[tokio::main]
async fn main() {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let client = async_nats::connect(nats_url).await.unwrap();

    let mut config = Config::default();
    config.stream.name = "events".to_owned();
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
