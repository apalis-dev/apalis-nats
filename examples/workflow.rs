#![allow(missing_docs)]
use apalis::prelude::*;
use apalis_nats::*;
use apalis_workflow::SteppedFlow;
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let client = async_nats::connect(nats_url).await.unwrap();

    let config = Config::new("workflow")
        .with_pull_consumer()
        .durable()
        .enable_scheduling() // Used for delay for
        .with_max_ack_pending(1);
    let mut backend = NatsJetStream::new(client).with_config(config);

    backend.push(42).await.unwrap();

    async fn task1(task: u32) -> String {
        println!("Executing task1 with input: {}", task);
        (task + 99).to_string()
    }
    async fn task2(task: String) -> u32 {
        println!("Executing task2 with input: {}", task);
        task.parse::<u32>().unwrap() + 1
    }
    async fn task3(task: u32, worker: WorkerContext) {
        println!("Executing task3 with input: {}", task);
        assert_eq!(task, 142);
        worker.stop().unwrap();
    }
    let workflow = SteppedFlow::new("test_workflow")
        .and_then(task1)
        .delay_for(Duration::from_secs(3))
        .and_then(task2)
        .and_then(task3);

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|_c, e| {
            println!("{e:?},");
        })
        .build(workflow);
    worker.run().await.unwrap();
}
