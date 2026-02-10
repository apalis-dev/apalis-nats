# apalis-nats

Background task processing in rust using `apalis` and `nats-jetstream`

## Features

- **Reliable message queue** using `nats-jetstream` as the backend.
- **Multiple Polling strategies**: pull and push polling.
- **Custom codecs** for serializing/deserializing job arguments as bytes.
- **Integration with `apalis` workers and middleware.**
- **Observability**: Monitor and manage tasks using [apalis-board](https://github.com/apalis-dev/apalis-board).

## Examples

### Setting up

The fastest way to get started is by running the Docker image:

```sh
docker run -p 4222:4222 nats -js
```

### Basic Worker Example

```rust,no_run
use apalis::prelude::*;
use apalis_nats::*;
use futures::stream::{self, StreamExt, SinkExt};

#[tokio::main]
async fn main() {
    let nats_url = env::var("NATS_URL")
        .unwrap_or_else(|_| "nats://localhost:4222".to_string());

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
```

## Observability

Track your messages using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)

## Roadmap

- [x] Pull Consumer
- [ ] Push Consumer
- [ ] Shared Fetcher (Multiple queues on the same Context)
- [x] Sink
- [ ] BackendExt
- [ ] Worker heartbeats
- [ ] Workflow support
- [ ] Extensive Docs

## License

Licensed under the MIT License.
