# apalis-nats

Background task processing in rust using `apalis` and `nats-jetstream`

## Features

- **Reliable message queue** using `nats-jetstream` as the backend.
- **Multiple Polling strategies**: pull and push polling.
- **Custom codecs** allowing features like compression and encryption.
- **Scheduling**: Supports scheduling and periodic messages.
- **Workflow and cron support**: Support for sequential workflows.
- **Integration with `apalis` workers and middleware.**
- **Observability**: Monitor and manage tasks using [apalis-board](https://github.com/apalis-dev/apalis-board).

## Examples

### Setting up

The fastest way to get started is by running the Docker image:

```sh
docker run -p 4222:4222 nats:2.14 -js
```

### Basic Worker Example

```rust,no_run
use apalis::prelude::*;
use apalis_nats::*;
use futures::{self, SinkExt};
use std::env;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let nats_url = env::var("NATS_URL")
        .unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let client = async_nats::connect(nats_url).await.unwrap();

    let mut backend = NatsJetStream::new(client);

    backend.push(42).await.unwrap();

    async fn send_reminder(
        args: u32,
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

## Compatibility

By default this crate supports `nats>=2.12` but some features like cron require `2.14`.

## Roadmap

- [x] Pull Consumer
- [x] Push Consumer
- [x] Sink
- [x] Workflow support
- [x] Cron Support
- [ ] Integration testing

## License

Licensed under the MIT License.
