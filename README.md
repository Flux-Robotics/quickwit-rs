# Quickwit Client

Rust client for the [Quickwit](https://quickwit.io) REST API.

The client API is generated from `quickwit-client/openapi.json` with
`progenitor`. It also provides `to_ndjson` and an asynchronous `Batcher` for
buffering JSON records and sending them to an index by record count or time.

## Usage

Add the crate:

```toml
[dependencies]
quickwit-client = { path = "path/to/quickwit-rs/quickwit-client" }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt"] }
```

Create a `Client` with the Quickwit REST URL and use the generated methods, or
start the batcher:

```rust
use std::time::Duration;
use quickwit_client::{
    batcher::{Batcher, BatcherConfig},
    Client,
};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("http://localhost:7280");
    let (ingestion, batcher) = Batcher::new(
        client,
        "my-index",
        BatcherConfig {
            interval: Duration::from_secs(2),
            max_records: 100,
            ..Default::default()
        },
    );
    let worker = tokio::spawn(batcher.run());

    ingestion.push_value(json!({"message": "hello"})).await?;

    drop(ingestion); // flush pending records and stop the worker
    worker.await??;
    Ok(())
}
```

`Ingestion` is cloneable. `push` accepts a JSON object and waits for queue
capacity; `try_push` returns immediately if the queue is full. The worker
flushes when `max_records` is reached, after `interval`, or when all ingestion
handles are dropped. Request and serialization errors stop the worker.

## Example

The example expects Quickwit at `http://localhost:7280` and an index named
`my-index`:

```sh
quickwit index create --config quickwit-client/examples/my-index.yaml
cargo run -p quickwit-client --example batching
```
