//! Push records to Quickwit through the background batcher.
//!
//! Run this example with a Quickwit node listening on `http://localhost:7280`
//! and an index named `my-index`:
//!
//! ```text
//! cargo run -p quickwit-client --example batching
//! ```

use std::{error::Error, time::Duration};

use quickwit_client::{
    Client,
    batcher::{Batcher, BatcherConfig},
};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::new("http://localhost:7280");
    let config = BatcherConfig {
        interval: Duration::from_secs(2),
        max_records: 100,
        ..Default::default()
    };

    let (ingestion, batcher) = Batcher::new(client, "my-index", config);

    // The batcher only starts sending records once its worker is spawned.
    let worker = tokio::spawn(batcher.run());

    // Ingestion is cloneable, so each application component can receive its
    // own handle and submit records to the same background worker.
    let application_ingestion = ingestion.clone();
    application_ingestion
        .push_value(json!({
            "service": "checkout",
            "message": "checkout started",
        }))
        .await?;

    ingestion
        .push_value(json!({
            "service": "checkout",
            "message": "checkout completed",
            "duration_ms": 42,
        }))
        .await?;

    // Dropping every handle tells the worker to flush its pending records and
    // exit. Awaiting the worker makes sure the final request has completed.
    drop(application_ingestion);
    drop(ingestion);
    worker.await??;

    Ok(())
}
