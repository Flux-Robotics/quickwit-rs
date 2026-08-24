use crate::{Client, Error, to_ndjson};
use serde_json::{Map, Value};
use std::time::Duration;
use tokio::sync::mpsc;

/// Configuration for a [`Batcher`].
#[derive(Clone, Debug)]
pub struct BatcherConfig {
    /// How long the batcher waits before flushing a non-empty batch.
    pub interval: Duration,
    /// The maximum number of records sent in one request.
    pub max_records: usize,
    /// The number of records that may wait in the channel before producers
    /// have to wait for the background batcher.
    pub channel_capacity: usize,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            max_records: 1_000,
            channel_capacity: 10_000,
        }
    }
}

/// A cloneable handle used to submit records to a background [`Batcher`].
#[derive(Clone, Debug)]
pub struct Ingestion {
    sender: mpsc::Sender<Map<String, Value>>,
}

impl Ingestion {
    /// Submit a record, waiting while the batcher's bounded queue is full.
    ///
    /// An error means that the corresponding batcher has stopped.
    pub async fn push(&self, record: Map<String, Value>) -> Result<(), IngestionError> {
        self.sender
            .send(record)
            .await
            .map_err(|_| IngestionError::Closed)
    }

    /// Alias for [`push`](Self::push), useful when the ingestion handle is
    /// used as a queue-like sink.
    pub async fn send(&self, record: Map<String, Value>) -> Result<(), IngestionError> {
        self.push(record).await
    }

    /// Alias for [`push`](Self::push).
    pub async fn ingest(&self, record: Map<String, Value>) -> Result<(), IngestionError> {
        self.push(record).await
    }

    /// Submit a record without waiting for queue capacity.
    pub fn try_push(&self, record: Map<String, Value>) -> Result<(), IngestionError> {
        self.sender.try_send(record).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => IngestionError::Full,
            mpsc::error::TrySendError::Closed(_) => IngestionError::Closed,
        })
    }

    /// Alias for [`try_push`](Self::try_push).
    pub fn try_send(&self, record: Map<String, Value>) -> Result<(), IngestionError> {
        self.try_push(record)
    }

    /// Submit a JSON value if it is an object.
    pub async fn push_value(&self, record: Value) -> Result<(), IngestionValueError> {
        self.push(
            record
                .as_object()
                .cloned()
                .ok_or(IngestionValueError::NotObject)?,
        )
        .await
        .map_err(IngestionValueError::Ingestion)
    }
}

/// Errors returned while submitting records to an [`Ingestion`].
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("the batcher is no longer running")]
    Closed,
    #[error("the batcher's queue is full")]
    Full,
}

/// Errors returned when submitting a JSON value.
#[derive(Debug, thiserror::Error)]
pub enum IngestionValueError {
    #[error("ingested records must be JSON objects")]
    NotObject,
    #[error("failed to submit record: {0}")]
    Ingestion(#[from] IngestionError),
}

/// A background worker that batches records and sends them to Quickwit.
pub struct Batcher {
    client: Client,
    index_id: String,
    receiver: mpsc::Receiver<Map<String, Value>>,
    config: BatcherConfig,
}

impl Batcher {
    /// Create a batcher and its cloneable ingestion handle.
    ///
    /// The returned batcher does not start doing work until its [`run`] method
    /// is spawned or awaited by the caller.
    pub fn new(
        client: Client,
        index_id: impl Into<String>,
        config: BatcherConfig,
    ) -> (Ingestion, Self) {
        assert!(
            config.max_records > 0,
            "max_records must be greater than zero"
        );
        assert!(
            config.channel_capacity > 0,
            "channel_capacity must be greater than zero"
        );

        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        (
            Ingestion { sender },
            Self {
                client,
                index_id: index_id.into(),
                receiver,
                config,
            },
        )
    }

    /// Run the worker until all [`Ingestion`] handles have been dropped.
    ///
    /// Any request or serialization error stops the worker and is returned to
    /// the caller. Records already removed from the queue are not retried.
    pub async fn run(mut self) -> Result<(), BatcherError> {
        let mut records = Vec::with_capacity(self.config.max_records);
        let delay = tokio::time::sleep(self.config.interval);
        tokio::pin!(delay);

        loop {
            tokio::select! {
                record = self.receiver.recv() => match record {
                    Some(record) => {
                        records.push(record);
                        if records.len() == self.config.max_records {
                            flush(&self.client, &self.index_id, &mut records).await?;
                            delay.as_mut().reset(tokio::time::Instant::now() + self.config.interval);
                        }
                    }
                    None => {
                        if !records.is_empty() {
                            flush(&self.client, &self.index_id, &mut records).await?;
                        }
                        return Ok(());
                    }
                },
                _ = &mut delay, if !records.is_empty() => {
                    flush(&self.client, &self.index_id, &mut records).await?;
                    delay.as_mut().reset(tokio::time::Instant::now() + self.config.interval);
                }
            }
        }
    }
}

async fn flush(
    client: &Client,
    index_id: &str,
    records: &mut Vec<Map<String, Value>>,
) -> Result<(), BatcherError> {
    let body = to_ndjson(records)?;
    client.ingest(index_id, None, None, body).await?;
    records.clear();
    Ok(())
}

/// Errors encountered by the background batcher.
#[derive(Debug, thiserror::Error)]
pub enum BatcherError {
    #[error("failed to serialize an ingestion batch: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("failed to send an ingestion batch: {0}")]
    Request(#[from] Error<()>),
}
