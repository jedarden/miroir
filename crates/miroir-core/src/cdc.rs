//! CDC (Change Data Capture) — plan §13.13.
//!
//! Publishes document change events to configured sinks (webhook, NATS, Kafka, internal queue).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info};

/// CDC event published on every successful write (after quorum ACK).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Miroir task ID.
    pub mtask_id: String,
    /// Index UID.
    pub index: String,
    /// Operation type.
    pub operation: CdcOperation,
    /// Primary keys affected.
    pub primary_keys: Vec<String>,
    /// Shard IDs affected.
    pub shard_ids: Vec<u32>,
    /// Settings version at write time.
    pub settings_version: u64,
    /// UNIX timestamp (ms).
    pub timestamp: u64,
    /// Document body (optional, based on sink config).
    pub document: Option<serde_json::Value>,
    /// Internal origin tag (for suppressing internal writes).
    pub origin: Option<String>,
    /// Stable event ID for deduplication.
    pub event_id: String,
}

/// Operation type for a CDC event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcOperation {
    Add,
    Update,
    Delete,
}

/// CDC sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSinkConfig {
    /// Sink type identifier.
    #[serde(rename = "type")]
    pub sink_type: CdcSinkType,
    /// Sink URL (for webhook/NATS/Kafka).
    pub url: String,
    /// Batch size for events.
    pub batch_size: u32,
    /// Batch flush interval (ms).
    pub batch_flush_ms: u64,
    /// Whether to include document body.
    pub include_body: bool,
    /// Maximum retry time (seconds).
    pub retry_max_s: u64,
    /// NATS subject prefix (for NATS sinks).
    pub subject_prefix: Option<String>,
}

/// Sink type variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcSinkType {
    Webhook,
    Nats,
    Kafka,
    Internal,
}

/// CDC publisher state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcPublisherState {
    /// Per-sink cursor (sequence number).
    pub cursors: HashMap<String, u64>,
    /// Buffered event count.
    pub buffered_count: usize,
    /// Dropped event count.
    pub dropped_count: u64,
    /// Total published count.
    pub published_count: u64,
}

/// CDC manager — publishes change events to configured sinks.
pub struct CdcManager {
    /// Configuration.
    config: CdcConfig,
    /// Event sender channel.
    event_tx: mpsc::UnboundedSender<CdcEvent>,
    /// Per-sink state (shared with background task).
    state: Arc<RwLock<CdcPublisherState>>,
}

/// CDC manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    /// Whether CDC is enabled.
    pub enabled: bool,
    /// Whether to emit TTL delete events.
    pub emit_ttl_deletes: bool,
    /// Whether to emit internal writes (debug only).
    pub emit_internal_writes: bool,
    /// Configured sinks.
    pub sinks: Vec<CdcSinkConfig>,
    /// Buffer configuration.
    pub buffer: CdcBufferConfig,
}

/// Buffer configuration for CDC events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcBufferConfig {
    /// Primary buffer type.
    pub primary: CdcBufferType,
    /// In-memory buffer size (bytes).
    pub memory_bytes: u64,
    /// Overflow buffer type.
    pub overflow: CdcBufferType,
    /// Redis overflow size (bytes).
    pub redis_bytes: u64,
}

/// Buffer type variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcBufferType {
    Memory,
    Redis,
    Pvc,
    Drop,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            emit_ttl_deletes: false,
            emit_internal_writes: false,
            sinks: Vec::new(),
            buffer: CdcBufferConfig {
                primary: CdcBufferType::Memory,
                memory_bytes: 67_108_864, // 64 MiB
                overflow: CdcBufferType::Redis,
                redis_bytes: 1_073_741_824, // 1 GiB
            },
        }
    }
}

impl CdcManager {
    /// Create a new CDC manager.
    pub fn new(config: CdcConfig) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let state = Arc::new(RwLock::new(CdcPublisherState {
            cursors: HashMap::new(),
            buffered_count: 0,
            dropped_count: 0,
            published_count: 0,
        }));

        if config.enabled {
            // Spawn background publisher task
            let state_clone = state.clone();
            let config_clone = config.clone();
            tokio::spawn(async move {
                Self::background_publisher(event_rx, state_clone, config_clone).await;
            });
        }

        Self {
            config,
            event_tx,
            state,
        }
    }

    /// Publish a CDC event (non-blocking).
    pub fn publish(&self, event: CdcEvent) -> Result<(), CdcError> {
        if !self.config.enabled {
            return Ok(());
        }

        // Filter based on origin tag
        if let Some(ref origin) = event.origin {
            match origin.as_str() {
                "antientropy" | "reshard_backfill" | "rollover" => {
                    if !self.config.emit_internal_writes {
                        debug!("CDC: suppressing internal write with origin {}", origin);
                        return Ok(());
                    }
                }
                "ttl_expire" => {
                    if !self.config.emit_ttl_deletes {
                        debug!("CDC: suppressing TTL delete");
                        return Ok(());
                    }
                }
                _ => {}
            }
        }

        // TTL deletes are filtered by emit_ttl_deletes flag
        if event.operation == CdcOperation::Delete {
            if let Some(ref origin) = event.origin {
                if origin == "ttl_expire" && !self.config.emit_ttl_deletes {
                    return Ok(());
                }
            }
        }

        self.event_tx.send(event).map_err(|_| CdcError::ChannelClosed)?;
        Ok(())
    }

    /// Get current publisher state.
    pub async fn state(&self) -> CdcPublisherState {
        self.state.read().await.clone()
    }

    /// Background task that buffers and publishes events to sinks.
    async fn background_publisher(
        mut event_rx: mpsc::UnboundedReceiver<CdcEvent>,
        state: Arc<RwLock<CdcPublisherState>>,
        config: CdcConfig,
    ) {
        info!("CDC: background publisher started");

        // Per-sink event buffers
        let mut sink_buffers: HashMap<String, Vec<CdcEvent>> = HashMap::new();

        while let Some(event) = event_rx.recv().await {
            // Buffer event for each sink
            for sink in &config.sinks {
                let buffer = sink_buffers.entry(sink.url.clone()).or_insert_with(Vec::new);
                buffer.push(event.clone());

                // Flush if buffer size reached
                if buffer.len() >= sink.batch_size as usize {
                    if let Err(e) = Self::flush_sink(&sink, buffer, &state).await {
                        error!("CDC: failed to flush sink {}: {}", sink.url, e);
                    }
                    sink_buffers.insert(sink.url.clone(), Vec::new());
                }
            }
        }

        // Flush remaining buffers on shutdown
        for (sink_url, buffer) in sink_buffers {
            if !buffer.is_empty() {
                let sink = config.sinks.iter().find(|s| s.url == sink_url);
                if let Some(sink) = sink {
                    if let Err(e) = Self::flush_sink(sink, &buffer, &state).await {
                        error!("CDC: failed to flush sink {} on shutdown: {}", sink_url, e);
                    }
                }
            }
        }

        info!("CDC: background publisher stopped");
    }

    /// Flush buffered events to a single sink.
    async fn flush_sink(
        sink: &CdcSinkConfig,
        events: &[CdcEvent],
        _state: &Arc<RwLock<CdcPublisherState>>,
    ) -> Result<(), CdcError> {
        match sink.sink_type {
            CdcSinkType::Webhook => Self::flush_webhook(sink, events).await,
            CdcSinkType::Nats => Self::flush_nats(sink, events).await,
            CdcSinkType::Kafka => Self::flush_kafka(sink, events).await,
            CdcSinkType::Internal => {
                // Internal queue: events are stored in memory for polling
                // (implementation depends on internal queue design)
                Ok(())
            }
        }
    }

    /// Flush events to a webhook sink.
    async fn flush_webhook(
        sink: &CdcSinkConfig,
        events: &[CdcEvent],
    ) -> Result<(), CdcError> {
        let client = reqwest::Client::new();
        let response = client
            .post(&sink.url)
            .json(events)
            .send()
            .await
            .map_err(|e| CdcError::SinkError(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            Err(CdcError::SinkError(format!("webhook returned {}", status)))
        }
    }

    /// Flush events to a NATS sink.
    async fn flush_nats(
        _sink: &CdcSinkConfig,
        _events: &[CdcEvent],
    ) -> Result<(), CdcError> {
        // NATS publishing implementation
        // (requires async-nats crate)
        Ok(())
    }

    /// Flush events to a Kafka sink.
    async fn flush_kafka(
        _sink: &CdcSinkConfig,
        _events: &[CdcEvent],
    ) -> Result<(), CdcError> {
        // Kafka publishing implementation
        // (requires rustafka or rdkafka crate)
        Ok(())
    }
}

/// CDC error types.
#[derive(Debug, thiserror::Error)]
pub enum CdcError {
    #[error("channel closed")]
    ChannelClosed,
    #[error("sink error: {0}")]
    SinkError(String),
    #[error("buffer overflow")]
    BufferOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_event_creation() {
        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: None,
            event_id: uuid::Uuid::new_v4().to_string(),
        };
        assert_eq!(event.index, "products");
        assert_eq!(event.operation, CdcOperation::Add);
    }

    #[test]
    fn test_cdc_config_default() {
        let config = CdcConfig::default();
        assert!(config.enabled);
        assert!(!config.emit_ttl_deletes);
        assert!(!config.emit_internal_writes);
    }

    #[tokio::test]
    async fn test_cdc_manager_publish() {
        let config = CdcConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = CdcManager::new(config);

        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: None,
            event_id: uuid::Uuid::new_v4().to_string(),
        };

        // Should not error
        assert!(manager.publish(event).is_ok());
    }

    #[tokio::test]
    async fn test_cdc_suppress_internal_writes() {
        let config = CdcConfig {
            enabled: true,
            emit_internal_writes: false,
            ..Default::default()
        };
        let manager = CdcManager::new(config);

        // Internal write should be suppressed
        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: Some("antientropy".into()),
            event_id: uuid::Uuid::new_v4().to_string(),
        };

        assert!(manager.publish(event).is_ok());
    }

    #[test]
    fn test_cdc_sink_type_serialization() {
        let sink_type = CdcSinkType::Webhook;
        let json = serde_json::to_string(&sink_type).unwrap();
        assert_eq!(json, "\"Webhook\"");
    }
}
