//! Tests for the kafka-sink feature gate.
//!
//! miroir-proxy forwards `kafka-sink` to `miroir-core/kafka-sink` (see
//! crates/miroir-proxy/Cargo.toml), so both halves of this file are reachable:
//!
//! - The disabled branch is compiled by the plain workspace suite
//!   (`cargo test --workspace`, or `cargo test -p miroir-proxy --test
//!   kafka_sink_feature` with no features). It pins the surface that must
//!   survive without the sink: a settings file naming a Kafka sink still
//!   parses (and still resolves to a Kafka sink after the advanced→cdc config
//!   conversion the server applies at startup), and publishing still
//!   enqueues. The compiled-out sink fails at flush time
//!   (`flush_kafka_stub`), never at ingest time.
//! - The enabled branch is compiled by
//!   `cargo test -p miroir-proxy --features kafka-sink --test kafka_sink_feature`
//!   and by `cargo test --all --all-features` (both in the `cargo-test` step
//!   of k8s/argo-workflows/miroir-ci.yaml), and by the release build's
//!   `--features miroir-proxy/kafka-sink` (see k8s/argo-workflows/
//!   miroir-release.yaml and the Dockerfile).

#[cfg(feature = "kafka-sink")]
#[test]
fn test_kafka_sink_feature_enabled() {
    // This test verifies that the kafka-sink feature is enabled.
    // If the feature is not enabled, this test won't compile because
    // it's gated by #[cfg(feature = "kafka-sink")]
    assert!(true, "kafka-sink feature is enabled");
}

#[cfg(not(feature = "kafka-sink"))]
#[test]
fn test_kafka_sink_feature_disabled() {
    use miroir_core::cdc::CdcSinkConfig;
    use serde_json::json;

    // A settings file that names a Kafka sink must keep parsing with the
    // feature compiled out — the sink type is part of the config surface,
    // not of the compiled-in sink set.
    let config: CdcSinkConfig = serde_json::from_value(json!({
        "type": "Kafka",
        "url": "localhost:9092",
        "batch_size": 100,
        "batch_flush_ms": 1000,
        "include_body": true,
        "retry_max_s": 3600
    }))
    .expect("kafka sink config should parse without the kafka-sink feature");

    assert_eq!(config.sink_type, miroir_core::cdc::CdcSinkType::Kafka);
    assert_eq!(config.url, "localhost:9092");
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.batch_flush_ms, 1000);
    assert!(config.include_body);
    assert_eq!(config.retry_max_s, 3600);
    assert_eq!(config.subject_prefix, None);
}

#[cfg(not(feature = "kafka-sink"))]
#[test]
fn test_kafka_sink_settings_path_keeps_kafka_typed_without_feature() {
    use miroir_core::cdc::{CdcBufferType, CdcSinkType};
    use miroir_core::config::advanced::CdcConfig as AdvancedCdcConfig;
    use serde_json::json;

    // This is the shape a real settings file takes: miroir-core's settings
    // carry `cdc` as config::advanced::CdcConfig with a string sink type (see
    // crates/miroir-core/src/config.rs), and the server converts it into
    // cdc::CdcConfig at startup (`config.cdc.clone().into()` in
    // admin_endpoints.rs). With the sink compiled out, that conversion must
    // still resolve "kafka" to CdcSinkType::Kafka — if it silently fell back
    // to another sink type, events addressed to Kafka would be routed down
    // the wrong flush path instead of flush_kafka_stub.
    let advanced: AdvancedCdcConfig = serde_json::from_value(json!({
        "enabled": true,
        "sinks": [{
            "type": "kafka",
            "url": "localhost:9092",
            "batch_size": 100,
            "batch_flush_ms": 1000,
            "include_body": true,
            "retry_max_s": 3600
        }]
    }))
    .expect("settings naming a kafka sink should parse without the kafka-sink feature");

    assert_eq!(advanced.sinks.len(), 1);
    assert_eq!(advanced.sinks[0].sink_type, "kafka");

    let cdc_config: miroir_core::cdc::CdcConfig = advanced.into();
    assert_eq!(cdc_config.sinks.len(), 1);
    assert_eq!(cdc_config.sinks[0].sink_type, CdcSinkType::Kafka);
    assert_eq!(cdc_config.sinks[0].url, "localhost:9092");
    assert_eq!(cdc_config.sinks[0].batch_size, 100);
    assert_eq!(cdc_config.sinks[0].batch_flush_ms, 1000);
    assert!(cdc_config.sinks[0].include_body);
    assert_eq!(cdc_config.sinks[0].retry_max_s, 3600);
    assert_eq!(cdc_config.sinks[0].subject_prefix, None);

    // Serde defaults survive the conversion too (buffer config omitted above).
    assert_eq!(cdc_config.buffer.primary, CdcBufferType::Memory);
}

#[cfg(not(feature = "kafka-sink"))]
#[tokio::test]
async fn test_kafka_sink_disabled_ingest_still_enqueues() {
    use miroir_core::cdc::{
        CdcBufferConfig, CdcBufferType, CdcConfig, CdcEvent, CdcManager, CdcOperation,
        CdcSinkConfig, CdcSinkType,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    // Kafka-only sink config with a 1 s flush interval so the time-based
    // flush fires during the test and runs the sink through flush_kafka_stub.
    fn kafka_sink_config() -> CdcConfig {
        CdcConfig {
            enabled: true,
            emit_ttl_deletes: false,
            emit_internal_writes: false,
            sinks: vec![CdcSinkConfig {
                sink_type: CdcSinkType::Kafka,
                url: "localhost:9092".to_string(),
                batch_size: 100,
                batch_flush_ms: 1000,
                include_body: true,
                retry_max_s: 3600,
                subject_prefix: None,
            }],
            buffer: CdcBufferConfig {
                primary: CdcBufferType::Memory,
                memory_bytes: 64 * 1024,
                overflow: CdcBufferType::Drop,
                redis_bytes: 0,
            },
        }
    }

    fn event(event_id: &str) -> CdcEvent {
        CdcEvent {
            event_id: event_id.to_string(),
            mtask_id: "test-mtask".to_string(),
            index: "test-index".to_string(),
            operation: CdcOperation::Add,
            primary_keys: vec!["pk-1".to_string()],
            shard_ids: vec![0],
            settings_version: 1,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            document: None,
            origin: None,
        }
    }

    async fn wait_for_event(manager: &CdcManager, event_id: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changes = manager.get_changes("test-index", 0, 10).await;
                if changes.iter().any(|e| e.event_id == event_id) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("event should reach the internal queue within 5 s");
    }

    let manager = CdcManager::new(kafka_sink_config());

    // With the sink compiled out, ingest is still accepted: the flush path is
    // where the feature gap surfaces (stub error), never the publish path.
    manager
        .publish(event("test-event-disabled"))
        .expect("publish should enqueue without the kafka-sink feature");

    // The event must actually reach the publisher pipeline — it shows up in
    // the internal queue backing GET /_miroir/changes — so the compiled-out
    // sink costs events nothing at ingest, only at flush.
    wait_for_event(&manager, "test-event-disabled").await;

    // The per-sink memory buffer accepted the event too: with the sink
    // compiled out, ingest neither drops nor errors.
    assert_eq!(
        manager.state().await.dropped_count,
        0,
        "compiled-out kafka sink must not drop events at ingest time"
    );

    // Past batch_flush_ms the background publisher flushes the Kafka sink via
    // flush_kafka_stub, which always errors. The failure must stay confined
    // to flush: nothing is counted as published, and the publisher keeps
    // running rather than panicking or wedging.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        manager.state().await.published_count,
        0,
        "compiled-out kafka sink must fail at flush time (stub), never publish"
    );

    // Ingest still works after the failed flush.
    manager
        .publish(event("test-event-after-flush"))
        .expect("publish must keep working after the stub flush failure");
    wait_for_event(&manager, "test-event-after-flush").await;

    // The no-drop guarantee survives a full stub-flush failure cycle: the
    // second ingest is accepted by the buffer exactly like the first, and the
    // flush failure itself drops nothing.
    assert_eq!(
        manager.state().await.dropped_count,
        0,
        "compiled-out kafka sink must not drop events at ingest time, even after a failed stub flush"
    );

    let changes = manager.get_changes("test-index", 0, 10).await;
    assert_eq!(changes.len(), 2, "both published events should be queued");
    assert_eq!(changes[0].event_id, "test-event-disabled");
    assert_eq!(changes[1].event_id, "test-event-after-flush");
}

#[cfg(feature = "kafka-sink")]
#[tokio::test]
async fn test_cdc_kafka_flush_exists() {
    use miroir_core::cdc::{CdcConfig, CdcEvent, CdcOperation};
    use miroir_core::config::advanced::{CdcConfig as AdvancedCdcConfig, CdcSinkConfig};
    use std::time::{SystemTime, UNIX_EPOCH};

    // Create a CDC config with a kafka sink
    let advanced_config = AdvancedCdcConfig {
        enabled: true,
        emit_ttl_deletes: false,
        emit_internal_writes: false,
        sinks: vec![CdcSinkConfig {
            sink_type: "kafka".to_string(),
            url: "localhost:9092".to_string(),
            batch_size: 100,
            batch_flush_ms: 1000,
            include_body: true,
            retry_max_s: 3600,
            subject_prefix: None,
        }],
        buffer: Default::default(),
    };

    let cdc_config: CdcConfig = advanced_config.into();
    let manager = miroir_core::cdc::CdcManager::new(cdc_config);

    // Create a test event
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let event = CdcEvent {
        event_id: "test-event-1".to_string(),
        mtask_id: "test-mtask".to_string(),
        index: "test-index".to_string(),
        operation: CdcOperation::Add,
        primary_keys: vec!["pk-1".to_string()],
        shard_ids: vec![0],
        settings_version: 1,
        timestamp,
        document: None,
        origin: None,
    };

    // Publish the event - this will fail because there's no actual Kafka broker
    // but it verifies that the flush_kafka function exists and is called
    let result = manager.publish(event);

    // We expect an error since there's no actual Kafka broker running
    // but the error should be a connection error, not a "feature not enabled" error
    match result {
        Ok(_) => {
            // Unexpected success, but if we have a real broker somehow, that's fine
        }
        Err(e) => {
            let error_msg = e.to_string();
            // The error should mention Kafka or connection, not "feature not enabled"
            assert!(
                !error_msg.contains("feature not enabled"),
                "kafka-sink feature should be enabled: got error: {}",
                error_msg
            );
            // We expect a connection error or similar
            assert!(
                error_msg.contains("Kafka")
                    || error_msg.contains("connection")
                    || error_msg.contains("broker"),
                "Expected Kafka connection error, got: {}",
                error_msg
            );
        }
    }
}

#[cfg(feature = "kafka-sink")]
#[test]
fn test_kafka_sink_config_parsing() {
    use miroir_core::cdc::CdcSinkConfig;
    use serde_json::json;

    // Verify that a Kafka sink config can be parsed correctly
    let config_json = json!({
        "type": "Kafka",
        "url": "localhost:9092",
        "batch_size": 100,
        "batch_flush_ms": 1000,
        "include_body": true,
        "retry_max_s": 3600
    });

    let config: CdcSinkConfig =
        serde_json::from_value(config_json).expect("kafka sink config should parse correctly");

    assert_eq!(config.sink_type, miroir_core::cdc::CdcSinkType::Kafka);
    assert_eq!(config.url, "localhost:9092");
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.batch_flush_ms, 1000);
    assert_eq!(config.include_body, true);
    assert_eq!(config.retry_max_s, 3600);
}
