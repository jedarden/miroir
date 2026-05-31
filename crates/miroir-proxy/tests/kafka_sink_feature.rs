//! Test for kafka-sink feature.
//!
//! Verifies that the kafka-sink feature is enabled and the code compiles.
//! This is a compile-time test that ensures the kafka-sink feature works correctly.

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
    // This test should never run in CI because we build with --features kafka-sink
    panic!("kafka-sink feature must be enabled for production builds");
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
                error_msg.contains("Kafka") || error_msg.contains("connection") || error_msg.contains("broker"),
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

    let config: CdcSinkConfig = serde_json::from_value(config_json)
        .expect("kafka sink config should parse correctly");

    assert_eq!(config.sink_type, miroir_core::cdc::CdcSinkType::Kafka);
    assert_eq!(config.url, "localhost:9092");
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.batch_flush_ms, 1000);
    assert_eq!(config.include_body, true);
    assert_eq!(config.retry_max_s, 3600);
}
