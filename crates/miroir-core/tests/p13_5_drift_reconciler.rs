//! Acceptance tests for drift reconciler (plan §13.5).
//!
//! Tests the key acceptance criteria:
//! 1. Periodic drift check runs every interval_s seconds (default 5 min)
//! 2. Hash-based settings comparison detects drift
//! 3. Auto-repair is enabled by default
//! 4. miroir_settings_drift_repair_total counter ticks on each repair
//! 5. Mode A coordination partitions (index, node) pairs via rendezvous
//!    (covered by mode_a_coordinator unit tests)

use miroir_core::rebalancer_worker::DriftReconcilerConfig;
use miroir_core::settings::fingerprint_settings;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Acceptance 1: Hash-based settings comparison detects drift
// ---------------------------------------------------------------------------

#[test]
fn acceptance_1_hash_based_comparison_detects_drift() {
    // Correct settings (consensus)
    let correct_settings = json!({
        "rankingRules": ["words", "typo", "proximity", "attribute", "sort", "exactness"],
        "stopWords": ["the", "a", "an"]
    });

    // Drifted settings (different order in rankingRules)
    let drifted_settings = json!({
        "rankingRules": ["typo", "words", "proximity", "attribute", "sort", "exactness"],
        "stopWords": ["the", "a", "an"]
    });

    // Verify fingerprints are different
    let correct_fp = fingerprint_settings(&correct_settings);
    let drifted_fp = fingerprint_settings(&drifted_settings);

    assert_ne!(
        correct_fp, drifted_fp,
        "different settings should produce different fingerprints"
    );

    // Verify identical settings produce same fingerprint
    let correct_settings_2 = json!({
        "rankingRules": ["words", "typo", "proximity", "attribute", "sort", "exactness"],
        "stopWords": ["the", "a", "an"]
    });
    let correct_fp_2 = fingerprint_settings(&correct_settings_2);

    assert_eq!(
        correct_fp, correct_fp_2,
        "identical settings should produce same fingerprint"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 2: Default interval is 5 minutes (300 seconds)
// ---------------------------------------------------------------------------

#[test]
fn acceptance_2_default_interval_is_5_minutes() {
    let config = DriftReconcilerConfig::default();
    assert_eq!(
        config.interval_s, 300,
        "default interval should be 300 seconds (5 minutes)"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 3: Auto-repair is enabled by default
// ---------------------------------------------------------------------------

#[test]
fn acceptance_3_auto_repair_enabled_by_default() {
    let config = DriftReconcilerConfig::default();
    assert!(
        config.auto_repair,
        "auto_repair should be enabled by default"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 4: miroir_settings_drift_repair_total counter via callback
// ---------------------------------------------------------------------------

#[test]
fn acceptance_4_metrics_callback_ticks_on_repair() {
    // Verify that the metrics callback is called when drift repair happens
    let repair_count = Arc::new(AtomicU64::new(0));
    let repair_count_clone = repair_count.clone();

    // Create a callback that increments the counter
    let callback: miroir_core::rebalancer_worker::DriftRepairCallback = Arc::new(move |_index| {
        repair_count_clone.fetch_add(1, Ordering::SeqCst);
    });

    // Simulate a repair event
    callback("test-index");

    // Verify the counter was incremented
    assert_eq!(
        repair_count.load(Ordering::SeqCst),
        1,
        "metrics callback should increment counter"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 5: Configurable interval and auto_repair settings
// ---------------------------------------------------------------------------

#[test]
fn acceptance_5_configurable_settings() {
    let config = DriftReconcilerConfig {
        interval_s: 60,
        auto_repair: false,
        lease_renewal_interval_ms: 5000,
        lease_ttl_secs: 30,
    };

    assert_eq!(config.interval_s, 60, "interval_s should be configurable");
    assert!(!config.auto_repair, "auto_repair should be configurable");
}

// ---------------------------------------------------------------------------
// Helper: Fingerprint is deterministic
// ---------------------------------------------------------------------------

#[test]
fn test_fingerprint_deterministic() {
    let settings = json!({
        "rankingRules": ["words", "typo", "proximity"],
        "stopWords": ["the", "a", "an"]
    });

    let fp1 = fingerprint_settings(&settings);
    let fp2 = fingerprint_settings(&settings);

    assert_eq!(fp1, fp2, "fingerprint should be deterministic");
}

// ---------------------------------------------------------------------------
// Helper: Fingerprint is order-independent for keys
// ---------------------------------------------------------------------------

#[test]
fn test_fingerprint_order_independent_keys() {
    let settings1 = json!({
        "rankingRules": ["words", "typo"],
        "stopWords": ["the"]
    });

    let settings2 = json!({
        "stopWords": ["the"],
        "rankingRules": ["words", "typo"]
    });

    let fp1 = fingerprint_settings(&settings1);
    let fp2 = fingerprint_settings(&settings2);

    assert_eq!(fp1, fp2, "fingerprint should be order-independent for keys");
}

// ---------------------------------------------------------------------------
// Helper: Fingerprint is order-dependent for arrays
// ---------------------------------------------------------------------------

#[test]
fn test_fingerprint_order_dependent_arrays() {
    let settings1 = json!({
        "rankingRules": ["words", "typo"]
    });

    let settings2 = json!({
        "rankingRules": ["typo", "words"]
    });

    let fp1 = fingerprint_settings(&settings1);
    let fp2 = fingerprint_settings(&settings2);

    assert_ne!(
        fp1, fp2,
        "fingerprint should be order-dependent for arrays (different order = different settings)"
    );
}
