//! P5.1.f: Verify backfill progress ratio is non-zero and monotonic from start.
//!
//! This test verifies the acceptance criteria for backfill progress:
//! - Progress ratio is > 0 immediately after backfill starts (not 0.0)
//! - Progress ratio is monotonically non-decreasing from start to completion
//! - Progress ratio reaches exactly 1.0 at completion
//! - No correctness/behavior change to the reshard state machine or document migration
//!
//! Run with: cargo test -p miroir-core p5_1_f_reshard_progress_verification

use miroir_core::reshard::{ReshardOperation, ReshardPhase};

#[test]
fn test_backfill_progress_at_start_is_zero() {
    // Verify that at the very start, before any backfill happens,
    // the progress ratio is 0.0 (this is expected behavior)
    let op = ReshardOperation::new("test-index".to_string(), 2, 4);

    // At initialization, both documents_backfilled and total_documents are 0
    assert_eq!(op.documents_backfilled, 0);
    assert_eq!(op.total_documents, 0);

    // Therefore, progress is 0.0
    assert_eq!(op.backfill_progress(), 0.0);
}

#[test]
fn test_backfill_progress_after_first_update_is_nonzero() {
    // Verify that after the first progress update, the ratio becomes > 0
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    // Simulate the first update: 100 documents backfilled out of 1000 total
    op.update_backfill_progress(100, 1000);

    // Progress should be > 0
    let progress = op.backfill_progress();
    assert!(
        progress > 0.0,
        "Progress should be > 0 after first update, got {}",
        progress
    );
    assert!(
        (progress - 0.1).abs() < 0.001,
        "Progress should be ~0.1, got {}",
        progress
    );
}

#[test]
fn test_backfill_progress_nonzero_once_backfill_in_progress() {
    // miroir-8ce63226: in a started backfill state — operation advanced to the
    // BackfillInProgress phase with documents migrating — the ratio must be > 0
    // even for the very first document out of many.
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    op.advance_phase(ReshardPhase::BackfillInProgress);
    assert_eq!(op.phase, ReshardPhase::BackfillInProgress);
    op.update_backfill_progress(1, 1000);

    let progress = op.backfill_progress();
    assert!(
        progress > 0.0,
        "Progress should be > 0 once backfill has started, got {}",
        progress
    );
    // The ratio must be the expected 1/1000, not some sentinel value.
    assert!(
        (progress - 0.001).abs() < 1e-9,
        "Expected 1/1000 = 0.001 once backfill has started, got {}",
        progress
    );
    // A backfill that just started must never read as complete.
    assert!(
        progress < 1.0,
        "Backfill that just started must not report completion, got {}",
        progress
    );
}

#[test]
fn test_backfill_progress_is_monotonic() {
    // Verify that progress never decreases
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    let total = 1000u64;
    let mut previous_progress = 0.0;

    // Simulate progress in chunks
    for chunk in 1..=10 {
        let backfilled = chunk * 100;
        op.update_backfill_progress(backfilled, total);

        let progress = op.backfill_progress();

        // Progress should be >= previous progress (monotonically non-decreasing)
        assert!(
            progress >= previous_progress,
            "Progress decreased from {} to {} at chunk {}",
            previous_progress,
            progress,
            chunk
        );

        previous_progress = progress;
    }

    // Final progress should be exactly 1.0
    assert_eq!(
        previous_progress, 1.0,
        "Final progress should be 1.0, got {}",
        previous_progress
    );
}

#[test]
fn test_backfill_progress_exactly_one_at_completion() {
    // Verify that progress reaches exactly 1.0 at completion
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    let total = 1000u64;

    // At completion, backfilled == total
    op.update_backfill_progress(total, total);

    let progress = op.backfill_progress();
    assert_eq!(
        progress, 1.0,
        "Progress should be exactly 1.0 at completion, got {}",
        progress
    );
}

#[test]
fn test_backfill_progress_with_partial_updates() {
    // Verify progress behaves correctly with partial updates
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    let total = 500u64;

    // Simulate various update patterns
    let updates = vec![
        (50, total),  // 10% complete
        (150, total), // 30% complete
        (250, total), // 50% complete
        (400, total), // 80% complete
        (500, total), // 100% complete
    ];

    let mut previous_progress = 0.0;

    for (backfilled, expected_total) in updates {
        op.update_backfill_progress(backfilled, expected_total);

        let progress = op.backfill_progress();

        // Verify monotonic increase
        assert!(
            progress >= previous_progress,
            "Progress decreased from {} to {}",
            previous_progress,
            progress
        );

        // Verify progress is in valid range [0, 1]
        assert!(
            progress >= 0.0 && progress <= 1.0,
            "Progress {} is outside valid range [0, 1]",
            progress
        );

        previous_progress = progress;
    }

    // Final progress should be exactly 1.0
    assert_eq!(previous_progress, 1.0);
}

#[test]
fn test_backfill_progress_zero_total_handling() {
    // Verify that when total_documents is 0, progress is 0.0 (not NaN)
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    // Update with zero total
    op.update_backfill_progress(0, 0);

    let progress = op.backfill_progress();

    // Should return 0.0, not NaN
    assert!(!progress.is_nan(), "Progress should not be NaN");
    assert_eq!(progress, 0.0, "Progress should be 0.0 when total is 0");
}

#[test]
fn test_backfill_progress_no_decrease_on_total_change() {
    // Verify that progress doesn't decrease when total_documents changes
    // (this should not happen in practice, but we verify robustness)
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    // First update: 100 out of 200 (50%)
    op.update_backfill_progress(100, 200);
    let progress1 = op.backfill_progress();

    // Second update: 100 out of 100 (this would be 100% if total decreased)
    op.update_backfill_progress(100, 100);
    let progress2 = op.backfill_progress();

    // Progress should not decrease (it should stay the same or increase)
    assert!(
        progress2 >= progress1,
        "Progress decreased from {} to {} when total changed",
        progress1,
        progress2
    );
}

#[test]
fn test_backfill_progress_large_values() {
    // Verify progress works correctly with large document counts
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    let total = 10_000_000u64; // 10 million documents

    // At 50% completion
    op.update_backfill_progress(5_000_000, total);
    let progress = op.backfill_progress();

    assert_eq!(
        progress, 0.5,
        "Progress should be 0.5 for 5M out of 10M, got {}",
        progress
    );

    // At completion
    op.update_backfill_progress(total, total);
    let final_progress = op.backfill_progress();

    assert_eq!(
        final_progress, 1.0,
        "Final progress should be 1.0, got {}",
        final_progress
    );
}

#[test]
fn test_backfill_progress_unchanged_when_no_update() {
    // Verify that progress doesn't change when no update is called
    let mut op = ReshardOperation::new("test-index".to_string(), 2, 4);

    op.update_backfill_progress(100, 1000);
    let progress1 = op.backfill_progress();

    // Don't call update_backfill_progress - progress should remain the same
    let progress2 = op.backfill_progress();

    assert_eq!(
        progress1, progress2,
        "Progress should remain unchanged when no update occurs"
    );
}
