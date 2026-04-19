//! Resharding load benchmark: runs the plan §15 OP#3 test matrix.
//!
//! Validates the 2× transient storage/write load caveat under varied
//! doc sizes, corpus sizes, write rates, and topology configurations.

use miroir_core::reshard::{simulate, SimParams};

const GB: u64 = 1024 * 1024 * 1024;
const TB: u64 = 1024 * GB;
const KB: u64 = 1024;
const MB: u64 = 1024 * KB;

fn main() {
    let test_matrix: Vec<(&str, SimParams)> = vec![
        (
            "Small docs, moderate corpus",
            SimParams {
                doc_size_bytes: KB,
                corpus_size_bytes: 10 * GB,
                write_rate_dps: 100,
                replica_groups: 2,
                replication_factor: 1,
                old_shards: 64,
                new_shards: 128,
                nodes_per_group: 3,
                backfill_throttle_dps: 10_000,
            },
        ),
        (
            "Medium docs, large corpus",
            SimParams {
                doc_size_bytes: 10 * KB,
                corpus_size_bytes: 100 * GB,
                write_rate_dps: 1000,
                replica_groups: 2,
                replication_factor: 2,
                old_shards: 64,
                new_shards: 128,
                nodes_per_group: 4,
                backfill_throttle_dps: 10_000,
            },
        ),
        (
            "Large blobs, very large corpus",
            SimParams {
                doc_size_bytes: MB,
                corpus_size_bytes: TB,
                write_rate_dps: 10,
                replica_groups: 2,
                replication_factor: 1,
                old_shards: 64,
                new_shards: 128,
                nodes_per_group: 4,
                backfill_throttle_dps: 5_000,
            },
        ),
    ];

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  Resharding Load Benchmark — Plan §15 OP#3 Validation         ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    let mut results = Vec::new();

    for (label, params) in &test_matrix {
        let start = std::time::Instant::now();
        let result = simulate(params);
        let elapsed = start.elapsed();
        results.push((label, params.clone(), result, elapsed));
    }

    // Print summary table.
    println!(
        "{:<30} {:>10} {:>10} {:>8} {:>8} {:>14} {:>14} {:>10} {:>10}",
        "Scenario", "DocSize", "Corpus", "RG", "RF", "StorageAmp", "PeakWriteAmp", "OldCV", "NewCV"
    );
    println!("{}", "-".repeat(130));

    for (label, params, result, elapsed) in &results {
        let doc_size_str = if params.doc_size_bytes >= MB {
            format!("{}MB", params.doc_size_bytes / MB)
        } else {
            format!("{}KB", params.doc_size_bytes / KB)
        };
        let corpus_str = if params.corpus_size_bytes >= TB {
            format!("{}TB", params.corpus_size_bytes / TB)
        } else {
            format!("{}GB", params.corpus_size_bytes / GB)
        };

        println!(
            "{:<30} {:>10} {:>10} {:>8} {:>8} {:>14.2}× {:>14.2}× {:>10.4} {:>10.4}",
            label,
            doc_size_str,
            corpus_str,
            params.replica_groups,
            params.replication_factor,
            result.storage_amplification,
            result.peak_write_amplification,
            result.old_shard_cv,
            result.new_shard_cv,
        );
        println!("  (computed in {:.2?})", elapsed);
    }

    println!();
    println!("═══════════════════════════════════════════════════════════════════");
    println!("Detailed Results (JSON)");
    println!("═══════════════════════════════════════════════════════════════════");
    println!();

    for (label, params, result, _elapsed) in &results {
        println!("--- {} ---", label);
        println!("  doc_size: {} bytes", params.doc_size_bytes);
        println!(
            "  corpus: {} bytes ({:.2} GB)",
            params.corpus_size_bytes,
            params.corpus_size_bytes as f64 / GB as f64
        );
        println!("  total_docs: {}", result.total_docs);
        println!("  write_rate: {} dps", params.write_rate_dps);
        println!(
            "  topology: RG={}, RF={}, nodes/group={}",
            params.replica_groups, params.replication_factor, params.nodes_per_group
        );
        println!(
            "  old_shards: {} → new_shards: {}",
            params.old_shards, params.new_shards
        );
        println!();
        println!("  STORAGE:");
        println!(
            "    normal (steady-state): {:.4} GB",
            result.normal_storage_bytes as f64 / GB as f64
        );
        println!(
            "    peak (resharding):     {:.4} GB",
            result.peak_storage_bytes as f64 / GB as f64
        );
        println!(
            "    amplification:         {:.2}×",
            result.storage_amplification
        );
        println!(
            "    per-node normal:       {:.4} GB",
            result.per_node_normal_storage_bytes as f64 / GB as f64
        );
        println!(
            "    per-node peak:         {:.4} GB",
            result.per_node_peak_storage_bytes as f64 / GB as f64
        );
        println!();
        println!("  WRITE LOAD:");
        println!(
            "    normal rate:    {} writes/sec",
            result.normal_write_rate
        );
        println!(
            "    dual-write:     {} writes/sec ({:.1}×)",
            result.dual_write_rate, result.dual_write_amplification
        );
        println!(
            "    peak (bf+dw):   {} writes/sec ({:.2}×)",
            result.peak_write_rate, result.peak_write_amplification
        );
        println!();
        println!("  BACKFILL:");
        println!("    throttle: {} docs/sec", params.backfill_throttle_dps);
        println!(
            "    duration: {:.2} seconds ({:.2} hours)",
            result.backfill_duration_secs,
            result.backfill_duration_secs / 3600.0
        );
        println!(
            "    total bytes written: {:.4} GB",
            result.total_bytes_written as f64 / GB as f64
        );
        println!();
        println!("  DISTRIBUTION:");
        println!(
            "    old shard CV: {:.6} ({:.2}%)",
            result.old_shard_cv,
            result.old_shard_cv * 100.0
        );
        println!(
            "    new shard CV: {:.6} ({:.2}%)",
            result.new_shard_cv,
            result.new_shard_cv * 100.0
        );
        println!();
    }

    // Validate invariants.
    println!("═══════════════════════════════════════════════════════════════════");
    println!("Invariant Checks");
    println!("═══════════════════════════════════════════════════════════════════");

    let mut all_pass = true;
    for (label, _params, result, _) in &results {
        let storage_ok = (result.storage_amplification - 2.0).abs() < 0.01;
        let dual_write_ok = (result.dual_write_amplification - 2.0).abs() < 0.01;
        let cv_ok = result.old_shard_cv < 0.05 && result.new_shard_cv < 0.05;

        let status = |ok| if ok { "PASS" } else { "FAIL" };
        println!("  {}:", label);
        println!(
            "    storage amplification == 2.0×:  {} ({:.4}×)",
            status(storage_ok),
            result.storage_amplification
        );
        println!(
            "    dual-write amplification == 2.0×: {} ({:.4}×)",
            status(dual_write_ok),
            result.dual_write_amplification
        );
        println!(
            "    hash distribution CV < 5%:      {} (old={:.4}%, new={:.4}%)",
            status(cv_ok),
            result.old_shard_cv * 100.0,
            result.new_shard_cv * 100.0
        );

        if !storage_ok || !dual_write_ok || !cv_ok {
            all_pass = false;
        }
    }

    println!();
    if all_pass {
        println!("All invariant checks PASSED. The 2× transient load caveat is confirmed.");
    } else {
        println!("Some invariant checks FAILED. Review results above.");
        std::process::exit(1);
    }
}
