//! Score comparability benchmark: Plan §15 OP#4 validation.
//!
//! Runs a matrix of tests to validate that `_rankingScore` values remain
//! comparable across shards with very different document-count distributions.
//!
//! For each configuration, we measure Kendall Tau correlation between
//! sharded and ground-truth (single-index) result orderings across many
//! random queries.

use miroir_core::score_comparability::{simulate, SimParams};

const THRESHOLD: f64 = 0.95;

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  Score Comparability Benchmark — Plan §15 OP#4 Validation      ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    let test_matrix: Vec<(&str, SimParams)> = vec![
        (
            "Baseline: uniform distribution, small corpus",
            SimParams {
                total_docs: 10_000,
                shard_count: 8,
                skew_factor: 1.0, // Uniform
                num_queries: 1000,
                top_k: 20,
                seed: 42,
            },
        ),
        (
            "Moderate skew (10×), medium corpus",
            SimParams {
                total_docs: 100_000,
                shard_count: 16,
                skew_factor: 10.0,
                num_queries: 1000,
                top_k: 20,
                seed: 42,
            },
        ),
        (
            "High skew (100×), large corpus",
            SimParams {
                total_docs: 1_000_000,
                shard_count: 32,
                skew_factor: 100.0,
                num_queries: 1000,
                top_k: 20,
                seed: 42,
            },
        ),
        (
            "Extreme skew (1000×), many shards",
            SimParams {
                total_docs: 500_000,
                shard_count: 64,
                skew_factor: 1000.0,
                num_queries: 1000,
                top_k: 20,
                seed: 42,
            },
        ),
        (
            "Worst case: tiny sparse shards (0.01× median)",
            SimParams {
                total_docs: 200_000,
                shard_count: 32,
                skew_factor: 10000.0,
                num_queries: 1000,
                top_k: 20,
                seed: 42,
            },
        ),
    ];

    let mut results = Vec::new();

    for (label, params) in &test_matrix {
        println!("Running: {}", label);
        let start = std::time::Instant::now();
        let result = simulate(params);
        let elapsed = start.elapsed();
        results.push((label, params.clone(), result, elapsed));
        println!("  Completed in {:.2?}", elapsed);
        println!();
    }

    // Print summary table.
    println!("═══════════════════════════════════════════════════════════════════");
    println!("Summary Table");
    println!("═══════════════════════════════════════════════════════════════════");
    println!();

    println!(
        "{:<45} {:>10} {:>10} {:>8} {:>8} {:>8} {:>8} {:>10}",
        "Scenario", "Docs", "Shards", "Skew", "PopCV", "Meanτ", "Stdτ", "%≥0.95"
    );
    println!("{}", "-".repeat(120));

    for (label, params, result, _elapsed) in &results {
        let docs_str = format!("{:.1}M", params.total_docs as f64 / 1_000_000.0);
        let skew_str = format!("{:.0}×", params.skew_factor);
        let cv_str = format!("{:.2}%", result.aggregate.shard_pop_cv * 100.0);

        println!(
            "{:<45} {:>10} {:>10} {:>8} {:>8} {:>8.3} {:>8.3} {:>10.1}%",
            label,
            docs_str,
            params.shard_count,
            skew_str,
            cv_str,
            result.aggregate.mean_kendall_tau,
            result.aggregate.std_kendall_tau,
            result.aggregate.percent_above_threshold
        );
    }

    println!();
    println!("═══════════════════════════════════════════════════════════════════");
    println!("Detailed Results (JSON)");
    println!("═══════════════════════════════════════════════════════════════════");
    println!();

    for (label, params, result, elapsed) in &results {
        println!("--- {} ---", label);
        println!("  configuration:");
        println!("    total_docs: {}", params.total_docs);
        println!("    shard_count: {}", params.shard_count);
        println!("    skew_factor: {:.0}×", params.skew_factor);
        println!("    num_queries: {}", params.num_queries);
        println!("    top_k: {}", params.top_k);
        println!();
        println!("  shard population:");
        println!("    cv: {:.4} ({:.2}%)", result.aggregate.shard_pop_cv, result.aggregate.shard_pop_cv * 100.0);
        println!(
            "    max/median ratio: {:.2}×",
            result.aggregate.shard_pop_ratio
        );
        println!("    per-shard counts: {:?}", result.shard_doc_counts);
        println!();
        println!("  kendall tau:");
        println!(
            "    mean: {:.4} ± {:.4}",
            result.aggregate.mean_kendall_tau, result.aggregate.std_kendall_tau
        );
        println!(
            "    min: {:.4}, max: {:.4}",
            result.aggregate.min_kendall_tau, result.aggregate.max_kendall_tau
        );
        println!(
            "    ≥ {}: {:.1}%",
            THRESHOLD, result.aggregate.percent_above_threshold
        );
        println!();
        println!("  jaccard similarity:");
        println!("    mean: {:.4}", result.aggregate.mean_jaccard);
        println!();
        println!("  first divergence:");
        println!(
            "    mean position: {:.1}",
            result.aggregate.mean_first_divergence
        );
        println!();
        println!("  computed in: {:.2?}", elapsed);
        println!();
    }

    // Print worst-case queries (those with lowest Kendall Tau).
    println!("═══════════════════════════════════════════════════════════════════");
    println!("Worst-Case Queries (lowest τ)");
    println!("═══════════════════════════════════════════════════════════════════");
    println!();

    for (label, _params, result, _elapsed) in &results {
        let mut worst_queries: Vec<_> = result.query_results.iter().collect();
        worst_queries.sort_by(|a, b| a.kendall_tau.partial_cmp(&b.kendall_tau).unwrap());

        println!("--- {} ---", label);
        for qr in worst_queries.iter().take(5) {
            println!(
                "  Query {}: τ={:.4}, Jaccard={:.4}, first_div={}",
                qr.query_id, qr.kendall_tau, qr.jaccard_similarity, qr.first_divergence_position
            );
            println!("    Shard stats:");
            for stat in &qr.shard_score_stats {
                println!(
                    "      Shard {}: {} docs, {} hits, score range [{:.3}, {:.3}]",
                    stat.shard_id, stat.doc_count, stat.hit_count, stat.min_score, stat.max_score
                );
            }
        }
        println!();
    }

    // Validate threshold.
    println!("═══════════════════════════════════════════════════════════════════");
    println!("Threshold Validation (τ ≥ {:.2})", THRESHOLD);
    println!("═══════════════════════════════════════════════════════════════════");
    println!();

    let mut all_pass = true;
    for (label, _params, result, _) in &results {
        let passes = result.aggregate.mean_kendall_tau >= THRESHOLD;
        let status = if passes { "PASS" } else { "FAIL" };
        println!(
            "  [{}] {}: mean τ = {:.4}",
            status, label, result.aggregate.mean_kendall_tau
        );

        if !passes {
            all_pass = false;
        }
    }

    println!();
    if all_pass {
        println!(
            "All scenarios PASSED the τ ≥ {:.2} threshold.",
            THRESHOLD
        );
        println!("Score comparability is maintained across shard population skew.");
    } else {
        println!(
            "Some scenarios FAILED the τ ≥ {:.2} threshold.",
            THRESHOLD
        );
        println!("Score normalization may be needed for skewed shard distributions.");
    }

    // Generate JSON output for programmatic consumption.
    println!();
    println!("═══════════════════════════════════════════════════════════════════");
    println!("JSON Output");
    println!("═══════════════════════════════════════════════════════════════════");
    println!();

    let json_output: Vec<serde_json::Value> = results
        .iter()
        .map(|(label, _params, result, _elapsed)| {
            serde_json::json!({
                "scenario": label,
                "aggregate": result.aggregate,
            })
        })
        .collect();

    println!("{}", serde_json::to_string_pretty(&json_output).unwrap());

    // Exit with appropriate code.
    if !all_pass {
        std::process::exit(1);
    }
}
