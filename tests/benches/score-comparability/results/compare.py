#!/usr/bin/env python3
"""
Compare result rankings and compute Kendall tau correlation.

Kendall tau measures the ordinal association between two ranked sequences.
τ = (concordant pairs - discordant pairs) / total pairs
Range: [-1, 1], where 1 = perfect agreement, 0 = independent, -1 = perfect disagreement
"""

import argparse
import json
import math
from pathlib import Path
from typing import List, Dict, Tuple


def kendall_tau(ranking1: List[str], ranking2: List[str]) -> Tuple[float, Dict]:
    """
    Compute Kendall tau correlation between two rankings.

    Uses O(n log n) algorithm via sorting.
    """
    # Build position maps
    pos1 = {doc_id: i for i, doc_id in enumerate(ranking1)}
    pos2 = {doc_id: i for i, doc_id in enumerate(ranking2)}

    # Get common documents (documents that appear in both rankings)
    common_docs = set(pos1.keys()) & set(pos2.keys())

    if len(common_docs) < 2:
        return 0.0, {
            "concordant": 0,
            "discordant": 0,
            "total_pairs": 0,
            "common_docs": len(common_docs),
            "only_in_r1": len(pos1) - len(common_docs),
            "only_in_r2": len(pos2) - len(common_docs),
        }

    # Sort common docs by position in ranking1
    sorted_by_r1 = sorted(common_docs, key=lambda x: pos1[x])

    # Count inversions in ranking2 order
    # Each inversion is a discordant pair
    r2_positions = [pos2[doc] for doc in sorted_by_r1]

    # Count discordant pairs using merge sort
    def count_inversions(arr):
        if len(arr) <= 1:
            return arr, 0

        mid = len(arr) // 2
        left, inv_left = count_inversions(arr[:mid])
        right, inv_right = count_inversions(arr[mid:])

        merged = []
        inv_count = inv_left + inv_right
        i = j = 0

        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                merged.append(left[i])
                i += 1
            else:
                merged.append(right[j])
                inv_count += len(left) - i
                j += 1

        merged.extend(left[i:])
        merged.extend(right[j:])

        return merged, inv_count

    _, discordant = count_inversions(r2_positions)
    total_pairs = len(common_docs) * (len(common_docs) - 1) // 2
    concordant = total_pairs - discordant

    tau = (concordant - discordant) / total_pairs if total_pairs > 0 else 0.0

    return tau, {
        "concordant": concordant,
        "discordant": discordant,
        "total_pairs": total_pairs,
        "common_docs": len(common_docs),
        "only_in_r1": len(pos1) - len(common_docs),
        "only_in_r2": len(pos2) - len(common_docs),
    }


def load_results(results_file: Path) -> Dict:
    """Load search results from JSON file."""
    with open(results_file) as f:
        return json.load(f)


def extract_ranking(results: Dict, top_k: int = None) -> List[str]:
    """Extract document IDs from search results in ranking order."""
    hits = results.get("hits", [])
    if top_k:
        hits = hits[:top_k]
    return [hit.get("id") or hit.get("_id", "") for hit in hits]


def compare_query_sets(
    ground_truth_file: Path,
    distributed_file: Path,
    top_k: int = 100,
) -> Dict:
    """
    Compare two query result sets.

    Returns statistics including:
    - Average Kendall tau across all queries
    - Per-query tau values
    - Query types where divergence is highest
    """
    with open(ground_truth_file) as f:
        ground_truth = {json.loads(line)["query_id"]: json.loads(line) for line in f}

    with open(distributed_file) as f:
        distributed = {json.loads(line)["query_id"]: json.loads(line) for line in f}

    results = []
    tau_by_type = {}

    for query_id, gt_result in ground_truth.items():
        if query_id not in distributed:
            continue

        dist_result = distributed[query_id]

        gt_ranking = extract_ranking(gt_result, top_k)
        dist_ranking = extract_ranking(dist_result, top_k)

        if not gt_ranking or not dist_ranking:
            continue

        tau, details = kendall_tau(gt_ranking, dist_ranking)

        query_type = gt_result.get("type", "unknown")
        if query_type not in tau_by_type:
            tau_by_type[query_type] = []
        tau_by_type[query_type].append(tau)

        results.append({
            "query_id": query_id,
            "query_type": query_type,
            "tau": tau,
            "details": details,
            "query": gt_result.get("q", ""),
        })

    if not results:
        return {"error": "No common queries found"}

    # Compute statistics
    tau_values = [r["tau"] for r in results]
    avg_tau = sum(tau_values) / len(tau_values)
    min_tau = min(tau_values)
    max_tau = max(tau_values)

    # Count queries below threshold
    below_095 = sum(1 for t in tau_values if t < 0.95)
    below_090 = sum(1 for t in tau_values if t < 0.90)
    below_080 = sum(1 for t in tau_values if t < 0.80)

    # 95% confidence intervals (normal approximation, n >= 10000)
    variance = sum((t - avg_tau) ** 2 for t in tau_values) / (len(tau_values) - 1)
    stddev = math.sqrt(variance)
    stderr = stddev / math.sqrt(len(tau_values))
    z = 1.96
    ci_low = avg_tau - z * stderr
    ci_high = avg_tau + z * stderr

    # Per-type statistics
    type_stats = {}
    for qtype, taus in tau_by_type.items():
        tn = len(taus)
        tmean = sum(taus) / tn if taus else 0
        tvar = sum((t - tmean) ** 2 for t in taus) / (tn - 1) if tn > 1 else 0
        tsd = math.sqrt(tvar)
        tse = tsd / math.sqrt(tn) if tn > 0 else 0
        type_stats[qtype] = {
            "count": tn,
            "avg_tau": tmean,
            "min_tau": min(taus) if taus else 0,
            "max_tau": max(taus) if taus else 0,
            "ci_95": [tmean - z * tse, tmean + z * tse] if tn > 1 else None,
            "stddev": tsd,
        }

    return {
        "total_queries": len(results),
        "avg_tau": avg_tau,
        "min_tau": min_tau,
        "max_tau": max_tau,
        "ci_95": [ci_low, ci_high],
        "stddev": stddev,
        "stderr": stderr,
        "below_095_count": below_095,
        "below_090_count": below_090,
        "below_080_count": below_080,
        "pass_criteria": avg_tau >= 0.95,
        "type_stats": type_stats,
        "per_query": results,
    }


def main():
    parser = argparse.ArgumentParser(description="Compare search result rankings")
    parser.add_argument("ground_truth", type=str, help="Ground truth results file (JSONL)")
    parser.add_argument("distributed", type=str, help="Distributed results file (JSONL)")
    parser.add_argument("--output", type=str, help="Output file for comparison report")
    parser.add_argument("--top-k", type=int, default=100, help="Compare top K results")
    parser.add_argument("--verbose", action="store_true", help="Show per-query details")

    args = parser.parse_args()

    result = compare_query_sets(
        Path(args.ground_truth),
        Path(args.distributed),
        args.top_k,
    )

    if "error" in result:
        print(f"Error: {result['error']}")
        return

    # Print summary
    print(f"Comparison Summary (top-{args.top_k})")
    print(f"=" * 50)
    print(f"Total queries: {result['total_queries']}")
    ci = result['ci_95']
    print(f"Avg Kendall tau: {result['avg_tau']:.4f} (95% CI: [{ci[0]:.4f}, {ci[1]:.4f}])")
    print(f"Min tau: {result['min_tau']:.4f}")
    print(f"Max tau: {result['max_tau']:.4f}")
    print(f"Queries below 0.95: {result['below_095_count']} ({100*result['below_095_count']/result['total_queries']:.1f}%)")
    print(f"Queries below 0.90: {result['below_090_count']} ({100*result['below_090_count']/result['total_queries']:.1f}%)")
    print(f"Queries below 0.80: {result['below_080_count']} ({100*result['below_080_count']/result['total_queries']:.1f}%)")
    print(f"Pass criteria (avg >= 0.95): {'PASS' if result['pass_criteria'] else 'FAIL'}")

    print(f"\nPer-query type:")
    for qtype, stats in result["type_stats"].items():
        ci_str = f", 95% CI: [{stats['ci_95'][0]:.4f}, {stats['ci_95'][1]:.4f}]" if stats.get('ci_95') else ""
        print(f"  {qtype}: avg={stats['avg_tau']:.4f}{ci_str}, min={stats['min_tau']:.4f}, max={stats['max_tau']:.4f} (n={stats['count']})")

    if args.verbose:
        print(f"\nPer-query details:")
        for qr in sorted(result["per_query"], key=lambda x: x["tau"])[:10]:
            print(f"  {qr['query_id']}: tau={qr['tau']:.4f} ({qr['query_type']}) - '{qr['query'][:50]}'")
        print(f"  ... (showing 10 worst)")

    # Save to file if requested
    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
