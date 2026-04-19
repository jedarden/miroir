#!/usr/bin/env python3
"""
Simulate score comparability experiments using a simplified BM25-like model.

This simulation demonstrates the theoretical issue with local statistics:
- Each shard computes IDF using only its local document frequencies
- When shards have very different document distributions, scores diverge
- The merger then produces incorrect global rankings

BM25 components:
- IDF(q) = log((N - df(q) + 0.5) / (df(q) + 0.5))
  where N = total docs in shard/index, df(q) = docs containing term q
- TF(q,d) = f(q,d) * (k1 + 1) / (f(q,d) + k1 * (1 - b + b * |d| / avgdl))
  where f(q,d) = term frequency in doc, |d| = doc length, avgdl = avg doc length
- Score = sum over query terms: IDF(q) * TF(q,d)

For this simulation:
- We use simplified TF = term frequency
- We focus on IDF divergence (the main issue)
- We compute global IDF for ground truth, local IDF per shard for distributed
"""

import argparse
import json
import math
import random
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict


def load_corpus(corpus_dir: Path) -> Tuple[List[Dict], Dict]:
    """Load corpus and metadata."""
    with open(corpus_dir / "metadata.json") as f:
        metadata = json.load(f)

    docs = []
    with open(corpus_dir / "corpus.jsonl") as f:
        for line in f:
            docs.append(json.loads(line))

    return docs, metadata


def load_queries(query_file: Path) -> List[Dict]:
    """Load query set."""
    queries = []
    with open(query_file) as f:
        for line in f:
            queries.append(json.loads(line))
    return queries


def tokenize(text: str) -> Set[str]:
    """Simple tokenizer."""
    return set(text.lower().split())


def idf_global(df: int, N: int) -> float:
    """Standard IDF formula: log((N - df + 0.5) / (df + 0.5) + 1)."""
    if df == 0:
        return 0.0
    return math.log((N - df + 0.5) / (df + 0.5) + 1.0)


# Pre-computed per-document data for fast BM25 scoring.
DocData = Dict[str, object]  # {"id": str, "tf": {term: count}, "len": int, "title": str, "category": str}


def precompute_doc_data(docs: List[Dict]) -> List[DocData]:
    """Pre-compute term frequencies and lengths for all documents."""
    result = []
    for doc in docs:
        text = f"{doc['title']} {doc['content']}".lower()
        words = text.split()
        tf: Dict[str, int] = defaultdict(int)
        for w in words:
            tf[w] += 1
        result.append({
            "id": doc["id"],
            "title": doc["title"],
            "category": doc.get("category", ""),
            "tf": dict(tf),
            "len": len(words),
        })
    return result


def compute_stats(doc_data_list: List[DocData]) -> Tuple[Dict, int, float]:
    """Compute index statistics (df, N, avgdl) from pre-computed doc data."""
    df: Dict[str, int] = defaultdict(int)
    total_length = 0
    for dd in doc_data_list:
        for term in dd["tf"]:
            df[term] += 1
        total_length += dd["len"]
    N = len(doc_data_list)
    avgdl = total_length / N if N > 0 else 0
    return dict(df), N, avgdl


def build_inverted_index(doc_data_list: List[DocData]) -> Dict[str, List[int]]:
    """Build inverted index: term -> [doc_index, ...]."""
    index: Dict[str, List[int]] = defaultdict(list)
    for i, dd in enumerate(doc_data_list):
        for term in dd["tf"]:
            index[term].append(i)
    return dict(index)


def score_bm25(
    dd: DocData,
    query_terms: Set[str],
    df: Dict[str, int],
    N: int,
    avgdl: float,
    k1: float = 1.2,
    b: float = 0.75,
) -> float:
    """Compute BM25 score using pre-computed per-doc data."""
    doc_len = dd["len"]
    tf = dd["tf"]
    score = 0.0
    for term in query_terms:
        if term not in df:
            continue
        idf = idf_global(df[term], N)
        freq = tf.get(term, 0)
        if freq == 0:
            continue
        tf_norm = freq * (k1 + 1) / (freq + k1 * (1 - b + b * doc_len / avgdl))
        score += idf * tf_norm
    return score


def _collect_candidates(
    inv_index: Dict[str, List[int]],
    doc_categories: List[str],
    query_terms: Set[str],
    category_filter: str | None,
) -> List[int]:
    """Collect unique candidate doc indices from inverted index."""
    seen: Set[int] = set()
    candidates = []
    for term in query_terms:
        if term not in inv_index:
            continue
        for doc_idx in inv_index[term]:
            if doc_idx in seen:
                continue
            if category_filter and doc_categories[doc_idx] != category_filter:
                continue
            seen.add(doc_idx)
            candidates.append(doc_idx)
    return candidates


def simulate_search(
    doc_data: List[DocData],
    inv_index: Dict[str, List[int]],
    doc_categories: List[str],
    query: Dict,
    stats: Tuple[Dict, int, float],
    limit: int = 100,
) -> Dict:
    """Simulate search on a single index/shard using pre-computed data."""
    df, N, avgdl = stats
    query_terms = tokenize(query["q"])
    category_filter = query["filter"].split("=")[1].strip() if query.get("filter") else None

    candidate_indices = _collect_candidates(inv_index, doc_categories, query_terms, category_filter)

    scores = []
    for idx in candidate_indices:
        dd = doc_data[idx]
        s = score_bm25(dd, query_terms, df, N, avgdl)
        if s > 0:
            scores.append((dd, s))

    scores.sort(key=lambda x: x[1], reverse=True)

    hits = []
    for dd, s in scores[:limit]:
        hits.append({"id": dd["id"], "title": dd["title"], "score": s})

    return {
        "query_id": query["id"],
        "type": query.get("type", "unknown"),
        "q": query["q"],
        "filter": query.get("filter"),
        "hits": hits,
        "total_hits": len(scores),
    }


def simulate_distributed_search(
    shard_doc_data: Dict[int, List[DocData]],
    shard_indexes: Dict[int, Dict[str, List[int]]],
    shard_doc_categories: Dict[int, List[str]],
    shard_stats: Dict[int, Tuple[Dict, int, float]],
    query: Dict,
    limit: int = 100,
) -> Dict:
    """Distributed search with score-based merge (the problematic approach)."""
    query_terms = tokenize(query["q"])
    category_filter = query["filter"].split("=")[1].strip() if query.get("filter") else None
    per_shard_limit = limit * 2
    all_hits = []

    for shard_id in shard_doc_data:
        df, N, avgdl = shard_stats[shard_id]
        doc_data = shard_doc_data[shard_id]
        inv_index = shard_indexes[shard_id]
        doc_cats = shard_doc_categories[shard_id]

        candidate_indices = _collect_candidates(inv_index, doc_cats, query_terms, category_filter)

        shard_scores = []
        for idx in candidate_indices:
            dd = doc_data[idx]
            s = score_bm25(dd, query_terms, df, N, avgdl)
            if s > 0:
                shard_scores.append((dd, s))

        shard_scores.sort(key=lambda x: x[1], reverse=True)
        for dd, s in shard_scores[:per_shard_limit]:
            all_hits.append((dd, s, shard_id))

    all_hits.sort(key=lambda x: x[1], reverse=True)

    hits = []
    for dd, s, shard_id in all_hits[:limit]:
        hits.append({"id": dd["id"], "title": dd["title"], "score": s, "shard": shard_id})

    return {
        "query_id": query["id"],
        "type": query.get("type", "unknown"),
        "q": query["q"],
        "filter": query.get("filter"),
        "hits": hits,
        "total_hits": len(all_hits),
        "shards_queried": list(shard_doc_data.keys()),
    }


RRF_K = 60  # RRF constant, matching merger.rs


def simulate_distributed_search_rrf(
    shard_doc_data: Dict[int, List[DocData]],
    shard_indexes: Dict[int, Dict[str, List[int]]],
    shard_doc_categories: Dict[int, List[str]],
    shard_stats: Dict[int, Tuple[Dict, int, float]],
    query: Dict,
    limit: int = 100,
) -> Dict:
    """Distributed search using Reciprocal Rank Fusion."""
    query_terms = tokenize(query["q"])
    category_filter = query["filter"].split("=")[1].strip() if query.get("filter") else None
    per_shard_limit = limit * 2

    rrf_scores: Dict[str, float] = defaultdict(float)
    doc_info: Dict[str, Tuple[DocData, int]] = {}

    for shard_id in shard_doc_data:
        df, N, avgdl = shard_stats[shard_id]
        doc_data = shard_doc_data[shard_id]
        inv_index = shard_indexes[shard_id]
        doc_cats = shard_doc_categories[shard_id]

        candidate_indices = _collect_candidates(inv_index, doc_cats, query_terms, category_filter)

        shard_scores = []
        for idx in candidate_indices:
            dd = doc_data[idx]
            s = score_bm25(dd, query_terms, df, N, avgdl)
            if s > 0:
                shard_scores.append((dd, s))

        shard_scores.sort(key=lambda x: x[1], reverse=True)

        for rank, (dd, _s) in enumerate(shard_scores[:per_shard_limit]):
            doc_id = dd["id"]
            rrf_contribution = 1.0 / (RRF_K + rank + 1)
            rrf_scores[doc_id] += rrf_contribution
            if doc_id not in doc_info:
                doc_info[doc_id] = (dd, shard_id)

    sorted_docs = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)

    hits = []
    for doc_id, rrf_score in sorted_docs[:limit]:
        dd, shard_id = doc_info[doc_id]
        hits.append({"id": doc_id, "title": dd["title"], "score": rrf_score, "shard": shard_id})

    return {
        "query_id": query["id"],
        "type": query.get("type", "unknown"),
        "q": query["q"],
        "filter": query.get("filter"),
        "hits": hits,
        "total_hits": len(sorted_docs),
        "shards_queried": list(shard_doc_data.keys()),
        "merge_strategy": "rrf",
    }


def run_experiment(
    corpus_dir: Path,
    query_file: Path,
    output_dir: Path,
    shard_count: int = 10,
    limit: int = 100,
) -> Dict:
    """Run the full experiment."""
    print("Loading corpus...")
    docs, metadata = load_corpus(corpus_dir)
    print(f"  Total documents: {len(docs)}")
    print(f"  Shard count: {shard_count}")

    # Load per-shard data
    shards: Dict[int, List[Dict]] = {}
    for i in range(shard_count):
        shard_file = corpus_dir / f"shard-{i:02d}.jsonl"
        if shard_file.exists():
            shard_docs = []
            with open(shard_file) as f:
                for line in f:
                    shard_docs.append(json.loads(line))
            shards[i] = shard_docs
    print(f"  Loaded {len(shards)} shards")

    # Pre-compute per-document data
    print("\nPre-computing document data...")
    global_doc_data = precompute_doc_data(docs)
    global_doc_categories = [dd["category"] for dd in global_doc_data]
    global_inv_index = build_inverted_index(global_doc_data)
    global_stats = compute_stats(global_doc_data)
    print(f"  Global: N={global_stats[1]}, avgdl={global_stats[2]:.1f}, {len(global_inv_index)} terms")

    shard_doc_data: Dict[int, List[DocData]] = {}
    shard_indexes: Dict[int, Dict[str, List[int]]] = {}
    shard_doc_categories: Dict[int, List[str]] = {}
    shard_stats: Dict[int, Tuple[Dict, int, float]] = {}

    for shard_id, shard_docs in shards.items():
        sd = precompute_doc_data(shard_docs)
        shard_doc_data[shard_id] = sd
        shard_doc_categories[shard_id] = [dd["category"] for dd in sd]
        shard_indexes[shard_id] = build_inverted_index(sd)
        shard_stats[shard_id] = compute_stats(sd)
        print(f"  Shard {shard_id}: N={shard_stats[shard_id][1]}, "
              f"avgdl={shard_stats[shard_id][2]:.1f}, {len(shard_indexes[shard_id])} terms")

    # Load queries
    print(f"\nLoading queries from {query_file}...")
    queries = load_queries(query_file)
    print(f"  {len(queries)} queries")

    # Run experiments
    output_dir.mkdir(parents=True, exist_ok=True)

    ground_truth_file = output_dir / "ground-truth.jsonl"
    distributed_file = output_dir / "distributed.jsonl"
    rrf_file = output_dir / "distributed-rrf.jsonl"

    print(f"\nRunning experiments...")

    with open(ground_truth_file, "w") as gt_f, \
         open(distributed_file, "w") as dist_f, \
         open(rrf_file, "w") as rrf_f:
        for i, query in enumerate(queries):
            if (i + 1) % 1000 == 0:
                print(f"  Processed {i + 1} queries...")

            gt_result = simulate_search(
                global_doc_data, global_inv_index, global_doc_categories,
                query, global_stats, limit,
            )
            gt_f.write(json.dumps(gt_result) + "\n")

            dist_result = simulate_distributed_search(
                shard_doc_data, shard_indexes, shard_doc_categories,
                shard_stats, query, limit,
            )
            dist_f.write(json.dumps(dist_result) + "\n")

            rrf_result = simulate_distributed_search_rrf(
                shard_doc_data, shard_indexes, shard_doc_categories,
                shard_stats, query, limit,
            )
            rrf_f.write(json.dumps(rrf_result) + "\n")

    print(f"  Completed {len(queries)} queries")
    print(f"\nResults saved to:")
    print(f"  {ground_truth_file}")
    print(f"  {distributed_file}")
    print(f"  {rrf_file}")

    # Save experiment metadata
    exp_meta = {
        "corpus_dir": str(corpus_dir),
        "query_file": str(query_file),
        "shard_count": shard_count,
        "limit": limit,
        "total_queries": len(queries),
        "merge_strategies": ["score", "rrf"],
        "rrf_k": RRF_K,
        "global_stats": {"N": global_stats[1], "avgdl": global_stats[2]},
        "shard_stats": {
            str(k): {"N": v[1], "avgdl": v[2]}
            for k, v in shard_stats.items()
        },
    }

    with open(output_dir / "experiment.json", "w") as f:
        json.dump(exp_meta, f, indent=2)

    return exp_meta


def main():
    parser = argparse.ArgumentParser(description="Simulate score comparability experiments")
    parser.add_argument("--corpus", type=str, default="corpus/",
                       help="Corpus directory")
    parser.add_argument("--queries", type=str, default="queries/queries.jsonl",
                       help="Query file")
    parser.add_argument("--output", type=str,
                       default="results/",
                       help="Output directory")
    parser.add_argument("--shards", type=int, default=10, help="Number of shards")
    parser.add_argument("--limit", type=int, default=100, help="Results per query")

    args = parser.parse_args()

    corpus_dir = Path(args.corpus)
    output_dir = Path(args.output)

    # Generate corpus if needed
    if not (corpus_dir / "corpus.jsonl").exists():
        print("Corpus not found. Generating...")
        import subprocess
        subprocess.run([
            "python3",
            corpus_dir / "generate.py",
            "--count", "100000",
            "--shards", str(args.shards),
        ], check=True)

    # Generate queries if needed
    query_file = Path(args.queries)
    if not query_file.exists():
        print("Queries not found. Generating...")
        import subprocess
        queries_dir = query_file.parent
        subprocess.run([
            "python3",
            queries_dir / "generate.py",
            "--total", "10000",
        ], check=True)

    # Run experiment
    run_experiment(corpus_dir, query_file, output_dir, args.shards, args.limit)

    print("\nTo compare results, run:")
    print(f"  python3 {output_dir}/compare.py {output_dir}/ground-truth.jsonl {output_dir}/distributed.jsonl --verbose")
    print(f"  python3 {output_dir}/compare.py {output_dir}/ground-truth.jsonl {output_dir}/distributed-rrf.jsonl --verbose")


if __name__ == "__main__":
    main()
