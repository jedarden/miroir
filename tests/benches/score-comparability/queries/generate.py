#!/usr/bin/env python3
"""
Generate query sets for score comparability experiments.

Query types:
1. Single-term queries - test basic term frequency handling
2. Multi-term AND queries - test phrase matching
3. Category-filtered queries - test filtered search
4. Rare-term queries - test IDF behavior on low-frequency terms
5. Common-term queries - test IDF behavior on high-frequency terms
"""

import argparse
import json
import random
from pathlib import Path
from typing import List, Dict, Set


def load_vocabulary(corpus_dir: Path) -> Dict:
    """Load vocabulary and corpus metadata."""
    vocab_file = corpus_dir / "vocabulary.json"
    metadata_file = corpus_dir / "metadata.json"

    with open(vocab_file) as f:
        vocab_data = json.load(f)

    with open(metadata_file) as f:
        metadata = json.load(f)

    # Load corpus to compute term frequencies
    corpus_file = corpus_dir / "corpus.jsonl"
    term_freq = {}

    with open(corpus_file) as f:
        for line in f:
            doc = json.loads(line)
            text = f"{doc['title']} {doc['content']}".lower()
            words = set(text.split())
            for word in words:
                if word in vocab_data["terms"]:
                    term_freq[word] = term_freq.get(word, 0) + 1

    # Sort terms by frequency
    sorted_terms = sorted(term_freq.items(), key=lambda x: x[1])

    return {
        "terms": vocab_data["terms"],
        "categories": vocab_data["categories"],
        "term_freq": dict(sorted_terms),
        "total_docs": metadata["total_documents"],
    }


def generate_single_term_queries(vocab_data: Dict, count: int) -> List[Dict]:
    """Generate single-term queries with random term selection."""
    queries = []
    terms = vocab_data["terms"]

    for i in range(count):
        term = random.choice(terms)
        queries.append({
            "id": f"q-single-{i:05d}",
            "type": "single_term",
            "q": term,
            "filter": None,
        })

    return queries


def generate_multi_term_queries(vocab_data: Dict, count: int, min_terms: int = 2, max_terms: int = 4) -> List[Dict]:
    """Generate multi-term queries."""
    queries = []
    terms = vocab_data["terms"]

    for i in range(count):
        num_terms = random.randint(min_terms, max_terms)
        selected = random.sample(terms, min(num_terms, len(terms)))
        queries.append({
            "id": f"q-multi-{i:05d}",
            "type": "multi_term",
            "q": " ".join(selected),
            "filter": None,
        })

    return queries


def generate_filtered_queries(vocab_data: Dict, count: int) -> List[Dict]:
    """Generate queries with category filters."""
    queries = []
    terms = vocab_data["terms"]
    categories = vocab_data["categories"]

    for i in range(count):
        term = random.choice(terms)
        category = random.choice(categories)
        queries.append({
            "id": f"q-filter-{i:05d}",
            "type": "filtered",
            "q": term,
            "filter": f"category = {category}",
        })

    return queries


def generate_rare_term_queries(vocab_data: Dict, count: int, percentile: float = 0.1) -> List[Dict]:
    """Generate queries using rare terms (low document frequency)."""
    queries = []
    term_freq = vocab_data["term_freq"]
    sorted_terms = list(term_freq.items())

    # Get rare terms (bottom percentile by frequency)
    cutoff = int(len(sorted_terms) * percentile)
    rare_terms = [t for t, _ in sorted_terms[:cutoff]]

    for i in range(count):
        if not rare_terms:
            break
        term = random.choice(rare_terms)
        queries.append({
            "id": f"q-rare-{i:05d}",
            "type": "rare_term",
            "q": term,
            "filter": None,
        })

    return queries


def generate_common_term_queries(vocab_data: Dict, count: int, percentile: float = 0.9) -> List[Dict]:
    """Generate queries using common terms (high document frequency)."""
    queries = []
    term_freq = vocab_data["term_freq"]
    sorted_terms = list(term_freq.items())

    # Get common terms (top percentile by frequency)
    cutoff = int(len(sorted_terms) * percentile)
    common_terms = [t for t, _ in sorted_terms[cutoff:]]

    for i in range(count):
        if not common_terms:
            break
        term = random.choice(common_terms)
        queries.append({
            "id": f"q-common-{i:05d}",
            "type": "common_term",
            "q": term,
            "filter": None,
        })

    return queries


def main():
    parser = argparse.ArgumentParser(description="Generate query sets for experiments")
    parser.add_argument("--corpus", type=str, default="corpus/", help="Corpus directory")
    parser.add_argument("--output", type=str, default="queries/", help="Output directory")
    parser.add_argument("--total", type=int, default=10000, help="Total number of queries")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    random.seed(args.seed)

    corpus_dir = Path(args.corpus)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading vocabulary from {corpus_dir}...")
    vocab_data = load_vocabulary(corpus_dir)

    print(f"Vocabulary: {len(vocab_data['terms'])} terms")
    print(f"Categories: {vocab_data['categories']}")
    print(f"Term frequency range: {min(vocab_data['term_freq'].values())} - {max(vocab_data['term_freq'].values())}")

    # Generate different query types
    print(f"\nGenerating {args.total} queries...")

    allocation = {
        "single_term": 0.25,
        "multi_term": 0.25,
        "filtered": 0.20,
        "rare_term": 0.15,
        "common_term": 0.15,
    }

    queries = []

    # Single-term queries
    count = int(args.total * allocation["single_term"])
    queries.extend(generate_single_term_queries(vocab_data, count))
    print(f"  Single-term: {count}")

    # Multi-term queries
    count = int(args.total * allocation["multi_term"])
    queries.extend(generate_multi_term_queries(vocab_data, count))
    print(f"  Multi-term: {count}")

    # Filtered queries
    count = int(args.total * allocation["filtered"])
    queries.extend(generate_filtered_queries(vocab_data, count))
    print(f"  Filtered: {count}")

    # Rare-term queries
    count = int(args.total * allocation["rare_term"])
    rare_queries = generate_rare_term_queries(vocab_data, count)
    queries.extend(rare_queries)
    print(f"  Rare-term: {len(rare_queries)}")

    # Common-term queries
    count = int(args.total * allocation["common_term"])
    common_queries = generate_common_term_queries(vocab_data, count)
    queries.extend(common_queries)
    print(f"  Common-term: {len(common_queries)}")

    # Shuffle to mix query types
    random.shuffle(queries)

    # Save query set
    output_file = output_dir / "queries.jsonl"
    with open(output_file, "w") as f:
        for q in queries:
            f.write(json.dumps(q) + "\n")

    # Save metadata
    metadata = {
        "total_queries": len(queries),
        "allocation": allocation,
        "random_seed": args.seed,
        "vocab_size": len(vocab_data["terms"]),
        "categories": vocab_data["categories"],
    }
    with open(output_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\nGenerated {len(queries)} queries")
    print(f"Saved to {output_file}")


if __name__ == "__main__":
    main()
