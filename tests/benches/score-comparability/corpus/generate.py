#!/usr/bin/env python3
"""
Generate test corpus for score comparability experiments.

Creates a synthetic document collection with:
- Controlled vocabulary (50 unique terms)
- Skewable shard distribution
- Realistic term frequency distributions following Zipf's law
"""

import argparse
import json
import random
from pathlib import Path
from typing import List, Dict


def generate_vocabulary(size: int = 50) -> List[str]:
    """Generate unique terms for the corpus."""
    categories = ["tech", "finance", "science", "health", "business"]
    terms = []

    # Add some category-specific terms
    cat_terms = {
        "tech": ["algorithm", "database", "server", "cloud", "network", "api", "code", "software"],
        "finance": ["stock", "market", "investment", "portfolio", "dividend", "yield", "asset", "trading"],
        "science": ["research", "experiment", "hypothesis", "data", "analysis", "theory", "laboratory", "discovery"],
        "health": ["treatment", "patient", "diagnosis", "symptom", "therapy", "medicine", "clinical", "wellness"],
        "business": ["strategy", "revenue", "customer", "product", "service", "growth", "operations", "management"],
    }

    for cat, cat_term_list in cat_terms.items():
        terms.extend(cat_term_list)

    # Add general terms
    general_terms = [
        "system", "process", "method", "approach", "solution", "platform", "framework",
        "model", "design", "implementation", "development", "deployment", "architecture",
        "performance", "scalability", "reliability", "security", "integration", "configuration",
        "monitoring", "testing", "validation", "optimization", "automation", "documentation"
    ]

    terms.extend(general_terms[: size - len(terms)])
    return terms[:size]


def zipf_distribution(n: int, s: float = 1.0) -> List[float]:
    """Generate Zipf distribution for term frequencies."""
    # Normalize: probability of rank i is proportional to 1/(i+1)^s
    ranks = list(range(1, n + 1))
    weights = [1.0 / (r ** s) for r in ranks]
    total = sum(weights)
    return [w / total for w in weights]


def generate_documents(
    count: int,
    vocabulary: List[str],
    categories: List[str],
    avg_doc_length: int = 50,
) -> List[Dict]:
    """Generate synthetic documents."""
    vocab_size = len(vocabulary)
    zipf_weights = zipf_distribution(vocab_size, s=1.2)

    documents = []
    for i in range(count):
        category = random.choice(categories)

        # Choose terms for this document using weighted sampling
        # Term count follows Poisson-like distribution
        term_count = max(5, int(random.gauss(avg_doc_length, avg_doc_length / 4)))
        doc_terms = random.choices(vocabulary, weights=zipf_weights, k=term_count)

        # Ensure some category-specific terms appear
        cat_related = [t for t in vocabulary if t.lower() in category.lower() or
                      any(c in t.lower() for c in category.lower().split())]
        if cat_related and random.random() < 0.7:
            doc_terms[0] = random.choice(cat_related)

        # Create title (first 3-5 terms)
        title_length = random.randint(3, 5)
        title_terms = doc_terms[:title_length]
        title = " ".join(title_terms).title()

        # Create content (all terms)
        content = " ".join(doc_terms).capitalize()

        documents.append({
            "id": f"doc-{i:06d}",
            "title": title,
            "content": content,
            "category": category,
        })

    return documents


def assign_shards_skewed(
    documents: List[Dict],
    shard_count: int,
    skew_factors: List[float],
) -> Dict[int, List[Dict]]:
    """
    Assign documents to shards with controlled skew.

    skew_factors[i] is the relative size multiplier for shard i.
    Normal shard = 1.0, 100× larger = 100.0, 0.01× smaller = 0.01
    """
    total_docs = len(documents)

    # Calculate target counts per shard
    base_per_shard = total_docs / (shard_count + sum(f - 1 for f in skew_factors))
    shard_targets = [int(base_per_shard * f) for f in skew_factors]

    # Normalize to total count
    total_target = sum(shard_targets)
    shard_targets = [int(t * total_docs / total_target) for t in shard_targets]

    # Ensure sum equals total
    while sum(shard_targets) < total_docs:
        shard_targets[random.randint(0, shard_count - 1)] += 1

    # Shuffle documents for random assignment
    shuffled = documents.copy()
    random.shuffle(shuffled)

    # Assign to shards
    shards = {}
    idx = 0
    for shard_id, target in enumerate(shard_targets):
        shards[shard_id] = shuffled[idx:idx + target]
        idx += target

    return shards


def main():
    parser = argparse.ArgumentParser(description="Generate test corpus for score comparability")
    parser.add_argument("--count", type=int, default=100000, help="Number of documents to generate")
    parser.add_argument("--shards", type=int, default=10, help="Number of shards")
    parser.add_argument("--output", type=str, default="corpus/", help="Output directory")
    parser.add_argument("--vocab-size", type=int, default=50, help="Vocabulary size")
    parser.add_argument("--categories", type=str,
                       default="tech,finance,science,health,business",
                       help="Comma-separated list of categories")

    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    categories = args.categories.split(",")

    print(f"Generating {args.count} documents...")
    print(f"Vocabulary size: {args.vocab_size}")
    print(f"Categories: {categories}")
    print(f"Shards: {args.shards}")

    # Generate vocabulary
    vocabulary = generate_vocabulary(args.vocab_size)
    with open(output_dir / "vocabulary.json", "w") as f:
        json.dump({"terms": vocabulary, "categories": categories}, f, indent=2)

    # Generate documents
    documents = generate_documents(args.count, vocabulary, categories)

    # Define skew factors for this experiment
    # Shard 0: normal (1.0)
    # Shard 1: 100× normal (100.0) - extreme outlier
    # Shard 2-7: normal (1.0)
    # Shard 8: slightly skewed (0.5)
    # Shard 9: 0.01× normal (0.01) - tiny shard
    skew_factors = [1.0, 100.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.5, 0.01]
    skew_factors = skew_factors[:args.shards]

    # Assign to shards
    shards = assign_shards_skewed(documents, args.shards, skew_factors)

    # Save combined corpus (for ground truth)
    with open(output_dir / "corpus.jsonl", "w") as f:
        for doc in documents:
            f.write(json.dumps(doc) + "\n")

    # Save per-shard corpora
    for shard_id, shard_docs in shards.items():
        filename = output_dir / f"shard-{shard_id:02d}.jsonl"
        with open(filename, "w") as f:
            for doc in shard_docs:
                f.write(json.dumps(doc) + "\n")
        print(f"  Shard {shard_id}: {len(shard_docs)} documents (skew factor: {skew_factors[shard_id]})")

    # Save metadata
    metadata = {
        "total_documents": args.count,
        "shard_count": args.shards,
        "vocabulary_size": args.vocab_size,
        "categories": categories,
        "skew_factors": skew_factors,
        "shard_sizes": {str(k): len(v) for k, v in shards.items()},
    }
    with open(output_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\nCorpus generated successfully in {output_dir}")
    print(f"  Total documents: {args.count}")
    print(f"  Vocabulary size: {len(vocabulary)}")
    print(f"  Categories: {len(categories)}")


if __name__ == "__main__":
    main()
