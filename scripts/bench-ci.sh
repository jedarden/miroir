#!/usr/bin/env bash
# CI benchmark runner script
#
# Runs all benchmarks and saves results for artifact upload and regression comparison.
#
# Usage:
#   ./scripts/bench-ci.sh <output-dir>
#
# The output directory will contain:
#   - criterion/     - Raw criterion HTML reports
#   - baseline.json  - Parsed baseline data for critcmp

set -euo pipefail

OUTPUT_DIR="${1:-target/ci-bench}"

echo "=== CI Benchmark Runner ==="
echo "Output directory: $OUTPUT_DIR"
echo

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Run benchmarks with criterion
echo "Running benchmarks..."
cargo bench -p miroir-core --bench router_bench -- --save-baseline main
cargo bench -p miroir-core --bench merger_bench -- --save-baseline main

# Copy criterion reports to output directory
echo "Copying benchmark reports..."
cp -r target/criterion "$OUTPUT_DIR/"

# Generate baseline JSON for critcmp
echo "Generating baseline data..."
if command -v critcmp &> /dev/null; then
    critcmp --export "$OUTPUT_DIR/baseline.json" target/criterion || true
    echo "Baseline saved to $OUTPUT_DIR/baseline.json"
else
    echo "Warning: critcmp not found, skipping baseline export"
    echo "Install with: cargo install critcmp"
fi

echo
echo "✅ Benchmarks complete"
echo "Results saved to: $OUTPUT_DIR"
echo
echo "To view reports:"
echo "  open $OUTPUT_DIR/criterion/*/report/index.html"
