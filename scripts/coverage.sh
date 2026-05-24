#!/usr/bin/env bash
# Coverage gate script for miroir-core
# Per plan §8: miroir-core must maintain ≥90% coverage from v1.0

set -e

COVERAGE_MIN=90
PACKAGE="miroir-core"
OUTPUT_DIR="target/coverage"

echo "=== Coverage gate: ${PACKAGE} ≥ ${COVERAGE_MIN}% ==="

# Install cargo-tarpaulin if not present
if ! command -v cargo-tarpaulin &>/dev/null; then
    echo "Installing cargo-tarpaulin..."
    cargo install cargo-tarpaulin --locked
fi

# Run tarpaulin with timeout
cargo tarpaulin \
    --workspace \
    --packages "${PACKAGE}" \
    --exclude-files "benches/*" \
    --exclude-files "tests/*" \
    --timeout 600 \
    --out Lcov \
    --out Xml \
    --output-dir "${OUTPUT_DIR}" \
    -- --test-threads=1

# Parse the XML output to get the coverage percentage
COVERAGE=$(grep -oP 'line-rate="\K[0-9.]+' "${OUTPUT_DIR}/cobertura.xml" | awk '{print $1 * 100}')

echo "Coverage: ${COVERAGE}%"

# Check if coverage meets the threshold
if (( $(echo "$COVERAGE < $COVERAGE_MIN" | bc -l) )); then
    echo "❌ Coverage ${COVERAGE}% is below minimum ${COVERAGE_MIN}%"
    exit 1
fi

echo "✅ Coverage ${COVERAGE}% meets minimum ${COVERAGE_MIN}%"
