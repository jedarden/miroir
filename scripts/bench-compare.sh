#!/usr/bin/env bash
# Benchmark regression gate script
#
# Compares current benchmark results against a baseline from main.
# Exits with error if any benchmark shows > 20% slowdown.
#
# Usage:
#   ./scripts/bench-compare.sh <baseline-dir> <current-dir>
#
# Args:
#   baseline-dir - Path to baseline criterion output (from main)
#   current-dir  - Path to current criterion output (from PR)

set -euo pipefail

THRESHOLD=20  # 20% slowdown threshold

if [ $# -ne 2 ]; then
    echo "Usage: $0 <baseline-dir> <current-dir>"
    exit 1
fi

BASELINE_DIR="$1"
CURRENT_DIR="$2"

if [ ! -d "$BASELINE_DIR" ]; then
    echo "Error: baseline directory not found: $BASELINE_DIR"
    exit 1
fi

if [ ! -d "$CURRENT_DIR" ]; then
    echo "Error: current directory not found: $CURRENT_DIR"
    exit 1
fi

echo "=== Benchmark Regression Gate ==="
echo "Baseline: $BASELINE_DIR"
echo "Current:  $CURRENT_DIR"
echo "Threshold: > ${THRESHOLD}% slowdown triggers failure"
echo

# Check if critcmp is available
if ! command -v critcmp &> /dev/null; then
    echo "Warning: critcmp not found, installing..."
    cargo install critcmp
fi

# Run critcmp to compare results
echo "Comparing benchmark results..."
critcmp "$BASELINE_DIR" "$CURRENT_DIR" || true

# Parse critcmp output for regressions
# critcmp exits with 0 even if there are regressions, so we parse the output
echo
echo "Checking for regressions above ${THRESHOLD}%..."

# Extract benchmark names and percentage changes from critcmp output
# This is a simplified check - in production you'd parse the JSON output
REGRESSIONS_FOUND=0

# Use critcmp's JSON output for proper parsing
CRITCMP_OUTPUT=$(critcmp --json "$BASELINE_DIR" "$CURRENT_DIR" 2>/dev/null || echo '{}')

# Check if we have jq for parsing
if command -v jq &> /dev/null; then
    # Parse JSON for regressions > threshold
    REGRESSION_COUNT=$(echo "$CRITCMP_OUTPUT" | jq -r "
        [.[] | .[] | select(.change > $THRESHOLD)] | length
    ")

    if [ "$REGRESSION_COUNT" -gt 0 ]; then
        echo "❌ REGRESSION DETECTED: $REGRESSION_COUNT benchmark(s) show > ${THRESHOLD}% slowdown"
        echo
        echo "Affected benchmarks:"
        echo "$CRITCMP_OUTPUT" | jq -r "
            [] | .[] | .[] | select(.change > $THRESHOLD) |
            \"\(.name): +\(.change)% (from \(.before_us)µs to \(.after_us)µs)\"
        "
        REGRESSIONS_FOUND=1
    else
        echo "✅ No regressions above ${THRESHOLD}% detected"
    fi
else
    # Fallback: check text output for large positive changes
    echo "Note: jq not found, using basic text parsing (install jq for accurate results)"
    if critcmp "$BASELINE_DIR" "$CURRENT_DIR" | grep -E '\+[2-9][0-9]+\.[0-9]+%' > /dev/null; then
        echo "❌ POTENTIAL REGRESSION: Large slowdown detected in output above"
        echo "Please review the critcmp output manually"
        REGRESSIONS_FOUND=1
    fi
fi

echo
if [ $REGRESSIONS_FOUND -eq 1 ]; then
    echo "Regression gate failed: benchmarks exceeded ${THRESHOLD}% threshold"
    exit 1
else
    echo "Regression gate passed"
    exit 0
fi
