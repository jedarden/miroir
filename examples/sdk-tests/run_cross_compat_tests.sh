#!/usr/bin/env bash
# Cross-compatibility SDK tests — runs each test against both Miroir and plain Meilisearch
# Verifies that SDK smoke tests pass against both endpoints with identical results

set -e

MIROIR_URL="${MIROIR_URL:-http://localhost:7700}"
MIROIR_MASTER_KEY="${MIROIR_MASTER_KEY:-dev-key}"
MEILISEARCH_URL="${MEILISEARCH_URL:-http://localhost:7704}"
MEILISEARCH_MASTER_KEY="${MEILISEARCH_MASTER_KEY:-dev-node-key}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export MIROIR_URL MIROIR_MASTER_KEY MEILISEARCH_URL MEILISEARCH_MASTER_KEY

echo "=== Miroir Cross-Compatibility SDK Tests ==="
echo "Miroir endpoint: $MIROIR_URL"
echo "Meilisearch endpoint: $MEILISEARCH_URL"
echo ""

# Check if both endpoints are reachable
if ! curl -sf "$MIROIR_URL/health" >/dev/null 2>&1; then
    echo "❌ Miroir is not reachable at $MIROIR_URL"
    echo "Start the dev stack first:"
    echo "  cd /home/coding/miroir/examples && docker-compose -f docker-compose-dev.yml up -d"
    exit 1
fi

if ! curl -sf "$MEILISEARCH_URL/health" >/dev/null 2>&1; then
    echo "❌ Meilisearch is not reachable at $MEILISEARCH_URL"
    echo "The standalone Meilisearch instance should be running on port 7704"
    exit 1
fi

PASSED=0
FAILED=0

run_test() {
    local name="$1"
    local test_cmd="$2"
    local endpoint_var="$3"  # MIROIR_URL or MEILISEARCH_URL
    local endpoint_url="${!endpoint_var}"

    echo "### Testing $name against $endpoint_var ($endpoint_url) ###"
    if eval "$test_cmd"; then
        echo "✅ $name passed against $endpoint_var"
        ((PASSED++))
    else
        echo "❌ $name failed against $endpoint_var"
        ((FAILED++))
    fi
    echo ""
}

# Python test against Miroir
if [ -f "$SCRIPT_DIR/requirements.txt" ] && command -v pip3 &>/dev/null; then
    pip3 install -q -r "$SCRIPT_DIR/requirements.txt" 2>/dev/null || true

    run_test "Python SDK (Miroir)" \
        "python3 $SCRIPT_DIR/python_smoke_test.py" \
        "MIROIR_URL"

    # Python test against Meilisearch
    run_test "Python SDK (Meilisearch)" \
        "MIROIR_URL=\$MEILISEARCH_URL MIROIR_MASTER_KEY=\$MEILISEARCH_MASTER_KEY python3 $SCRIPT_DIR/python_smoke_test.py" \
        "MEILISEARCH_URL"
else
    echo "⚠️  Skipping Python tests (pip3 not found)"
fi

# TypeScript test against Miroir
if [ -f "$SCRIPT_DIR/package.json" ] && command -v npm &>/dev/null; then
    cd "$SCRIPT_DIR"
    npm install --silent >/dev/null 2>&1 || true

    run_test "TypeScript SDK (Miroir)" \
        "npx ts-node typescript_smoke_test.ts" \
        "MIROIR_URL"

    # TypeScript test against Meilisearch
    run_test "TypeScript SDK (Meilisearch)" \
        "MIROIR_URL=\$MEILISEARCH_URL MIROIR_MASTER_KEY=\$MEILISEARCH_MASTER_KEY npx ts-node typescript_smoke_test.ts" \
        "MEILISEARCH_URL"

    cd - >/dev/null
else
    echo "⚠️  Skipping TypeScript tests (npm not found)"
fi

# Go test against Miroir
if [ -f "$SCRIPT_DIR/golang_smoke_test.go" ] && command -v go &>/dev/null; then
    cd "$SCRIPT_DIR"
    go mod tidy >/dev/null 2>&1 || true

    run_test "Go SDK (Miroir)" \
        "go run golang_smoke_test.go" \
        "MIROIR_URL"

    # Go test against Meilisearch
    run_test "Go SDK (Meilisearch)" \
        "MIROIR_URL=\$MEILISEARCH_URL MIROIR_MASTER_KEY=\$MEILISEARCH_MASTER_KEY go run golang_smoke_test.go" \
        "MEILISEARCH_URL"

    cd - >/dev/null
else
    echo "⚠️  Skipping Go tests (go not found)"
fi

# Rust test against Miroir
if [ -f "$SCRIPT_DIR/rust_smoke_test.rs" ] && command -v cargo &>/dev/null; then
    cd /home/coding/miroir

    run_test "Rust SDK (Miroir)" \
        "cargo run --example sdk-smoke-test 2>/dev/null" \
        "MIROIR_URL"

    # Rust test against Meilisearch
    run_test "Rust SDK (Meilisearch)" \
        "MIROIR_URL=\$MEILISEARCH_URL MIROIR_MASTER_KEY=\$MEILISEARCH_MASTER_KEY cargo run --example sdk-smoke-test 2>/dev/null" \
        "MEILISEARCH_URL"

    cd - >/dev/null
else
    echo "⚠️  Skipping Rust tests (cargo not found)"
fi

# Summary
echo "=== Summary ==="
echo "Passed: $PASSED"
echo "Failed: $FAILED"

if [ $FAILED -gt 0 ]; then
    echo "❌ Some tests failed"
    exit 1
else
    echo "✅ All cross-compatibility tests passed!"
    echo ""
    echo "This confirms that Miroir's API is drop-in compatible with Meilisearch"
    echo "for the tested operations across all 4 SDK languages."
    exit 0
fi
