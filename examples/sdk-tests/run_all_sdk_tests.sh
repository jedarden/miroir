#!/usr/bin/env bash
# Run all SDK smoke tests against docker-compose-dev stack

set -e

MIROIR_URL="${MIROIR_URL:-http://localhost:7700}"
MIROIR_MASTER_KEY="${MIROIR_MASTER_KEY:-dev-key}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export MIROIR_URL MIROIR_MASTER_KEY

echo "=== Miroir SDK Smoke Tests ==="
echo "Target: $MIROIR_URL"
echo ""

# Check if docker-compose stack is running
if ! curl -sf "$MIROIR_URL/health" >/dev/null 2>&1; then
    echo "❌ Miroir is not reachable at $MIROIR_URL"
    echo "Start the dev stack first:"
    echo "  cd /home/coding/miroir/examples && docker-compose -f docker-compose-dev.yml up -d"
    exit 1
fi

PASSED=0
FAILED=0

# Python test
echo "### Python SDK Test ###"
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    if command -v pip3 &>/dev/null; then
        pip3 install -q -r "$SCRIPT_DIR/requirements.txt" 2>/dev/null || true
        if python3 "$SCRIPT_DIR/python_smoke_test.py"; then
            ((PASSED++))
        else
            ((FAILED++))
        fi
    else
        echo "⚠️  pip3 not found, skipping Python test"
    fi
else
    echo "⚠️  requirements.txt not found, skipping Python test"
fi
echo ""

# TypeScript test
echo "### TypeScript SDK Test ###"
if [ -f "$SCRIPT_DIR/package.json" ]; then
    if command -v npm &>/dev/null; then
        cd "$SCRIPT_DIR"
        npm install --silent >/dev/null 2>&1 || true
        if npx ts-node typescript_smoke_test.ts; then
            ((PASSED++))
        else
            ((FAILED++))
        fi
        cd - >/dev/null
    else
        echo "⚠️  npm not found, skipping TypeScript test"
    fi
else
    echo "⚠️  package.json not found, skipping TypeScript test"
fi
echo ""

# Go test
echo "### Go SDK Test ###"
if [ -f "$SCRIPT_DIR/golang_smoke_test.go" ]; then
    if command -v go &>/dev/null; then
        cd "$SCRIPT_DIR"
        go mod tidy >/dev/null 2>&1 || true
        if go run golang_smoke_test.go; then
            ((PASSED++))
        else
            ((FAILED++))
        fi
        cd - >/dev/null
    else
        echo "⚠️  go not found, skipping Go test"
    fi
else
    echo "⚠️  golang_smoke_test.go not found, skipping Go test"
fi
echo ""

# Rust test
echo "### Rust SDK Test ###"
if [ -f "$SCRIPT_DIR/rust_smoke_test.rs" ]; then
    if command -v cargo &>/dev/null; then
        cd /home/coding/miroir
        if cargo run --example sdk-smoke-test 2>/dev/null; then
            ((PASSED++))
        else
            ((FAILED++))
        fi
        cd - >/dev/null
    else
        echo "⚠️  cargo not found, skipping Rust test"
    fi
else
    echo "⚠️  rust_smoke_test.rs not found, skipping Rust test"
fi
echo ""

# Summary
echo "=== Summary ==="
echo "Passed: $PASSED"
echo "Failed: $FAILED"

if [ $FAILED -gt 0 ]; then
    echo "❌ Some tests failed"
    exit 1
else
    echo "✅ All tests passed!"
    exit 0
fi
