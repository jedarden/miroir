#!/bin/bash
# Verification script for P7.1 Core metrics families
# Tests that all core metrics are properly registered and accessible

set -e

ADMIN_KEY="${MIROIR_ADMIN_API_KEY:-admin123}"
BASE_URL="${MIROIR_BASE_URL:-http://localhost:7700}"
METRICS_URL="${MIROIR_METRICS_URL:-http://localhost:9090}"

echo "=== P7.1 Core Metrics Verification ==="
echo ""

# Expected core metric names (from plan §10)
CORE_METRICS=(
    "miroir_request_duration_seconds"
    "miroir_requests_total"
    "miroir_requests_in_flight"
    "miroir_node_healthy"
    "miroir_node_request_duration_seconds"
    "miroir_node_errors_total"
    "miroir_shard_coverage"
    "miroir_degraded_shards_total"
    "miroir_shard_distribution"
    "miroir_task_processing_age_seconds"
    "miroir_tasks_total"
    "miroir_task_registry_size"
    "miroir_scatter_fan_out_size"
    "miroir_scatter_partial_responses_total"
    "miroir_scatter_retries_total"
    "miroir_rebalance_in_progress"
    "miroir_rebalance_documents_migrated_total"
    "miroir_rebalance_duration_seconds"
)

echo "1. Checking port 9090 /metrics endpoint (unauthenticated)..."
METRICS_9090=$(curl -s "${METRICS_URL}/metrics" 2>/dev/null || echo "")
if [ -z "$METRICS_9090" ]; then
    echo "   ❌ FAILED: Could not connect to ${METRICS_URL}/metrics"
    exit 1
fi
echo "   ✓ Connected to ${METRICS_URL}/metrics"

echo ""
echo "2. Checking port 7700 /_miroir/metrics endpoint (admin-key gated)..."
METRICS_7700=$(curl -s -H "Authorization: Bearer ${ADMIN_KEY}" "${BASE_URL}/_miroir/metrics" 2>/dev/null || echo "")
if [ -z "$METRICS_7700" ]; then
    echo "   ❌ FAILED: Could not connect to ${BASE_URL}/_miroir/metrics"
    exit 1
fi
echo "   ✓ Connected to ${BASE_URL}/_miroir/metrics"

echo ""
echo "3. Verifying /_miroir/metrics requires admin authentication..."
UNAUTH_RESPONSE=$(curl -s -w "%{http_code}" "${BASE_URL}/_miroir/metrics" -o /dev/null 2>/dev/null || echo "000")
if [ "$UNAUTH_RESPONSE" != "401" ] && [ "$UNAUTH_RESPONSE" != "403" ]; then
    echo "   ❌ FAILED: /_miroir/metrics returned ${UNAUTH_RESPONSE} (expected 401/403)"
    exit 1
fi
echo "   ✓ /_miroir/metrics requires authentication (returned ${UNAUTH_RESPONSE})"

echo ""
echo "4. Verifying all core metrics are present..."
MISSING_COUNT=0
for metric in "${CORE_METRICS[@]}"; do
    if echo "$METRICS_9090" | grep -q "^${metric}"; then
        echo "   ✓ ${metric}"
    else
        echo "   ❌ MISSING: ${metric}"
        MISSING_COUNT=$((MISSING_COUNT + 1))
    fi
done

if [ $MISSING_COUNT -gt 0 ]; then
    echo ""
    echo "❌ FAILED: ${MISSING_COUNT} core metrics are missing"
    exit 1
fi

echo ""
echo "5. Verifying path_template labels have no UUIDs..."
# Check for potential UUID patterns in path_template labels
if echo "$METRICS_9090" | grep -q 'path_template=".*[0-9a-f]\{8\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{12\}'; then
    echo "   ❌ FAILED: Found potential UUID in path_template label"
    exit 1
fi
echo "   ✓ No UUIDs found in path_template labels"

echo ""
echo "6. Verifying both endpoints return identical data..."
# Sort and compare the metrics output (ignore HELP/TYPE lines for comparison)
METRICS_9090_SORTED=$(echo "$METRICS_9090" | grep -v "^#" | grep -v "^$" | sort)
METRICS_7700_SORTED=$(echo "$METRICS_7700" | grep -v "^#" | grep -v "^$" | sort)
if [ "$METRICS_9090_SORTED" != "$METRICS_7700_SORTED" ]; then
    echo "   ❌ FAILED: Metrics differ between :9090/metrics and :7700/_miroir/metrics"
    exit 1
fi
echo "   ✓ Both endpoints return identical metrics data"

echo ""
echo "=== All Verifications Passed ✓ ==="
echo ""
echo "Summary:"
echo "  - All 18 core metrics are registered"
echo "  - Port 9090 /metrics is accessible (unauthenticated, pod-internal)"
echo "  - Port 7700 /_miroir/metrics requires admin authentication"
echo "  - Both endpoints return identical data"
echo "  - path_template labels contain no UUIDs"
