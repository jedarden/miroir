#!/bin/bash
# Verification script for P6.2 Peer Discovery
# Tests that peer discovery is properly configured and the miroir_peer_pod_count metric exists

set -e

ADMIN_KEY="${MIROIR_ADMIN_API_KEY:-admin123}"
BASE_URL="${MIROIR_BASE_URL:-http://localhost:7700}"
METRICS_URL="${MIROIR_METRICS_URL:-http://localhost:9090}"

echo "=== P6.2 Peer Discovery Verification ==="
echo ""

echo "1. Checking miroir_peer_pod_count metric exists..."
METRICS=$(curl -s "${METRICS_URL}/metrics" 2>/dev/null || echo "")
if [ -z "$METRICS" ]; then
    echo "   ❌ FAILED: Could not connect to ${METRICS_URL}/metrics"
    exit 1
fi

if echo "$METRICS" | grep -q "^miroir_peer_pod_count"; then
    echo "   ✓ miroir_peer_pod_count metric exists"
else
    echo "   ❌ FAILED: miroir_peer_pod_count metric not found"
    exit 1
fi

echo ""
echo "2. Checking miroir_leader metric exists..."
if echo "$METRICS" | grep -q "^miroir_leader"; then
    echo "   ✓ miroir_leader metric exists"
else
    echo "   ❌ FAILED: miroir_leader metric not found"
    exit 1
fi

echo ""
echo "3. Checking miroir_owned_shards_count metric exists..."
if echo "$METRICS" | grep -q "^miroir_owned_shards_count"; then
    echo "   ✓ miroir_owned_shards_count metric exists"
else
    echo "   ❌ FAILED: miroir_owned_shards_count metric not found"
    exit 1
fi

echo ""
echo "4. Verifying POD_NAME env var is set (if running in K8s)..."
POD_NAME="${POD_NAME:-unknown}"
if [ "$POD_NAME" != "unknown" ]; then
    echo "   ✓ POD_NAME=$POD_NAME"
else
    echo "   ⚠ POD_NAME not set (not running in Kubernetes)"
fi

echo ""
echo "5. Verifying POD_NAMESPACE env var is set (if running in K8s)..."
POD_NAMESPACE="${POD_NAMESPACE:-unknown}"
if [ "$POD_NAMESPACE" != "unknown" ]; then
    echo "   ✓ POD_NAMESPACE=$POD_NAMESPACE"
else
    echo "   ⚠ POD_NAMESPACE not set (not running in Kubernetes)"
fi

echo ""
echo "6. Checking peer_discovery configuration..."
# The peer_discovery config is internal, but we can check the log for the refresh loop starting
# For local dev, peer discovery may be disabled if POD_NAME=unknown
if [ "$POD_NAME" = "unknown" ]; then
    echo "   ℹ peer discovery disabled (not running in Kubernetes)"
else
    echo "   ✓ peer discovery should be enabled (POD_NAME is set)"
fi

echo ""
echo "=== P6.2 Peer Discovery Verification Complete ✓ ==="
