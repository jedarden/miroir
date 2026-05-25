#!/usr/bin/env bash
# P6.8 Template Verification (without helm)
#
# This script verifies that the Helm chart templates are correctly configured
# for multi-pod Phase 6 deployment by checking the template files directly.
#
# Usage: ./tests/verify_p6_8_templates_direct.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHART_DIR="$PROJECT_ROOT/charts/miroir"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Test 1: HPA template exists
echo ""
echo "=== Test 1: HPA Template Exists ==="

HPA_TEMPLATE="$CHART_DIR/templates/miroir-hpa.yaml"
if [ -f "$HPA_TEMPLATE" ]; then
    log_success "HPA template exists: miroir-hpa.yaml"
else
    log_error "HPA template not found"
    exit 1
fi

# Test 2: HPA has correct metrics
echo ""
echo "=== Test 2: HPA Metrics ==="

if grep -q "miroir_requests_in_flight" "$HPA_TEMPLATE"; then
    log_success "HPA includes miroir_requests_in_flight metric"
else
    log_error "HPA missing miroir_requests_in_flight metric"
    exit 1
fi

if grep -q "miroir_background_queue_depth" "$HPA_TEMPLATE"; then
    log_success "HPA includes miroir_background_queue_depth metric"
else
    log_error "HPA missing miroir_background_queue_depth metric"
    exit 1
fi

# Test 3: HPA uses correct metric types
echo ""
echo "=== Test 3: HPA Metric Types ==="

if grep -B 5 "miroir_requests_in_flight" "$HPA_TEMPLATE" | grep -q "type: Pods"; then
    log_success "miroir_requests_in_flight uses type: Pods (correct for per-pod metric)"
else
    log_error "miroir_requests_in_flight does not use type: Pods"
    exit 1
fi

if grep -B 5 "miroir_background_queue_depth" "$HPA_TEMPLATE" | grep -q "type: External"; then
    log_success "miroir_background_queue_depth uses type: External (correct for global metric)"
else
    log_error "miroir_background_queue_depth does not use type: External"
    exit 1
fi

# Test 4: PrometheusRule exists
echo ""
echo "=== Test 4: PrometheusRule Template ==="

PROM_RULE_TEMPLATE="$CHART_DIR/templates/miroir-prometheusrule.yaml"
if [ -f "$PROM_RULE_TEMPLATE" ]; then
    log_success "PrometheusRule template exists"
else
    log_error "PrometheusRule template not found"
    exit 1
fi

# Test 5: PrometheusRule has §14.9 alerts
echo ""
echo "=== Test 5: §14.9 Resource-Pressure Alerts ==="

P14_9_ALERTS=(
    "MiroirMemoryPressure"
    "MiroirRequestQueueBacklog"
    "MiroirBackgroundJobBacklog"
    "MiroirPeerDiscoveryGap"
    "MiroirNoLeader"
)

for alert in "${P14_9_ALERTS[@]}"; do
    if grep -q "alert: $alert" "$PROM_RULE_TEMPLATE"; then
        log_success "Alert $alert is present"
    else
        log_error "Alert $alert is missing"
        exit 1
    fi
done

# Test 6: Headless Service for peer discovery
echo ""
echo "=== Test 6: Headless Service ==="

HEADLESS_SVC="$CHART_DIR/templates/miroir-headless.yaml"
if [ -f "$HEADLESS_SVC" ]; then
    log_success "Headless Service template exists"
else
    log_error "Headless Service template not found"
    exit 1
fi

if grep -q "clusterIP: None" "$HEADLESS_SVC"; then
    log_success "Headless Service has clusterIP: None"
else
    log_error "Headless Service does not have clusterIP: None"
    exit 1
fi

# Test 7: POD_NAME, POD_NAMESPACE, POD_IP env vars in deployment
echo ""
echo "=== Test 7: Downward API Environment Variables ==="

DEPLOYMENT_TEMPLATE="$CHART_DIR/templates/miroir-deployment.yaml"
DOWNWARD_VARS=("POD_NAME" "POD_NAMESPACE" "POD_IP")

for var in "${DOWNWARD_VARS[@]}"; do
    if grep -q "name: $var" "$DEPLOYMENT_TEMPLATE"; then
        log_success "Environment variable $var is present"
    else
        log_error "Environment variable $var not found"
        exit 1
    fi
done

# Test 8: Service Monitor for metrics
echo ""
echo "=== Test 8: Service Monitor ==="

SERVICEMONITOR_TEMPLATE="$CHART_DIR/templates/miroir-servicemonitor.yaml"
if [ -f "$SERVICEMONITOR_TEMPLATE" ]; then
    log_success "ServiceMonitor template exists"
else
    log_error "ServiceMonitor template not found"
    exit 1
fi

# Test 9: values.schema.json validation
echo ""
echo "=== Test 9: values.schema.json Validation ==="

SCHEMA="$CHART_DIR/values.schema.json"

if [ ! -f "$SCHEMA" ]; then
    log_error "values.schema.json not found"
    exit 1
fi

# Check for HPA validation rule
if grep -qi "HPA requires replicas" "$SCHEMA"; then
    log_success "values.schema.json has HPA validation rule"
else
    log_error "values.schema.json missing HPA validation rule"
    exit 1
fi

# Test 10: Check for prometheus-adapter ConfigMap (for HPA custom metrics)
echo ""
echo "=== Test 10: Prometheus Adapter ConfigMap ==="

PROM_ADAPTER_CM="$CHART_DIR/templates/miroir-prometheus-adapter-configmap.yaml"
if [ -f "$PROM_ADAPTER_CM" ]; then
    log_success "Prometheus adapter ConfigMap template exists"
else
    log_info "Prometheus adapter ConfigMap not found (may be optional)"
fi

# Test 11: Verify §14.8 defaults in values.yaml
echo ""
echo "=== Test 11: §14.8 Resource-Aware Defaults ==="

VALUES_YAML="$CHART_DIR/values.yaml"

# Check for peer_discovery config
if grep -q "peer_discovery:" "$VALUES_YAML"; then
    log_success "peer_discovery configuration exists in values.yaml"
else
    log_info "peer_discovery config not in values.yaml (may use code defaults)"
fi

# Check for leader_election config
if grep -q "leader_election:" "$VALUES_YAML"; then
    log_success "leader_election configuration exists in values.yaml"
else
    log_info "leader_election config not in values.yaml (may use code defaults)"
fi

# Test 12: Check that HPA is disabled by default
echo ""
echo "=== Test 12: HPA Default Configuration ==="

if grep -q "enabled: false" "$VALUES_YAML" | grep -A 2 "hpa:"; then
    log_success "HPA is disabled by default (correct for dev)"
else
    log_info "Could not verify HPA default (may be structured differently)"
fi

# Test 13: Verify metrics port is exposed
echo ""
echo "=== Test 13: Metrics Port ==="

if grep -q "name: metrics" "$DEPLOYMENT_TEMPLATE"; then
    log_success "Metrics port is exposed in deployment"
else
    log_error "Metrics port not found in deployment"
    exit 1
fi

if grep -q "containerPort: 9090" "$DEPLOYMENT_TEMPLATE"; then
    log_success "Metrics containerPort 9090 is configured"
else
    log_error "Metrics containerPort 9090 not found"
    exit 1
fi

echo ""
echo "========================================"
echo -e "${GREEN}All template verification tests passed!${NC}"
echo "========================================"
echo ""
echo "Verified:"
echo "  ✓ HPA template with correct metrics (Pods/External types)"
echo "  ✓ PrometheusRule with all §14.9 alerts"
echo "  ✓ Headless Service for peer discovery"
echo "  ✓ Downward API env vars (POD_NAME, POD_NAMESPACE, POD_IP)"
echo "  ✓ ServiceMonitor for metrics scraping"
echo "  ✓ values.schema.json HPA validation"
echo "  ✓ Metrics port 9090 exposed"
echo ""
echo "Note: This validates the Helm chart templates are correct."
echo "For full end-to-end testing with a real cluster, use:"
echo "  ./tests/p6_8_multi_pod_acceptance.sh (requires kind)"
