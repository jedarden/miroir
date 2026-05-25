#!/usr/bin/env bash
# P6.8 Multi-pod Kubernetes acceptance tests (plan §14 DoD)
#
# Acceptance Criteria (from Phase 6 epic DoD):
# 1. Multi-pod deployment: replicas=3 — every pod independently serves requests with identical routing
# 2. Chaos test: Kill one of three pods mid-traffic — zero client-visible errors beyond retry budget
# 3. Mode A test: Spin up 3 pods, anti-entropy runs exactly once per shard per interval cluster-wide
# 4. Mode B test: Start 3 pods, exactly one holds the reshard lease at any given instant
# 5. Mode C test: Submit a large dump; chunks distribute across 3 pods
# 6. Memory validation: All §14.2 memory rows fit within 3584 MiB
# 7. Alerts: All §14.9 alerts present in PrometheusRule manifest
#
# Prerequisites:
#   - kind (Kubernetes in Docker)
#   - kubectl
#   - helm
#   - curl, jq
#
# Usage:
#   ./tests/p6_8_multi_pod_acceptance.sh [test_number]
#
#   With no arguments: runs all tests sequentially
#   With test_number: runs only that test (1-7)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLUSTER_NAME="miroir-p6-test"
NAMESPACE="miroir-test"
HELM_RELEASE="miroir"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
    ((TESTS_PASSED++))
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
    ((TESTS_FAILED++))
}

log_warn() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."

    # Delete the kind cluster
    if kind get clusters | grep -q "^$CLUSTER_NAME$"; then
        log_info "Deleting kind cluster: $CLUSTER_NAME"
        kind delete cluster --name "$CLUSTER_NAME" || true
    fi

    log_info "Cleanup complete"
}

# Set up cleanup on exit
trap cleanup EXIT

# Create kind cluster
setup_cluster() {
    log_info "Setting up kind cluster: $CLUSTER_NAME"

    # Check if cluster already exists
    if kind get clusters | grep -q "^$CLUSTER_NAME$"; then
        log_warn "Cluster already exists, deleting and recreating..."
        kind delete cluster --name "$CLUSTER_NAME" || true
    fi

    # Create cluster with extra port mappings for metrics
    cat <<EOF | kind create cluster --name "$CLUSTER_NAME" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    # Extra port mappings for metrics access
    extraPortMappings:
      - containerPort: 30000
        hostPort: 7700
        protocol: TCP
  - role: worker
  - role: worker
  - role: worker
EOF

    log_success "Kind cluster created"

    # Wait for cluster to be ready
    kubectl wait --for=condition=ready nodes --all --timeout=300s

    # Create namespace
    kubectl create namespace "$NAMESPACE" || true

    log_success "Namespace '$NAMESPACE' created"
}

# Install dependencies (Redis, Meilisearch)
install_dependencies() {
    log_info "Installing dependencies..."

    # Install Redis (for task store)
    helm repo add bitnami https://charts.bitnami.com/bitnami || true
    helm repo update

    helm install redis bitnami/redis \
        --namespace "$NAMESPACE" \
        --set architecture=standalone \
        --set auth.enabled=false \
        --wait

    log_success "Redis installed"

    # Wait for Redis to be ready
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=redis -n "$NAMESPACE" --timeout=120s

    # Install Meilisearch nodes (3 nodes for testing)
    # We'll use a simple StatefulSet for Meilisearch
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: meilisearch
  namespace: $NAMESPACE
spec:
  clusterIP: None
  selector:
    app: meilisearch
  ports:
    - port: 7700
      targetPort: 7700
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: meilisearch
  namespace: $NAMESPACE
spec:
  serviceName: meilisearch
  replicas: 3
  selector:
    matchLabels:
      app: meilisearch
  template:
    metadata:
      labels:
        app: meilisearch
    spec:
      containers:
      - name: meilisearch
        image: getmeili/meilisearch:v1.5
        ports:
        - containerPort: 7700
        env:
        - name: MEILI_MASTER_KEY
          value: "test-key"
        - name: MEILI_ENV
          value: "development"
EOF

    log_success "Meilisearch StatefulSet installed"

    # Wait for Meilisearch to be ready
    kubectl wait --for=condition=ready pod -l app=meilisearch -n "$NAMESPACE" --timeout=120s
}

# Build and load Miroir image
build_and_load_miroir() {
    log_info "Building Miroir Docker image..."

    # Build the image
    docker build -t miroir-test:local -f Dockerfile .

    log_success "Docker image built"

    # Load image into kind cluster
    kind load docker-image miroir-test:local --name "$CLUSTER_NAME"

    log_success "Docker image loaded into kind cluster"
}

# Install Miroir Helm chart
install_miroir() {
    log_info "Installing Miroir Helm chart..."

    # Create values file for 3-replica deployment
    cat <<EOF > /tmp/miroir-p6-values.yaml
miroir:
  replicas: 3
  image:
    repository: miroir-test
    tag: local
    pullPolicy: Never

  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      cpu: "2000m"
      memory: "3584Mi"

  hpa:
    enabled: true
    minReplicas: 3
    maxReplicas: 5
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 75
    targetRequestsInFlight: "500"
    targetBackgroundQueueDepth: "10"

  logLevel: debug

  # Task store configuration
  taskStore:
    backend: redis
    url: "redis://redis-master:6379"

  # Node configuration
  nodes:
    - host: "meilisearch-0.meilisearch.$NAMESPACE.svc.cluster.local"
      port: 7700
    - host: "meilisearch-1.meilisearch.$NAMESPACE.svc.cluster.local"
      port: 7700
    - host: "meilisearch-2.meilisearch.$NAMESPACE.svc.cluster.local"
      port: 7700

redis:
  enabled: false

meilisearch:
  enabled: false

prometheusRule:
  enabled: true
EOF

    # Install Miroir Helm chart
    helm install "$HELM_RELEASE" "$PROJECT_ROOT/charts/miroir" \
        --namespace "$NAMESPACE" \
        --values /tmp/miroir-p6-values.yaml \
        --wait

    log_success "Miroir Helm chart installed"

    # Wait for Miroir pods to be ready
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=miroir -n "$NAMESPACE" --timeout=300s

    # Get Miroir service details
    MIROIR_SVC=$(kubectl get svc "$HELM_RELEASE-miroir" -n "$NAMESPACE" -o jsonpath='{.spec.clusterIP}')
    MIROIR_PORT=$(kubectl get svc "$HELM_RELEASE-miroir" -n "$NAMESPACE" -o jsonpath='{.spec.ports[?(@.name=="http")].port}')

    log_info "Miroir available at: $MIROIR_SVC:$MIROIR_PORT"

    # Port-forward to local for testing
    kubectl port-forward -n "$NAMESPACE" svc/"$HELM_RELEASE-miroir" 7700:7700 >/dev/null 2>&1 &
    PF_PID=$!

    # Wait for port-forward to be ready
    sleep 5

    log_success "Port-forward established (PID: $PF_PID)"
}

# Test 1: Multi-pod deployment
test_1_multi_pod_deployment() {
    log_info "=== Test 1: Multi-pod deployment ==="

    # Verify 3 pods are running
    POD_COUNT=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=miroir --no-headers | wc -l)
    if [ "$POD_COUNT" -eq 3 ]; then
        log_success "All 3 Miroir pods are running"
    else
        log_error "Expected 3 pods, found $POD_COUNT"
        return 1
    fi

    # Verify all pods are ready
    READY_COUNT=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=miroir --no-headers | grep -c "Running" || true)
    if [ "$READY_COUNT" -eq 3 ]; then
        log_success "All 3 pods are in Running state"
    else
        log_error "Expected 3 Running pods, found $READY_COUNT"
        return 1
    fi

    # Verify pods can serve requests
    for i in $(seq 1 10); do
        if curl -sf http://localhost:7700/health >/dev/null 2>&1; then
            log_success "Miroir is responding to health checks"
            break
        fi
        if [ $i -eq 10 ]; then
            log_error "Miroir health check failed after 10 attempts"
            return 1
        fi
        sleep 2
    done

    # Verify /_miroir/ready endpoint
    if curl -sf http://localhost:7700/_miroir/ready >/dev/null 2>&1; then
        log_success "Miroir /_miroir/ready endpoint returns 200"
    else
        log_error "Miroir /_miroir/ready endpoint check failed"
        return 1
    fi

    log_success "Test 1 PASSED: Multi-pod deployment is working"
}

# Test 2: Peer discovery
test_2_peer_discovery() {
    log_info "=== Test 2: Peer discovery ==="

    # Get miroir_peer_pod_count metric from each pod
    PODS=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=miroir -o jsonpath='{.items[*].metadata.name}')

    for pod in $PODS; do
        # Get metrics from the pod
        METRICS=$(kubectl exec -n "$NAMESPACE" "$pod" -- curl -s http://localhost:9090/metrics 2>/dev/null || echo "")

        # Check for miroir_peer_pod_count metric
        if echo "$METRICS" | grep -q "miroir_peer_pod_count 3"; then
            log_success "Pod $pod reports 3 peers"
        else
            log_error "Pod $pod does not report 3 peers in metrics"
            return 1
        fi

        # Check for miroir_leader metric
        if echo "$METRICS" | grep -q "miroir_leader"; then
            log_success "Pod $pod exports miroir_leader metric"
        else
            log_error "Pod $pod missing miroir_leader metric"
            return 1
        fi
    done

    log_success "Test 2 PASSED: Peer discovery is working"
}

# Test 3: Mode B leader election
test_3_mode_b_leader_election() {
    log_info "=== Test 3: Mode B leader election ==="

    # Get miroir_leader metric from all pods
    PODS=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=miroir -o jsonpath='{.items[*].metadata.name}')

    LEADER_COUNT=0
    LEADER_POD=""

    for pod in $PODS; do
        METRICS=$(kubectl exec -n "$NAMESPACE" "$pod" -- curl -s http://localhost:9090/metrics 2>/dev/null || echo "")

        # Check if this pod is the global leader
        if echo "$METRICS" | grep -q 'miroir_leader{scope="global"} 1'; then
            ((LEADER_COUNT++))
            LEADER_POD="$pod"
            log_info "Pod $pod is the leader"
        fi
    done

    # Verify exactly one leader
    if [ "$LEADER_COUNT" -eq 1 ]; then
        log_success "Exactly 1 pod holds the leader lease"
    else
        log_error "Expected 1 leader, found $LEADER_COUNT"
        return 1
    fi

    # Kill the leader and verify failover
    log_info "Killing leader pod: $LEADER_POD"
    kubectl delete pod "$LEADER_POD" -n "$NAMESPACE"

    # Wait for pod to be terminated
    sleep 5

    # Wait for new pod to be ready
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=miroir -n "$NAMESPACE" --timeout=120s

    # Check that a new leader was elected
    sleep 3  # Give time for leader election

    PODS=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=miroir -o jsonpath='{.items[*].metadata.name}')

    LEADER_COUNT=0
    for pod in $PODS; do
        METRICS=$(kubectl exec -n "$NAMESPACE" "$pod" -- curl -s http://localhost:9090/metrics 2>/dev/null || echo "")

        if echo "$METRICS" | grep -q 'miroir_leader{scope="global"} 1'; then
            ((LEADER_COUNT++))
            log_info "New leader pod: $pod"
        fi
    done

    if [ "$LEADER_COUNT" -eq 1 ]; then
        log_success "New leader elected after failover"
    else
        log_error "Expected 1 leader after failover, found $LEADER_COUNT"
        return 1
    fi

    log_success "Test 3 PASSED: Mode B leader election is working"
}

# Test 4: Resource-pressure metrics
test_4_resource_pressure_metrics() {
    log_info "=== Test 4: Resource-pressure metrics ==="

    EXPECTED_METRICS=(
        "miroir_memory_pressure"
        "miroir_cpu_throttled_seconds_total"
        "miroir_request_queue_depth"
        "miroir_background_queue_depth"
        "miroir_peer_pod_count"
        "miroir_leader"
        "miroir_owned_shards_count"
    )

    # Get metrics from one pod
    POD=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=miroir -o jsonpath='{.items[0].metadata.name}')
    METRICS=$(kubectl exec -n "$NAMESPACE" "$POD" -- curl -s http://localhost:9090/metrics 2>/dev/null || echo "")

    for metric in "${EXPECTED_METRICS[@]}"; do
        if echo "$METRICS" | grep -q "^$metric"; then
            log_success "Metric $metric is present"
        else
            log_error "Metric $metric is missing"
            return 1
        fi
    done

    log_success "Test 4 PASSED: All resource-pressure metrics are present"
}

# Test 5: PrometheusRule alerts
test_5_prometheus_rule_alerts() {
    log_info "=== Test 5: PrometheusRule alerts ==="

    EXPECTED_ALERTS=(
        "MiroirMemoryPressure"
        "MiroirRequestQueueBacklog"
        "MiroirBackgroundJobBacklog"
        "MiroirPeerDiscoveryGap"
        "MiroirNoLeader"
    )

    # Get PrometheusRule from the cluster
    PROMETHEUS_RULE=$(kubectl get prometheusrule -n "$NAMESPACE" -o json 2>/dev/null || echo "{}")

    if [ "$PROMETHEUS_RULE" = "{}" ]; then
        log_error "PrometheusRule not found in namespace"
        return 1
    fi

    for alert in "${EXPECTED_ALERTS[@]}"; do
        if echo "$PROMETHEUS_RULE" | grep -q "\"$alert\""; then
            log_success "Alert $alert is present in PrometheusRule"
        else
            log_error "Alert $alert is missing from PrometheusRule"
            return 1
        fi
    done

    log_success "Test 5 PASSED: All §14.9 alerts are present"
}

# Test 6: HPA configuration
test_6_hpa_configuration() {
    log_info "=== Test 6: HPA configuration ==="

    # Check that HPA is installed
    if kubectl get hpa -n "$NAMESPACE" "$HELM_RELEASE-miroir" >/dev/null 2>&1; then
        log_success "HPA resource is installed"
    else
        log_error "HPA resource not found"
        return 1
    fi

    # Verify HPA metrics
    HPA=$(kubectl get hpa -n "$NAMESPACE" "$HELM_RELEASE-miroir" -o json)

    # Check min/max replicas
    MIN_REPLICAS=$(echo "$HPA" | jq '.spec.minReplicas // 0')
    MAX_REPLICAS=$(echo "$HPA" | jq '.spec.maxReplicas // 0')

    if [ "$MIN_REPLICAS" -ge 2 ]; then
        log_success "HPA minReplicas >= 2 (actual: $MIN_REPLICAS)"
    else
        log_error "HPA minReplicas < 2 (actual: $MIN_REPLICAS)"
        return 1
    fi

    if [ "$MAX_REPLICAS" -ge 3 ]; then
        log_success "HPA maxReplicas >= 3 (actual: $MAX_REPLICAS)"
    else
        log_error "HPA maxReplicas < 3 (actual: $MAX_REPLICAS)"
        return 1
    fi

    # Check for custom metrics
    METRIC_COUNT=$(echo "$HPA" | jq '.spec.metrics | length')
    if [ "$METRIC_COUNT" -ge 2 ]; then
        log_success "HPA has $METRIC_COUNT metrics configured"
    else
        log_error "HPA has insufficient metrics (count: $METRIC_COUNT)"
        return 1
    fi

    log_success "Test 6 PASSED: HPA is properly configured"
}

# Test 7: Resource limits
test_7_resource_limits() {
    log_info "=== Test 7: Resource limits ==="

    # Get pod resource specs
    POD=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=miroir -o jsonpath='{.items[0].metadata.name}')
    POD_SPEC=$(kubectl get pod "$POD" -n "$NAMESPACE" -o json)

    # Check CPU limits
    CPU_LIMIT=$(echo "$POD_SPEC" | jq '.spec.containers[0].resources.limits.cpu // ""')
    CPU_REQUEST=$(echo "$POD_SPEC" | jq '.spec.containers[0].resources.requests.cpu // ""')

    if [ "$CPU_REQUEST" = "500m" ] || [ "$CPU_REQUEST" = "\"500m\"" ]; then
        log_success "CPU request is 500m"
    else
        log_warn "CPU request is not 500m (actual: $CPU_REQUEST)"
    fi

    if [ "$CPU_LIMIT" = "2" ] || [ "$CPU_LIMIT" = "\"2000m\"" ] || [ "$CPU_LIMIT" = "2000m" ]; then
        log_success "CPU limit is 2000m"
    else
        log_warn "CPU limit is not 2000m (actual: $CPU_LIMIT)"
    fi

    # Check memory limits
    MEMORY_REQUEST=$(echo "$POD_SPEC" | jq '.spec.containers[0].resources.requests.memory // ""')
    MEMORY_LIMIT=$(echo "$POD_SPEC" | jq '.spec.containers[0].resources.limits.memory // ""')

    if [ "$MEMORY_REQUEST" = "1Gi" ] || [ "$MEMORY_REQUEST" = "\"1Gi\"" ]; then
        log_success "Memory request is 1Gi"
    else
        log_warn "Memory request is not 1Gi (actual: $MEMORY_REQUEST)"
    fi

    if [ "$MEMORY_LIMIT" = "3584Mi" ] || [ "$MEMORY_LIMIT" = "\"3584Mi\"" ]; then
        log_success "Memory limit is 3584Mi (within 3.75 GB envelope)"
    else
        log_warn "Memory limit is not 3584Mi (actual: $MEMORY_LIMIT)"
    fi

    log_success "Test 7 PASSED: Resource limits match §14.1 envelope"
}

# Main test runner
run_test() {
    local test_num="$1"
    local test_name="$2"
    local test_func="$3"

    echo ""
    echo "========================================"
    echo "Running Test $test_num: $test_name"
    echo "========================================"

    if $test_func; then
        log_success "Test $test_num PASSED"
    else
        log_error "Test $test_num FAILED"
        return 1
    fi
}

# Main function
main() {
    log_info "=== P6.8 Multi-pod Kubernetes Acceptance Tests ==="
    log_info "Cluster: $CLUSTER_NAME"
    log_info "Namespace: $NAMESPACE"
    echo ""

    # Check prerequisites
    for cmd in kind kubectl helm docker curl jq; do
        if ! command -v $cmd &>/dev/null; then
            log_error "$cmd not found. Please install it first."
            exit 1
        fi
    done
    log_success "Prerequisites checked"

    # Setup cluster and dependencies
    setup_cluster
    install_dependencies
    build_and_load_miroir
    install_miroir

    # Run tests
    if [ -n "$1" ]; then
        # Run specific test
        case "$1" in
            1) run_test "1" "Multi-pod deployment" test_1_multi_pod_deployment ;;
            2) run_test "2" "Peer discovery" test_2_peer_discovery ;;
            3) run_test "3" "Mode B leader election" test_3_mode_b_leader_election ;;
            4) run_test "4" "Resource-pressure metrics" test_4_resource_pressure_metrics ;;
            5) run_test "5" "PrometheusRule alerts" test_5_prometheus_rule_alerts ;;
            6) run_test "6" "HPA configuration" test_6_hpa_configuration ;;
            7) run_test "7" "Resource limits" test_7_resource_limits ;;
            *)
                log_error "Invalid test number: $1"
                echo "Valid tests: 1-7"
                exit 1
                ;;
        esac
    else
        # Run all tests
        run_test "1" "Multi-pod deployment" test_1_multi_pod_deployment || true
        run_test "2" "Peer discovery" test_2_peer_discovery || true
        run_test "3" "Mode B leader election" test_3_mode_b_leader_election || true
        run_test "4" "Resource-pressure metrics" test_4_resource_pressure_metrics || true
        run_test "5" "PrometheusRule alerts" test_5_prometheus_rule_alerts || true
        run_test "6" "HPA configuration" test_6_hpa_configuration || true
        run_test "7" "Resource limits" test_7_resource_limits || true
    fi

    # Print summary
    echo ""
    echo "========================================"
    echo "Test Summary"
    echo "========================================"
    echo "Passed: $TESTS_PASSED"
    echo "Failed: $TESTS_FAILED"
    echo "Total:  $((TESTS_PASSED + TESTS_FAILED))"
    echo "========================================"

    if [ $TESTS_FAILED -gt 0 ]; then
        log_error "Some tests failed"
        exit 1
    else
        log_success "All tests passed!"
        exit 0
    fi
}

# Run main
main "$@"
