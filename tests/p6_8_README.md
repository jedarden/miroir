# P6.8 Multi-pod Kubernetes Acceptance Tests

This directory contains acceptance tests for Phase 6 (Horizontal Scaling + HPA) as specified in plan §14 Definition of Done.

## Prerequisites

- [kind](https://kind.sigs.k8s.io/) (Kubernetes in Docker)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [helm](https://helm.sh/)
- [docker](https://www.docker.com/)
- `curl`, `jq`

## Installing Prerequisites

```bash
# Install kind
go install sigs.k8s.io/kind@latest

# Install kubectl (Linux)
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

# Install helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

## Running Tests

### Run All Tests

```bash
./tests/p6_8_multi_pod_acceptance.sh
```

### Run Specific Test

```bash
./tests/p6_8_multi_pod_acceptance.sh 1  # Test 1: Multi-pod deployment
./tests/p6_8_multi_pod_acceptance.sh 2  # Test 2: Peer discovery
./tests/p6_8_multi_pod_acceptance.sh 3  # Test 3: Mode B leader election
./tests/p6_8_multi_pod_acceptance.sh 4  # Test 4: Resource-pressure metrics
./tests/p6_8_multi_pod_acceptance.sh 5  # Test 5: PrometheusRule alerts
./tests/p6_8_multi_pod_acceptance.sh 6  # Test 6: HPA configuration
./tests/p6_8_multi_pod_acceptance.sh 7  # Test 7: Resource limits
```

## Test Coverage

| Test | Description | Acceptance Criterion |
|------|-------------|---------------------|
| 1 | Multi-pod deployment | replicas=3 — every pod independently serves requests |
| 2 | Peer discovery | Pods discover each other via headless Service |
| 3 | Mode B leader election | Exactly one leader; failover within lease_ttl_s |
| 4 | Resource-pressure metrics | All §14.9 metrics present |
| 5 | PrometheusRule alerts | All §14.9 alerts in manifest |
| 6 | HPA configuration | HPA installed with correct metrics |
| 7 | Resource limits | Pod resources match §14.1 envelope |

## What Gets Tested

The test script:

1. **Creates a kind cluster** with 1 control-plane and 3 worker nodes
2. **Installs dependencies**: Redis (task store), Meilisearch (3 nodes)
3. **Builds Miroir** Docker image from local source
4. **Installs Miroir Helm chart** with 3 replicas
5. **Runs acceptance tests** against the live cluster
6. **Cleans up** by deleting the kind cluster

## Manual Verification (Without kind)

If you don't have kind installed, you can verify the Helm chart templates manually:

```bash
# Render the Helm chart with 3-replica values
helm template miroir charts/miroir \
  --set miroir.replicas=3 \
  --set miroir.hpa.enabled=true \
  --set miroir.taskStore.backend=redis \
  --set redis.enabled=true \
  > /tmp/miroir-rendered.yaml

# Check HPA is present
grep -A 30 "kind: HorizontalPodAutoscaler" /tmp/miroir-rendered.yaml

# Check PrometheusRule is present
grep -A 50 "kind: PrometheusRule" /tmp/miroir-rendered.yaml

# Check headless Service for peer discovery
grep -A 10 "miroir-headless" /tmp/miroir-rendered.yaml
```

## CI Integration

These tests should be integrated into the Argo Workflow `miroir-ci` (Phase 8) as a separate step that runs on tag/release, not on every commit.

## Known Limitations

1. **Mode A test** (anti-entropy partitioning) requires more complex setup with actual anti-entropy passes - not yet automated
2. **Mode C test** (dump import chunking) requires a 10GB test dump - not yet automated
3. **Chaos test** (pod kill mid-traffic) requires background traffic generation - not yet automated

The current tests verify the infrastructure is correctly configured. Full behavioral tests will be added in future iterations.

## Troubleshooting

### kind cluster fails to create

```bash
# Check Docker is running
docker ps

# Delete any existing cluster
kind delete cluster --name miroir-p6-test
```

### Pods not ready

```bash
# Check pod status
kubectl get pods -n miroir-test

# Check pod logs
kubectl logs -n miroir-test <pod-name>
```

### Port-forward fails

```bash
# Check if port 7700 is already in use
lsof -i :7700

# Kill any process using the port
kill -9 <pid>
```
