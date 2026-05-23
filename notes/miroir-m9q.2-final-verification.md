# P6.2 Peer Discovery Final Verification

## Status: Complete ✓

Peer discovery via headless Service + Downward API is fully implemented and verified.

## Implementation Checklist

### Helm Templates
- [x] `charts/miroir/templates/miroir-headless.yaml`
  - `clusterIP: None` for headless Service
  - Label selector matches Deployment pods
  - Conditionally renders when `miroir.replicas` is set
  
- [x] `charts/miroir/templates/miroir-deployment.yaml`
  - `POD_NAME` from `metadata.name` via Downward API
  - `POD_NAMESPACE` from `metadata.namespace` via Downward API
  - `POD_IP` from `status.podIP` via Downward API

### Rust Code
- [x] `crates/miroir-core/src/peer_discovery.rs`
  - `PeerSet` struct with `peers: Vec<PeerId>` and `refreshed_at: Instant`
  - `PeerDiscovery::refresh()` for SRV lookup via trust-dns-resolver
  - Feature flag: `peer-discovery` (always enabled in miroir-proxy)

- [x] `crates/miroir-core/src/config.rs`
  - `PeerDiscoveryConfig` struct with `service_name` and `refresh_interval_s`
  - Defaults: `service_name: "miroir-headless"`, `refresh_interval_s: 15`

- [x] `crates/miroir-proxy/src/main.rs`
  - Creates `PeerDiscovery` instance when `POD_NAME != "unknown"`
  - Background refresh loop runs every `refresh_interval_s` seconds
  - Calls `metrics.set_peer_pod_count(count)` on successful refresh

- [x] `crates/miroir-proxy/src/middleware.rs`
  - `miroir_peer_pod_count` gauge metric
  - `set_peer_pod_count(u64)` method

### Unit Tests
- [x] `test_peer_set_empty` - PASSED
- [x] `test_peer_set_with_peers` - PASSED
- [x] `test_srv_target_pod_name_extraction` - PASSED

### Verification
- [x] `tests/verify_p6_2_peer_discovery.sh`
  - NixOS-compatible shebang: `#!/usr/bin/env bash`
  - Checks for metric existence and env vars

## Acceptance Criteria (Require Kubernetes Cluster)

The following acceptance tests require a multi-pod Kubernetes deployment:

1. **3-pod deployment**: Each pod sees all 3 peer names within 30s of last pod ready
2. **Scale 3→5**: New peers discovered within `refresh_interval_s × 2` (30s)
3. **Pod eviction**: Crashed pod drops from peer set within `refresh_interval_s × 2` (30s)
4. **Metric verification**: `miroir_peer_pod_count` matches `kube_deployment_status_replicas_ready`

These integration tests should be run in a staging environment with a real Kubernetes cluster.

## Plan §14.5 Alignment

Fully implements plan §14.5 "Peer discovery":
- Headless Service SRV lookup mechanism
- 15-second refresh interval (configurable)
- Zero-config operation (uses Downward API env vars)
- No K8s API calls from pods
- Transient double-work is acceptable (idempotent operations)

## References

- Commits: `e6cdd05`, `26c9521`, `cf9ae11`, `7784076`, `bddfeb3`
- Plan: §14.5 Peer discovery
