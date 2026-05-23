# P6.2 Peer Discovery Implementation Verification

## Summary

Verified that peer discovery per plan §14.5 is fully implemented:

### 1. Helm Templates ✓
- `charts/miroir/templates/miroir-headless.yaml` - Headless Service with label selector
- `charts/miroir/templates/miroir-deployment.yaml` - POD_NAME, POD_NAMESPACE, POD_IP env vars via Downward API

### 2. Rust Implementation ✓
- `crates/miroir-core/src/peer_discovery.rs` - SRV-based peer discovery module
  - `PeerSet` struct with `peers: Vec<PeerId>` and `refreshed_at: Instant`
  - `PeerDiscovery::refresh()` method for SRV lookup
  - Feature flag: `peer-discovery` (uses `trust-dns-resolver`)

### 3. Configuration ✓
- `crates/miroir-core/src/config.rs` - `PeerDiscoveryConfig` struct
  - `service_name: "miroir-headless"` (default)
  - `refresh_interval_s: 15` (default)
- `charts/miroir/values.yaml` - Config section with same defaults

### 4. Main Loop Integration ✓
- `crates/miroir-proxy/src/main.rs` (lines 407-438)
  - Creates `PeerDiscovery` instance when POD_NAME is set
  - Spawns background refresh loop with configurable interval
  - Calls `metrics.set_peer_pod_count(count)` on successful refresh

### 5. Metrics ✓
- `crates/miroir-proxy/src/middleware.rs` (line 823-825, 1582-1584)
  - `miroir_peer_pod_count` gauge metric
  - `miroir_leader` gauge metric
  - `miroir_owned_shards_count` gauge metric

### 6. Verification Script ✓
- `tests/verify_p6_2_peer_discovery.sh` - Checks metrics and env vars
  - Shebang: `#!/usr/bin/env bash` (NixOS compatible)

## Acceptance Tests (require K8s environment)

The following acceptance tests require a real Kubernetes deployment:

1. **3-pod deployment**: Each pod sees all 3 peer names within 30s of last pod ready
2. **Scale 3→5**: New peers discovered within `refresh_interval_s × 2`
3. **Pod eviction**: Crashed pod drops from peer set within `refresh_interval_s × 2`
4. **Metric verification**: `miroir_peer_pod_count` matches `kube_deployment_status_replicas_ready`

## Unit Tests

All peer discovery unit tests pass:
- `test_peer_set_empty` ✓
- `test_peer_set_with_peers` ✓
- `test_srv_target_pod_name_extraction` ✓

## Implementation Notes

- The peer discovery implementation was already complete in the codebase
- No code changes were required - this task was verification-only
- The `peer-discovery` feature flag must be enabled for SRV lookups to work
- Peer discovery automatically disables when `POD_NAME=unknown` (local dev)

## Plan §14.5 Alignment

Fully implements plan §14.5 "Peer discovery" with:
- Headless Service SRV lookup mechanism
- 15-second refresh interval (configurable)
- Zero-config operation (uses Downward API env vars)
- No K8s API calls from pods
- Transient double-work is acceptable (idempotent operations)
