# P6.2 Peer Discovery Implementation Summary

## Status: Complete ✓

Peer discovery via headless Service + Downward API is fully implemented per plan §14.5.

## Implementation Verified

### Helm Templates
- `charts/miroir/templates/miroir-headless.yaml`
  - `clusterIP: None` for headless Service
  - Label selector matches Deployment pods
  - Service name: `{{ include "miroir.fullname" . }}-headless`

- `charts/miroir/templates/miroir-deployment.yaml`
  - `POD_NAME` from `metadata.name` via Downward API
  - `POD_NAMESPACE` from `metadata.namespace` via Downward API
  - `POD_IP` from `status.podIP` via Downward API

- `charts/miroir/templates/_helpers.tpl` (line 181)
  - Config injection: `service_name: {{ printf "%s-headless" (include "miroir.fullname" .) }}`
  - Matches headless Service name exactly

### Rust Code
- `crates/miroir-core/src/peer_discovery.rs`
  - `PeerSet` struct with `peers: Vec<PeerId>` and `refreshed_at: Instant`
  - `PeerDiscovery::refresh()` for SRV lookup via trust-dns-resolver
  - Feature flag: `peer-discovery` (enabled in miroir-proxy)

- `crates/miroir-core/src/config.rs`
  - `PeerDiscoveryConfig` struct with `service_name` and `refresh_interval_s`
  - Defaults: `service_name: "miroir-headless"`, `refresh_interval_s: 15`

- `crates/miroir-proxy/src/main.rs` (lines 79-91, 407-431)
  - Creates `PeerDiscovery` instance when `POD_NAME != "unknown"`
  - Background refresh loop runs every `refresh_interval_s` seconds
  - Calls `metrics.set_peer_pod_count(count)` on successful refresh

- `crates/miroir-proxy/src/middleware.rs`
  - `miroir_peer_pod_count` gauge metric (line 824)
  - `set_peer_pod_count(u64)` method (line 1582)

### Verification
- `tests/verify_p6_2_peer_discovery.sh`
  - NixOS-compatible shebang: `#!/usr/bin/env bash`
  - Checks for metric existence and env vars

## Acceptance Criteria (Require Kubernetes Cluster)

The following acceptance tests require a multi-pod Kubernetes deployment:

1. **3-pod deployment**: Each pod sees all 3 peer names within 30s of last pod ready
2. **Scale 3→5**: New peers discovered within `refresh_interval_s × 2` (30s)
3. **Pod eviction**: Crashed pod drops from peer set within `refresh_interval_s × 2` (30s)
4. **Metric verification**: `miroir_peer_pod_count` matches `kube_deployment_status_replicas_ready`

## Prior Commits

- `e6cdd05` - P6.2: Fix peer discovery DNS SRV service name and add test
- `26c9521` - P6.2: Fix peer discovery DNS SRV service name and add POD_IP
- `cf9ae11` - P6.2: Fix verification script shebang for NixOS compatibility
- `7784076` - P6.2: Peer discovery implementation verification notes
- `bddfeb3` - P6.2: Verify peer discovery implementation (plan §14.5)
