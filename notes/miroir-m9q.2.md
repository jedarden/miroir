# P6.2 Peer Discovery Implementation Summary

## Status: Already Implemented

The peer discovery feature described in the task was already implemented in prior commits:
- `e6cdd05` - P6.2: Fix peer discovery DNS SRV service name and add test
- `26c9521` - P6.2: Fix peer discovery DNS SRV service name and add POD_IP

## Implementation Checklist

### Helm Chart
- [x] `charts/miroir/templates/miroir-headless.yaml` - Headless Service with label selector
- [x] `charts/miroir/templates/miroir-deployment.yaml` - Downward API env vars (POD_NAME, POD_NAMESPACE, POD_IP)
- [x] `charts/miroir/templates/_helpers.tpl` - Auto-derived service_name default using `miroir.fullname`
- [x] `charts/miroir/values.yaml` - peer_discovery config with auto-documented defaults

### Rust Code
- [x] `crates/miroir-core/src/peer_discovery.rs` - SRV lookup implementation with trust-dns-resolver
- [x] `crates/miroir-core/src/config.rs` - PeerDiscoveryConfig struct
- [x] `crates/miroir-proxy/src/main.rs` - Background refresh loop (every 15s by default)
- [x] `crates/miroir-proxy/src/middleware.rs` - miroir_peer_pod_count metric

### Verification
- [x] `tests/verify_p6_2_peer_discovery.sh` - Verification script for metrics and env vars
- [x] Unit tests in `peer_discovery.rs` - test_srv_target_pod_name_extraction, test_peer_set_empty, test_peer_set_with_peers

## Acceptance Criteria

The following acceptance criteria require a multi-pod Kubernetes deployment to verify:

1. **3-pod deployment: each pod sees all 3 peer names within 30s of last pod ready**
   - Requires real K8s cluster with 3-pod deployment
   - Verification: Check logs for "peer discovery refresh completed" with peer_count=3

2. **Scale 3→5: new peers discovered within refresh_interval_s × 2**
   - Requires real K8s cluster with scale operation
   - Verification: Scale deployment and observe peer_count update

3. **Pod eviction: crashed pod drops from peer set within refresh_interval_s × 2**
   - Requires real K8s cluster with pod deletion
   - Verification: Delete pod and observe peer_count decrease

4. **miroir_peer_pod_count gauge matches kube_deployment_status_replicas_ready**
   - Requires real K8s cluster with Prometheus scraping
   - Verification: Compare metrics endpoint with deployment status

These are integration tests that should be run in a staging environment. The verification script covers local development smoke testing.

## Notes

- The SRV lookup uses `_http._tcp.<service>.<namespace>.svc.cluster.local` format
- Pod names are extracted from the first component of SRV target FQDNs
- The implementation uses the system DNS resolver configuration from /etc/resolv.conf
- Transient double-work during the 15-second discovery window is acceptable per plan §14.5
