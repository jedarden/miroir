# P12.OP6: ARM64 Support (Deferred to v1.x+)

## Status: DEFERRED

This bead remains open as a placeholder for future arm64 support. Per Plan §15 Open Problem #6: "Not planned for v0.x. Added when K8s ARM node support is required."

## Current State (2026-05-08)

### Architecture
- **Target**: `x86_64-unknown-linux-musl` only
- **CI**: GitHub Actions `ubuntu-latest` (amd64 runners)
- **Fleet**: All amd64 (iad-ci cluster, ardenone nodes)
- **Helm**: Architecture-agnostic (no changes needed when arm64 is added)

### Build Artifacts
- `miroir-proxy`: Static musl binary (amd64 only)
- `miroir-ctl`: Control plane binary (amd64 only)

## When Prioritized (v1.x+)

### Required Changes

1. **CI Pipeline** (`.github/workflows/test.yml` or separate build workflow):
   ```yaml
   - name: Build multi-arch
     run: |
       rustup target add x86_64-unknown-linux-musl
       rustup target add aarch64-unknown-linux-musl
       cargo build --release --target x86_64-unknown-linux-musl -p miroir-proxy
       cargo build --release --target aarch64-unknown-linux-musl -p miroir-proxy
   ```

2. **Docker Image**:
   - Build manifest list spanning `linux/amd64` + `linux/arm64`
   - Publish to `ghcr.io/jedarden/miroir:<version>`

3. **Testing**:
   - Phase 9 CI: Add arm64 test runs on arm64 runners

## Triggers for Promotion

Promote this bead to in-progress when:
- [ ] Fleet includes ARM nodes (Hetzner Ampere, AWS Graviton, GCP Tau T2A, Rackspace Spot)
- [ ] Concrete deployment requirement emerges
- [ ] CI infrastructure for arm64 is available/justified

## References

- Plan §15 Open Problem #6
- https://github.com/jedarden/miroir/issues/[TBD]
