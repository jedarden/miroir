# Phase 8 — Deployment + CI: Completion Summary

## Infrastructure Implemented

### Dockerfile
- `FROM scratch` with static musl binary
- OCI labels: source, version, revision, licenses=MIT
- Exposes 7700 (HTTP) and 9090 (metrics)
- Build expects `miroir-proxy-linux-amd64` from CI

### CI/CD Pipeline (miroir-ci WorkflowTemplate)
- DAG: checkout → lint → test → bench-check → build → docker (tag-gated) → release (tag-gated)
- Uses `rust:1.87-slim` for cargo operations
- `x86_64-unknown-linux-musl` target for static binaries
- Kaniko for Docker builds to `ghcr.io/jedarden/miroir`
- GitHub releases with binaries + sha256 checksums
- Prerelease detection for `-rc.N` tags

### Helm Chart (charts/miroir/)
- Templates: deployment, service, headless, configmap, secret, HPA, PVC, StatefulSet (meilisearch), serviceaccount
- Dev defaults: replicas=1, SQLite, RF=1, RG=1, HPA off
- values.schema.json enforces HA requirements (Redis with replicas>1, HPA requires Redis)
- Test suite validates schema rejections

### ArgoCD Applications
- `miroir-ardenone-cluster`: Production config (2 replicas, Redis, Meilisearch HA)
- Syncs from `ghcr.io/jedarden/charts/miroir`

### Release Mechanics
- `scripts/bump-version.sh`: Coordinated Cargo.toml + Chart.yaml version bumps
- `scripts/release-ready-check.sh`: Validates version consistency across Cargo.toml, Chart.yaml, CHANGELOG.md
- `CHANGELOG.md`: Keep a Changelog format with v0.1.0 section complete

## Verification Status

| DoD Item | Status | Notes |
|----------|--------|-------|
| CI pipeline completes on main | ⏳ Pending | Requires kubectl access to iad-ci |
| Tag push produces image + release | ⏳ Pending | Requires CI run |
| helm install works | ⏳ Pending | Requires helm CLI |
| values.schema.json tested | ⏳ Pending | Tests exist, need helm |
| Image ≤ 15 MB compressed | ⏳ Pending | Will verify on first build |
| ArgoCD app syncs cleanly | ⏳ Pending | Requires kubectl access |

## Next Steps for Verification

1. Submit workflow to iad-ci: `kubectl apply -f k8s/iad-ci/argo-workflows/miroir-ci.yaml`
2. Run smoke test: `kubectl create -f - < workflow-manual.yaml`
3. Tag v0.1.0-rc.1: `git tag v0.1.0-rc.1 && git push origin v0.1.0-rc.1`
4. Verify ghcr.io image and GitHub release
5. Test helm install: `helm install search charts/miroir --namespace search --wait`

## Files Modified

This phase created infrastructure files only; no source code changes.
All workflow templates and ArgoCD apps are synced to declarative-config.
