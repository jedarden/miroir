# v0.1.0 Release Final Status (bf-1qbie)

## Summary

The v0.1.0 release was successfully initiated on 2026-07-02 with the core components completed, but CI infrastructure issues prevent full completion of Docker image and Helm chart publication.

## Completed Components

### ✅ Git Tag v0.1.0
- Tag exists on origin (Forgejo): `4edb8e7420fc3ff89db0b7951081a9fced349152`
- Tag synced to GitHub mirror
- Tag is annotated with comprehensive release notes
- Tag points to the fix commit that resolved `.dockerignore` issue

### ✅ GitHub Release
- Release v0.1.0 exists: `github.com/jedarden/miroir/releases/tag/v0.1.0`
- All required binaries present and accessible:
  - `miroir-proxy-linux-amd64` (32,743,640 bytes)
  - `miroir-ctl-linux-amd64` (7,554,184 bytes)
  - `miroir-proxy-linux-amd64.sha256` (91 bytes)
  - `miroir-ctl-linux-amd64.sha256` (89 bytes)
- Release is live (not draft, not prerelease)
- Release notes properly extracted from CHANGELOG.md

### ✅ Version Consistency
- `Cargo.toml` workspace package version: `0.1.0` ✅
- `CHANGELOG.md` contains `[0.1.0]` section dated 2026-04-19 ✅
- Tag version matches workspace version ✅
- No unreleased changes requiring reconciliation ✅

## Incomplete Components (Blocked by CI Infrastructure)

### ❌ Docker Image on GHCR
- **Expected**: `ghcr.io/jedarden/miroir:0.1.0` with float tags (0.1, 0, latest)
- **Status**: Cannot verify (GHCR requires authentication)
- **CI Status**: Multiple workflow runs failed due to iad-ci cluster PVC/volume attachment issues

### ❌ Helm Chart Publication
- **GitHub Pages**: Returns 404 "Site not found" (not configured)
- **OCI Registry**: Cannot verify (GHCR requires authentication)
- **Expected**: `oci://ghcr.io/jedarden/charts/miroir:0.1.0`
- **CI Status**: Workflows failing before reaching Helm chart publication step

## CI Infrastructure Issues

### Workflow Failure Pattern
Multiple `miroir-release` workflow runs have failed with consistent errors:
- Volume attachment failures for PVCs in argo-workflows namespace
- Error: `"Invalid volume: volume status must be 'available'. Currently in 'in-use'"`
- Root cause: Rackspace Spot cluster (iad-ci) volume attachment problems

### Recent Workflow Attempts
```
miroir-release-v0.1.0-manual-2f2gw    Failed  build step errors
miroir-release-v0.1.0-vgw2p           Running (currently stuck at build-binaries)
miroir-release-v0.1.0-xlcr9          Failed  child workflow failure
miroir-release-v0.1.0-p4mpl          Failed  child workflow failure
... and more
```

### Workflow Impact
- Workflows cannot progress past `build-binaries` or `build-image` steps
- Kaniko build steps fail due to volume attachment issues
- Helm chart publication never reached due to earlier failures

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Tag v0.1.0 visible on origin | ✅ COMPLETE | `git ls-remote --tags origin` shows tag |
| GitHub Release with binaries and checksums | ✅ COMPLETE | GitHub API confirms release with 4 assets |
| Image ghcr.io/jedarden/miroir:0.1.0 exists | ❌ BLOCKED | CI infrastructure prevents verification |
| Helm chart published to OCI | ❌ BLOCKED | CI infrastructure prevents publication |

**Overall**: 2/4 complete, 2/4 blocked by external infrastructure issues

## Recommendations

### Immediate Actions
1. **Resolve CI infrastructure**: Fix Rackspace Spot volume attachment issues in iad-ci cluster
2. **Manual publication**: If CI remains broken, manually build/push Docker image and Helm chart
3. **GitHub Pages setup**: Initialize GitHub Pages for Helm chart repository

### Alternative Approaches
1. **Migrate CI**: Consider moving to different CI infrastructure if iad-ci issues persist
2. **Simplified workflow**: Modify workflow to avoid PVC usage if possible
3. **Manual release process**: Document manual release steps as fallback

## Notes

- The CHANGELOG.md `[Unreleased]` section is empty - no reconciliation needed
- Release notes in GitHub release properly match CHANGELOG.md `[0.1.0]` section
- The `.dockerignore` fix (commit `4edb8e7`) was critical for including CI-built binaries
- Tag creation followed proper git tagging workflow with annotated tag

## Date

2026-07-02 (Final verification attempt)
