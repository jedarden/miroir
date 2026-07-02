# v0.1.0 Release Verification (bf-1qbie)

## Summary

The v0.1.0 release was cut on 2026-07-02. Tag and GitHub release are complete with all required binaries and checksums. Docker image and Helm chart publication remain incomplete due to CI infrastructure issues in the iad-ci cluster.

## Completed Components

### ✅ Git Tag
- Tag `v0.1.0` exists on origin (Forgejo)
- Tag `v0.1.0` synced to GitHub mirror
- Tag is annotated with release notes
- Tag points to commit `4edb8e7` (fix(release): remove miroir-proxy from .dockerignore to fix CI build)

### ✅ GitHub Release
- Release `v0.1.0` exists on GitHub (jedarden/miroir)
- Includes all required binaries:
  - `miroir-proxy-linux-amd64` (32.7 MB)
  - `miroir-ctl-linux-amd64` (7.5 MB)
  - `miroir-proxy-linux-amd64.sha256`
  - `miroir-ctl-linux-amd64.sha256`
- Release is NOT draft, NOT prerelease
- Release notes extracted from CHANGELOG.md
- Assets are downloadable and accessible

### ✅ Version Consistency
- `Cargo.toml` workspace package version: `0.1.0` ✅
- `CHANGELOG.md` contains `[0.1.0]` release section ✅
- Tag version matches workspace version ✅
- No unreleased changes in CHANGELOG.md that need reconciliation ✅

## Incomplete Components

### ❌ Docker Image on GHCR
- **Status**: Cannot verify; GHCR requires authentication
- **Expected**: `ghcr.io/jedarden/miroir:0.1.0`
- **Float tags**: `ghcr.io/jedarden/miroir:0.1`, `ghcr.io/jedarden/miroir:0`, `ghcr.io/jedarden/miroir:latest`
- **Issue**: CI workflow unable to complete due to infrastructure issues

### ❌ Helm Chart Publication
- **GitHub Pages**: Site not configured (404 error)
- **OCI**: Cannot verify; GHCR requires authentication
- **Expected**: `oci://ghcr.io/jedarden/charts/miroir:0.1.0`
- **Issue**: CI workflow unable to complete due to infrastructure issues

## CI/CD Infrastructure Issues

### Workflow Failures
- **Workflow**: `miroir-release` on iad-ci cluster
- **Issue**: Volume attachment failures for PVCs
- **Error**: `"Invalid volume: volume '...' status must be 'available'. Currently in 'in-use'"`
- **Root cause**: Rackspace Spot cluster (iad-ci) volume attachment issues
- **Impact**: Cannot run release workflow to build and push Docker image or publish Helm chart

### Attempted Actions
1. Submitted manual workflow run `miroir-release-v0.1.0-manual-642bq`
2. Workflow stuck at `build-binaries` step due to PVC attachment failures
3. Multiple previous workflow runs failed with same volume attachment errors
4. Cleaned up stuck PVCs from previous failed runs

## Verification Summary

| Artifact | Status | Location |
|----------|--------|----------|
| Git tag v0.1.0 | ✅ Complete | origin, GitHub |
| Tag annotation | ✅ Complete | Signed with release notes |
| GitHub release | ✅ Complete | github.com/jedarden/miroir/releases/tag/v0.1.0 |
| Binaries | ✅ Complete | Attached to release (32.7 MB + 7.5 MB) |
| Checksums | ✅ Complete | SHA256 files attached to release |
| Version consistency | ✅ Complete | Cargo.toml, CHANGELOG.md, tag all match |
| Docker image | ❓ Unknown | Cannot verify (GHCR requires auth) |
| Helm chart (OCI) | ❓ Unknown | Cannot verify (GHCR requires auth) |
| Helm chart (Pages) | ❌ Not found | github.io/miroir returns 404 |

## Task Acceptance Criteria

Per bead bf-1qbie acceptance criteria:
- ✅ Tag v0.1.0 visible on origin
- ✅ GitHub Release exists with both binaries and checksums
- ❓ Image ghcr.io/jedarden/miroir:0.1.0 exists (cannot verify without auth)
- ❓ Helm chart published to oci://ghcr.io/jedarden/charts/miroir (cannot verify without auth)

**Overall Status**: Core release complete (2/4 verified), CI infrastructure blocked

## Next Steps

To complete the Docker image and Helm chart publication:

1. **Resolve CI infrastructure issues**:
   - Fix Rackspace Spot volume attachment problems in iad-ci cluster
   - May require cluster admin intervention or migration to different infrastructure

2. **Manual publication (if CI remains broken)**:
   - Build Docker image locally with proper GHCR authentication
   - Push image to ghcr.io/jedarden/miroir:0.1.0
   - Package and push Helm chart to OCI registry
   - Initialize GitHub Pages site and publish chart index

3. **Verify artifacts once published**:
   - Test Docker image pull: `docker pull ghcr.io/jedarden/miroir:0.1.0`
   - Test Helm chart install: `helm install myrelease oci://ghcr.io/jedarden/charts/miroir --version 0.1.0`

## Notes

- The CHANGELOG.md `[Unreleased]` section is empty - no unreleased changes need reconciliation
- The release notes in the GitHub release match the `[0.1.0]` section of CHANGELOG.md
- The tag was created after fixing `.dockerignore` to include CI-built binaries
- Previous attempt (commit `9c61700`) had issues; tag was updated to `4edb8e7` with the fix

## Date

2026-07-02
