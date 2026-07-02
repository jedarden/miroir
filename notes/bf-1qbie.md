# v0.1.0 Release Status (bf-1qbie)

## Completed Components

### ✅ Git Tag
- Tag `v0.1.0` exists on origin (Forgejo)
- Tag `v0.1.0` synced to GitHub
- Tag points to commit 9c61700 (fix(ci): add make to build dependencies)

### ✅ GitHub Release
- Release `v0.1.0` exists on GitHub (jedarden/miroir)
- Includes all required binaries:
  - miroir-proxy-linux-amd64 (32 MB)
  - miroir-ctl-linux-amd64 (7.3 MB)
  - miroir-proxy-linux-amd64.sha256
  - miroir-ctl-linux-amd64.sha256
- Release is not draft, not prerelease
- Release notes extracted from CHANGELOG.md

### ✅ Local Artifacts
- Binaries built locally and match release checksums
- Docker image built locally: ghcr.io/jedarden/miroir:0.1.0 (32.7 MB)
- Additional tags: 0, 0.1, latest, v0.1.0

## Incomplete Components

### ❌ Docker Image on Registry
- **Status**: Image exists locally but NOT pushed to ghcr.io
- **Issue**: GitHub token lacks `write:packages` scope
- **Error**: `denied: permission_denied: The token provided does not match expected scopes`
- **Required**: GitHub PAT with `write:packages` scope to push to ghcr.io

### ❌ Helm Chart Publication
- **Status**: Chart not published to GitHub Pages or OCI
- **GitHub Pages**: Returns 404 (no GitHub Pages site configured)
- **OCI**: Not published to oci://ghcr.io/jedarden/charts/miroir
- **Issues**:
  - No `helm` command available in local environment
  - GitHub Pages site not created/initialized
  - Requires GitHub token with proper scopes

### ❌ CI/CD Workflow Status
- **Workflow**: miroir-release on iad-ci cluster
- **Status**: All recent runs FAILED at build-binaries step (exit code 101)
- **Last 5 runs**: All failed
- **Issue**: Build step failing in cargo build

## Verification Summary

| Artifact | Status | Location |
|----------|--------|----------|
| Git tag v0.1.0 | ✅ Complete | origin, GitHub |
| GitHub release | ✅ Complete | github.com/jedarden/miroir |
| Binaries | ✅ Complete | Attached to release |
| Checksums | ✅ Complete | Attached to release |
| Docker image | ❌ Incomplete | Local only, not on ghcr.io |
| Helm chart | ❌ Incomplete | Not published to Pages or OCI |

## Next Steps

To complete the release:

1. **Fix GitHub Token Scopes**:
   - Update GitHub PAT to include `write:packages` scope
   - Token currently has: `[REDACTED]`

2. **Push Docker Image**:
   ```bash
   docker login ghcr.io -u jedarden --password-stdin < token_with_write_packages
   docker push ghcr.io/jedarden/miroir:0.1.0
   ```

3. **Publish Helm Chart**:
   - Initialize GitHub Pages site for the repo
   - Package and push chart to OCI
   - Publish to GitHub Pages

4. **Fix CI Workflow** (optional):
   - Debug why build-binaries step fails with exit code 101
   - May be related to cargo build failures

## Task Acceptance Criteria

Per bead bf-1qbie requirements:
- ✅ Tag v0.1.0 visible on origin
- ✅ GitHub Release exists with both binaries and checksums
- ❌ Image ghcr.io/jedarden/miroir:0.1.0 exists (local only)
- ❌ Helm chart published to oci://ghcr.io/jedarden/charts/miroir

**Overall Status**: 50% complete (2/4 criteria met)

## Date
2026-07-02
