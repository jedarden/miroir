# Release v{{VERSION}}

This is a release PR for version {{VERSION}}. It will be tagged as `v{{VERSION}}` after merge.

## Summary

<!-- Brief summary of this release -->

## Release checklist

Before merging, verify each item:

### Version consistency

- [ ] `scripts/bump-version.sh {{VERSION}}` executed successfully
- [ ] `scripts/release-ready-check.sh {{VERSION}}` passes locally
- [ ] `Cargo.toml` workspace version is `{{VERSION}}`
- [ ] `charts/miroir/Chart.yaml` `version` is `{{VERSION}}`
- [ ] `charts/miroir/Chart.yaml` `appVersion` is `{{VERSION}}`

### CHANGELOG

- [ ] `CHANGELOG.md` has new section `## [{{VERSION}}] - YYYY-MM-DD`
- [ ] All changes since last release are documented under appropriate subheadings
- [ ] Release notes extracted from CHANGELOG will be used for GitHub release

### Testing

- [ ] All tests pass on `main` branch
  ```bash
  cargo test --workspace
  ```
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo fmt --check` passes

### Breaking changes / Migration

- [ ] If task store schema changed: Migration notes added to CHANGELOG.md
- [ ] If config schema changed: Migration path documented
- [ ] If API compatibility changed: Versioning policy followed

### Post-merge actions (automated via Argo)

After this PR is merged:
1. Tag will be created automatically: `git tag -a v{{VERSION}}`
2. CI will build release artifacts
3. GitHub release will be created with CHANGELOG notes

## CHANGELOG diff

<!-- Paste the relevant CHANGELOG section for this release -->

```markdown
## [{{VERSION}}] - YYYY-MM-DD

### Added
- Feature X

### Changed
- Behavior Y modified

### Deprecated
- Old feature Z deprecated

### Removed
- Deprecated feature W removed

### Fixed
- Bug V fixed

### Security
- Security issue U addressed
```

## Test plan

<!-- How was this release tested? -->

- [ ] Smoke tests pass on dev cluster
- [ ] Upgrade tested from previous version
- [ ] Rollback tested if applicable
