## Summary

<!-- Describe what this PR does and why. -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor
- [ ] Documentation
- [ ] Release

## Release checklist

If this is a release PR, verify each item before merging:

- [ ] All tests pass on `main` (`cargo test --workspace`)
- [ ] `CHANGELOG.md` updated with new version section (`## [x.y.z] - YYYY-MM-DD`)
- [ ] `Cargo.toml` workspace version bumped (`scripts/bump-version.sh x.y.z`)
- [ ] `Chart.yaml` `version` and `appVersion` updated (done by bump script)
- [ ] `scripts/release-ready-check.sh` passes locally
- [ ] Migration notes written in `CHANGELOG.md` if task store schema changed
- [ ] PR diff includes `CHANGELOG.md` changes

## CHANGELOG diff

<!-- Paste the relevant CHANGELOG section for this release, e.g.:

## [0.2.0] - 2026-04-19

### Added
- Feature X

### Fixed
- Bug Y
-->

## Test plan

<!-- How was this tested? -->
