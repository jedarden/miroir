## What changed

<!-- Brief description of the change. -->

## Why

<!-- Link to issue or explain the motivation. -->

## Type of change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Refactor (internal restructuring, no external behavior change)
- [ ] Documentation
- [ ] Release

## CHANGELOG entry

<!-- IMPORTANT: Every PR that changes behavior MUST add an entry under [Unreleased] in CHANGELOG.md -->

<!-- Add your entry under the appropriate subheading (Added/Changed/Deprecated/Removed/Fixed/Security) -->

<!-- Example entry for a bug fix:
```markdown
### Fixed
- Search now returns correct results when query contains special characters (#123)
```
-->

## Breaking changes

<!-- Note any breaking changes and migration steps. Else write "N/A" -->

## Test plan

<!-- How was this tested? -->

---

## Release checklist (only for release PRs)

If this is a release PR, use `.github/release_pr_template.md` instead and verify:

- [ ] All tests pass on `main` (`cargo test --workspace`)
- [ ] `CHANGELOG.md` updated with new version section (`## [x.y.z] - YYYY-MM-DD`)
- [ ] `Cargo.toml` workspace version bumped (`scripts/bump-version.sh x.y.z`)
- [ ] `Chart.yaml` `version` and `appVersion` updated (done by bump script)
- [ ] `scripts/release-ready-check.sh` passes locally
- [ ] Migration notes written in `CHANGELOG.md` if task store schema changed
