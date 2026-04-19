# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security

## [0.1.0] - TBD

### Added
- Initial release.
- Dockerfile: scratch-based image with static musl binary (~4 MB compressed).
- Helm chart: deployment, service, headless, configmap, secret, HPA, optional PVC, StatefulSet for Meilisearch, Meilisearch service, optional Redis deployment, serviceaccount, PrometheusRule, ServiceMonitor, Grafana dashboard.
- `values.schema.json` rejects incompatible configs: SQLite with HA, HPA without Redis, local rate limits in multi-replica, scoped key rotation >= max age.
- Argo WorkflowTemplate `miroir-ci`: checkout → lint → test → musl build → Kaniko push (tag-gated) → GitHub release (tag-gated).
- Argo WorkflowTemplate `miroir-ci-smoke`: quick lint+test on push.
- ArgoCD Application `miroir-dev-ardenone-cluster` deployed to ardenone-cluster.
- `scripts/bump-version.sh` for coordinated Cargo.toml + Chart.yaml version bumps.
- `scripts/release-ready-check.sh` validates version consistency across Cargo.toml, Chart.yaml, CHANGELOG.md.
