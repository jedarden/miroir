# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## Versioning Policy

Miroir provides backward-compatibility guarantees for four surfaces starting at v1.0:
- Meilisearch API compatibility layer
- `miroir-ctl` CLI flags
- Config file schema
- Helm chart values schema

See [`docs/versioning-policy.md`](docs/versioning-policy.md) for the full policy, including what constitutes a breaking change for each surface, deprecation procedures, and the pre-1.0 policy.

## [Unreleased]

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security

## [0.1.0] - 2026-04-19

### Added
- Initial release.
- Dockerfile: scratch-based image with static musl binary (~4 MB compressed).
- Helm chart: deployment, service, headless, configmap, secret, HPA, optional PVC, StatefulSet for Meilisearch, Meilisearch service, optional Redis deployment, serviceaccount, PrometheusRule, ServiceMonitor, Grafana dashboard.
- `values.schema.json` rejects incompatible configs: SQLite with HA, HPA without Redis, local rate limits in multi-replica, scoped key rotation >= max age.
- Argo WorkflowTemplate `miroir-ci`: checkout → lint → test → musl build → Kaniko push (tag-gated) → GitHub release (tag-gated).
- Argo WorkflowTemplate `miroir-ci-smoke`: quick lint+test on push.
- Argo WorkflowTemplate `miroir-release`: release-ready gate → Kaniko build → Helm chart publish → GitHub release with binaries.
- Argo WorkflowTemplate `miroir-release-ready`: PR validation gate checking version consistency.
- ArgoCD Application `miroir-dev-ardenone-cluster` (1 replica, SQLite, dev defaults).
- ArgoCD Application `miroir-ardenone-cluster` (2 replicas, Redis, Meilisearch HA).
- `scripts/bump-version.sh` for coordinated Cargo.toml + Chart.yaml version bumps.
- `scripts/release-ready-check.sh` validates version consistency across Cargo.toml, Chart.yaml, CHANGELOG.md.
