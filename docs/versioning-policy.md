# Versioning Policy

This document defines Miroir's backward-compatibility commitments and versioning practices.

## Overview

Miroir follows [Semantic Versioning](https://semver.org/):
- **MAJOR**: Incompatible changes
- **MINOR**: Backward-compatible functionality additions
- **PATCH**: Backward-compatible bug fixes

Starting with v1.0, Miroir provides the following backward-compatibility guarantees.

## v1.0+ Commitments

Once Miroir reaches v1.0, the following surfaces are guaranteed backward compatibility within MINOR versions:

### 1. Meilisearch API Compatibility Layer

**Commitment**: No breaking changes in minor versions.

The `/miroir/*` API endpoints that proxy to Meilisearch will maintain compatibility with the Meilisearch API specification. Changes that would break existing clients require a MAJOR version bump.

**Breaking changes include**:
- Removing or renaming endpoints
- Changing request/response field names or types
- Modifying required query parameters
- Changing HTTP status codes for success/error conditions
- Altering authentication behavior

**Non-breaking changes include**:
- Adding optional request fields or query parameters
- Adding new response fields
- Adding new endpoints
- Performance improvements that preserve API semantics

### 2. `miroir-ctl` CLI Flags

**Commitment**: No incompatible changes in minor versions.

Command-line flags and subcommands will remain stable. Existing invocations and scripts using `miroir-ctl` must continue to work across MINOR versions.

**Breaking changes include**:
- Removing or renaming flags or subcommands
- Changing flag syntax (e.g., `--flag` to `--different-flag`)
- Inverting boolean flag semantics (e.g., `--enabled` defaulting to `true` vs `false`)
- Requiring additional mandatory flags for existing commands

**Non-breaking changes include**:
- Adding new optional flags
- Adding new subcommands
- Changing default values for optional flags (if the flag is not explicitly set)
- Improving help text and error messages

### 3. Config File Schema

**Commitment**: Backward-compatible in minor versions; new fields always optional with defaults.

Existing configuration files must remain valid across MINOR versions. New configuration options are always optional and provide sensible defaults when not specified.

**Breaking changes include**:
- Removing or renaming configuration keys
- Changing the type of an existing key
- Making a previously optional key required
- Changing the interpretation of a key's value incompatibly

**Non-breaking changes include**:
- Adding new optional keys with documented defaults
- Adding new sections to the config
- Relaxing validation constraints (e.g., expanding an enum)
- Deprecating keys (with warnings) before removal

### 4. Helm Chart Values Schema

**Commitment**: Backward-compatible in minor versions.

Existing `values.yaml` files must deploy successfully across MINOR versions. The chart maintains stability for GitOps and automated deployment workflows.

**Breaking changes include**:
- Removing or renaming values
- Changing the structure or nesting of values
- Making previously optional values required
- Changing default values in a way that breaks existing deployments

**Non-breaking changes include**:
- Adding new optional values with documented defaults
- Adding new nested structures under existing keys
- Introducing feature flags gated behind new optional values

## Deprecation Policy

When a feature must be removed:

1. **Mark as deprecated**: Announce in the CHANGELOG with the `[deprecated]` tag
2. **Provide migration path**: Document how to update to the replacement
3. **Wait one MINOR cycle**: The deprecated feature remains functional for at least one full MINOR version
4. **Remove in MAJOR or later MINOR**: Removal is announced with `[removed]` in CHANGELOG

**Example**:
- v1.2: Feature X is marked `[deprecated]`, replacement Y introduced
- v1.3: Feature X still works, warnings logged
- v1.4 or v2.0: Feature X removed, marked `[removed]`

For v1.x, removals that violate MINOR commitments require a MAJOR bump. However, features added in v1.2 may be removed in v1.4 if properly deprecated in v1.3.

## Pre-1.0 Policy (v0.x)

Before v1.0, **MINOR version bumps may include breaking changes**.

This is explicitly permitted under SemVer for pre-1.0 software. However, Miroir will:

- Document all breaking changes in CHANGELOG.md
- Use the `[breaking]` tag for any breaking change
- Avoid breaking changes in PATCH versions unless necessary for security/critical fixes

Once v1.0 is released, this policy transitions to the v1.0+ commitments above.

## CHANGELOG Tagging Convention

To make compatibility changes discoverable, CHANGELOG entries use these tags:

| Tag | Meaning |
|-----|---------|
| `[breaking]` | Breaking change; MAJOR bump required (v1.x+) |
| `[deprecated]` | Feature marked for future removal |
| `[removed]` | Previously deprecated feature now removed |

**Example entry**:
```markdown
## [1.2.0] - 2026-06-01

### Changed
- [breaking] `/miroir/indexes` endpoint now returns `shard_count` instead of `replica_count`
- [deprecated] `--legacy-mode` flag in `miroir-ctl` (will be removed in v1.4)
```

## Version Bump Decision Tree

```
Change affects:
├── API/CLI/config/Helm commitments (v1.x+)
│   ├── Breaking? → MAJOR bump
│   └── Non-breaking? → MINOR bump
├── Bug fix → PATCH bump
└── v0.x (pre-1.0)
    └── Any change may require MINOR bump; document with [breaking]
```

## References

- [Semantic Versioning 2.0.0](https://semver.org/)
- [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
- [Miroir Plan §12: Versioning commitments](/docs/plan/plan.md#versioning-commitments-from-v10)
