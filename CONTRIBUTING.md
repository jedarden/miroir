# Contributing to Miroir

Thank you for your interest in contributing to Miroir! This document covers the development workflow, code submission guidelines, and local testing instructions.

## Development Workflow

### Prerequisites

- **Rust** 1.87 or later (see `rust-toolchain.toml`)
- **Docker** and **Docker Compose** for local integration testing
- **make** for convenience targets
- **clang** and **musl-tools** for static binary builds

### Getting Started

1. **Clone the repository:**

```bash
git clone https://github.com/jedarden/miroir.git
cd miroir
```

2. **Install Rust toolchain:**

```bash
rustup show
# If Rust is not installed, follow https://rustup.rs/
```

3. **Build the project:**

```bash
cargo build --all
```

4. **Run tests:**

```bash
cargo test --all
```

5. **Start the development stack:**

```bash
docker compose -f examples/docker-compose-dev.yml up -d
```

### Project Structure

```
miroir/
├── crates/
│   ├── miroir-core/          # Core library (routing, merging, topology)
│   ├── miroir-proxy/          # HTTP proxy binary
│   └── miroir-ctl/            # Management CLI binary
├── tests/                     # Integration and chaos tests
├── benches/                   # Performance benchmarks
├── examples/                  # Docker Compose configs and SDK tests
├── charts/miroir/             # Helm chart
├── docs/                      # Documentation
└── k8s/                       # ArgoCD and Argo Workflow templates
```

### Development Commands

```bash
# Format code
cargo fmt

# Run linter
cargo clippy --all-targets --all-features -- -D warnings

# Run all tests
cargo test --all

# Run integration tests (requires dev stack running)
cargo test -p miroir-proxy --test docker_compose_integration -- --test-threads=1

# Run benchmarks
cargo bench

# Build static binaries
make build-static
```

## Code Submission Guidelines

### Pull Request Process

1. **Fork the repository** and create a branch for your work:

```bash
git checkout -b feature/your-feature-name
```

2. **Make your changes** following the coding standards below.

3. **Ensure all checks pass:**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

4. **Commit your changes** with a clear message:

```
feat(scope): brief description

Detailed explanation of the change and why it was made.

Closes: #gh-issue-number
```

5. **Push to your fork** and create a pull request:

```bash
git push origin feature/your-feature-name
```

### Coding Standards

- **Follow Rust style guidelines** enforced by `cargo fmt`
- **No `unwrap()` or `expect()` in production code** — all fallible operations must return `Result<T>`
- **Add unit tests** for new functionality in `#[cfg(test)]` modules
- **Document public APIs** with rustdoc comments
- **Keep functions focused** — single responsibility, clear names
- **Error messages** should be actionable and include context

### Commit Message Conventions

Use conventional commit prefixes:

- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation changes
- `test:` — adding or updating tests
- `refactor:` — code refactoring (no behavior change)
- `perf:` — performance improvement
- `ci:` — CI/CD changes
- `chore:` — maintenance tasks

## Local Testing

### Unit Tests

Run unit tests for all crates:

```bash
cargo test --all
```

Run tests for a specific crate:

```bash
cargo test -p miroir-core
cargo test -p miroir-proxy
```

### Integration Tests

Start the development stack first:

```bash
docker compose -f examples/docker-compose-dev.yml up -d
```

Run integration tests:

```bash
cargo test -p miroir-proxy --test docker_compose_integration -- --test-threads=1
```

Clean up after testing:

```bash
docker compose -f examples/docker-compose-dev.yml down -v
```

### Chaos Tests

Chaos tests simulate node failures and network partitions:

```bash
# Ensure dev stack is running
docker compose -f examples/docker-compose-dev.yml up -d

# Run chaos tests
./tests/chaos/p4_topology_chaos.sh

# Clean up
docker compose -f examples/docker-compose-dev.yml down -v
```

### SDK Compatibility Tests

Test SDK compatibility with Python, TypeScript, and Go:

```bash
cd examples/sdk-tests
./run_all_sdk_tests.sh
```

## CI/CD Pipeline

Miroir uses **Argo Workflows** for CI/CD (GitHub Actions are disabled). See `k8s/argo-workflows/miroir-ci.yaml` for the full pipeline.

### Pipeline Stages

1. **Checkout** — Clone repository
2. **Lint** — `cargo fmt` and `cargo clippy`
3. **Test** — `cargo test --all --all-features`
4. **Build** — Static musl binaries for `miroir-proxy` and `miroir-ctl`
5. **Docker build** — Push to `ghcr.io/jedarden/miroir` (tag-gated)
6. **GitHub Release** — Create release with binaries (tag-gated)

### Manual CI Trigger

Trigger a CI run manually:

```bash
kubectl --kubeconfig=/home/coding/.kube/iad-ci.kubeconfig create -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: miroir-ci-manual-
  namespace: argo-workflows
spec:
  workflowTemplateRef:
    name: miroir-ci
  arguments:
    parameters:
    - name: revision
      value: main
EOF
```

### Release Process

See the [Release Checklist](docs/ctl/release.md) in the runbooks for the complete release process.

## Documentation

### Plan Documentation

The authoritative design document is [`docs/plan/plan.md`](docs/plan/plan.md). All architectural decisions should reference specific sections of this document.

### Code Documentation

Public APIs must be documented with rustdoc:

```rust
/// Assigns a shard to `rf` nodes within a single replica group.
///
/// # Arguments
///
/// * `shard_id` - The shard identifier to assign
/// * `group_nodes` - Nodes belonging to the target replica group
/// * `rf` - Replication factor (number of nodes to select)
///
/// # Returns
///
/// A vector of node IDs selected for this shard, sorted by score descending.
///
/// # Example
///
/// ```no_run
/// use miroir_core::router::assign_shard_in_group;
/// let nodes = vec!["node-0".to_string(), "node-1".to_string()];
/// let assigned = assign_shard_in_group(0, &nodes, 1);
/// assert_eq!(assigned, vec!["node-0".to_string()]);
/// ```
pub fn assign_shard_in_group(shard_id: u32, group_nodes: &[NodeId], rf: usize) -> Vec<NodeId> {
    // ...
}
```

### User Documentation

User-facing documentation lives in:
- `docs/onboarding/` — Production deployment guides
- `docs/troubleshooting/` — Common issues and diagnostics
- `docs/runbooks/` — Operational procedures
- `docs/ctl/` — `miroir-ctl` command documentation
- `examples/README.md` — Quick start and local development

## Getting Help

- **Documentation:** Start with [`README.md`](README.md) and [`docs/plan/plan.md`](docs/plan/plan.md)
- **Issues:** Search [GitHub Issues](https://github.com/jedarden/miroir/issues) before creating a new one
- **Discussions:** Use [GitHub Discussions](https://github.com/jedarden/miroir/discussions) for questions and design conversations
- **Troubleshooting:** See [`docs/troubleshooting.md`](docs/troubleshooting.md) for common issues

## License

By contributing to Miroir, you agree that your contributions will be licensed under the [MIT License](LICENSE).
