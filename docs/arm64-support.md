# ARM64 Support

## Status

ARM64 (aarch64) support is available for cross-compilation. Production ARM64 deployments are planned for v1.x when K8s ARM node support is required.

## Building for ARM64

### Prerequisites

Install the musl cross-compilation toolchain for ARM64:

```bash
# On Ubuntu/Debian
sudo apt-get install musl-tools musl-dev gcc-aarch64-linux-gnu

# Add musl target
rustup target add aarch64-unknown-linux-musl
```

### Cross-Compilation

Build static musl binaries for ARM64:

```bash
cargo build --release --target aarch64-unknown-linux-musl -p miroir-proxy -p miroir-ctl
```

The project's `rust-toolchain.toml` includes both `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` targets.

## Architecture-Specific Considerations

### Dependencies

All dependencies are architecture-agnostic Rust crates. No C dependencies or architecture-specific code paths exist in the current implementation.

### Performance

- **Hashing**: `twox-hash` (XXHash) is used for consistent hashing. Performance is comparable across x86_64 and ARM64.
- **Serialization**: `serde` and `bincode` have no architecture-specific behavior.
- **Networking**: Tokio runtime and axum HTTP server are architecture-agnostic.

### Memory Alignment

The codebase uses standard Rust types with default alignment. No manual memory management or SIMD optimizations are present that would require ARM64-specific handling.

## Testing

ARM64 builds should be validated on ARM64 infrastructure before production deployment. Recommended testing:

1. **Unit tests**: Run `cargo test --target aarch64-unknown-linux-musl`
2. **Integration tests**: Deploy to ARM64 K8s nodes with Meilisearch backend
3. **Performance benchmarks**: Validate throughput and latency against x86_64 baseline

## CI Pipeline

Future ARM64 CI integration should:

1. Add `aarch64-unknown-linux-musl` target to GitHub Actions matrix
2. Use cross-compilation or ARM64 runners for builds
3. Validate binary execution via QEMU or native ARM64 runners

## Related

- OP#6: ARM64 Support (parent task)
- Phase 8 (CI): CI pipeline enhancements
