.PHONY: help test lint bench build coverage clean docker-coverage

help: ## Show this help message
	@echo "Miroir development commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run all tests
	cargo test --all --all-features

test-verbose: ## Run tests with output
	cargo test --all --all-features -- --nocapture

lint: ## Run linters (fmt + clippy)
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings

lint-fix: ## Fix lint issues automatically
	cargo fmt --all
	cargo clippy --all-targets --fix --allow-dirty --allow-staged

bench: ## Run benchmarks
	cargo bench -p miroir-core

bench-check: ## Verify benchmarks compile (CI check)
	cargo bench --no-run -p miroir-core

build: ## Build all workspace members
	cargo build --all

build-release: ## Build release binaries
	cargo build --release -p miroir-proxy -p miroir-ctl

coverage: ## Run coverage with 90% gate (plan §8)
	./scripts/coverage.sh

coverage-local: ## Run coverage without gate (local dev)
	cargo tarpaulin \
		--workspace \
		--package miroir-core \
		--exclude-files "benches/*" \
		--exclude-files "tests/*" \
		--timeout 600 \
		--out Html \
		--output-dir target/coverage \
		-- --test-threads=1
	@echo "Coverage report: target/coverage/index.html"

docker-coverage: ## Run coverage in Docker (for artifacts)
	docker run --rm -v "$$(pwd):/miroir" -w /miroir \
		rust:1.87-slim \
		bash -c "apt-get update -qq && apt-get install -y -qq pkg-config libssl-dev libcurl4-openssl-dev zlib1g-dev >/dev/null 2>&1 \
		&& cargo install cargo-tarpaulin --locked \
		&& cargo tarpaulin --workspace --package miroir-core --timeout 600 --out Lcov --out Xml --output-dir target/coverage -- --test-threads=1"

clean: ## Clean build artifacts
	cargo clean

docker-compose-up: ## Start docker-compose dev stack
	cd examples && docker-compose -f docker-compose-dev.yml up -d

docker-compose-down: ## Stop docker-compose dev stack
	cd examples && docker-compose -f docker-compose-dev.yml down

docker-compose-logs: ## Show docker-compose logs
	cd examples && docker-compose -f docker-compose-dev.yml logs -f
