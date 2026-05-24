# Miroir SDK Smoke Tests

Basic smoke tests for Miroir's Meilisearch-compatible API in four languages:
- Python
- TypeScript
- Go
- Rust

Each test verifies:
1. Create index
2. Add documents
3. Search
4. Update settings
5. Delete index

## Prerequisites

Start the docker-compose dev stack:

```bash
cd /home/coding/miroir/examples
docker-compose -f docker-compose-dev.yml up -d
```

Wait for all services to be healthy (check with `docker-compose ps`).

## Running Tests

### Cross-compatibility tests (recommended)

Runs each SDK test against **both** Miroir and plain Meilisearch to verify drop-in compatibility:

```bash
cd /home/coding/miroir/examples/sdk-tests
./run_cross_compat_tests.sh
```

This ensures that Miroir's API behaves identically to Meilisearch for all tested operations.

### Quick tests against Miroir only

```bash
cd /home/coding/miroir/examples/sdk-tests
./run_all_sdk_tests.sh
```

### Individual tests

**Python:**
```bash
cd /home/coding/miroir/examples/sdk-tests
pip install -r requirements.txt
MIROIR_URL=http://localhost:7700 MIROIR_MASTER_KEY=dev-key python3 python_smoke_test.py
```

**TypeScript:**
```bash
cd /home/coding/miroir/examples/sdk-tests
npm install
npx ts-node typescript_smoke_test.ts
```

**Go:**
```bash
cd /home/coding/miroir/examples/sdk-tests
go mod tidy
MIROIR_URL=http://localhost:7700 MIROIR_MASTER_KEY=dev-key go run golang_smoke_test.go
```

**Rust:**
```bash
cd /home/coding/miroir
cargo run --example sdk-smoke-test
```

## Expected Output

Each test should print:
```
=== Miroir <Language> SDK Smoke Test ===
Target: http://localhost:7700
✓ Cleaned up existing index 'test_<lang>_sdk'

1. Creating index...
   ✓ Created index 'test_<lang>_sdk' with primary key 'id'

2. Adding documents...
   ✓ Added 3 documents (task N)

3. Searching...
   ✓ Found 1 hits for 'gatsby'

4. Updating settings...
   ✓ Updated settings (task N)

5. Deleting index...
   ✓ Deleted index 'test_<lang>_sdk' (task N)

=== All <Language> SDK tests passed! ===
```

## Troubleshooting

**Connection refused:** Ensure docker-compose stack is running:
```bash
docker-compose -f ../docker-compose-dev.yml ps
```

**Index already exists:** Tests clean up existing indices automatically.

**Timeout:** Increase sleep values if running on slower hardware.

## API Compatibility Notes

### Intentional Differences

The following differences between Miroir and Meilisearch are **intentional** and do not break drop-in compatibility:

1. **Response Headers** — Miroir adds additional headers for observability and degradation handling:
   - `X-Miroir-Degraded` — Present when a request completed with reduced redundancy
   - `X-Miroir-Settings-Version` — Monotonically increasing version of committed index settings
   - `X-Miroir-Min-Settings-Version` — Client-supplied floor for settings freshness
   - `X-Miroir-Settings-Inconsistent` — Warning when response was served during two-phase settings broadcast
   - `X-Miroir-Session` — Session UUID for read-your-writes semantics
   - `Idempotency-Key` — Client-supplied UUID for write deduplication
   - `X-Miroir-Over-Fetch` — Per-request override for vector search over-fetch factor
   - `X-Miroir-Tenant` — Tenant identifier for tenant affinity routing

2. **Error Codes** — Miroir extends the Meilisearch error vocabulary with Miroir-specific codes:
   - `miroir_primary_key_required` — Document batch without resolvable primary key
   - `miroir_no_quorum` — No replica group met quorum for a shard (HTTP 503)
   - `miroir_shard_unavailable` — One or more shards fully unavailable
   - `miroir_reserved_field` — Document contains a reserved field name
   - `miroir_idempotency_key_reused` — Idempotency key reused with different body (HTTP 409)
   - `miroir_settings_version_stale` — No covering set could meet settings version floor (HTTP 503)
   - `miroir_jwt_invalid` — Bearer token parsed as JWT but failed validation (HTTP 401)
   - `miroir_jwt_scope_denied` — JWT scope does not include action or index mismatch (HTTP 403)
   - `miroir_invalid_auth` — Credentials did not match any expected key (HTTP 401)

3. **Admin Endpoints** — Miroir adds admin-only endpoints under `/_miroir/`:
   - `GET /_miroir/health` — Miroir's own health (not proxied to nodes)
   - `GET /_miroir/topology` — Current shard assignment and node topology
   - `GET /_miroir/metrics` — Prometheus metrics
   - `GET /_miroir/tasks` — Miroir task registry
   - `POST /_miroir/admin/login` — Admin UI login endpoint

### Compatibility Guarantees

For all standard Meilisearch operations (index CRUD, document CRUD, search, settings):
- **HTTP status codes** are identical
- **Error JSON structure** (`{message, code, type, link}`) is identical
- **Request/response shapes** are identical
- **Search results** are semantically equivalent

The SDK smoke tests verify these guarantees by running the same operations against both Miroir and plain Meilisearch.
