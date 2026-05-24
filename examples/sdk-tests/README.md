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

### All tests (via script)

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
