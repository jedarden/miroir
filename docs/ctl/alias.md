# `miroir-ctl alias`

## Purpose
Manage index aliases for zero-downtime index swaps (e.g., for resharding, blue-green deployments, or ILM time-series data).

## Alias Types

Miroir supports two types of aliases:

### Single-Target Aliases
- **Writable**: Client writes are accepted and routed to the target index
- **Operator-managed**: Created and modified via `miroir-ctl alias create`
- **Use case**: Blue-green deployments, zero-downtime reindexing

### Multi-Target Aliases
- **Read-only**: Client writes are rejected with `miroir_multi_alias_not_writable`
- **ILM-managed**: Created and updated exclusively by ILM policies
- **Use case**: Time-series data where read queries span multiple date-based indexes

## Preconditions
- Source and target indexes must exist
- Admin API key configured

## Examples

### Single-Target Alias Operations

```bash
# Create a single-target alias (writable)
miroir-ctl alias create products --target products_v3

# List all aliases (shows kind and manager)
miroir-ctl alias list

# Show alias details
miroir-ctl alias show products

# Update an alias to point to a new index (zero-downtime swap)
miroir-ctl alias update products --target products_v4

# Delete an alias
miroir-ctl alias delete products
```

### Multi-Target Alias (ILM-Managed)

```bash
# Multi-target aliases are created automatically by ILM policies
# They cannot be created or modified directly via miroir-ctl

# Example ILM policy creates:
# - logs (single-target, writable) -> points to current day's index
# - logs-search (multi-target, read-only) -> spans last N days

# View ILM-managed aliases
miroir-ctl alias list
# Output:
#   logs        single     operator   v42
#   └─ target: logs-2026-05-26
#   logs-search multi      ILM        v12
#   └─ targets (3):
#       └─ logs-2026-05-26
#       └─ logs-2026-05-25
#       └─ logs-2026-05-24

# Attempting to modify a multi-target alias returns an error:
# miroir-ctl alias update logs-search --target logs-new
# Error: HTTP 409 — miroir_multi_alias_not_writable
# "multi-target aliases are managed exclusively by ILM; use the ILM policy API to modify"
```

## Alias Lifecycle

### Single-Target Alias Lifecycle
1. **Create**: `miroir-ctl alias create <name> --target <index>`
2. **Use**: Queries and writes resolve to the target index
3. **Update**: `miroir-ctl alias update <name> --target <new-index>` (atomic flip)
4. **Delete**: `miroir-ctl alias delete <name>` (alias only, index remains)

### Multi-Target Alias Lifecycle (ILM-Managed)
1. **ILM Policy Creates**: When an ILM rollover policy is created, ILM creates:
   - A write alias (single-target, writable) pointing to the current index
   - A read alias (multi-target, read-only) spanning retained indexes
2. **ILM Updates**: On each rollover:
   - Write alias flips to new index (atomic)
   - Read alias updates to include new index and remove expired indexes
3. **Operator Read-Only**: Operators cannot modify multi-target aliases
4. **ILM Deletes**: If the ILM policy is deleted, aliases remain (manual cleanup required)

## ILM Integration

### Rollover Flow
1. Trigger fires (max_docs, max_age, or max_size_gb)
2. ILM creates new index: `logs-2026-05-27`
3. ILM flips write alias: `logs → logs-2026-05-27`
4. ILM updates read alias: `logs-search → [logs-2026-05-27, logs-2026-05-26, ...]`
5. ILM deletes expired indexes per retention policy

### Write Semantics
```bash
# Write to single-target alias (OK)
curl -X POST "http://miroir:7700/indexes/logs/documents" \
  -H "Authorization: Bearer $MASTER_KEY" \
  --data '[{"id": "1", "message": "hello"}]'
# ✅ Success - writes to logs-2026-05-26

# Write to multi-target alias (REJECTED)
curl -X POST "http://miroir:7700/indexes/logs-search/documents" \
  -H "Authorization: Bearer $MASTER_KEY" \
  --data '[{"id": "1", "message": "hello"}]'
# ❌ HTTP 409 — miroir_multi_alias_not_writable
# "alias 'logs-search' is a multi-target alias and is read-only (managed by ILM)"
```

### Read Semantics
```bash
# Read from single-target alias
curl "http://miroir:7700/indexes/logs/search"
# ✅ Returns results from logs-2026-05-26

# Read from multi-target alias
curl "http://miroir:7700/indexes/logs-search/search"
# ✅ Returns results from ALL targets (logs-2026-05-26, logs-2026-05-25, ...)
```

## Gotchas
- **Multi-target aliases are ILM-managed**: Cannot be created or modified via `miroir-ctl alias create`
- **Writes to multi-target aliases fail**: Use the write alias or concrete index UID
- **Alias resolution is query-time**: No data is copied; aliases are just pointers
- **Deleting an alias does not delete the underlying index**: Use `miroir-ctl node` or direct Meilisearch API
- **ILM policies must be deleted separately**: Removing aliases does not remove ILM policies
- **Resharding uses single-target aliases**: See `miroir-ctl reshard` for alias swap phase

## See also
- Plan §13.7 — Atomic index aliases and zero-downtime swaps
- Plan §13.17 — ILM rollover and multi-target alias lifecycle
- Plan §13.1 — Resharding alias swap phase
- `miroir-ctl reshard` — uses aliases for zero-downtime migration
- `miroir-ctl ilm` — manage ILM policies (not yet implemented)
