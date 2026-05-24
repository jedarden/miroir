# `miroir-ctl key`

## Purpose
Manage Meilisearch API keys with scoped permissions and tenant isolation.

## Preconditions
- Admin API key configured
- Key management enabled in config

## Examples

```bash
# Create a search-only key for a tenant
miroir-ctl key create --name acme-search --action search --index acme-*

# Create a key with document write permissions
miroir-ctl key create --name writer --action documentsAdd,indexUpdate --index logs

# Create a key with expiration
miroir-ctl key create --name temp --action search --expires 2024-12-31

# Create a scoped key (UI-only operation, see Plan §13.21)
miroir-ctl key create-scoped --name ui-key --tenant acme

# List all keys
miroir-ctl key list

# Get key details (including usage stats)
miroir-ctl key get --name acme-search

# Delete a key
miroir-ctl key delete --name acme-search

# Rotate the master key (UI-only, see Plan §13.21)
miroir-ctl key rotate-master
```

## Gotchas
- **Partially implemented** — basic CRUD works, scoped keys are UI-only
- Keys are returned once at creation — save the key value immediately
- Deleting a key revokes access immediately — no grace period
- Scoped keys embed tenant and index filters — verify before use
- Master key rotation requires downtime — schedule maintenance window

## See also
- Plan §13.21 — scoped key rotation and master key management
- Plan §9 — security and key scoping
- Admin UI — preferred interface for key rotation (not CLI)
