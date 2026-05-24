# `miroir-ctl dump`

## Purpose
Export and import index data for backup, migration, or bulk loading.

## Preconditions
- For export: index must exist and be healthy
- For import: target index must exist with matching schema

## Examples

```bash
# Export an index to a compressed dump file
miroir-ctl dump export --index myindex --output myindex.dump

# Export with chunking (for large indexes)
miroir-ctl dump export --index myindex --output myindex.dump --chunk-size 10000

# Import a dump file
miroir-ctl dump import --input myindex.dump --index myindex

# Import with throttling (limit write rate)
miroir-ctl dump import --input myindex.dump --index myindex --throttle 5000

# Verify dump integrity without importing
miroir-ctl dump verify --input myindex.dump
```

## Gotchas
- Export creates a compressed archive — includes documents, settings, and tasks
- Import is idempotent — running twice won't duplicate documents (PK-based upsert)
- Large dumps are chunked automatically — use `--chunk-size` to control memory usage
- Import bypasses the write path's sharding layer — writes directly to target shards
- Use `miroir-ctl task status` to track async import jobs

## See also
- Plan §13.9 — dump format and chunking
- `miroir-ctl verify` — verify imported data
- `docs/dump-import/` — detailed dump format specification
