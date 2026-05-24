# `miroir-ctl shadow`

## Purpose
Manage shadow indexing for A/B testing and validation without affecting production traffic.

## Preconditions
- Source index exists and is healthy
- Sufficient capacity for shadow writes (2x write amplification)

## Examples

```bash
# Create a shadow index for testing
miroir-ctl shadow create --source prod --shadow test --sync-writes

# Enable query shadowing (mirror queries to both, compare results)
miroir-ctl shadow query --source prod --shadow test --compare

# Check shadow index lag
miroir-ctl shadow status --source prod

# Stop shadowing and delete shadow index
miroir-ctl shadow delete --source prod
```

## Gotchas
- **Not yet implemented** — see tracking bead for details
- Shadow writes are synchronous — adds latency to production writes
- Query shadowing is asynchronous — doesn't affect production latency
- Use shadow indexing for schema validation, not load testing (write amplification)
- Delete shadow indexes after testing to free storage

## See also
- Plan §13.16 — shadow indexing architecture
- `miroir-ctl verify` — compare shadow and source results
