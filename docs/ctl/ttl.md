# `miroir-ctl ttl`

## Purpose
Manage time-to-live (TTL) policies for automatic document expiration.

## Preconditions
- Index must exist with a timestamp field
- TTL feature enabled in config

## Examples

```bash
# Set a TTL policy on an index (documents expire after 30 days)
miroir-ctl ttl set --index logs --field created_at --duration 30d

# Set TTL with custom check interval
miroir-ctl ttl set --index logs --field created_at --duration 7d --check-interval 1h

# Get current TTL policy for an index
miroir-ctl ttl get --index logs

# Remove TTL policy
miroir-ctl ttl remove --index logs
```

## Gotchas
- **Not yet implemented** — see tracking bead for details
- TTL is enforced via background tasks — expired documents are deleted on next pass
- Duration format: `30d` (days), `12h` (hours), `15m` (minutes)
- TTL field must be a ISO 8601 timestamp or Unix epoch
- Deleting expired documents is irreversible — backup first if needed

## See also
- Plan §13.14 — TTL implementation and task scheduling
- `miroir-ctl task status` — monitor TTL cleanup jobs
