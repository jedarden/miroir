# `miroir-ctl explain`

## Purpose
Analyze query plans and show how search queries are routed across shards.

## Preconditions
- Index must exist
- Admin API key configured

## Examples

```bash
# Explain a simple search query
miroir-ctl explain --index myindex --q "test query"

# Explain with filters
miroir-ctl explain --index logs --q "error" --filter "timestamp > 2024-01-01"

# Explain with ranking details
miroir-ctl explain --index myindex --q "test" --show-ranking

# Show shard routing for a query
miroir-ctl explain --index myindex --q "test" --show-routing
```

## Gotchas
- **Not yet implemented** — see tracking bead for details
- Explain doesn't execute the query — it only shows the plan
- Use `--show-routing` to understand which nodes are contacted
- Ranking details show score breakdown per field
- Explain is useful for debugging slow queries — check per-shard latency

## See also
- Plan §13.20 — query planning and optimization
- Plan §2 — rendezvous hashing and shard routing
