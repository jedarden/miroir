# `miroir-ctl cdc`

## Purpose
Configure change data capture (CDC) for streaming index changes to external systems.

## Preconditions
- External sink configured (Kafka, webhook, etc.)
- CDC feature enabled in config

## Examples

```bash
# Enable CDC for an index to a Kafka topic
miroir-ctl cdc create --index myindex --sink kafka:mytopic --format json

# Enable CDC to a webhook endpoint
miroir-ctl cdc create --index myindex --sink webhook:https://example.com/cdc --format ndjson

# List CDC streams
miroir-ctl cdc list

# Get CDC stream status and lag
miroir-ctl cdc status --index myindex

# Pause a CDC stream
miroir-ctl cdc pause --index myindex

# Resume a paused stream
miroir-ctl cdc resume --index myindex

# Delete a CDC stream
miroir-ctl cdc delete --index myindex
```

## Gotchas
- **Not yet implemented** — see tracking bead for details
- CDC captures writes, updates, and deletes — not reads
- Lag can build up if sink is slow — check `cdc status` regularly
- Pausing a stream stops delivery but doesn't lose events — they buffer until resume
- Webhook sinks must return 200 OK — retries with exponential backoff on failure

## See also
- Plan §13.13 — CDC architecture and sink types
- Plan §10 — CDC metrics and monitoring
