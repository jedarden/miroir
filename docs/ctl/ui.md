# `miroir-ctl ui`

## Purpose
Launch the web Admin UI for interactive cluster management.

## Preconditions
- Admin API key configured
- Browser available (opens localhost:3000 by default)

## Examples

```bash
# Launch the Admin UI
miroir-ctl ui

# Launch on a custom port
miroir-ctl ui --port 8080

# Launch without auto-opening browser
miroir-ctl ui --no-open

# Launch with specific API endpoint
miroir-ctl ui --api-url https://miroir.example.com
```

## Gotchas
- UI runs locally — it makes API calls to the orchestrator from your browser
- Credentials are stored in browser local storage — clear after use on shared machines
- Most UI operations have CLI equivalents — prefer CLI for scripts and CI/CD
- UI requires CSRF token exchange — first load may be slower
- Some advanced operations (e.g., scoped key rotation) are UI-only

## See also
- Plan §13.19 — Admin UI features and architecture
- Plan §13.21 — scoped key rotation (UI-only operation)
- `docs/ctl/*.md` — CLI equivalents for UI operations
