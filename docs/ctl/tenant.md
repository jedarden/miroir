# `miroir-ctl tenant`

## Purpose
Manage multi-tenancy by isolating indexes and API keys per tenant.

## Preconditions
- Admin API key configured
- Multi-tenancy enabled in config

## Examples

```bash
# Create a new tenant
miroir-ctl tenant create --name acme --quota indexes=10,docs=1000000

# List all tenants
miroir-ctl tenant list

# Get tenant details and quota usage
miroir-ctl tenant get --name acme

# Update tenant quota
miroir-ctl tenant update --name acme --quota indexes=20

# Delete a tenant (and all its indexes)
miroir-ctl tenant delete --name acme
```

## Gotchas
- **Not yet implemented** — see tracking bead for details
- Tenant isolation is logical — all data lives in the same cluster
- Quota enforcement is best-effort — brief overages are possible during batch operations
- Deleting a tenant deletes all its indexes — this is irreversible
- API keys are scoped per tenant — cross-tenant access is impossible

## See also
- Plan §13.15 — multi-tenancy architecture
- Plan §9 — security and key scoping
