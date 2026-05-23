# P10.1 Secret Inventory + ESO ExternalSecret Wiring - Completion

## Summary

Verified that all acceptance criteria for P10.1 are already implemented in the codebase.

## Acceptance Criteria Verification

### 1. ESO ExternalSecret deploys cleanly against ardenone-cluster's OpenBao
- **Template**: `charts/miroir/templates/miroir-externalsecret.yaml`
- **Example**: `charts/miroir/examples/eso-external-secret.yaml`
- **Target**: Points at `openbao-backend` ClusterSecretStore (default)
- **Secret Path**: `kv/search/miroir`

### 2. Missing SEARCH_UI_JWT_SECRET with search_ui.enabled: true → refuse-to-start with explicit error
- **Location**: `crates/miroir-proxy/src/main.rs` lines 293-307
- **Behavior**: When `search_ui.enabled: true`, the proxy checks for `SEARCH_UI_JWT_SECRET` env var (or configured `jwt_secret_env`)
- **Error message**: "search_ui is enabled but {env_var} is not set — refusing to start. Either set the env var or disable search_ui (search_ui.enabled: false)"

### 3. examples/eso-external-secret.yaml documents every key in the inventory
- **8 secretKey entries** documented:
  1. masterKey (master_key)
  2. nodeMasterKey (node_master_key)
  3. adminApiKey (admin_api_key)
  4. adminSessionSealKey (admin_session_seal_key)
  5. searchUiJwtSecret (search_ui_jwt_secret)
  6. searchUiJwtSecretPrevious (search_ui_jwt_secret_previous) - rotation only
  7. searchUiSharedKey (search_ui_shared_key) - shared_key mode only
  8. redis-password (redis_password) - optional

## Secret Inventory (plan §9)

| Secret | Consumer | Rotation |
|--------|----------|----------|
| master_key | Miroir proxy | manual/infrequent |
| node_master_key | Miroir → Meilisearch | admin-scoped child key rotation (P10.2) |
| meilisearch_master_key | Meilisearch startup | planned-maintenance (not in ESO) |
| admin_api_key | Operators, miroir-ctl | rotate with ADMIN_SESSION_SEAL_KEY |
| ADMIN_SESSION_SEAL_KEY | Miroir proxy | P10.4 |
| SEARCH_UI_JWT_SECRET | Miroir proxy | P10.3 dual-secret overlap |
| search_ui_shared_key | Miroir + host apps | only in shared_key mode |
| ghcr_credentials | Kaniko (iad-ci) | infrastructure; not in scope |
| github_token | gh CLI (iad-ci) | infrastructure; not in scope |
| redis_password | Miroir proxy | optional |

## Documentation

- `docs/operations/secrets-setup.md` - Complete setup guide for OpenBao + ESO
- `charts/miroir/examples/eso-external-secret.yaml` - Example ExternalSecret manifest
- `charts/miroir/templates/miroir-externalsecret.yaml` - Helm template for ESO

## Status

**COMPLETE** - All acceptance criteria verified. No code changes were required as the implementation was already in place.
