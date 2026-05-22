# P2.6 Error Mapping Verification

## Task
Implement the error response shape from plan §5 and all `miroir_*` error codes.

## Finding
The implementation was already complete in `crates/miroir-core/src/api_error.rs`.

## Verification

### Required Error Codes (all present)
- `miroir_primary_key_required` ✓
- `miroir_no_quorum` ✓
- `miroir_shard_unavailable` ✓
- `miroir_reserved_field` ✓
- `miroir_idempotency_key_reused` ✓
- `miroir_settings_version_stale` ✓
- `miroir_multi_alias_not_writable` ✓
- `miroir_jwt_invalid` ✓
- `miroir_jwt_scope_denied` ✓
- `miroir_invalid_auth` ✓

### HTTP Status Code Mappings (matches plan §5)
- 400: `PrimaryKeyRequired`, `ReservedField` ✓
- 401: `JwtInvalid`, `InvalidAuth` ✓
- 403: `JwtScopeDenied` ✓
- 409: `IdempotencyKeyReused`, `MultiAliasNotWritable` ✓
- 503: `NoQuorum`, `ShardUnavailable`, `SettingsVersionStale` ✓

### Tests
All 23 api_error tests pass:
- Per-code JSON shape tests for each miroir_* code
- Meilisearch-native error forwarding (preserved verbatim)
- HTTP status code mapping verification
- Round-trip serialization
- Code string round-trip

### Error Shape
```json
{"message": "...", "code": "...", "type": "invalid_request|auth|internal|system", "link": "..."}
```

Matches Meilisearch format exactly.
