# BF-2GUJ4: Rate-Limit Fix Verification - Findings

## Scope
Static/code verification of rate-limit fix for search handlers - verify no hardcoded unknown stubs and IP precedence matches admin_endpoints.rs.

## Findings

### ❌ FAILED: Hardcoded "unknown" source_ip in BOTH search handlers

#### Location 1: `search_handler` (line 189-190)
```rust
// TODO: Extract source IP from headers - need to add back HeaderMap extraction
let source_ip = "unknown".to_string();
```

#### Location 2: `search_multi_targets` (line 1026-1027)
```rust
// TODO: Extract source IP from headers
let source_ip = "unknown".to_string();
```

### Impact
**CRITICAL BUG**: All search UI rate limiting is collapsed to a single bucket. Every client shares the same rate limit counter keyed by "unknown", making the per-IP rate limit ineffective.

### Correct Pattern from `admin_endpoints.rs` (lines 1374-1382)
```rust
let source_ip = headers
    .get("x-forwarded-for")
    .and_then(|v| v.to_str().ok())
    .and_then(|s| s.split(',').next())
    .or_else(|| headers.get("x-real-ip").and_then(|v| v.to_str().ok()))
    .unwrap_or("unknown")
    .trim()
    .to_string();
```

### Precedence Order (admin_endpoints.rs)
1. **X-Forwarded-For** (first hop only - via `.split(',').next()`)
2. **X-Real-IP**
3. **"unknown"** fallback

### Call Chain Analysis

#### search_handler (search.rs)
- Line 190: `source_ip = "unknown"` ❌ HARDCODED
- Line 205: `redis.check_rate_limit_search_ui(&source_ip, ...)` - uses hardcoded value
- Line 229: `local_search_ui_rate_limiter.check(&source_ip, ...)` - uses hardcoded value

#### search_multi_targets (search.rs)
- Line 1027: `source_ip = "unknown"` ❌ HARDCODED
- Line 1042: `redis.check_rate_limit_search_ui(&source_ip, ...)` - uses hardcoded value
- Line 1065: `local_search_ui_rate_limiter.check(&source_ip, ...)` - uses hardcoded value

### Root Cause
The TODO comments indicate the IP extraction was intentionally left out during a refactoring:
- "TODO: Extract source IP from headers - need to add back HeaderMap extraction"
- "TODO: Extract source IP from headers"

This suggests the fix is straightforward: inline the same header extraction logic used in admin_endpoints.rs.

## Acceptance Criteria Status

| Criterion | Status | Details |
|-----------|--------|---------|
| No hardcoded "unknown" in handler bodies | ❌ FAILED | Both handlers have `source_ip = "unknown"` |
| Precedence matches admin_endpoints.rs | ❌ N/A | No extraction exists to compare |
| Call chain intact | ⚠️ PARTIAL | Call chain exists but uses hardcoded "unknown" |

## Recommendation
Apply the same inline header extraction pattern from admin_endpoints.rs to both search_handler and search_multi_targets functions. The extraction should happen immediately after the `headers` parameter is available (lines 189-190 and 1026-1027).
