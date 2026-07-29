# Bead bf-2w4i9: Source-IP Extraction Test Identification Findings

## Task Summary
**Child 2 of 4 for bf-4x1ju**
SCOPE: Within the search_ui_rate_limit test target, identify all tests covering source-IP extraction logic from proxy headers.

## Investigation Results

### Source-IP Extraction Implementation Location
**File:** `/home/coding/miroir/crates/miroir-proxy/src/routes/search_ui.rs` (lines 171-179)

**Extraction Logic:**
```rust
// Extract source IP from X-Forwarded-For or X-Real-IP (trust proxy)
let source_ip = headers
    .get("x-forwarded-for")
    .and_then(|v| v.to_str().ok())
    .and_then(|s| s.split(',').next())
    .or_else(|| headers.get("x-real-ip").and_then(|v| v.to_str().ok()))
    .unwrap_or("unknown")
    .trim()
    .to_string();
```

**Precedence Order:**
1. **Primary:** X-Forwarded-For header (first IP from comma-separated list)
2. **Fallback:** X-Real-IP header
3. **Default:** "unknown" if neither header present

### Test Target Status

**❌ CRITICAL FINDING: No search_ui_rate_limit test file exists**

The test file `/home/coding/miroir/crates/miroir-proxy/tests/search_ui_rate_limit.rs` does NOT exist in the codebase.

### Existing Rate Limit Test (Not Source-IP Extraction)

**File:** `/home/coding/miroir/crates/miroir-core/src/task_store/redis.rs` (line 3903)
**Test:** `test_redis_rate_limit_searchui`

This test validates the Redis rate limiting mechanics (INCR + EXPIRE pattern, key TTL, remaining count) but does NOT test source-IP extraction from proxy headers. It passes a hardcoded IP string `"192.168.1.1"` rather than extracting from headers.

### Test Coverage Gaps

**❌ NO tests found for:**
- X-Forwarded-For first hop extraction
- X-Real-IP fallback behavior
- X-Forwarded-For precedence over X-Real-IP
- Default to "unknown" when no proxy headers present
- Distinct forwarded headers yielding distinct IPs
- Edge cases (malformed headers, whitespace handling, port stripping)

### Comparison Pattern

The codebase has `p10_7_admin_login_rate_limit.rs` which tests admin login rate limiting, but no equivalent test file exists for search_ui rate limiting or source-IP extraction.

## Deliverables Status

### Requested Test Names (DO NOT EXIST)

**X-Forwarded-For First Hop Extraction Tests:**
- ❌ `test_x_forwarded_for_single_ip_extraction` - NOT FOUND
- ❌ `test_x_forwarded_for_multiple_ips_first_hop` - NOT FOUND
- ❌ `test_x_forwarded_for_whitespace_handling` - NOT FOUND

**X-Real-IP Fallback Behavior Tests:**
- ❌ `test_x_real_ip_fallback_when_no_x_forwarded_for` - NOT FOUND
- ❌ `test_x_real_ip_with_x_forwarded_for_precedence` - NOT FOUND

**Default Behavior Tests:**
- ❌ `test_no_proxy_headers_defaults_to_unknown` - NOT FOUND
- ❌ `test_empty_headers_defaults_to_unknown` - NOT FOUND

## Conclusion

**RESULT:** No source-IP extraction tests exist within the search_ui_rate_limit test target because the test target itself does not exist.

**Production Code:** EXISTS - Source-IP extraction is implemented in search_ui.rs
**Test Code:** DOES NOT EXIST - No tests validate the extraction logic

**Acceptance Criteria Status:**
- ❌ X-Forwarded-For extraction tests: NONE FOUND
- ❌ X-Real-IP fallback tests: NONE FOUND
- ✅ Test names documented: Documented as NON-EXISTENT

**Next Steps:** This finding indicates that either:
1. The tests need to be created (likely the intent of parent bead bf-4x1ju)
2. The tests exist in a different location not yet discovered
3. The tests are planned but not yet implemented
