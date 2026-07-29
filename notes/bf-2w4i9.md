# Bead bf-2w4i9: Source-IP Extraction Test Identification

## Task Summary
Child bead of bf-4x1ju (verify search_ui_rate_limit test target exists).
SCOPE: Identify tests covering source-IP extraction logic from proxy headers within the search_ui_rate_limit test target.

## Investigation Results

### Source-IP Extraction Implementation
Found in `/home/coding/miroir/crates/miroir-proxy/src/routes/search_ui.rs` (lines 171-179):

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

**Extraction Logic:**
1. **Primary:** X-Forwarded-For header, first IP (before comma)
2. **Fallback:** X-Real-IP header
3. **Default:** "unknown" if neither header present

### Search Results for Existing Tests

**Test File Status:**
- ❌ File `/home/coding/miroir/crates/miroir-proxy/tests/search_ui_rate_limit.rs` does NOT exist
- ❌ No test files found containing "x-forwarded-for" or "x-real-ip" header testing
- ❌ No tests found for source-IP extraction logic in search_ui_rate_limit context

**Searched Locations:**
- All `.rs` files in `/home/coding/miroir/crates/miroir-proxy/tests/`
- All `.rs` files in `/home/coding/miroir/crates/` 
- Bead references to `tests/search_ui_rate_limit.rs`

**Bead Context:**
- Bead bf-146g8 mentions "tests/search_ui_rate_limit.rs" as existing
- Bead bf-1e1d6 references "the two tokio Redis-backend isolation tests in the search_ui_rate_limit binary"
- Parent bead bf-4x1ju is about verifying the test target exists

## FINDING: No Tests Currently Exist

### Tests That Should Exist (Based on Implementation Logic)

**X-Forwarded-For First Hop Extraction Tests:**
1. `test_x_forwarded_for_single_ip_extraction` - Single IP in X-Forwarded-For
2. `test_x_forwarded_for_multiple_ips_first_hop` - Multiple IPs in X-Forwarded-For (comma-separated)
3. `test_x_forwarded_for_whitespace_handling` - X-Forwarded-For with extra whitespace

**X-Real-IP Fallback Behavior Tests:**
1. `test_x_real_ip_fallback_when_no_x_forwarded_for` - X-Real-IP used when X-Forwarded-For absent
2. `test_x_real_ip_with_x_forwarded_for_precedence` - X-Forwarded-For takes precedence over X-Real-IP

**Default Behavior Tests:**
1. `test_no_proxy_headers_defaults_to_unknown` - No proxy headers present → "unknown"
2. `test_empty_headers_defaults_to_unknown` - Empty proxy headers → "unknown"

**Edge Cases:**
1. `test_malformed_x_forwarded_for_ignored` - Invalid X-Forwarded-For format falls back correctly
2. `test_x_forwarded_for_with_port_stripping` - X-Forwarded-For with port numbers

## Conclusion
The search_ui_rate_limit test target appears to be referenced in beads but does not currently exist in the codebase. No tests covering source-IP extraction from proxy headers are present in the miroir-proxy test suite.

**Status:** TESTS NOT FOUND - Test target needs to be created

**Next Steps:** The parent bead (bf-4x1ju) or sibling beads likely need to create these tests.
