# Task bf-49o5j: Locate search_ui_rate_limit test target

## Task Completion Summary

Successfully located the `search_ui_rate_limit` test target in the miroir-proxy crate structure.

## Test Target Location

**File Path:** `/home/coding/miroir/crates/miroir-core/src/task_store/redis.rs`

**Test Function:** `test_redis_rate_limit_searchui` (line 3903)

**Module:** `miroir_core::task_store::redis::RedisTaskStore`

## Test Structure Overview

The test is implemented as a `#[tokio::test]` unit test embedded within the Redis task store implementation. It validates the search_ui rate limiting functionality using Redis.

### Test Coverage

The `test_redis_rate_limit_searchui` function tests:

1. **Basic rate limiting behavior:**
   - First 3 requests are allowed (limit = 3)
   - 4th request is blocked
   - Proper remaining count tracking (2, 1, 0)

2. **Redis key management:**
   - Validates that rate limit keys have proper TTL/EXPIRE set
   - Confirms TTL does not exceed the configured window (60 seconds)
   - Key pattern: `miroir:ratelimit:searchui:{ip_address}`

### Related Implementation Files

The rate limiting functionality is implemented in:

1. **Core logic:** `/home/coding/miroir/crates/miroir-core/src/task_store/redis.rs`
   - Function: `check_rate_limit_searchui()` (line 3065)
   - Uses INCR + EXPIRE pattern for sliding window rate limiting

2. **Route integration:** `/home/coding/miroir/crates/miroir-proxy/src/routes/search_ui.rs`
   - Uses `local_search_ui_rate_limiter` for local backend
   - Uses `redis.check_rate_limit_searchui()` for Redis backend
   - Returns `RateLimitInfo` structure with limit, remaining, and reset_in fields

3. **Configuration:** Rate limit is configured via `search_ui.rate_limit.backend` (local/redis)

### Comparison with Similar Tests

The pattern follows the same structure as the admin login rate limit test:
- `p10_7_admin_login_rate_limit.rs` tests `check_rate_limit_admin_login()`
- `test_redis_rate_limit_searchui` tests `check_rate_limit_searchui()`

Both use Redis-backed rate limiting with similar INCR + EXPIRE patterns.

## Acceptance Criteria Status

✅ Test target file path identified: `/home/coding/miroir/crates/miroir-core/src/task_store/redis.rs:3903`

✅ File exists and is readable: Confirmed

✅ Basic structure overview: Documented above
