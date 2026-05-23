# P5.5.a Proposal: Phase 1 — Parallel PATCH with Task Succession

**Bead:** miroir-uhj.5.1
**Date:** 2026-05-23
**Plan Reference:** §13.5 Two-phase settings broadcast with verification

## Current State Analysis

### Existing Implementation Location
`/home/coding/miroir/crates/miroir-proxy/src/routes/indexes.rs` — `two_phase_settings_broadcast()` function (lines 809-1039)

### Current Phase 1 Flow (Sequential)

```rust
// Lines 839-868: Current implementation
for address in &nodes {
    match client.patch_raw(address, &full_path, body).await {
        Ok((status, text)) if status >= 200 && status < 300 => {
            // Extract taskUid if present
            if let Ok(resp) = serde_json::from_str::<Value>(&text) {
                if let Some(task_uid) = resp.get("taskUid").and_then(|v| v.as_u64()) {
                    node_task_uids.insert(address.clone(), task_uid);
                }
            }
        }
        // ... error handling
    }
}
```

**Key Findings:**

1. **Sequential execution**: The `for` loop processes nodes one-by-one, not in parallel
2. **No task awaiting**: The code collects `task_uid`s but does NOT poll/wait for tasks to reach "succeeded"
3. **Misleading comment**: Line 887 says "Wait for all node tasks to complete" but the `run_verify` closure immediately does GET requests instead of polling task status
4. **Verification bypasses task status**: Phase 2 uses GET /indexes/{uid}/settings to verify hashes, not task status polling

**Problem**: This violates plan §13.5 which explicitly states "await all task_uids to reach succeeded" as Phase 1's completion condition.

## Proposed Phase 1: Parallel PATCH with Task Succession

### Architecture

```
Phase 1: Propose (parallel)
┌─────────────────────────────────────────────────────────────┐
│ 1. Spawn parallel PATCH requests to all nodes                │
│    └─ Use futures_util::future::join_all (existing pattern) │
│                                                              │
│ 2. Collect task_uid from each PATCH response                │
│    └─ Store in HashMap<String, u64> (node_id -> task_uid)    │
│                                                              │
│ 3. Poll each node's task until all reach "succeeded"        │
│    └─ Parallel polling with exponential backoff             │
│    └─ Use existing NodePoller trait from task_registry.rs   │
│    └─ Timeout: verify_timeout_s from config                 │
│                                                              │
│ 4. On all succeeded → enter Phase 2                          │
│    On any failed/timeout → abort broadcast                   │
└─────────────────────────────────────────────────────────────┘

During Phase 1: Response header X-Miroir-Settings-Inconsistent: true
```

### Implementation Details

#### Step 1: Parallel PATCH (using existing pattern from scatter.rs)

```rust
// Location: crates/miroir-proxy/src/routes/indexes.rs
// After line 838 in two_phase_settings_broadcast()

use futures_util::future::join_all;

// Parallel PATCH to all nodes
let patch_tasks: Vec<_> = nodes.iter().map(|address| {
    let client = client.clone();
    let address = address.clone();
    let full_path = full_path.clone();
    let body = body.clone();
    async move {
        (address.clone(), client.patch_raw(&address, &full_path, &body).await)
    }
}).collect();

let patch_results = join_all(patch_tasks).await;

// Collect task_uids and first response
let mut node_task_uids = HashMap::new();
let mut first_response: Option<Value> = None;
let mut errors: Vec<String> = Vec::new();

for (address, result) in patch_results {
    match result {
        Ok((status, text)) if status >= 200 && status < 300 => {
            if first_response.is_none() {
                first_response = serde_json::from_str(&text).ok();
            }
            if let Ok(resp) = serde_json::from_str::<Value>(&text) {
                if let Some(task_uid) = resp.get("taskUid").and_then(|v| v.as_u64()) {
                    node_task_uids.insert(address, task_uid);
                }
            }
        }
        Ok((status, text)) => {
            errors.push(format!("{}: HTTP {} — {}", address, status, text));
        }
        Err(e) => {
            errors.push(format!("{}: {}", address, e));
        }
    }
}

if !errors.is_empty() {
    return Err(MeilisearchError::new(
        MiroirCode::NoQuorum,
        format!("Phase 1 propose failed: {}", errors.join("; ")),
    ));
}
```

#### Step 2: Task Succession (polling until all succeeded)

```rust
// New helper function: poll_all_tasks_until_succeeded()
// Location: crates/miroir-proxy/src/routes/indexes.rs

use crate::scatter::TaskStatusRequest;
use std::time::Duration;

/// Poll all node tasks until they reach terminal state.
/// Returns Ok(()) if all succeeded, Err on any failure or timeout.
async fn poll_all_tasks_until_succeeded(
    client: &MeilisearchClient,
    nodes: &[String],
    node_task_uids: &HashMap<String, u64>,
    timeout_s: u64,
) -> Result<(), MeilisearchError> {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_s);
    let mut delay_ms = 25u64;
    let max_delay_ms = 1000u64;

    loop {
        // Check timeout
        if start.elapsed() > timeout {
            return Err(MeilisearchError::new(
                MiroirCode::Timeout,
                format!("Phase 1 task succession timed out after {}s", timeout_s),
            ));
        }

        // Parallel task status polling
        let poll_tasks: Vec<_> = node_task_uids.iter().map(|(address, &task_uid)| {
            let client = client.clone();
            let address = address.clone();
            async move {
                let req = TaskStatusRequest { task_uid };
                (address.clone(), client.get_task_status_raw(&address, &req).await)
            }
        }).collect();

        let poll_results = join_all(poll_tasks).await;

        // Track terminal states
        let mut all_succeeded = true;
        let mut any_failed = false;
        let mut all_terminal = true;

        for (address, result) in poll_results {
            match result {
                Ok((status, text)) if status >= 200 && status < 300 => {
                    if let Ok(resp) = serde_json::from_str::<Value>(&text) {
                        if let Some(task_status) = resp.get("status").and_then(|v| v.as_str()) {
                            match task_status {
                                "succeeded" => {}
                                "failed" => {
                                    any_failed = true;
                                    all_terminal = true;
                                }
                                _ => {
                                    all_terminal = false;
                                    all_succeeded = false;
                                }
                            }
                        }
                    }
                }
                Ok(_) | Err(_) => {
                    // Treat poll errors as non-terminal (retry)
                    all_terminal = false;
                    all_succeeded = false;
                }
            }
        }

        // Check completion conditions
        if any_failed {
            return Err(MeilisearchError::new(
                MiroirCode::NoQuorum,
                "Phase 1: at least one node task failed",
            ));
        }

        if all_terminal && all_succeeded {
            return Ok(());
        }

        // Exponential backoff
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        delay_ms = (delay_ms * 2).min(max_delay_ms);
    }
}

// Call it after collecting task_uids:
poll_all_tasks_until_succeeded(&client, &nodes, &node_task_uids, config.settings_broadcast.verify_timeout_s).await?;
```

#### Step 3: Add MeilisearchClient::get_task_status_raw()

```rust
// Location: crates/miroir-proxy/src/client.rs (or wherever MeilisearchClient is defined)

impl MeilisearchClient {
    /// Get task status from a node (raw response).
    pub async fn get_task_status_raw(
        &self,
        address: &str,
        req: &TaskStatusRequest,
    ) -> Result<(u16, String), reqwest::Error> {
        let path = format!("/tasks/{}", req.task_uid);
        self.get_raw(address, &path).await
    }
}
```

### Performance Characteristics

| Metric | Sequential (Current) | Parallel (Proposed) |
|--------|---------------------|---------------------|
| PATCH latency | O(N × t_patch) | O(max(t_patch)) |
| Task poll rounds | O(N × rounds) | O(rounds) |
| Total Phase 1 time | ~N × (50ms + 500ms) | ~max(50ms) + rounds × 25ms |
| 10 nodes @ 50ms PATCH | ~550ms | ~50ms + poll time |

**Example**: For 10 nodes with 50ms PATCH latency:
- Sequential: 10 × 50ms = 500ms just for PATCH requests
- Parallel: max(50ms) = 50ms for all PATCH requests
- **9x speedup** on the PATCH phase alone

### X-Miroir-Settings-Inconsistent Header

During Phase 1, search responses should include the warning header:

```rust
// Location: crates/miroir-proxy/src/routes/search.rs
// In search handler, after scattering results:

if state.settings_broadcast.is_in_flight(index).await {
    headers.insert("X-Miroir-Settings-Inconsistent", "true".parse().unwrap());
}
```

This signals to clients that ranking scores may be inconsistent across nodes during the broadcast.

### Error Handling

| Scenario | Action |
|----------|--------|
| Any PATCH fails (HTTP 4xx/5xx) | Abort immediately, return error |
| Task reaches "failed" status | Abort immediately, return error |
| Task succession timeout | Abort with Timeout error |
| Leadership loss during Phase 1 | Persist state via ModeBOpLeader, new leader resumes |

### Integration with Existing Components

1. **SettingsBroadcast**: Already has `start_propose()` and `enter_verify()` methods
2. **NodePoller trait**: Exists in `task_registry.rs` for polling abstraction
3. **Parallel execution pattern**: `join_all` already used in `scatter.rs`
4. **MeilisearchClient**: Has `patch_raw()` method, needs `get_task_status_raw()`

## Testing Strategy

### Unit Tests
1. **Parallel PATCH success**: Mock 10 nodes, verify all PATCH requests issued concurrently
2. **Task succession happy path**: Mock task status progression enqueued → processing → succeeded
3. **Task failure handling**: Mock one task reaching "failed", verify abort
4. **Timeout handling**: Mock tasks never completing, verify timeout error

### Integration Tests
```rust
// crates/miroir-proxy/tests/p5_5_parallel_propose.rs
#[tokio::test]
async fn test_parallel_propose_all_succeed() {
    // 3-node cluster
    // PATCH settings
    // Verify all tasks polled in parallel
    // Verify Phase 2 entered after all succeeded
}

#[tokio::test]
async fn test_parallel_propose_one_fails() {
    // 3-node cluster, node-1 returns 500
    // Verify immediate abort, no Phase 2
}

#[tokio::test]
async fn test_settings_inconsistent_header() {
    // Start Phase 1
    // Issue search request
    // Verify X-Miroir-Settings-Inconsistent: true
}
```

## Open Questions

1. **Should we use the existing InMemoryTaskRegistry polling or inline polling?**
   - Inline polling is simpler for this one-shot use case
   - TaskRegistry polling is better for long-lived task tracking

2. **Should Phase 1 persist task_uids to task_store before polling?**
   - Yes: allows new leader to resume after leadership loss
   - Use `SettingsBroadcastCoordinator.enter_verify()` which persists

3. **What about PATCH requests that return without a taskUid?**
   - Should not happen in normal Meilisearch operation
   - Treat as error and abort

## Next Steps

1. **Implement** `poll_all_tasks_until_succeeded()` function
2. **Add** `MeilisearchClient::get_task_status_raw()` method
3. **Refactor** `two_phase_settings_broadcast()` Phase 1 to use parallel PATCH
4. **Add** task succession polling after PATCH completes
5. **Persist** task_uids via `enter_verify()` before polling
6. **Add** X-Miroir-Settings-Inconsistent header to search responses
7. **Test** with 3-node docker-compose cluster

## References

- Plan §13.5: `/home/coding/miroir/docs/plan/plan.md`
- Current implementation: `/home/coding/miroir/crates/miroir-proxy/src/routes/indexes.rs:809-1039`
- Parallel pattern: `/home/coding/miroir/crates/miroir-core/src/scatter.rs:515-542`
- Task polling: `/home/coding/miroir/crates/miroir-core/src/task_registry.rs:339-447`
