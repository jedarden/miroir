# bf-1p4v: Borrow of moved value `state`

The reported compile error was already fixed in the codebase. Line 568 in `crates/miroir-proxy/src/main.rs` correctly uses `.with_state(state.clone())` instead of `.with_state(state)`.

The fix works because:
1. `UnifiedState` derives `Clone` (line 39)
2. `state.clone()` is passed to `.with_state()`, leaving the original `state` available for the metrics server on line 590

Build verified: `cargo build` succeeds with no errors.
