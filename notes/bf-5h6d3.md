# bf-5h6d3 — Disk Pre-Flight Check

Split-child of bf-146g8 (build-only decomposition), **step 1 of 4**.
DISK PRE-FLIGHT ONLY — no build, no code changes.

## Result

```
$ df -BG --output=avail / | tail -1
 28G
```

**Free disk on `/`: 28G** — above the ~20G threshold.

## Action taken

None required. Per the task acceptance criteria, since free space (28G) is
≥ 20G, no `target/` cleanup was performed. `~/miroir/target` was left
untouched regardless (not mid-build).

## Acceptance criteria

- [x] GB free on `/` recorded (28G) — see comment 15 on the bead and above.
- [x] Below-threshold case not triggered (28G ≥ 20G), so no idle
      other-repo `target/` was cleared.
- [x] No code changes.
