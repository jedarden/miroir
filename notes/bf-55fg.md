# P6.8 Per-feature scaling behavior reference doc verification

Verified that `docs/horizontal-scaling/per-feature.md` exists and meets all acceptance criteria for bead bf-55fg.

## Acceptance criteria status

- [x] `docs/horizontal-scaling/per-feature.md` exists and reproduces the §14.6 table
- [x] Each row links to the relevant §13.x feature bead (or its closed predecessor)
- [x] Forced-mode constraints subsection enumerates every Helm `values.schema.json` rejection driven by horizontal-scaling concerns
- [x] README.md links to it
- [x] Doc is referenced from `miroir-m9q.3/4/5` descriptions for cross-navigation

## Notes

The file was already created in a previous attempt. The forced-mode constraints section (Rules 0-4) accurately reflects the validation rules in `charts/miroir/values.schema.json`:
- Rule 0: `taskStore.backend: redis` requires `miroir.replicas > 1`
- Rule 1: `miroir.replicas > 1` requires `taskStore.backend: redis`
- Rule 2: `hpa.enabled: true` requires `replicas >= 2` AND `taskStore.backend: redis`
- Rule 3: `search_ui.rate_limit.backend: local` rejected when `miroir.replicas > 1`
- Rule 4: `admin_ui.rate_limit.backend: local` rejected when `miroir.replicas > 1`

The doc is well-structured and provides operators with a clear reference for horizontal scaling requirements per feature.

## Additional work (2026-05-20)

Added cross-reference comments to beads `miroir-m9q.3`, `miroir-m9q.4`, and `miroir-m9q.5` pointing to `docs/horizontal-scaling/per-feature.md`. This enables bidirectional navigation between the mode implementation beads and the per-feature scaling reference.
