# Dump Import Documentation

This directory contains documentation for Miroir's dump import functionality, including compatibility information and operational guidance.

## Overview

Miroir supports two dump import modes:

1. **Streaming Mode (Recommended)**: Routes documents per-shard via the public API. Efficient storage distribution, scales horizontally.
2. **Broadcast Mode (Legacy)**: Sends all documents to all nodes. Requires post-import rebalance. Used as fallback for incompatible dump variants.

## Documents

- **[compatibility-matrix.md](./compatibility-matrix.md)**: Comprehensive matrix of dump variants, streaming compatibility, and workarounds

## Quick Reference

```bash
# Import a dump (streaming mode by default)
miroir-ctl dump import --file products.dump --index products

# Force broadcast mode
miroir-ctl dump import --file products.dump --index products --mode broadcast

# Analyze a dump for compatibility
miroir-ctl dump analyze --file products.dump
```

## Related Plan Sections

- [Plan §13.9: Streaming routed dump import](../plan/plan.md#139-streaming-routed-dump-import)
- [Plan §13.5: Two-phase settings broadcast](../plan/plan.md#135-two-phase-settings-broadcast)

## Enhancement Tracking

| Bead | Description | Status |
|------|-------------|--------|
| `miroir-zc2.5` | Dump import compatibility matrix | Complete |
| `miroir-zc2.6` | Configurable shard metadata field name | Open |
| `miroir-zc2.7` | Pre-import validation and field conflict detection | Open |
| `miroir-zc2.8` | EE-to-CE dump conversion tool | Open |
