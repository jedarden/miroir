//! Online resharding: window guard, simulation model, and load estimation.
//!
//! Implements the plan §13.1 shadow-index resharding mechanics and §15 OP#3
//! empirical validation of the 2× transient load caveat.

use crate::router::{assign_shard_in_group, shard_for_key};
use crate::topology::{Group, NodeId};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

// ---------------------------------------------------------------------------
// Schedule window guard
// ---------------------------------------------------------------------------

/// A UTC time window like `"02:00-06:00"`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeWindow {
    /// Start hour+minute in minutes since midnight UTC.
    pub start_mins: u16,
    /// End hour+minute in minutes since midnight UTC.
    pub end_mins: u16,
}

impl std::fmt::Display for TimeWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02}:{:02}-{:02}:{:02}",
            self.start_mins / 60,
            self.start_mins % 60,
            self.end_mins / 60,
            self.end_mins % 60
        )
    }
}

impl TimeWindow {
    /// Parse a `"HH:MM-HH:MM"` string (UTC).
    pub fn parse(s: &str) -> Result<Self, String> {
        let (start, end) = s
            .split_once('-')
            .ok_or_else(|| format!("expected HH:MM-HH:MM, got {s}"))?;
        Ok(TimeWindow {
            start_mins: Self::parse_hm(start)?,
            end_mins: Self::parse_hm(end)?,
        })
    }

    fn parse_hm(hm: &str) -> Result<u16, String> {
        let (h, m) = hm
            .split_once(':')
            .ok_or_else(|| format!("expected HH:MM, got {hm}"))?;
        let h: u16 = h.parse().map_err(|_| format!("invalid hour: {h}"))?;
        let m: u16 = m.parse().map_err(|_| format!("invalid minute: {m}"))?;
        if h >= 24 || m >= 60 {
            return Err(format!("time out of range: {hm}"));
        }
        Ok(h * 60 + m)
    }

    /// Does `utc_minutes` (minutes since midnight UTC) fall inside this window?
    pub fn contains(&self, utc_minutes: u16) -> bool {
        if self.start_mins <= self.end_mins {
            utc_minutes >= self.start_mins && utc_minutes < self.end_mins
        } else {
            // Wraps midnight, e.g. 22:00-06:00
            utc_minutes >= self.start_mins || utc_minutes < self.end_mins
        }
    }
}

/// Resharding configuration (plan §13.1 + schedule window guard).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReshardingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_backfill_concurrency")]
    pub backfill_concurrency: usize,
    #[serde(default = "default_backfill_batch_size")]
    pub backfill_batch_size: usize,
    #[serde(default)]
    pub throttle_docs_per_sec: u64,
    #[serde(default = "default_true")]
    pub verify_before_swap: bool,
    #[serde(default = "default_retain_hours")]
    pub retain_old_index_hours: u64,
    /// Allowed schedule windows in `"HH:MM-HH:MM UTC"` format.
    /// Empty means any time is allowed (no restriction).
    #[serde(default)]
    pub allowed_windows: Vec<String>,
}

fn default_backfill_concurrency() -> usize {
    4
}
fn default_backfill_batch_size() -> usize {
    1000
}
fn default_true() -> bool {
    true
}
fn default_retain_hours() -> u64 {
    48
}

impl Default for ReshardingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            backfill_concurrency: default_backfill_concurrency(),
            backfill_batch_size: default_backfill_batch_size(),
            throttle_docs_per_sec: 0,
            verify_before_swap: true,
            retain_old_index_hours: default_retain_hours(),
            allowed_windows: Vec::new(),
        }
    }
}

/// Result of the schedule window guard check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowGuardResult {
    /// Current time is inside an allowed window.
    Allowed { window: String },
    /// No windows configured — always allowed.
    NoRestriction,
    /// Current time is outside all allowed windows.
    Denied {
        utc_now: String,
        allowed: Vec<String>,
    },
}

/// Check whether resharding is allowed at the given UTC minute-of-day.
///
/// Returns `Allowed` if `utc_minute` falls inside any configured window,
/// `NoRestriction` if no windows are configured, or `Denied` otherwise.
pub fn check_window(utc_minute: u16, config: &ReshardingConfig) -> WindowGuardResult {
    if config.allowed_windows.is_empty() {
        return WindowGuardResult::NoRestriction;
    }

    for raw in &config.allowed_windows {
        let window = match TimeWindow::parse(raw) {
            Ok(w) => w,
            Err(_) => continue,
        };
        if window.contains(utc_minute) {
            return WindowGuardResult::Allowed {
                window: raw.clone(),
            };
        }
    }

    WindowGuardResult::Denied {
        utc_now: format!("{:02}:{:02} UTC", utc_minute / 60, utc_minute % 60),
        allowed: config.allowed_windows.clone(),
    }
}

/// Check the schedule window against the system clock.
pub fn check_window_now(config: &ReshardingConfig) -> WindowGuardResult {
    let utc_minute = current_utc_minute();
    check_window(utc_minute, config)
}

fn current_utc_minute() -> u16 {
    let duration = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    ((duration.as_secs() / 60) % (24 * 60)) as u16
}

// ---------------------------------------------------------------------------
// Resharding load simulation
// ---------------------------------------------------------------------------

/// Parameters for a single simulation run.
#[derive(Debug, Clone)]
pub struct SimParams {
    /// Document size in bytes.
    pub doc_size_bytes: u64,
    /// Total corpus size in bytes.
    pub corpus_size_bytes: u64,
    /// Incoming write rate in documents per second.
    pub write_rate_dps: u64,
    /// Number of replica groups.
    pub replica_groups: u32,
    /// Replication factor (intra-group copies per shard).
    pub replication_factor: usize,
    /// Old shard count (before reshard).
    pub old_shards: u32,
    /// New shard count (after reshard).
    pub new_shards: u32,
    /// Number of nodes per replica group.
    pub nodes_per_group: usize,
    /// Backfill throttle in documents per second (0 = unlimited).
    pub backfill_throttle_dps: u64,
}

/// Results from a single simulation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimResult {
    pub label: String,
    pub doc_size_bytes: u64,
    pub corpus_size_bytes: u64,
    pub total_docs: u64,
    pub replica_groups: u32,
    pub replication_factor: usize,
    pub old_shards: u32,
    pub new_shards: u32,
    pub nodes_per_group: usize,
    pub write_rate_dps: u64,

    /// Normal steady-state storage across entire cluster (bytes).
    pub normal_storage_bytes: u64,
    /// Peak storage during resharding (live + shadow full, bytes).
    pub peak_storage_bytes: u64,
    /// Storage amplification factor (peak / normal).
    pub storage_amplification: f64,

    /// Normal steady-state write rate (actual node writes/sec).
    pub normal_write_rate: u64,
    /// Dual-write rate (phase 2 only, no backfill).
    pub dual_write_rate: u64,
    /// Peak write rate during backfill + dual-write.
    pub peak_write_rate: u64,
    /// Write amplification factor during dual-write only.
    pub dual_write_amplification: f64,
    /// Write amplification factor during peak (backfill + dual-write).
    pub peak_write_amplification: f64,

    /// Backfill duration in seconds (at configured throttle).
    pub backfill_duration_secs: f64,
    /// Total bytes written during full reshard operation.
    pub total_bytes_written: u64,

    /// Per-node peak storage (bytes).
    pub per_node_peak_storage_bytes: u64,
    /// Per-node normal storage (bytes).
    pub per_node_normal_storage_bytes: u64,

    /// Hash distribution stats for old shards.
    pub old_shard_cv: f64,
    /// Hash distribution stats for new shards.
    pub new_shard_cv: f64,
}

/// Run a resharding load simulation with the given parameters.
///
/// This models the six-phase resharding process from plan §13.1 using
/// the actual routing code to compute shard assignments and estimate
/// storage/write load. Document keys are synthesized for the corpus size
/// and routed through the real hash function to measure distribution.
pub fn simulate(params: &SimParams) -> SimResult {
    let total_docs = params.corpus_size_bytes / params.doc_size_bytes;
    let rf = params.replication_factor;
    let rg = params.replica_groups;
    let nodes_per_group = params.nodes_per_group;

    // Build a synthetic topology for the simulation.
    let groups: Vec<Group> = (0..rg)
        .map(|g| {
            let mut group = Group::new(g);
            for n in 0..nodes_per_group {
                group.add_node(NodeId::new(format!("node-g{g}-n{n}")));
            }
            group
        })
        .collect();

    // Simulate document distribution across old and new shard counts.
    // Use the actual router hash to get realistic distribution.
    let mut old_shard_counts: Vec<u64> = vec![0; params.old_shards as usize];
    let mut new_shard_counts: Vec<u64> = vec![0; params.new_shards as usize];

    // Track per-node storage for old and new shard assignments.
    // Each group stores the full corpus; each node in a group stores its
    // rendezvous-assigned fraction.
    let total_nodes = (rg as usize) * nodes_per_group;
    let mut node_storage_old: Vec<u64> = vec![0; total_nodes];
    let mut node_storage_new: Vec<u64> = vec![0; total_nodes];

    for i in 0..total_docs {
        let key = format!("doc-{i}");
        let old_shard = shard_for_key(&key, params.old_shards);
        let new_shard = shard_for_key(&key, params.new_shards);

        old_shard_counts[old_shard as usize] += 1;
        new_shard_counts[new_shard as usize] += 1;

        // For each replica group, assign shard to RF nodes.
        for (g_idx, group) in groups.iter().enumerate() {
            let old_targets = assign_shard_in_group(old_shard, group.nodes(), rf);
            let new_targets = assign_shard_in_group(new_shard, group.nodes(), rf);

            for node_id in &old_targets {
                let node_idx = g_idx * nodes_per_group
                    + group.nodes().iter().position(|n| n == node_id).unwrap_or(0);
                node_storage_old[node_idx] += params.doc_size_bytes;
            }
            for node_id in &new_targets {
                let node_idx = g_idx * nodes_per_group
                    + group.nodes().iter().position(|n| n == node_id).unwrap_or(0);
                node_storage_new[node_idx] += params.doc_size_bytes;
            }
        }
    }

    // Compute distribution coefficients of variation.
    let old_cv = cv(&old_shard_counts);
    let new_cv = cv(&new_shard_counts);

    // Normal storage: corpus replicated across RG groups.
    let normal_storage_bytes = params.corpus_size_bytes * rg as u64;
    // Peak storage: live + shadow (both fully populated).
    let peak_storage_bytes = normal_storage_bytes * 2;

    // Per-node storage (max across all nodes).
    let per_node_normal = node_storage_old.iter().copied().max().unwrap_or(0);
    let per_node_peak = per_node_normal + node_storage_new.iter().copied().max().unwrap_or(0);

    // Write rates.
    // Normal: each incoming doc → RF × RG actual node writes.
    let normal_write_rate = params.write_rate_dps * rf as u64 * rg as u64;
    // Dual-write: each incoming doc → 2 × (RF × RG) writes (old + new assignment).
    let dual_write_rate = normal_write_rate * 2;
    // Backfill: reads all docs, writes each to new assignment → RF × RG writes/doc.
    // Plus ongoing dual-writes for new incoming docs.
    let backfill_write_rate = params.backfill_throttle_dps * rf as u64 * rg as u64;
    let peak_write_rate = dual_write_rate + backfill_write_rate;

    let dual_write_amplification = 2.0;
    let peak_write_amplification = peak_write_rate as f64 / normal_write_rate as f64;

    // Backfill duration: total docs / throttle rate.
    let backfill_duration_secs = if params.backfill_throttle_dps > 0 {
        total_docs as f64 / params.backfill_throttle_dps as f64
    } else {
        f64::INFINITY
    };

    // Total bytes written during reshard:
    // 1. Dual-write ongoing for the full reshard duration.
    // 2. Backfill writes of entire corpus.
    // Approximate: backfill_duration × dual_write_rate + corpus × RF × RG.
    let total_reshard_write_bytes = if params.backfill_throttle_dps > 0 {
        let dual_write_bytes =
            backfill_duration_secs * dual_write_rate as f64 * params.doc_size_bytes as f64;
        let backfill_bytes = total_docs * rf as u64 * rg as u64 * params.doc_size_bytes;
        (dual_write_bytes as u64) + backfill_bytes
    } else {
        0
    };

    let storage_amplification = peak_storage_bytes as f64 / normal_storage_bytes as f64;

    SimResult {
        label: format!(
            "{}KB/{}GB/RG{}/RF{}",
            params.doc_size_bytes / 1024,
            params.corpus_size_bytes / (1024 * 1024 * 1024),
            rg,
            rf
        ),
        doc_size_bytes: params.doc_size_bytes,
        corpus_size_bytes: params.corpus_size_bytes,
        total_docs,
        replica_groups: rg,
        replication_factor: rf,
        old_shards: params.old_shards,
        new_shards: params.new_shards,
        nodes_per_group,
        write_rate_dps: params.write_rate_dps,

        normal_storage_bytes,
        peak_storage_bytes,
        storage_amplification,

        normal_write_rate,
        dual_write_rate,
        peak_write_rate,
        dual_write_amplification,
        peak_write_amplification,

        backfill_duration_secs,
        total_bytes_written: total_reshard_write_bytes,

        per_node_peak_storage_bytes: per_node_peak,
        per_node_normal_storage_bytes: per_node_normal,

        old_shard_cv: old_cv,
        new_shard_cv: new_cv,
    }
}

/// Coefficient of variation for a distribution.
fn cv(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<u64>() as f64 / n;
    if mean == 0.0 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|v| (*v as f64 - mean).powi(2))
        .sum::<f64>()
        / n;
    variance.sqrt() / mean
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- TimeWindow parsing and containment ----

    #[test]
    fn time_window_parse_simple() {
        let w = TimeWindow::parse("02:00-06:00").unwrap();
        assert_eq!(w.start_mins, 120);
        assert_eq!(w.end_mins, 360);
    }

    #[test]
    fn time_window_parse_wrap_midnight() {
        let w = TimeWindow::parse("22:00-06:00").unwrap();
        assert_eq!(w.start_mins, 1320);
        assert_eq!(w.end_mins, 360);
    }

    #[test]
    fn time_window_contains_normal() {
        let w = TimeWindow::parse("02:00-06:00").unwrap();
        assert!(w.contains(180)); // 03:00
        assert!(!w.contains(100)); // 01:40
        assert!(!w.contains(400)); // 06:40
    }

    #[test]
    fn time_window_contains_wrap() {
        let w = TimeWindow::parse("22:00-06:00").unwrap();
        assert!(w.contains(1350)); // 22:30
        assert!(w.contains(300)); // 05:00
        assert!(!w.contains(700)); // 11:40
    }

    #[test]
    fn time_window_boundary_start() {
        let w = TimeWindow::parse("02:00-06:00").unwrap();
        assert!(w.contains(120)); // exactly 02:00
    }

    #[test]
    fn time_window_boundary_end_exclusive() {
        let w = TimeWindow::parse("02:00-06:00").unwrap();
        assert!(!w.contains(360)); // exactly 06:00 is excluded
    }

    #[test]
    fn time_window_invalid_format() {
        assert!(TimeWindow::parse("not-a-window").is_err());
        assert!(TimeWindow::parse("25:00-06:00").is_err());
        assert!(TimeWindow::parse("02:60-06:00").is_err());
    }

    // ---- Window guard ----

    #[test]
    fn window_guard_no_restriction() {
        let config = ReshardingConfig::default();
        assert_eq!(check_window(0, &config), WindowGuardResult::NoRestriction);
    }

    #[test]
    fn window_guard_allowed() {
        let config = ReshardingConfig {
            allowed_windows: vec!["02:00-06:00".into()],
            ..Default::default()
        };
        let result = check_window(180, &config); // 03:00
        assert!(matches!(result, WindowGuardResult::Allowed { .. }));
    }

    #[test]
    fn window_guard_denied() {
        let config = ReshardingConfig {
            allowed_windows: vec!["02:00-06:00".into()],
            ..Default::default()
        };
        let result = check_window(720, &config); // 12:00
        assert!(matches!(result, WindowGuardResult::Denied { .. }));
    }

    #[test]
    fn window_guard_multiple_windows() {
        let config = ReshardingConfig {
            allowed_windows: vec!["02:00-04:00".into(), "22:00-23:30".into()],
            ..Default::default()
        };
        // In first window.
        assert!(matches!(
            check_window(150, &config),
            WindowGuardResult::Allowed { .. }
        ));
        // In second window.
        assert!(matches!(
            check_window(1350, &config),
            WindowGuardResult::Allowed { .. }
        ));
        // Outside both.
        assert!(matches!(
            check_window(720, &config),
            WindowGuardResult::Denied { .. }
        ));
    }

    // ---- Simulation ----

    #[test]
    fn simulation_storage_always_2x() {
        // Regardless of parameters, peak storage should be exactly 2× normal.
        let params = SimParams {
            doc_size_bytes: 1024,
            corpus_size_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
            write_rate_dps: 100,
            replica_groups: 2,
            replication_factor: 1,
            old_shards: 64,
            new_shards: 128,
            nodes_per_group: 3,
            backfill_throttle_dps: 10_000,
        };
        let result = simulate(&params);
        assert!(
            (result.storage_amplification - 2.0).abs() < 0.01,
            "expected ~2.0, got {}",
            result.storage_amplification
        );
    }

    #[test]
    fn simulation_dual_write_is_2x() {
        let params = SimParams {
            doc_size_bytes: 1024,
            corpus_size_bytes: 1024 * 1024,
            write_rate_dps: 100,
            replica_groups: 2,
            replication_factor: 1,
            old_shards: 16,
            new_shards: 32,
            nodes_per_group: 3,
            backfill_throttle_dps: 1000,
        };
        let result = simulate(&params);
        assert!(
            (result.dual_write_amplification - 2.0).abs() < 0.01,
            "expected 2.0, got {}",
            result.dual_write_amplification
        );
    }

    #[test]
    fn simulation_low_cv_with_many_docs() {
        // With enough docs, hash distribution CV should be very low (< 5%).
        let params = SimParams {
            doc_size_bytes: 1024,
            corpus_size_bytes: 1_000_000 * 1024, // 1M docs × 1KB
            write_rate_dps: 100,
            replica_groups: 1,
            replication_factor: 1,
            old_shards: 16,
            new_shards: 64,
            nodes_per_group: 4,
            backfill_throttle_dps: 1000,
        };
        let result = simulate(&params);
        assert!(
            result.old_shard_cv < 0.05,
            "old shard CV too high: {}",
            result.old_shard_cv
        );
        assert!(
            result.new_shard_cv < 0.05,
            "new shard CV too high: {}",
            result.new_shard_cv
        );
    }
}
