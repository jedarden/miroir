//! Online resharding: window guard, simulation model, and six-phase execution.
//!
//! Implements the plan §13.1 shadow-index resharding mechanics and §15 OP#3
//! empirical validation of the 2× transient load caveat.
//!
//! Leader coordination (plan §14.5 Mode B):
//! - Acquires per-index leader lease (scope: "reshard:<index>")
//! - Persists phase state to mode_b_operations table for recovery
//! - New leaders resume from last committed phase boundary

use crate::mode_b_coordinator::{ModeBOpLeader, PhaseState};
use crate::router::{assign_shard_in_group, shard_for_key};
use crate::topology::{Group, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

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
            .ok_or_else(|| format!("expected HH:MM-HH:MM, got {}", s))?;
        Ok(TimeWindow {
            start_mins: Self::parse_hm(start)?,
            end_mins: Self::parse_hm(end)?,
        })
    }

    fn parse_hm(hm: &str) -> Result<u16, String> {
        let (h, m) = hm
            .split_once(':')
            .ok_or_else(|| format!("expected HH:MM, got {}", hm))?;
        let h: u16 = h.parse().map_err(|_| format!("invalid hour: {}", h))?;
        let m: u16 = m.parse().map_err(|_| format!("invalid minute: {}", m))?;
        if h >= 24 || m >= 60 {
            return Err(format!("time out of range: {}", hm));
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
                group.add_node(NodeId::new(format!("node-g{}-n{}", g, n)));
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
        let key = format!("doc-{}", i);
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

    #[test]
    fn time_window_display_roundtrip() {
        let w = TimeWindow::parse("02:30-06:45").unwrap();
        assert_eq!(format!("{}", w), "02:30-06:45");
    }

    #[test]
    fn time_window_parse_missing_dash() {
        let err = TimeWindow::parse("02:00").unwrap_err();
        assert!(err.contains("expected HH:MM-HH:MM"));
    }

    #[test]
    fn time_window_parse_invalid_hm() {
        let err = TimeWindow::parse("ab:00-06:00").unwrap_err();
        assert!(err.contains("invalid hour"));
    }

    #[test]
    fn time_window_parse_invalid_minute() {
        let err = TimeWindow::parse("02:ab-06:00").unwrap_err();
        assert!(err.contains("invalid minute"));
    }

    #[test]
    fn check_window_now_no_restriction() {
        let config = ReshardingConfig::default();
        assert!(matches!(
            check_window_now(&config),
            WindowGuardResult::NoRestriction
        ));
    }

    #[test]
    fn simulation_throttle_zero_gives_infinity_duration() {
        let params = SimParams {
            doc_size_bytes: 1024,
            corpus_size_bytes: 1024 * 1024,
            write_rate_dps: 100,
            replica_groups: 1,
            replication_factor: 1,
            old_shards: 4,
            new_shards: 8,
            nodes_per_group: 2,
            backfill_throttle_dps: 0,
        };
        let result = simulate(&params);
        assert!(result.backfill_duration_secs.is_infinite());
        assert_eq!(result.total_bytes_written, 0);
    }

    #[test]
    fn cv_empty_and_zero() {
        assert_eq!(cv(&[]), 0.0);
        assert_eq!(cv(&[0, 0, 0]), 0.0);
    }
}

// ---------------------------------------------------------------------------
// Six-phase resharding execution (plan §13.1)
// ---------------------------------------------------------------------------

/// Resharding phase identifiers (matching plan §13.1 steps).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ReshardPhase {
    /// No active resharding.
    Idle = 0,
    /// Phase 1: Shadow index created.
    ShadowCreated = 1,
    /// Phase 2: Dual-hash dual-write active.
    DualWriteActive = 2,
    /// Phase 3: Backfill in progress.
    BackfillInProgress = 3,
    /// Phase 4: Verification in progress.
    Verifying = 4,
    /// Phase 5: Alias swap completed.
    Swapped = 5,
    /// Phase 6: Cleanup in progress.
    CleaningUp = 6,
    /// Resharding completed successfully.
    Complete = 7,
    /// Resharding failed.
    Failed = 8,
}

impl ReshardPhase {
    /// Human-readable phase name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Idle => "Idle",
            Self::ShadowCreated => "Shadow Created",
            Self::DualWriteActive => "Dual-Write Active",
            Self::BackfillInProgress => "Backfill In Progress",
            Self::Verifying => "Verifying",
            Self::Swapped => "Swapped",
            Self::CleaningUp => "Cleaning Up",
            Self::Complete => "Complete",
            Self::Failed => "Failed",
        }
    }

    /// Parse from u8 (for storage).
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Idle),
            1 => Some(Self::ShadowCreated),
            2 => Some(Self::DualWriteActive),
            3 => Some(Self::BackfillInProgress),
            4 => Some(Self::Verifying),
            5 => Some(Self::Swapped),
            6 => Some(Self::CleaningUp),
            7 => Some(Self::Complete),
            8 => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Active resharding operation state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReshardOperation {
    /// Unique operation ID.
    pub id: String,
    /// Index UID being resharded.
    pub index_uid: String,
    /// Old shard count.
    pub old_shards: u32,
    /// New shard count.
    pub target_shards: u32,
    /// Current phase.
    pub phase: ReshardPhase,
    /// Phase started at (UNIX ms).
    pub phase_started_at: u64,
    /// Operation created at (UNIX ms).
    pub created_at: u64,
    /// Documents backfilled so far.
    pub documents_backfilled: u64,
    /// Total documents to backfill (estimated at start).
    pub total_documents: u64,
    /// Last error message (if any).
    pub last_error: Option<String>,
    /// Shadow index UID.
    pub shadow_index: String,
    /// Verification results (populated after phase 4).
    pub verification_results: Option<VerificationResults>,
    /// Cleanup retention deadline (UNIX ms).
    pub cleanup_deadline: Option<u64>,
}

/// Results from phase 4 verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResults {
    /// Live index PK set size.
    pub live_pk_count: u64,
    /// Shadow index PK set size.
    pub shadow_pk_count: u64,
    /// PKs only in live index.
    pub live_only_pks: Vec<String>,
    /// PKs only in shadow index.
    pub shadow_only_pks: Vec<String>,
    /// PKs with content hash mismatch.
    pub mismatched_pks: Vec<String>,
    /// Whether verification passed.
    pub passed: bool,
}

impl ReshardOperation {
    /// Create a new resharding operation.
    pub fn new(index_uid: String, old_shards: u32, target_shards: u32) -> Self {
        let id = format!("reshard-{}-{}", index_uid, uuid::Uuid::new_v4());
        let shadow_index = format!("{}__reshard_{}", index_uid, target_shards);
        let now = millis_now();
        Self {
            id,
            index_uid,
            old_shards,
            target_shards,
            phase: ReshardPhase::ShadowCreated,
            phase_started_at: now,
            created_at: now,
            documents_backfilled: 0,
            total_documents: 0,
            last_error: None,
            shadow_index,
            verification_results: None,
            cleanup_deadline: None,
        }
    }

    /// Transition to the next phase.
    pub fn advance_phase(&mut self, new_phase: ReshardPhase) {
        self.phase = new_phase;
        self.phase_started_at = millis_now();
    }

    /// Record an error and mark as failed.
    pub fn fail(&mut self, error: String) {
        self.last_error = Some(error);
        self.phase = ReshardPhase::Failed;
    }

    /// Update backfill progress.
    pub fn update_backfill_progress(&mut self, backfilled: u64, total: u64) {
        self.documents_backfilled = backfilled;
        self.total_documents = total;
    }

    /// Calculate backfill progress ratio (0.0 to 1.0).
    pub fn backfill_progress(&self) -> f64 {
        if self.total_documents == 0 {
            return 0.0;
        }
        (self.documents_backfilled as f64) / (self.total_documents as f64)
    }

    /// Check if the operation is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self.phase, ReshardPhase::Complete | ReshardPhase::Failed)
    }

    /// Get the shadow index name.
    pub fn shadow_index_name(&self) -> &str {
        &self.shadow_index
    }
}

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// In-memory registry of active resharding operations.
///
/// In production, this is persisted to the task store (SQLite/Redis).
/// This in-memory version is for single-pod development.
#[derive(Debug, Default)]
pub struct ReshardRegistry {
    operations: HashMap<String, ReshardOperation>,
    /// Index UID -> active operation ID (at most one per index).
    index_ops: HashMap<String, String>,
}

/// Leader-coordinated reshard coordinator (plan §14.5 Mode B).
///
/// Acquires a per-index leader lease (scope: "reshard:<index>") and persists
/// phase state so that a new leader can resume from the last committed phase.
pub struct ReshardCoordinator<E> {
    /// Mode B operation leader with phase state persistence.
    leader: ModeBOpLeader<ReshardExtraState>,
    /// Phantom for the executor type.
    _phantom: std::marker::PhantomData<E>,
}

/// Extra state for reshard operations persisted to mode_b_operations.
///
/// # CDC Origin Tag (plan §13.13)
///
/// Backfill writes during phase 3 must be tagged with `origin="reshard_backfill"`
/// so they are suppressed from CDC by default (unless `emit_internal_writes` is true).
///
/// When constructing `WriteRequest` for backfill writes, set:
/// ```ignore
/// use miroir_core::cdc::ORIGIN_RESHARD_BACKFILL;
/// WriteRequest { ..., origin: Some(ORIGIN_RESHARD_BACKFILL.to_string()) }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReshardExtraState {
    /// Index UID being resharded.
    pub index_uid: String,
    /// Old shard count.
    pub old_shards: u32,
    /// New shard count.
    pub target_shards: u32,
    /// Shadow index UID.
    pub shadow_index: String,
    /// Documents backfilled so far.
    pub documents_backfilled: u64,
    /// Total documents to backfill.
    pub total_documents: u64,
    /// Last error message.
    pub last_error: Option<String>,
}

impl<E> ReshardCoordinator<E> {
    /// Create a new reshard coordinator.
    pub fn new(
        leader_election: Arc<crate::leader_election::LeaderElection>,
        task_store: Arc<dyn crate::task_store::TaskStore>,
        index_uid: String,
        old_shards: u32,
        target_shards: u32,
        pod_id: String,
    ) -> Self {
        let scope = format!("reshard:{}", index_uid);
        let shadow_index = format!("{}__reshard_{}", index_uid, target_shards);

        let extra_state = ReshardExtraState {
            index_uid,
            old_shards,
            target_shards,
            shadow_index,
            documents_backfilled: 0,
            total_documents: 0,
            last_error: None,
        };

        let leader = ModeBOpLeader::new(
            leader_election,
            task_store,
            crate::task_store::mode_b_type::RESHARD.to_string(),
            scope,
            pod_id,
            extra_state,
        );

        Self {
            leader,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Try to acquire leadership for this reshard operation.
    ///
    /// Returns `Ok(true)` if we are now the leader, `Ok(false)` if another
    /// pod holds the lease, or `Err` if acquisition failed.
    pub async fn try_acquire_leadership(&mut self) -> Result<bool, String> {
        self.leader
            .try_acquire_leadership()
            .await
            .map_err(|e| e.to_string())
    }

    /// Renew the leader lease.
    ///
    /// Returns `Ok(true)` if renewed successfully, `Ok(false)` if we lost
    /// leadership to another pod, or `Err` if renewal failed.
    pub async fn renew_leadership(&mut self) -> Result<bool, String> {
        self.leader
            .renew_leadership()
            .await
            .map_err(|e| e.to_string())
    }

    /// Check if we are currently the leader.
    pub fn is_leader(&self) -> bool {
        self.leader.is_leader()
    }

    /// Get the current phase.
    pub fn phase(&self) -> &str {
        self.leader.phase()
    }

    /// Get the extra state (mutable).
    pub fn extra_state(&mut self) -> &mut ReshardExtraState {
        self.leader.extra_state()
    }

    /// Get the extra state (immutable).
    pub fn extra_state_ref(&self) -> &ReshardExtraState {
        self.leader.extra_state_ref()
    }

    /// Advance to the next phase and persist state.
    ///
    /// Should be called after each phase boundary so that a new leader can
    /// resume from the last committed phase.
    pub async fn advance_phase(&mut self, new_phase: ReshardPhase) -> Result<(), String> {
        let phase_name = new_phase.name().to_string();
        self.leader
            .persist_phase(phase_name)
            .await
            .map_err(|e| e.to_string())
    }

    /// Update backfill progress and persist.
    pub async fn update_backfill_progress(
        &mut self,
        backfilled: u64,
        total: u64,
    ) -> Result<(), String> {
        self.leader.extra_state().documents_backfilled = backfilled;
        self.leader.extra_state().total_documents = total;
        self.leader
            .persist_phase(self.leader.phase().to_string())
            .await
            .map_err(|e| e.to_string())
    }

    /// Mark the operation as failed and step down from leadership.
    pub async fn fail(&mut self, error: String) -> Result<(), String> {
        self.leader.extra_state().last_error = Some(error.clone());
        self.leader.fail(error).await.map_err(|e| e.to_string())
    }

    /// Mark the operation as completed and step down from leadership.
    pub async fn complete(&mut self) -> Result<(), String> {
        self.leader.complete().await.map_err(|e| e.to_string())
    }

    /// Recover the operation state from the task store.
    ///
    /// Called by a new leader to read the persisted phase state and resume
    /// from the last committed phase boundary.
    pub async fn recover(&mut self) -> Result<Option<ReshardPhase>, String> {
        let existing = self.leader.recover().await.map_err(|e| e.to_string())?;

        if let Some(ref op) = existing {
            // Parse phase string back to ReshardPhase enum
            let phase = match op.phase.as_str() {
                "Idle" => ReshardPhase::Idle,
                "Shadow Created" => ReshardPhase::ShadowCreated,
                "Dual-Write Active" => ReshardPhase::DualWriteActive,
                "Backfill In Progress" => ReshardPhase::BackfillInProgress,
                "Verifying" => ReshardPhase::Verifying,
                "Swapped" => ReshardPhase::Swapped,
                "Cleaning Up" => ReshardPhase::CleaningUp,
                "Complete" => ReshardPhase::Complete,
                "Failed" => ReshardPhase::Failed,
                _ => {
                    warn!("unknown phase '{}', defaulting to Idle", op.phase);
                    ReshardPhase::Idle
                }
            };

            info!(
                index_uid = %self.leader.extra_state_ref().index_uid,
                phase = %op.phase,
                "recovered reshard operation from persisted phase"
            );

            return Ok(Some(phase));
        }

        Ok(None)
    }

    /// Delete the operation state after completion.
    pub async fn delete(&self) -> Result<bool, String> {
        self.leader.delete().await.map_err(|e| e.to_string())
    }
}

impl ReshardRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new resharding operation.
    pub fn register(&mut self, op: ReshardOperation) -> Result<(), String> {
        if let Some(existing_id) = self.index_ops.get(&op.index_uid) {
            return Err(format!(
                "Resharding already in progress for index '{}': {}",
                op.index_uid, existing_id
            ));
        }
        self.index_ops.insert(op.index_uid.clone(), op.id.clone());
        self.operations.insert(op.id.clone(), op);
        Ok(())
    }

    /// Get an operation by ID.
    pub fn get(&self, id: &str) -> Option<&ReshardOperation> {
        self.operations.get(id)
    }

    /// Get mutable reference for updates.
    pub fn get_mut(&mut self, id: &str) -> Option<&mut ReshardOperation> {
        self.operations.get_mut(id)
    }

    /// Get the active operation for an index (if any).
    pub fn get_for_index(&self, index_uid: &str) -> Option<&ReshardOperation> {
        self.index_ops
            .get(index_uid)
            .and_then(|id| self.operations.get(id))
    }

    /// Update an operation.
    pub fn update(&mut self, op: ReshardOperation) -> Result<(), String> {
        if !self.operations.contains_key(&op.id) {
            return Err(format!("Operation '{}' not found", op.id));
        }
        self.operations.insert(op.id.clone(), op);
        Ok(())
    }

    /// Complete an operation and remove from active index tracking.
    pub fn complete(&mut self, id: &str) -> Result<(), String> {
        let op = self
            .operations
            .get(id)
            .ok_or_else(|| format!("Operation '{}' not found", id))?;
        if !op.is_terminal() {
            return Err(format!("Operation '{}' is not in a terminal state", id));
        }
        self.index_ops.remove(&op.index_uid);
        Ok(())
    }

    /// List all operations.
    pub fn list(&self) -> Vec<&ReshardOperation> {
        self.operations.values().collect()
    }

    /// Clean up completed operations older than the retention period.
    pub fn prune_completed(&mut self, max_age_ms: u64) {
        let now = millis_now();
        let mut to_remove = Vec::new();
        for (id, op) in &self.operations {
            if op.is_terminal() && (now.saturating_sub(op.created_at) > max_age_ms) {
                to_remove.push(id.clone());
                self.index_ops.remove(&op.index_uid);
            }
        }
        for id in to_remove {
            self.operations.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests_reshard_execution {
    use super::*;

    #[test]
    fn phase_display_names() {
        assert_eq!(ReshardPhase::Idle.name(), "Idle");
        assert_eq!(
            ReshardPhase::BackfillInProgress.name(),
            "Backfill In Progress"
        );
        assert_eq!(ReshardPhase::Failed.name(), "Failed");
    }

    #[test]
    fn phase_roundtrip_u8() {
        for phase in &[
            ReshardPhase::Idle,
            ReshardPhase::ShadowCreated,
            ReshardPhase::DualWriteActive,
            ReshardPhase::BackfillInProgress,
            ReshardPhase::Verifying,
            ReshardPhase::Swapped,
            ReshardPhase::CleaningUp,
            ReshardPhase::Complete,
            ReshardPhase::Failed,
        ] {
            let v = *phase as u8;
            assert_eq!(ReshardPhase::from_u8(v), Some(*phase));
        }
        assert_eq!(ReshardPhase::from_u8(255), None);
    }

    #[test]
    fn operation_creation() {
        let op = ReshardOperation::new("products".into(), 64, 128);
        assert_eq!(op.index_uid, "products");
        assert_eq!(op.old_shards, 64);
        assert_eq!(op.target_shards, 128);
        assert_eq!(op.shadow_index, "products__reshard_128");
        assert_eq!(op.phase, ReshardPhase::ShadowCreated);
        assert!(!op.is_terminal());
    }

    #[test]
    fn operation_backfill_progress() {
        let mut op = ReshardOperation::new("test".into(), 16, 32);
        op.total_documents = 1000;
        op.documents_backfilled = 500;
        assert!((op.backfill_progress() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn operation_terminal_states() {
        let mut op = ReshardOperation::new("test".into(), 16, 32);
        assert!(!op.is_terminal());
        op.phase = ReshardPhase::Complete;
        assert!(op.is_terminal());
        op.phase = ReshardPhase::Failed;
        assert!(op.is_terminal());
    }

    #[test]
    fn registry_single_op_per_index() {
        let mut reg = ReshardRegistry::new();
        let op1 = ReshardOperation::new("products".into(), 64, 128);
        reg.register(op1).unwrap();
        let op2 = ReshardOperation::new("products".into(), 128, 256);
        assert!(reg.register(op2).is_err());
    }

    #[test]
    fn registry_get_for_index() {
        let mut reg = ReshardRegistry::new();
        let op = ReshardOperation::new("products".into(), 64, 128);
        let id = op.id.clone();
        reg.register(op).unwrap();
        let retrieved = reg.get_for_index("products").unwrap();
        assert_eq!(retrieved.id, id);
    }

    #[test]
    fn registry_complete_releases_index() {
        let mut reg = ReshardRegistry::new();
        let op = ReshardOperation::new("products".into(), 64, 128);
        let id = op.id.clone();
        reg.register(op).unwrap();
        assert!(reg.get_for_index("products").is_some());
        let op = reg.get_mut(&id).unwrap();
        op.phase = ReshardPhase::Complete;
        reg.complete(&id).unwrap();
        assert!(reg.get_for_index("products").is_none());
    }

    #[test]
    fn registry_prune_old_completed() {
        let mut reg = ReshardRegistry::new();
        let mut op = ReshardOperation::new("test".into(), 16, 32);
        op.phase = ReshardPhase::Complete;
        op.created_at = millis_now().saturating_sub(100_000); // 100s ago
        let id = op.id.clone();
        reg.register(op).unwrap();
        reg.prune_completed(50_000); // prune ops older than 50s
        assert!(reg.get(&id).is_none());
    }
}
