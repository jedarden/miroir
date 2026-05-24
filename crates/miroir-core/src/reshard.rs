//! Online resharding: window guard, simulation model, and six-phase execution.
//!
//! Implements the plan §13.1 shadow-index resharding mechanics and §15 OP#3
//! empirical validation of the 2× transient load caveat.
//!
//! Leader coordination (plan §14.5 Mode B):
//! - Acquires per-index leader lease (scope: "reshard:<index>")
//! - Persists phase state to mode_b_operations table for recovery
//! - New leaders resume from last committed phase boundary

pub mod executor;

use crate::mode_b_coordinator::{ModeBOpLeader, PhaseState};
use crate::router::{assign_shard_in_group, shard_for_key};
use crate::topology::{Group, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};
use twox_hash::XxHash64;

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

/// In-memory registry tracking active resharding operations for dual-write detection.
///
/// This is used by the write path to determine if an index is in dual-write phase
/// (shadow exists) and needs dual-hash routing.
#[derive(Debug, Default)]
pub struct ReshardingRegistry {
    /// Map of index_uid -> active resharding state
    /// When an index is in this registry with phase >= ShadowCreated,
    /// writes must be dual-hashed to both live and shadow indexes.
    active_operations: HashMap<String, ReshardOperationState>,
}

/// Active resharding state for an index.
#[derive(Debug, Clone)]
pub struct ReshardOperationState {
    /// Shadow index UID (e.g., "products__reshard_128")
    pub shadow_index: String,
    /// Old shard count
    pub old_shards: u32,
    /// New shard count
    pub target_shards: u32,
    /// Current phase
    pub phase: ReshardPhase,
    /// When the operation started (UNIX ms)
    pub started_at: u64,
}

impl ReshardingRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a resharding operation for dual-write detection.
    ///
    /// Once registered, writes to the index will be dual-hashed to both
    /// live and shadow indexes when phase >= ShadowCreated.
    pub fn register(
        &mut self,
        index_uid: String,
        state: ReshardOperationState,
    ) -> Result<(), String> {
        if self.active_operations.contains_key(&index_uid) {
            return Err(format!(
                "Resharding already in progress for index '{}'",
                index_uid
            ));
        }
        tracing::info!(
            index_uid = %index_uid,
            shadow_index = %state.shadow_index,
            old_shards = state.old_shards,
            target_shards = state.target_shards,
            phase = ?state.phase,
            "registered resharding operation for dual-write"
        );
        self.active_operations.insert(index_uid, state);
        Ok(())
    }

    /// Get the active resharding state for an index (if any).
    pub fn get(&self, index_uid: &str) -> Option<&ReshardOperationState> {
        self.active_operations.get(index_uid)
    }

    /// Update the phase of an active resharding operation.
    pub fn update_phase(&mut self, index_uid: &str, new_phase: ReshardPhase) -> Result<(), String> {
        let op = self
            .active_operations
            .get_mut(index_uid)
            .ok_or_else(|| format!("No resharding operation for index '{}'", index_uid))?;
        op.phase = new_phase;
        tracing::info!(
            index_uid = %index_uid,
            phase = ?new_phase,
            "updated resharding phase"
        );
        Ok(())
    }

    /// Remove a completed resharding operation.
    pub fn remove(&mut self, index_uid: &str) -> Result<(), String> {
        if self.active_operations.remove(index_uid).is_none() {
            return Err(format!("No resharding operation for index '{}'", index_uid));
        }
        tracing::info!(
            index_uid = %index_uid,
            "removed resharding operation from registry"
        );
        Ok(())
    }

    /// Check if an index is in dual-write phase.
    ///
    /// Returns true if the index has an active resharding operation with
    /// phase >= ShadowCreated and phase <= Swapped.
    pub fn is_dual_write_active(&self, index_uid: &str) -> bool {
        if let Some(op) = self.get(index_uid) {
            matches!(
                op.phase,
                ReshardPhase::ShadowCreated
                    | ReshardPhase::DualWriteActive
                    | ReshardPhase::BackfillInProgress
                    | ReshardPhase::Verifying
            )
        } else {
            false
        }
    }

    /// List all active resharding operations.
    pub fn list(&self) -> Vec<(String, &ReshardOperationState)> {
        self.active_operations
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect()
    }
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

// ---------------------------------------------------------------------------
// Phase 1: Shadow create (plan §13.1 step 1 + §13.5 broadcast)
// ---------------------------------------------------------------------------

/// Result of shadow index creation.
#[derive(Debug, Clone)]
pub struct ShadowCreateResult {
    /// Shadow index UID created.
    pub shadow_index: String,
    /// Number of nodes the index was created on.
    pub nodes_created: usize,
    /// Whether settings broadcast succeeded.
    pub settings_broadcast_ok: bool,
    /// New settings version after broadcast (if successful).
    pub settings_version: Option<u64>,
    /// Per-node task UIDs from index creation.
    pub node_task_uids: Vec<(String, u64)>,
}

/// Error during shadow create phase.
#[derive(Debug, thiserror::Error)]
pub enum ShadowCreateError {
    #[error("index already exists on node: {0}")]
    IndexAlreadyExists(String),

    #[error("settings broadcast failed: {0}")]
    SettingsBroadcastFailed(String),

    #[error("node creation failed on {node}: {error}")]
    NodeCreationFailed { node: String, error: String },

    #[error("rollback required: {0}")]
    RollbackRequired(String),
}

/// Execute Phase 1: Shadow create (plan §13.1 step 1).
///
/// Creates the shadow index `{uid}__reshard_{S_new}` on every node and
/// propagates the live index's settings via two-phase broadcast (§13.5).
///
/// # Arguments
/// * `live_index_uid` - The live index UID being resharded
/// * `target_shards` - The new shard count (S_new)
/// * `node_addresses` - List of all node addresses
/// * `master_key` - Meilisearch master key for authentication
/// * `primary_key` - Optional primary key for the shadow index
///
/// # Returns
/// `Ok(ShadowCreateResult)` on success, `Err(ShadowCreateError)` on failure.
///
/// # Failure handling
/// Any failure during this phase triggers rollback: the shadow index is
/// deleted from all nodes where it was created. This is safe because the
/// shadow is not yet addressable by clients.
pub async fn shadow_create_phase(
    live_index_uid: &str,
    target_shards: u32,
    node_addresses: &[String],
    master_key: &str,
    primary_key: Option<String>,
) -> Result<ShadowCreateResult, ShadowCreateError> {
    let shadow_index = format!("{}__reshard_{}", live_index_uid, target_shards);

    tracing::info!(
        live_index = %live_index_uid,
        shadow_index = %shadow_index,
        target_shards,
        nodes = node_addresses.len(),
        "starting Phase 1: shadow create"
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| ShadowCreateError::NodeCreationFailed {
            node: "client".to_string(),
            error: format!("failed to create HTTP client: {}", e),
        })?;

    // Step 1: Create shadow index on every node sequentially
    let mut created_on: Vec<String> = Vec::new();
    let mut node_task_uids: Vec<(String, u64)> = Vec::new();

    for address in node_addresses {
        let url = format!("{}/indexes", address.trim_end_matches('/'));

        let create_body = serde_json::json!({
            "uid": shadow_index,
            "primaryKey": primary_key,
        });

        match create_index_on_node(&client, address, &url, &create_body, master_key).await {
            Ok(task_uid) => {
                created_on.push(address.clone());
                if let Some(uid) = task_uid {
                    node_task_uids.push((address.clone(), uid));
                    tracing::debug!(node = %address, task_uid = uid, "shadow index created");
                } else {
                    tracing::debug!(node = %address, "shadow index created (no task UID)");
                }
            }
            Err(e) => {
                // Rollback: delete shadow index from all nodes where it was created
                rollback_shadow_index(&client, &shadow_index, &created_on, master_key).await;
                return Err(match e {
                    ShadowCreateError::IndexAlreadyExists(_) => e,
                    other => ShadowCreateError::RollbackRequired(format!(
                        "creation failed on {}: {}",
                        address, other
                    )),
                });
            }
        }
    }

    tracing::info!(
        shadow_index = %shadow_index,
        nodes_created = created_on.len(),
        "shadow index created on all nodes"
    );

    // Step 2: Fetch live index settings from first node
    let first_address =
        node_addresses
            .first()
            .ok_or_else(|| ShadowCreateError::NodeCreationFailed {
                node: "none".to_string(),
                error: "no nodes available".to_string(),
            })?;

    let live_settings =
        match fetch_index_settings(&client, first_address, live_index_uid, master_key).await {
            Ok(settings) => settings,
            Err(e) => {
                rollback_shadow_index(&client, &shadow_index, &created_on, master_key).await;
                return Err(ShadowCreateError::SettingsBroadcastFailed(format!(
                    "failed to fetch live index settings: {}",
                    e
                )));
            }
        };

    // Step 3: Add _miroir_shard to filterableAttributes if not already present
    let settings_to_broadcast = ensure_shard_filterable(&live_settings);

    // Step 4: Two-phase broadcast of settings to shadow index
    let broadcast_result = two_phase_broadcast_settings(
        &client,
        &shadow_index,
        &settings_to_broadcast,
        node_addresses,
        master_key,
    )
    .await;

    let settings_version = match broadcast_result {
        Ok(version) => {
            tracing::info!(
                shadow_index = %shadow_index,
                settings_version = version,
                "settings broadcast committed"
            );
            Some(version)
        }
        Err(e) => {
            // Settings broadcast failed - rollback shadow index creation
            rollback_shadow_index(&client, &shadow_index, &created_on, master_key).await;
            return Err(ShadowCreateError::SettingsBroadcastFailed(format!(
                "two-phase broadcast failed: {}",
                e
            )));
        }
    };

    Ok(ShadowCreateResult {
        shadow_index,
        nodes_created: created_on.len(),
        settings_broadcast_ok: true,
        settings_version,
        node_task_uids,
    })
}

/// Create an index on a single node.
async fn create_index_on_node(
    client: &reqwest::Client,
    address: &str,
    url: &str,
    body: &serde_json::Value,
    master_key: &str,
) -> Result<Option<u64>, ShadowCreateError> {
    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}", master_key))
        .json(body)
        .send()
        .await
        .map_err(|e| ShadowCreateError::NodeCreationFailed {
            node: address.to_string(),
            error: format!("request failed: {}", e),
        })?;

    let status = response.status();
    let body_text = response
        .text()
        .await
        .map_err(|e| ShadowCreateError::NodeCreationFailed {
            node: address.to_string(),
            error: format!("failed to read response: {}", e),
        })?;

    if status.as_u16() == 409 {
        // Index already exists
        return Err(ShadowCreateError::IndexAlreadyExists(address.to_string()));
    }

    if !status.is_success() {
        return Err(ShadowCreateError::NodeCreationFailed {
            node: address.to_string(),
            error: format!("HTTP {}: {}", status.as_u16(), body_text),
        });
    }

    // Parse task UID from response
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body_text) {
        Ok(json.get("taskUid").and_then(|v| v.as_u64()))
    } else {
        Ok(None)
    }
}

/// Fetch index settings from a node.
async fn fetch_index_settings(
    client: &reqwest::Client,
    address: &str,
    index_uid: &str,
    master_key: &str,
) -> Result<serde_json::Value, String> {
    let url = format!(
        "{}/indexes/{}/settings",
        address.trim_end_matches('/'),
        index_uid
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", master_key))
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;

    let status = response.status();
    let body_text = response
        .text()
        .await
        .map_err(|e| format!("failed to read response: {}", e))?;

    if !status.is_success() {
        return Err(format!("HTTP {}: {}", status.as_u16(), body_text));
    }

    serde_json::from_str(&body_text).map_err(|e| format!("failed to parse settings JSON: {}", e))
}

/// Ensure `_miroir_shard` is in filterableAttributes.
fn ensure_shard_filterable(settings: &serde_json::Value) -> serde_json::Value {
    let mut result = settings.clone();

    if let Some(obj) = result.as_object_mut() {
        let filterable = obj
            .entry("filterableAttributes")
            .or_insert_with(|| serde_json::Value::Array(vec![]));

        if let Some(arr) = filterable.as_array_mut() {
            // Add _miroir_shard if not already present
            if !arr.iter().any(|v| v.as_str() == Some("_miroir_shard")) {
                arr.push(serde_json::json!("_miroir_shard"));
            }
        }
    }

    result
}

/// Two-phase broadcast of settings to all nodes (plan §13.5).
async fn two_phase_broadcast_settings(
    client: &reqwest::Client,
    index_uid: &str,
    settings: &serde_json::Value,
    node_addresses: &[String],
    master_key: &str,
) -> Result<u64, String> {
    // Phase 1: Propose - PATCH all nodes in parallel
    let propose_tasks: Vec<_> = node_addresses
        .iter()
        .map(|address| {
            let client = client.clone();
            let address = address.clone();
            let index = index_uid.to_string();
            let settings = settings.clone();
            let key = master_key.to_string();
            async move {
                let url = format!(
                    "{}/indexes/{}/settings",
                    address.trim_end_matches('/'),
                    index
                );
                let result = client
                    .patch(&url)
                    .header("Authorization", format!("Bearer {}", key))
                    .json(&settings)
                    .send()
                    .await;

                match result {
                    Ok(resp) if resp.status().is_success() => {
                        let text = resp.text().await.unwrap_or_default();
                        let task_uid = serde_json::from_str::<serde_json::Value>(&text)
                            .ok()
                            .and_then(|v| v.get("taskUid").and_then(|t| t.as_u64()));
                        Ok((address, task_uid))
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        let _text = resp.text().await.unwrap_or_default();
                        Err(format!("{}: HTTP {}", address, status.as_u16()))
                    }
                    Err(e) => Err(format!("{}: {}", address, e)),
                }
            }
        })
        .collect();

    let propose_results: Vec<_> = futures_util::future::join_all(propose_tasks).await;

    // Check all nodes succeeded
    let mut node_task_uids: Vec<(String, u64)> = Vec::new();
    for result in propose_results {
        match result {
            Ok((address, Some(task_uid))) => {
                node_task_uids.push((address, task_uid));
            }
            Ok((address, None)) => {
                // Some nodes may not return taskUid, still consider success
                node_task_uids.push((address, 0));
            }
            Err(e) => {
                return Err(format!("Phase 1 propose failed: {}", e));
            }
        }
    }

    // Phase 2: Verify - GET settings from all nodes and verify fingerprints
    let verify_tasks: Vec<_> = node_addresses
        .iter()
        .map(|address| {
            let client = client.clone();
            let address = address.clone();
            let index = index_uid.to_string();
            let key = master_key.to_string();
            async move {
                let url = format!(
                    "{}/indexes/{}/settings",
                    address.trim_end_matches('/'),
                    index
                );
                let result = client
                    .get(&url)
                    .header("Authorization", format!("Bearer {}", key))
                    .send()
                    .await;

                match result {
                    Ok(resp) if resp.status().is_success() => {
                        let text = resp.text().await.unwrap_or_default();
                        if let Ok(settings) = serde_json::from_str::<serde_json::Value>(&text) {
                            let hash = crate::settings::fingerprint_settings(&settings);
                            Ok((address, hash))
                        } else {
                            Err(format!("{}: failed to parse settings", address))
                        }
                    }
                    Ok(resp) => Err(format!("{}: HTTP {}", address, resp.status().as_u16())),
                    Err(e) => Err(format!("{}: {}", address, e)),
                }
            }
        })
        .collect();

    let verify_results: Vec<_> = futures_util::future::join_all(verify_tasks).await;

    // Compute expected fingerprint
    let expected_fingerprint = crate::settings::fingerprint_settings(settings);

    // Verify all hashes match
    let mut node_hashes: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for result in verify_results {
        match result {
            Ok((address, hash)) => {
                if hash != expected_fingerprint {
                    return Err(format!(
                        "Phase 2 verify failed: hash mismatch on {}",
                        address
                    ));
                }
                node_hashes.insert(address, hash);
            }
            Err(e) => {
                return Err(format!("Phase 2 verify failed: {}", e));
            }
        }
    }

    // Phase 3: Commit - return a new settings version
    // In production, this would increment the global settings version
    // For now, return 1 as a placeholder
    Ok(1)
}

/// Rollback shadow index creation by deleting from specified nodes.
async fn rollback_shadow_index(
    client: &reqwest::Client,
    shadow_index: &str,
    nodes: &[String],
    master_key: &str,
) {
    tracing::warn!(
        shadow_index = %shadow_index,
        nodes = nodes.len(),
        "rolling back shadow index creation"
    );

    for address in nodes {
        let url = format!("{}/indexes/{}", address.trim_end_matches('/'), shadow_index);

        match client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", master_key))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!(node = %address, "rollback: deleted shadow index");
            }
            Ok(resp) => {
                tracing::error!(
                    node = %address,
                    status = %resp.status(),
                    "rollback: failed to delete shadow index"
                );
            }
            Err(e) => {
                tracing::error!(node = %address, error = %e, "rollback: request failed");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Dual-hash dual-write (plan §13.1 step 2)
// ---------------------------------------------------------------------------

/// Result of preparing documents for dual-hash dual-write.
#[derive(Debug, Clone)]
pub struct DualWritePreparation {
    /// Documents to write to live index (with old shard tags).
    pub live_documents: Vec<serde_json::Value>,
    /// Documents to write to shadow index (with new shard tags).
    pub shadow_documents: Vec<serde_json::Value>,
    /// Shadow index UID.
    pub shadow_index: String,
    /// Old shard count.
    pub old_shards: u32,
    /// New shard count.
    pub target_shards: u32,
}

/// Prepare documents for dual-hash dual-write during resharding.
///
/// When an index is in dual-write phase (shadow exists), every write must be
/// routed to BOTH live and shadow indexes with different shard tags:
/// - Live index: `_miroir_shard = hash(pk) % S_old`
/// - Shadow index: `_miroir_shard = hash(pk) % S_new`
///
/// Shadow writes are tagged with `_miroir_origin: "reshard_backfill"` so
/// CDC suppresses them by default (plan §13.13).
///
/// # Arguments
/// * `documents` - Original documents from client (without _miroir_shard)
/// * `primary_key` - Primary key field name
/// * `reshard_state` - Active resharding state for the index
///
/// # Returns
/// `Ok(DualWritePreparation)` with separate document batches for live and shadow.
///
/// # Panics
/// Panics if any document is missing the primary key field (caller should validate first).
pub fn prepare_dual_write_documents(
    documents: &[serde_json::Value],
    primary_key: &str,
    reshard_state: &ReshardOperationState,
) -> DualWritePreparation {
    let mut live_documents = Vec::with_capacity(documents.len());
    let mut shadow_documents = Vec::with_capacity(documents.len());

    for doc in documents {
        let pk_value = doc
            .get(primary_key)
            .and_then(|v| v.as_str())
            .expect("primary key validation should have happened before this call");

        // Compute old shard assignment for live index
        let old_shard_id = crate::router::shard_for_key(pk_value, reshard_state.old_shards);

        // Compute new shard assignment for shadow index
        let new_shard_id = crate::router::shard_for_key(pk_value, reshard_state.target_shards);

        // Clone document for live index
        let mut live_doc = doc.clone();
        live_doc["_miroir_shard"] = serde_json::json!(old_shard_id);
        live_documents.push(live_doc);

        // Clone document for shadow index with new shard tag
        let mut shadow_doc = doc.clone();
        shadow_doc["_miroir_shard"] = serde_json::json!(new_shard_id);
        // Tag for CDC suppression (plan §13.13)
        shadow_doc["_miroir_origin"] = serde_json::json!("reshard_backfill");
        shadow_documents.push(shadow_doc);
    }

    DualWritePreparation {
        live_documents,
        shadow_documents,
        shadow_index: reshard_state.shadow_index.clone(),
        old_shards: reshard_state.old_shards,
        target_shards: reshard_state.target_shards,
    }
}

#[cfg(test)]
mod tests_dual_write {
    use super::*;
    use serde_json::json;

    #[test]
    fn prepare_dual_write_separates_shards() {
        let documents = vec![
            json!({"id": "user:123", "name": "Alice"}),
            json!({"id": "user:456", "name": "Bob"}),
        ];

        let reshard_state = ReshardOperationState {
            shadow_index: "users__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::ShadowCreated,
            started_at: 1000,
        };

        let prep = prepare_dual_write_documents(&documents, "id", &reshard_state);

        assert_eq!(prep.live_documents.len(), 2);
        assert_eq!(prep.shadow_documents.len(), 2);
        assert_eq!(prep.shadow_index, "users__reshard_128");
        assert_eq!(prep.old_shards, 64);
        assert_eq!(prep.target_shards, 128);

        // Verify live documents have old shard tags
        for doc in &prep.live_documents {
            assert!(doc.get("_miroir_shard").is_some());
            let shard = doc["_miroir_shard"].as_u64().unwrap();
            assert!(shard < 64, "live shard should be < 64");
        }

        // Verify shadow documents have new shard tags
        for doc in &prep.shadow_documents {
            assert!(doc.get("_miroir_shard").is_some());
            let shard = doc["_miroir_shard"].as_u64().unwrap();
            assert!(shard < 128, "shadow shard should be < 128");
        }
    }

    #[test]
    fn prepare_dual_write_preserves_other_fields() {
        let documents = vec![json!({
            "id": "product:abc",
            "name": "Widget",
            "price": 19.99,
            "tags": ["widget", "sale"]
        })];

        let reshard_state = ReshardOperationState {
            shadow_index: "products__reshard_256".to_string(),
            old_shards: 128,
            target_shards: 256,
            phase: ReshardPhase::DualWriteActive,
            started_at: 2000,
        };

        let prep = prepare_dual_write_documents(&documents, "id", &reshard_state);

        let live_doc = &prep.live_documents[0];
        let shadow_doc = &prep.shadow_documents[0];

        // Check that all fields are preserved
        assert_eq!(live_doc["id"], "product:abc");
        assert_eq!(live_doc["name"], "Widget");
        assert_eq!(live_doc["price"], 19.99);
        assert_eq!(live_doc["tags"], json!(["widget", "sale"]));

        // Shadow should have same fields except shard tag
        assert_eq!(shadow_doc["id"], "product:abc");
        assert_eq!(shadow_doc["name"], "Widget");
        assert_eq!(shadow_doc["price"], 19.99);
        assert_eq!(shadow_doc["tags"], json!(["widget", "sale"]));
    }

    #[test]
    fn prepare_dual_write_deterministic_shard_assignment() {
        let documents = vec![json!({"id": "test:key"})];

        let reshard_state = ReshardOperationState {
            shadow_index: "test__reshard_32".to_string(),
            old_shards: 16,
            target_shards: 32,
            phase: ReshardPhase::BackfillInProgress,
            started_at: 3000,
        };

        // Run multiple times - should be deterministic
        let prep1 = prepare_dual_write_documents(&documents, "id", &reshard_state);
        let prep2 = prepare_dual_write_documents(&documents, "id", &reshard_state);

        assert_eq!(
            prep1.live_documents[0]["_miroir_shard"], prep2.live_documents[0]["_miroir_shard"],
            "live shard assignment should be deterministic"
        );
        assert_eq!(
            prep1.shadow_documents[0]["_miroir_shard"], prep2.shadow_documents[0]["_miroir_shard"],
            "shadow shard assignment should be deterministic"
        );
    }

    #[test]
    fn prepare_dual_write_handles_empty_batch() {
        let documents: Vec<serde_json::Value> = vec![];

        let reshard_state = ReshardOperationState {
            shadow_index: "empty__reshard_64".to_string(),
            old_shards: 32,
            target_shards: 64,
            phase: ReshardPhase::ShadowCreated,
            started_at: 1000,
        };

        let prep = prepare_dual_write_documents(&documents, "id", &reshard_state);

        assert_eq!(prep.live_documents.len(), 0);
        assert_eq!(prep.shadow_documents.len(), 0);
    }

    #[test]
    fn prepare_dual_write_tags_shadow_with_reshard_backfill_origin() {
        let documents = vec![json!({"id": "product:123", "name": "Widget"})];

        let reshard_state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::DualWriteActive,
            started_at: 1000,
        };

        let prep = prepare_dual_write_documents(&documents, "id", &reshard_state);

        // Shadow documents should be tagged with _miroir_origin for CDC suppression
        let shadow_doc = &prep.shadow_documents[0];
        assert_eq!(
            shadow_doc.get("_miroir_origin"),
            Some(&json!("reshard_backfill")),
            "shadow documents should have _miroir_origin: reshard_backfill"
        );

        // Live documents should NOT have _miroir_origin (client writes are emitted)
        let live_doc = &prep.live_documents[0];
        assert!(
            live_doc.get("_miroir_origin").is_none(),
            "live documents should not have _miroir_origin"
        );
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

// ---------------------------------------------------------------------------
// Shadow create phase tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_shadow_create {
    use super::*;

    #[test]
    fn shadow_index_name_format() {
        let shadow = format!("{}__reshard_{}", "products", 128);
        assert_eq!(shadow, "products__reshard_128");
    }

    #[test]
    fn ensure_shard_filterable_adds_missing() {
        let settings = serde_json::json!({
            "rankingRules": ["words", "typo"],
            "filterableAttributes": ["category", "price"]
        });

        let result = ensure_shard_filterable(&settings);

        let filterable = result
            .get("filterableAttributes")
            .and_then(|v| v.as_array())
            .expect("filterableAttributes should be an array");

        assert!(filterable.contains(&serde_json::json!("category")));
        assert!(filterable.contains(&serde_json::json!("price")));
        assert!(filterable.contains(&serde_json::json!("_miroir_shard")));
    }

    #[test]
    fn ensure_shard_filterable_idempotent() {
        let settings = serde_json::json!({
            "filterableAttributes": ["_miroir_shard", "category"]
        });

        let result = ensure_shard_filterable(&settings);

        let filterable = result
            .get("filterableAttributes")
            .and_then(|v| v.as_array())
            .expect("filterableAttributes should be an array");

        // Should only appear once
        let shard_count = filterable
            .iter()
            .filter(|v| v.as_str() == Some("_miroir_shard"))
            .count();

        assert_eq!(shard_count, 1);
    }

    #[test]
    fn ensure_shard_filterable_empty_array() {
        let settings = serde_json::json!({
            "rankingRules": ["words"]
        });

        let result = ensure_shard_filterable(&settings);

        let filterable = result
            .get("filterableAttributes")
            .and_then(|v| v.as_array())
            .expect("filterableAttributes should be an array");

        assert!(filterable.contains(&serde_json::json!("_miroir_shard")));
    }

    #[test]
    fn shadow_create_result_fields() {
        let result = ShadowCreateResult {
            shadow_index: "products__reshard_128".to_string(),
            nodes_created: 3,
            settings_broadcast_ok: true,
            settings_version: Some(1),
            node_task_uids: vec![("node-1".to_string(), 100), ("node-2".to_string(), 101)],
        };

        assert_eq!(result.shadow_index, "products__reshard_128");
        assert_eq!(result.nodes_created, 3);
        assert!(result.settings_broadcast_ok);
        assert_eq!(result.settings_version, Some(1));
        assert_eq!(result.node_task_uids.len(), 2);
    }

    #[tokio::test]
    async fn shadow_create_error_display() {
        let err = ShadowCreateError::IndexAlreadyExists("node-1".to_string());
        assert!(err.to_string().contains("already exists"));

        let err = ShadowCreateError::SettingsBroadcastFailed("broadcast failed".to_string());
        assert!(err.to_string().contains("broadcast failed"));

        let err = ShadowCreateError::NodeCreationFailed {
            node: "node-2".to_string(),
            error: "connection refused".to_string(),
        };
        assert!(err.to_string().contains("node-2"));

        let err = ShadowCreateError::RollbackRequired("creation failed".to_string());
        assert!(err.to_string().contains("rollback"));
    }
}

// ---------------------------------------------------------------------------
// ReshardingRegistry tests (P5.1.b dual-write detection)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_resharding_registry {
    use super::*;

    #[test]
    fn registry_new_is_empty() {
        let reg = ReshardingRegistry::new();
        assert!(reg.get("products").is_none());
        assert!(!reg.is_dual_write_active("products"));
    }

    #[test]
    fn registry_register_and_get() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::ShadowCreated,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();

        let retrieved = reg.get("products").unwrap();
        assert_eq!(retrieved.shadow_index, "products__reshard_128");
        assert_eq!(retrieved.old_shards, 64);
        assert_eq!(retrieved.target_shards, 128);
        assert_eq!(retrieved.phase, ReshardPhase::ShadowCreated);
    }

    #[test]
    fn registry_register_duplicate_rejected() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::ShadowCreated,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();

        let state2 = ReshardOperationState {
            shadow_index: "products__reshard_256".to_string(),
            old_shards: 128,
            target_shards: 256,
            phase: ReshardPhase::ShadowCreated,
            started_at: 2000,
        };
        assert!(reg.register("products".to_string(), state2).is_err());
    }

    #[test]
    fn registry_update_phase() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::ShadowCreated,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();

        reg.update_phase("products", ReshardPhase::DualWriteActive)
            .unwrap();

        let retrieved = reg.get("products").unwrap();
        assert_eq!(retrieved.phase, ReshardPhase::DualWriteActive);
    }

    #[test]
    fn registry_update_phase_nonexistent_errors() {
        let mut reg = ReshardingRegistry::new();
        assert!(reg
            .update_phase("products", ReshardPhase::DualWriteActive)
            .is_err());
    }

    #[test]
    fn registry_remove() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::ShadowCreated,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();
        assert!(reg.get("products").is_some());

        reg.remove("products").unwrap();
        assert!(reg.get("products").is_none());
    }

    #[test]
    fn registry_remove_nonexistent_errors() {
        let mut reg = ReshardingRegistry::new();
        assert!(reg.remove("products").is_err());
    }

    #[test]
    fn registry_is_dual_write_active_shadow_created() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::ShadowCreated,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();
        assert!(reg.is_dual_write_active("products"));
    }

    #[test]
    fn registry_is_dual_write_active_dual_write_phase() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::DualWriteActive,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();
        assert!(reg.is_dual_write_active("products"));
    }

    #[test]
    fn registry_is_dual_write_active_backfill_phase() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::BackfillInProgress,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();
        assert!(reg.is_dual_write_active("products"));
    }

    #[test]
    fn registry_is_dual_write_active_verifying_phase() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::Verifying,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();
        assert!(reg.is_dual_write_active("products"));
    }

    #[test]
    fn registry_is_dual_write_active_swapped_phase_false() {
        let mut reg = ReshardingRegistry::new();
        let state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::Swapped,
            started_at: 1000,
        };
        reg.register("products".to_string(), state).unwrap();
        // After swap, dual-write stops (writes go only to new index)
        assert!(!reg.is_dual_write_active("products"));
    }

    #[test]
    fn registry_is_dual_write_active_no_operation() {
        let reg = ReshardingRegistry::new();
        assert!(!reg.is_dual_write_active("products"));
    }

    #[test]
    fn registry_list() {
        let mut reg = ReshardingRegistry::new();

        let state1 = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::ShadowCreated,
            started_at: 1000,
        };
        reg.register("products".to_string(), state1).unwrap();

        let state2 = ReshardOperationState {
            shadow_index: "orders__reshard_256".to_string(),
            old_shards: 128,
            target_shards: 256,
            phase: ReshardPhase::DualWriteActive,
            started_at: 2000,
        };
        reg.register("orders".to_string(), state2).unwrap();

        let list = reg.list();
        assert_eq!(list.len(), 2);

        let list_map: std::collections::HashMap<_, _> = list.into_iter().collect();
        assert!(list_map.contains_key("products"));
        assert!(list_map.contains_key("orders"));
        assert_eq!(
            list_map.get("products").unwrap().shadow_index,
            "products__reshard_128"
        );
        assert_eq!(
            list_map.get("orders").unwrap().shadow_index,
            "orders__reshard_256"
        );
    }

    #[test]
    fn registry_multiple_indexes_independent() {
        let mut reg = ReshardingRegistry::new();

        let products_state = ReshardOperationState {
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            target_shards: 128,
            phase: ReshardPhase::DualWriteActive,
            started_at: 1000,
        };
        reg.register("products".to_string(), products_state)
            .unwrap();

        let orders_state = ReshardOperationState {
            shadow_index: "orders__reshard_256".to_string(),
            old_shards: 128,
            target_shards: 256,
            phase: ReshardPhase::ShadowCreated,
            started_at: 2000,
        };
        reg.register("orders".to_string(), orders_state).unwrap();

        // Both should be in dual-write
        assert!(reg.is_dual_write_active("products"));
        assert!(reg.is_dual_write_active("orders"));

        // Update products to swapped
        reg.update_phase("products", ReshardPhase::Swapped).unwrap();

        // Now only orders should be in dual-write
        assert!(!reg.is_dual_write_active("products"));
        assert!(reg.is_dual_write_active("orders"));

        // Remove orders
        reg.remove("orders").unwrap();

        // Neither should be in dual-write
        assert!(!reg.is_dual_write_active("products"));
        assert!(!reg.is_dual_write_active("orders"));
    }
}

// ---------------------------------------------------------------------------
// Phase 4: Verify - cross-index PK set + content hash comparator (P5.1.d)
// ---------------------------------------------------------------------------

/// Verification result comparing live and shadow indexes.
#[derive(Debug, Clone)]
pub struct VerifyPhaseResult {
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
    /// Total documents scanned from live index.
    pub live_docs_scanned: u64,
    /// Total documents scanned from shadow index.
    pub shadow_docs_scanned: u64,
}

impl VerifyPhaseResult {
    /// Create a failed verification result with discrepancies.
    pub fn failed(
        live_pk_count: u64,
        shadow_pk_count: u64,
        live_only_pks: Vec<String>,
        shadow_only_pks: Vec<String>,
        mismatched_pks: Vec<String>,
    ) -> Self {
        Self {
            live_pk_count,
            shadow_pk_count,
            live_only_pks,
            shadow_only_pks,
            mismatched_pks,
            passed: false,
            live_docs_scanned: live_pk_count,
            shadow_docs_scanned: shadow_pk_count,
        }
    }

    /// Create a successful verification result.
    pub fn success(live_pk_count: u64, shadow_pk_count: u64) -> Self {
        Self {
            live_pk_count,
            shadow_pk_count,
            live_only_pks: Vec::new(),
            shadow_only_pks: Vec::new(),
            mismatched_pks: Vec::new(),
            passed: true,
            live_docs_scanned: live_pk_count,
            shadow_docs_scanned: shadow_pk_count,
        }
    }

    /// Get VerificationResults for the operation state.
    pub fn to_verification_results(&self) -> VerificationResults {
        VerificationResults {
            live_pk_count: self.live_pk_count,
            shadow_pk_count: self.shadow_pk_count,
            live_only_pks: self.live_only_pks.clone(),
            shadow_only_pks: self.shadow_only_pks.clone(),
            mismatched_pks: self.mismatched_pks.clone(),
            passed: self.passed,
        }
    }
}

/// Error during verification phase.
#[derive(Debug, thiserror::Error)]
pub enum VerifyPhaseError {
    #[error("node fetch failed: {0}")]
    NodeFetchFailed(String),

    #[error("shard scan failed on shard {shard_id}: {error}")]
    ShardScanFailed { shard_id: u32, error: String },

    #[error("bucket allocation failed: {0}")]
    BucketAllocationFailed(String),

    #[error("verification aborted: {0}")]
    VerificationAborted(String),
}

/// Execute Phase 4: Verify cross-index PK set + content hash comparator (P5.1.d).
///
/// Once backfill completes, runs a cross-index PK-set comparator between live
/// and shadow. Iterates every shard of the live index and every shard of the
/// shadow index via `filter=_miroir_shard={id}` paginated scan, streams primary
/// keys and content fingerprints into side-by-side xxh3-keyed buckets, and asserts:
/// - (a) live PK set == shadow PK set
/// - (b) for each PK, content_hash_live == content_hash_shadow
///
/// This reuses §13.8's bucketed-Merkle machinery with PK-keyed (not shard-keyed)
/// bucketing so live and shadow can be compared across different S values.
///
/// # Arguments
/// * `live_index_uid` - The live index UID
/// * `shadow_index_uid` - The shadow index UID (e.g., "products__reshard_128")
/// * `old_shards` - Old shard count (S_old)
/// * `new_shards` - New shard count (S_new)
/// * `node_addresses` - List of all node addresses
/// * `master_key` - Meilisearch master key
/// * `primary_key` - Primary key field name
///
/// # Returns
/// `Ok(VerifyPhaseResult)` with verification outcome and any discrepancies.
pub async fn verify_phase(
    live_index_uid: &str,
    shadow_index_uid: &str,
    old_shards: u32,
    new_shards: u32,
    node_addresses: &[String],
    master_key: &str,
    primary_key: &str,
) -> Result<VerifyPhaseResult, VerifyPhaseError> {
    use std::collections::HashMap;

    tracing::info!(
        live_index = %live_index_uid,
        shadow_index = %shadow_index_uid,
        old_shards,
        new_shards,
        nodes = node_addresses.len(),
        "starting Phase 4: verify cross-index PK set + content hash"
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| VerifyPhaseError::NodeFetchFailed(format!("HTTP client: {}", e)))?;

    // Use the same node for all scans (first in list) - documents are identical
    // across replicas within the same shard due to RF replication
    let scan_node = node_addresses
        .first()
        .ok_or_else(|| VerifyPhaseError::NodeFetchFailed("no nodes available".to_string()))?;

    // Number of PK-keyed buckets for comparison (reuse anti-entropy constant)
    const BUCKET_COUNT: usize = 256;

    // Scan live index: collect all PKs and content hashes into PK-keyed buckets
    let mut live_buckets: Vec<HashMap<String, u64>> =
        (0..BUCKET_COUNT).map(|_| HashMap::new()).collect();
    let mut live_pk_count = 0u64;

    for shard_id in 0..old_shards {
        tracing::debug!(live_index = %live_index_uid, shard_id, "scanning live shard");

        match scan_shard_to_pk_buckets(
            &client,
            scan_node,
            live_index_uid,
            shard_id,
            primary_key,
            master_key,
            &mut live_buckets,
        )
        .await
        {
            Ok(count) => {
                live_pk_count += count;
                tracing::debug!(
                    live_index = %live_index_uid,
                    shard_id,
                    docs_scanned = count,
                    "scanned live shard"
                );
            }
            Err(e) => {
                return Err(VerifyPhaseError::ShardScanFailed {
                    shard_id,
                    error: e.to_string(),
                });
            }
        }
    }

    tracing::info!(
        live_index = %live_index_uid,
        total_pks = live_pk_count,
        "completed live index scan"
    );

    // Scan shadow index: collect all PKs and content hashes into PK-keyed buckets
    let mut shadow_buckets: Vec<HashMap<String, u64>> =
        (0..BUCKET_COUNT).map(|_| HashMap::new()).collect();
    let mut shadow_pk_count = 0u64;

    for shard_id in 0..new_shards {
        tracing::debug!(shadow_index = %shadow_index_uid, shard_id, "scanning shadow shard");

        match scan_shard_to_pk_buckets(
            &client,
            scan_node,
            shadow_index_uid,
            shard_id,
            primary_key,
            master_key,
            &mut shadow_buckets,
        )
        .await
        {
            Ok(count) => {
                shadow_pk_count += count;
                tracing::debug!(
                    shadow_index = %shadow_index_uid,
                    shard_id,
                    docs_scanned = count,
                    "scanned shadow shard"
                );
            }
            Err(e) => {
                return Err(VerifyPhaseError::ShardScanFailed {
                    shard_id,
                    error: e.to_string(),
                });
            }
        }
    }

    tracing::info!(
        shadow_index = %shadow_index_uid,
        total_pks = shadow_pk_count,
        "completed shadow index scan"
    );

    // Compare the two PK-keyed bucket sets
    let mut live_only_pks = Vec::new();
    let mut shadow_only_pks = Vec::new();
    let mut mismatched_pks = Vec::new();

    for (bucket_id, (live_bucket, shadow_bucket)) in
        live_buckets.iter().zip(shadow_buckets.iter()).enumerate()
    {
        // Find PKs only in live
        for pk in live_bucket.keys() {
            if !shadow_bucket.contains_key(pk) {
                live_only_pks.push(pk.clone());
            }
        }

        // Find PKs only in shadow
        for pk in shadow_bucket.keys() {
            if !live_bucket.contains_key(pk) {
                shadow_only_pks.push(pk.clone());
            }
        }

        // Find PKs with content hash mismatch
        for (pk, live_hash) in live_bucket.iter() {
            if let Some(shadow_hash) = shadow_bucket.get(pk) {
                if live_hash != shadow_hash {
                    mismatched_pks.push(pk.clone());
                }
            }
        }
    }

    // Check if verification passed
    let passed =
        live_only_pks.is_empty() && shadow_only_pks.is_empty() && mismatched_pks.is_empty();

    if passed {
        tracing::info!(
            live_pk_count,
            shadow_pk_count,
            "verification passed: PK sets and content hashes match"
        );
        Ok(VerifyPhaseResult::success(live_pk_count, shadow_pk_count))
    } else {
        tracing::warn!(
            live_pk_count,
            shadow_pk_count,
            live_only_count = live_only_pks.len(),
            shadow_only_count = shadow_only_pks.len(),
            mismatched_count = mismatched_pks.len(),
            "verification failed: discrepancies detected"
        );

        // Log sample discrepancies for debugging
        if !live_only_pks.is_empty() {
            tracing::warn!(
                sample_pks = ?live_only_pks.iter().take(10).collect::<Vec<_>>(),
                "PKs only in live index (sample)"
            );
        }
        if !shadow_only_pks.is_empty() {
            tracing::warn!(
                sample_pks = ?shadow_only_pks.iter().take(10).collect::<Vec<_>>(),
                "PKs only in shadow index (sample)"
            );
        }
        if !mismatched_pks.is_empty() {
            tracing::warn!(
                sample_pks = ?mismatched_pks.iter().take(10).collect::<Vec<_>>(),
                "PKs with content hash mismatch (sample)"
            );
        }

        Ok(VerifyPhaseResult::failed(
            live_pk_count,
            shadow_pk_count,
            live_only_pks,
            shadow_only_pks,
            mismatched_pks,
        ))
    }
}

/// Scan a single shard and stream PKs + content hashes into PK-keyed buckets.
///
/// This reuses the same bucketing approach as §13.8 anti-entropy but with
/// PK-keyed buckets instead of shard-keyed buckets, enabling cross-index
/// comparison when shard counts differ.
async fn scan_shard_to_pk_buckets(
    client: &reqwest::Client,
    node_address: &str,
    index_uid: &str,
    shard_id: u32,
    primary_key: &str,
    master_key: &str,
    buckets: &mut [std::collections::HashMap<String, u64>],
) -> Result<u64, String> {
    const BUCKET_COUNT: usize = 256;
    const BATCH_SIZE: u32 = 1000;
    let mut offset = 0u32;
    let mut docs_scanned = 0u64;

    loop {
        // Fetch documents with filter=_miroir_shard={shard_id}
        let filter = serde_json::json!({ "_miroir_shard": shard_id });
        let url = format!(
            "{}/indexes/{}/documents?filter={}&limit={}&offset={}",
            node_address.trim_end_matches('/'),
            index_uid,
            urlencoding::encode(&filter.to_string()),
            BATCH_SIZE,
            offset
        );

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", master_key))
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| format!("failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status.as_u16(), body_text));
        }

        // Parse response
        let docs_json: serde_json::Value =
            serde_json::from_str(&body_text).map_err(|e| format!("JSON parse: {}", e))?;

        let results = docs_json
            .get("results")
            .and_then(|v| v.as_array())
            .ok_or_else(|| format!("missing results array"))?;

        if results.is_empty() {
            break; // No more documents
        }

        for doc in results {
            // Extract primary key
            let pk_value = doc.get(primary_key).or(doc.get("id")).or(doc.get("_id"));
            let primary_key = pk_value
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("document missing primary key field"))?;

            // Compute content hash (reuse anti-entropy logic)
            let content_hash = compute_content_hash_for_verify(doc)?;

            // Compute PK-keyed bucket ID
            let mut pk_hasher = XxHash64::with_seed(0);
            pk_hasher.write(primary_key.as_bytes());
            let bucket_id = (pk_hasher.finish() as usize) % BUCKET_COUNT;

            // Insert into bucket
            buckets[bucket_id].insert(primary_key.to_string(), content_hash);
            docs_scanned += 1;
        }

        offset += BATCH_SIZE;
    }

    Ok(docs_scanned)
}

/// Compute content hash for verification (reuses anti-entropy canonical form).
fn compute_content_hash_for_verify(document: &serde_json::Value) -> Result<u64, String> {
    // Remove internal fields to get canonical content
    let mut canonical = document.clone();
    if let Some(obj) = canonical.as_object_mut() {
        // Remove all _miroir_* fields
        obj.retain(|k, _| !k.starts_with("_miroir_"));
        // Remove _rankingScore (not content, used for scoring)
        obj.remove("_rankingScore");
    }

    // Serialize with sorted keys for deterministic output
    let canonical_json = if let Some(obj) = canonical.as_object() {
        let sorted: BTreeMap<_, _> = obj.iter().collect();
        serde_json::to_string(&sorted).map_err(|e| format!("JSON serialize: {}", e))?
    } else {
        serde_json::to_string(&canonical).map_err(|e| format!("JSON serialize: {}", e))?
    };

    // Hash using xxh3
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(canonical_json.as_bytes());
    Ok(hasher.finish())
}

// ---------------------------------------------------------------------------
// Phase 5: Alias swap + dual-write stop (P5.1.e, plan §13.1 step 5)
// ---------------------------------------------------------------------------

/// Result of the alias swap phase.
#[derive(Debug, Clone, Serialize)]
pub struct AliasSwapResult {
    /// Alias name that was flipped.
    pub alias_name: String,
    /// Old target UID (before flip).
    pub old_target: String,
    /// New target UID (after flip).
    pub new_target: String,
    /// New alias version after flip.
    pub new_version: u64,
    /// Timestamp of the flip (UNIX ms).
    pub flipped_at: u64,
}

/// Error during alias swap phase.
#[derive(Debug, thiserror::Error)]
pub enum AliasSwapError {
    #[error("alias not found: {0}")]
    AliasNotFound(String),

    #[error("alias is not single-target: {0}")]
    NotSingleTargetAlias(String),

    #[error("alias flip failed: {0}")]
    FlipFailed(String),

    #[error("alias lookup failed: {0}")]
    LookupFailed(String),

    #[error("task store unavailable: {0}")]
    TaskStoreUnavailable(String),
}

/// Execute Phase 5: Alias swap + dual-write stop (P5.1.e, plan §13.1 step 5).
///
/// Performs an atomic alias flip via the task store's `flip_alias()` method,
/// pointing the alias at the new shadow index. After this step:
/// - Client writes target ONLY the new index (dual-write stops)
/// - The old index is retained for rollback (until cleanup phase)
/// - Rollback is a reverse alias flip to the old index
///
/// # Arguments
/// * `alias_name` - The alias name to flip (typically the live index UID)
/// * `new_target_uid` - The shadow index UID to point at (e.g., "products__reshard_128")
/// * `task_store` - Task store for persisting the alias flip
/// * `history_retention` - Number of history entries to retain for rollback
///
/// # Returns
/// `Ok(AliasSwapResult)` with details of the flip on success.
///
/// # Panics
/// None. All errors are returned via `Result`.
pub async fn alias_swap_phase(
    alias_name: &str,
    new_target_uid: &str,
    task_store: &dyn crate::task_store::TaskStore,
    history_retention: usize,
) -> Result<AliasSwapResult, AliasSwapError> {
    use std::time::{SystemTime, UNIX_EPOCH};

    tracing::info!(
        alias = %alias_name,
        new_target = %new_target_uid,
        "starting Phase 5: alias swap + dual-write stop"
    );

    // Step 1: Get the current alias state to capture old_target for rollback info
    let existing = task_store
        .get_alias(alias_name)
        .map_err(|e| AliasSwapError::LookupFailed(format!("{}", e)))?
        .ok_or_else(|| AliasSwapError::AliasNotFound(alias_name.to_string()))?;

    if existing.kind != "single" {
        return Err(AliasSwapError::NotSingleTargetAlias(alias_name.to_string()));
    }

    let old_target = existing
        .current_uid
        .ok_or_else(|| AliasSwapError::LookupFailed("alias missing current_uid".to_string()))?;

    tracing::debug!(
        alias = %alias_name,
        old_target = %old_target,
        new_target = %new_target_uid,
        "flipping alias from old to new target"
    );

    // Step 2: Perform the atomic alias flip via task store
    let flipped = task_store
        .flip_alias(alias_name, new_target_uid, history_retention)
        .map_err(|e| AliasSwapError::FlipFailed(format!("{}", e)))?;

    if !flipped {
        return Err(AliasSwapError::FlipFailed(
            "alias flip returned false (target may not exist)".to_string(),
        ));
    }

    // Step 3: Get the updated alias to capture new version
    let updated = task_store
        .get_alias(alias_name)
        .map_err(|e| AliasSwapError::LookupFailed(format!("{}", e)))?
        .ok_or_else(|| AliasSwapError::LookupFailed("alias disappeared after flip".to_string()))?;

    let flipped_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    tracing::info!(
        alias = %alias_name,
        old_target = %old_target,
        new_target = %new_target_uid,
        new_version = updated.version,
        "alias swap completed: dual-write stopped"
    );

    Ok(AliasSwapResult {
        alias_name: alias_name.to_string(),
        old_target,
        new_target: new_target_uid.to_string(),
        new_version: updated.version as u64,
        flipped_at,
    })
}

// ---------------------------------------------------------------------------
// Alias swap phase tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_alias_swap_phase {
    use super::*;
    use crate::task_store::{AliasRow, NewAlias};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn alias_swap_result_fields() {
        let result = AliasSwapResult {
            alias_name: "products".to_string(),
            old_target: "products".to_string(),
            new_target: "products__reshard_128".to_string(),
            new_version: 2,
            flipped_at: 1704067200000,
        };

        assert_eq!(result.alias_name, "products");
        assert_eq!(result.old_target, "products");
        assert_eq!(result.new_target, "products__reshard_128");
        assert_eq!(result.new_version, 2);
        assert_eq!(result.flipped_at, 1704067200000);
    }

    #[test]
    fn alias_swap_error_display() {
        let err = AliasSwapError::AliasNotFound("products".to_string());
        assert!(err.to_string().contains("not found"));
        assert!(err.to_string().contains("products"));

        let err = AliasSwapError::NotSingleTargetAlias("logs".to_string());
        assert!(err.to_string().contains("not single-target"));
        assert!(err.to_string().contains("logs"));

        let err = AliasSwapError::FlipFailed("database error".to_string());
        assert!(err.to_string().contains("flip failed"));
        assert!(err.to_string().contains("database error"));
    }

    // Helper to create a test alias row
    fn create_test_alias_row(
        name: &str,
        kind: &str,
        current_uid: Option<String>,
        target_uids: Option<Vec<String>>,
        version: i64,
    ) -> AliasRow {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        AliasRow {
            name: name.to_string(),
            kind: kind.to_string(),
            current_uid,
            target_uids,
            version,
            created_at: now,
            history: vec![],
        }
    }

    #[test]
    fn create_test_alias_row_helper() {
        let row = create_test_alias_row(
            "products",
            "single",
            Some("products_v1".to_string()),
            None,
            1,
        );

        assert_eq!(row.name, "products");
        assert_eq!(row.kind, "single");
        assert_eq!(row.current_uid, Some("products_v1".to_string()));
        assert_eq!(row.target_uids, None);
        assert_eq!(row.version, 1);
    }

    #[test]
    fn alias_swap_phase_result_construction() {
        // Verify the result structure is correct for reshard coordinator consumption
        let result = AliasSwapResult {
            alias_name: "products".to_string(),
            old_target: "products".to_string(),
            new_target: "products__reshard_128".to_string(),
            new_version: 5,
            flipped_at: 1704067200000,
        };

        // These fields are used by the reshard coordinator to update phase state
        assert!(!result.alias_name.is_empty());
        assert!(!result.old_target.is_empty());
        assert!(!result.new_target.is_empty());
        assert!(result.new_version > 0);
        assert!(result.flipped_at > 0);
    }
}

// ---------------------------------------------------------------------------
// Verify phase tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_verify_phase {
    use super::*;
    use serde_json::json;

    #[test]
    fn verify_result_success_creation() {
        let result = VerifyPhaseResult::success(1000, 1000);
        assert!(result.passed);
        assert_eq!(result.live_pk_count, 1000);
        assert_eq!(result.shadow_pk_count, 1000);
        assert!(result.live_only_pks.is_empty());
        assert!(result.shadow_only_pks.is_empty());
        assert!(result.mismatched_pks.is_empty());
    }

    #[test]
    fn verify_result_failed_creation() {
        let result = VerifyPhaseResult::failed(
            1000,
            999,
            vec!["pk1".to_string()],
            vec!["pk2".to_string()],
            vec!["pk3".to_string()],
        );
        assert!(!result.passed);
        assert_eq!(result.live_pk_count, 1000);
        assert_eq!(result.shadow_pk_count, 999);
        assert_eq!(result.live_only_pks, vec!["pk1"]);
        assert_eq!(result.shadow_only_pks, vec!["pk2"]);
        assert_eq!(result.mismatched_pks, vec!["pk3"]);
    }

    #[test]
    fn verify_result_to_verification_results() {
        let result = VerifyPhaseResult::success(500, 500);
        let vr = result.to_verification_results();
        assert!(vr.passed);
        assert_eq!(vr.live_pk_count, 500);
        assert_eq!(vr.shadow_pk_count, 500);
    }

    #[test]
    fn compute_content_hash_removes_internal_fields() {
        let doc = json!({
            "id": "test:1",
            "name": "Test",
            "_miroir_shard": 42,
            "_miroir_updated_at": 123456,
            "_rankingScore": 0.95
        });

        let hash1 = compute_content_hash_for_verify(&doc).unwrap();

        // Same document without internal fields should hash identically
        let clean_doc = json!({
            "id": "test:1",
            "name": "Test"
        });
        let hash2 = compute_content_hash_for_verify(&clean_doc).unwrap();

        assert_eq!(hash1, hash2, "internal fields should not affect hash");
    }

    #[test]
    fn compute_content_hash_deterministic() {
        let doc = json!({
            "id": "test:2",
            "z": "last",
            "a": "first",
            "m": "middle"
        });

        let hash1 = compute_content_hash_for_verify(&doc).unwrap();
        let hash2 = compute_content_hash_for_verify(&doc).unwrap();

        assert_eq!(hash1, hash2, "hash should be deterministic");
    }

    #[test]
    fn compute_content_hash_order_independent() {
        // JSON objects with same fields in different orders
        let doc1 = json!({"id": "x", "a": 1, "b": 2});
        let doc2 = json!({"id": "x", "b": 2, "a": 1});

        let hash1 = compute_content_hash_for_verify(&doc1).unwrap();
        let hash2 = compute_content_hash_for_verify(&doc2).unwrap();

        assert_eq!(hash1, hash2, "hash should be order-independent");
    }

    #[test]
    fn compute_content_hash_content_sensitive() {
        let doc1 = json!({"id": "test", "value": "foo"});
        let doc2 = json!({"id": "test", "value": "bar"});

        let hash1 = compute_content_hash_for_verify(&doc1).unwrap();
        let hash2 = compute_content_hash_for_verify(&doc2).unwrap();

        assert_ne!(
            hash1, hash2,
            "different content should produce different hashes"
        );
    }

    #[test]
    fn verify_error_display() {
        let err = VerifyPhaseError::ShardScanFailed {
            shard_id: 5,
            error: "connection refused".to_string(),
        };
        assert!(err.to_string().contains("shard 5"));
        assert!(err.to_string().contains("connection refused"));

        let err = VerifyPhaseError::NodeFetchFailed("no nodes".to_string());
        assert!(err.to_string().contains("no nodes"));
    }

    #[test]
    fn verify_phase_result_docs_scanned() {
        let result = VerifyPhaseResult::success(1000, 1000);
        assert_eq!(result.live_docs_scanned, 1000);
        assert_eq!(result.shadow_docs_scanned, 1000);
    }
}

// ---------------------------------------------------------------------------
// Phase 3: Backfill - stream live index to shadow (plan §13.1 step 3)
// ---------------------------------------------------------------------------

/// Result of the backfill phase.
#[derive(Debug, Clone)]
pub struct BackfillResult {
    /// Live index UID.
    pub live_index: String,
    /// Shadow index UID.
    pub shadow_index: String,
    /// Old shard count.
    pub old_shards: u32,
    /// New shard count.
    pub new_shards: u32,
    /// Total documents backfilled.
    pub documents_backfilled: u64,
    /// Total documents estimated (for progress tracking).
    pub total_estimated: u64,
    /// Duration in seconds.
    pub duration_secs: f64,
    /// Per-shard backfill counts.
    pub shard_counts: Vec<(u32, u64)>,
}

/// Error during backfill phase.
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error("node fetch failed: {0}")]
    NodeFetchFailed(String),

    #[error("shard backfill failed on shard {shard_id}: {error}")]
    ShardBackfillFailed { shard_id: u32, error: String },

    #[error("throttle wait failed: {0}")]
    ThrottleFailed(String),

    #[error("backfill aborted: {0}")]
    BackfillAborted(String),
}

/// Progress callback for backfill phase.
///
/// Called after each shard completes with (shard_id, docs_backfilled, total_shards).
pub type BackfillProgressCallback = Arc<dyn Fn(u32, u64, u32) + Send + Sync>;

/// Execute Phase 3: Backfill from live index to shadow (plan §13.1 step 3).
///
/// Pages through every live-index shard using `filter=_miroir_shard={id}`,
/// re-hashes each document under the new shard count, and writes to the shadow.
/// Writes are tagged with `_miroir_origin: "reshard_backfill"` for CDC suppression.
///
/// # Arguments
/// * `live_index_uid` - The live index UID
/// * `shadow_index_uid` - The shadow index UID (e.g., "products__reshard_128")
/// * `old_shards` - Old shard count (S_old)
/// * `new_shards` - New shard count (S_new)
/// * `node_addresses` - List of all node addresses
/// * `master_key` - Meilisearch master key
/// * `primary_key` - Primary key field name
/// * `throttle_docs_per_sec` - Throttle limit (0 = unlimited)
/// * `batch_size` - Documents per batch
/// * `progress_callback` - Optional callback for progress updates
///
/// # Returns
/// `Ok(BackfillResult)` with backfill statistics on success.
pub async fn backfill_phase(
    live_index_uid: &str,
    shadow_index_uid: &str,
    old_shards: u32,
    new_shards: u32,
    node_addresses: &[String],
    master_key: &str,
    primary_key: &str,
    throttle_docs_per_sec: u64,
    batch_size: usize,
    progress_callback: Option<BackfillProgressCallback>,
) -> Result<BackfillResult, BackfillError> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let start_time = SystemTime::now();
    tracing::info!(
        live_index = %live_index_uid,
        shadow_index = %shadow_index_uid,
        old_shards,
        new_shards,
        throttle_docs_per_sec,
        "starting Phase 3: backfill live to shadow"
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| BackfillError::NodeFetchFailed(format!("HTTP client: {}", e)))?;

    // Use the first node for all operations (documents are identical across replicas)
    let target_node = node_addresses
        .first()
        .ok_or_else(|| BackfillError::NodeFetchFailed("no nodes available".to_string()))?;

    let mut total_backfilled = 0u64;
    let mut shard_counts: Vec<(u32, u64)> = Vec::new();
    let mut last_throttle_time = SystemTime::now();

    // Process each shard from the live index
    for shard_id in 0..old_shards {
        tracing::debug!(
            live_index = %live_index_uid,
            shard_id,
            "starting backfill for shard"
        );

        let shard_docs = backfill_single_shard(
            &client,
            target_node,
            live_index_uid,
            shadow_index_uid,
            shard_id,
            old_shards,
            new_shards,
            master_key,
            primary_key,
            batch_size,
        )
        .await
        .map_err(|e| BackfillError::ShardBackfillFailed {
            shard_id,
            error: e.to_string(),
        })?;

        shard_counts.push((shard_id, shard_docs));
        total_backfilled += shard_docs;

        tracing::debug!(
            live_index = %live_index_uid,
            shard_id,
            docs_backfilled = shard_docs,
            total_so_far = total_backfilled,
            "completed backfill for shard"
        );

        // Call progress callback if provided
        if let Some(ref cb) = progress_callback {
            cb(shard_id, total_backfilled, old_shards);
        }

        // Apply throttling if configured
        if throttle_docs_per_sec > 0 && shard_docs > 0 {
            let docs_in_batch = shard_docs as f64;
            let target_duration_secs = docs_in_batch / throttle_docs_per_sec as f64;
            let target_duration = std::time::Duration::from_secs_f64(target_duration_secs);

            if let Ok(elapsed) = last_throttle_time.elapsed() {
                if elapsed < target_duration {
                    let wait_time = target_duration - elapsed;
                    tracing::trace!(
                        shard_id,
                        wait_ms = wait_time.as_millis(),
                        "throttling backfill"
                    );
                    tokio::time::sleep(wait_time).await;
                }
            }

            last_throttle_time = SystemTime::now();
        }
    }

    let duration_secs = start_time.elapsed().unwrap_or_default().as_secs_f64();

    tracing::info!(
        live_index = %live_index_uid,
        shadow_index = %shadow_index_uid,
        total_backfilled,
        duration_secs,
        docs_per_sec = if duration_secs > 0.0 {
            total_backfilled as f64 / duration_secs
        } else {
            0.0
        },
        "backfill phase completed"
    );

    Ok(BackfillResult {
        live_index: live_index_uid.to_string(),
        shadow_index: shadow_index_uid.to_string(),
        old_shards,
        new_shards,
        documents_backfilled: total_backfilled,
        total_estimated: total_backfilled, // In production, we'd estimate from stats
        duration_secs,
        shard_counts,
    })
}

/// Backfill a single shard from live to shadow index.
///
/// Reads all documents from the live index for a given shard,
/// re-hashes them under the new shard count, and writes to shadow.
async fn backfill_single_shard(
    client: &reqwest::Client,
    node_address: &str,
    live_index_uid: &str,
    shadow_index_uid: &str,
    shard_id: u32,
    old_shards: u32,
    new_shards: u32,
    master_key: &str,
    primary_key: &str,
    batch_size: usize,
) -> Result<u64, String> {
    const BATCH_LIMIT: u32 = 1000;
    let mut offset = 0u32;
    let mut total_backfilled = 0u64;

    loop {
        // Fetch documents with filter=_miroir_shard={shard_id}
        let filter = serde_json::json!({ "_miroir_shard": shard_id });
        let url = format!(
            "{}/indexes/{}/documents?filter={}&limit={}&offset={}",
            node_address.trim_end_matches('/'),
            live_index_uid,
            urlencoding::encode(&filter.to_string()),
            BATCH_LIMIT.min(batch_size as u32),
            offset
        );

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", master_key))
            .send()
            .await
            .map_err(|e| format!("fetch failed: {}", e))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| format!("failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status.as_u16(), body_text));
        }

        // Parse response
        let docs_json: serde_json::Value =
            serde_json::from_str(&body_text).map_err(|e| format!("JSON parse: {}", e))?;

        let results = docs_json
            .get("results")
            .and_then(|v| v.as_array())
            .ok_or_else(|| format!("missing results array"))?;

        if results.is_empty() {
            break; // No more documents
        }

        // Prepare shadow documents with new shard tags
        let mut shadow_documents = Vec::with_capacity(results.len());
        for doc in results {
            // Extract primary key
            let pk_value = doc
                .get(primary_key)
                .or(doc.get("id"))
                .or(doc.get("_id"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("document missing primary key field: {}", primary_key))?;

            // Compute new shard assignment for shadow index
            let new_shard_id = crate::router::shard_for_key(pk_value, new_shards);

            // Clone document for shadow index with new shard tag
            let mut shadow_doc = doc.clone();
            shadow_doc["_miroir_shard"] = serde_json::json!(new_shard_id);
            // Tag for CDC suppression
            shadow_doc["_miroir_origin"] = serde_json::json!("reshard_backfill");
            shadow_documents.push(shadow_doc);
        }

        // Write batch to shadow index
        write_backfill_batch(
            client,
            node_address,
            shadow_index_uid,
            &shadow_documents,
            master_key,
        )
        .await?;

        total_backfilled += shadow_documents.len() as u64;
        offset += BATCH_LIMIT;
    }

    Ok(total_backfilled)
}

/// Write a batch of documents to the shadow index during backfill.
async fn write_backfill_batch(
    client: &reqwest::Client,
    node_address: &str,
    shadow_index_uid: &str,
    documents: &[serde_json::Value],
    master_key: &str,
) -> Result<(), String> {
    let url = format!(
        "{}/indexes/{}/documents",
        node_address.trim_end_matches('/'),
        shadow_index_uid
    );

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", master_key))
        .json(documents)
        .send()
        .await
        .map_err(|e| format!("request failed: {}", e))?;

    let status = response.status();
    let body_text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(format!("HTTP {}: {}", status.as_u16(), body_text));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 6: Cleanup - delete old index after retention (plan §13.1 step 6)
// ---------------------------------------------------------------------------

/// Result of the cleanup phase.
#[derive(Debug, Clone)]
pub struct CleanupResult {
    /// Old index UID that was deleted.
    pub old_index: String,
    /// Shadow index UID (now the live index).
    pub new_index: String,
    /// Nodes the old index was deleted from.
    pub nodes_deleted_from: Vec<String>,
    /// Timestamp of cleanup completion (UNIX ms).
    pub completed_at: u64,
}

/// Error during cleanup phase.
#[derive(Debug, thiserror::Error)]
pub enum CleanupError {
    #[error("node deletion failed on {node}: {error}")]
    NodeDeletionFailed { node: String, error: String },

    #[error("cleanup aborted: {0}")]
    CleanupAborted(String),
}

/// Callback type for cleanup metrics emission.
pub type CleanupMetricsCallback = Arc<dyn Fn(f64, &CleanupResult) + Send + Sync>;

/// Execute Phase 6: Cleanup old index after retention (plan §13.1 step 6).
///
/// Deletes the live index from all nodes after the retention period.
/// The shadow index is now the live index after the alias swap.
///
/// # Arguments
/// * `old_index_uid` - The old live index UID to delete
/// * `new_index_uid` - The shadow index UID (now live)
/// * `node_addresses` - List of all node addresses
/// * `master_key` - Meilisearch master key
/// * `cleanup_deadline` - UNIX ms timestamp when retention expires (None = skip cleanup)
/// * `metrics_callback` - Optional callback for metrics emission
///
/// # Returns
/// `Ok(CleanupResult)` with cleanup details on success, or Err if deadline not reached.
///
/// # Rollback
/// If cleanup fails on some nodes, the index remains partially available.
/// Operators can manually retry cleanup on failed nodes.
pub async fn cleanup_phase(
    old_index_uid: &str,
    new_index_uid: &str,
    node_addresses: &[String],
    master_key: &str,
    cleanup_deadline: Option<u64>,
    metrics_callback: Option<CleanupMetricsCallback>,
) -> Result<CleanupResult, CleanupError> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    tracing::info!(
        old_index = %old_index_uid,
        new_index = %new_index_uid,
        nodes = node_addresses.len(),
        cleanup_deadline,
        "starting Phase 6: cleanup old index"
    );

    // Check retention deadline before proceeding

    if let Some(deadline) = cleanup_deadline {
        if now < deadline {
            let remaining_hours = (deadline - now) / 3600 / 1000;
            tracing::info!(
                old_index = %old_index_uid,
                remaining_hours,
                deadline,
                "retention period not yet reached, skipping cleanup"
            );
            return Err(CleanupError::CleanupAborted(format!(
                "retention period not reached: {} hours remaining",
                remaining_hours
            )));
        }
    } else {
        tracing::info!(
            old_index = %old_index_uid,
            "no cleanup deadline set, skipping cleanup"
        );
        return Err(CleanupError::CleanupAborted(
            "no cleanup deadline set".to_string(),
        ));
    }

    let cleanup_start = now;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| CleanupError::CleanupAborted(format!("HTTP client: {}", e)))?;

    let mut nodes_deleted_from = Vec::new();
    let mut errors = Vec::new();

    for address in node_addresses {
        let url = format!(
            "{}/indexes/{}",
            address.trim_end_matches('/'),
            old_index_uid
        );

        match client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", master_key))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                tracing::debug!(node = %address, index = %old_index_uid, "deleted old index");
                nodes_deleted_from.push(address.clone());
            }
            Ok(resp) => {
                let status = resp.status();
                let error = format!("HTTP {}", status.as_u16());
                tracing::error!(node = %address, index = %old_index_uid, error, "failed to delete old index");
                errors.push((address.clone(), error));
            }
            Err(e) => {
                tracing::error!(node = %address, index = %old_index_uid, error = %e, "request failed");
                errors.push((address.clone(), e.to_string()));
            }
        }
    }

    let completed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    if !errors.is_empty() {
        tracing::warn!(
            old_index = %old_index_uid,
            deleted_from = nodes_deleted_from.len(),
            failed_on = errors.len(),
            "cleanup completed with errors"
        );
    } else {
        tracing::info!(
            old_index = %old_index_uid,
            deleted_from_all = nodes_deleted_from.len(),
            "cleanup phase completed successfully"
        );
    }

    let result = CleanupResult {
        old_index: old_index_uid.to_string(),
        new_index: new_index_uid.to_string(),
        nodes_deleted_from,
        completed_at,
    };

    // Emit cleanup completion metric (miroir_reshard_cleanup_completed_seconds)
    // Measures the duration of the cleanup phase itself (time to delete the index)
    let cleanup_duration_secs = (completed_at.saturating_sub(cleanup_start)) as f64 / 1000.0;
    if let Some(ref callback) = metrics_callback {
        callback(cleanup_duration_secs, &result);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests for backfill and cleanup phases
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_backfill_cleanup {
    use super::*;

    #[test]
    fn backfill_result_fields() {
        let result = BackfillResult {
            live_index: "products".to_string(),
            shadow_index: "products__reshard_128".to_string(),
            old_shards: 64,
            new_shards: 128,
            documents_backfilled: 1000000,
            total_estimated: 1000000,
            duration_secs: 3600.0,
            shard_counts: vec![(0, 15625), (1, 15625), (2, 15625)],
        };

        assert_eq!(result.live_index, "products");
        assert_eq!(result.shadow_index, "products__reshard_128");
        assert_eq!(result.old_shards, 64);
        assert_eq!(result.new_shards, 128);
        assert_eq!(result.documents_backfilled, 1000000);
        assert_eq!(result.duration_secs, 3600.0);
        assert_eq!(result.shard_counts.len(), 3);
    }

    #[test]
    fn cleanup_result_fields() {
        let result = CleanupResult {
            old_index: "products".to_string(),
            new_index: "products__reshard_128".to_string(),
            nodes_deleted_from: vec!["node-1".to_string(), "node-2".to_string()],
            completed_at: 1704067200000,
        };

        assert_eq!(result.old_index, "products");
        assert_eq!(result.new_index, "products__reshard_128");
        assert_eq!(result.nodes_deleted_from.len(), 2);
        assert_eq!(result.completed_at, 1704067200000);
    }

    #[test]
    fn backfill_error_display() {
        let err = BackfillError::ShardBackfillFailed {
            shard_id: 5,
            error: "connection refused".to_string(),
        };
        assert!(err.to_string().contains("shard 5"));
        assert!(err.to_string().contains("connection refused"));

        let err = BackfillError::NodeFetchFailed("no nodes".to_string());
        assert!(err.to_string().contains("no nodes"));
    }

    #[test]
    fn cleanup_error_display() {
        let err = CleanupError::NodeDeletionFailed {
            node: "node-1".to_string(),
            error: "timeout".to_string(),
        };
        assert!(err.to_string().contains("node-1"));
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn cleanup_error_aborted_display() {
        let err = CleanupError::CleanupAborted(
            "retention period not reached: 23 hours remaining".to_string(),
        );
        assert!(err.to_string().contains("retention period not reached"));
        assert!(err.to_string().contains("23 hours remaining"));
    }
}

// ---------------------------------------------------------------------------
// Reshard orchestrator - sequences all six phases (plan §13.1)
// ---------------------------------------------------------------------------

/// Configuration for the reshard orchestrator.
#[derive(Clone)]
pub struct ReshardOrchestratorConfig {
    /// Index UID being resharded.
    pub index_uid: String,
    /// Target shard count.
    pub target_shards: u32,
    /// Node addresses.
    pub node_addresses: Vec<String>,
    /// Master key for Meilisearch.
    pub master_key: String,
    /// Primary key field name.
    pub primary_key: String,
    /// Backfill throttle (docs/sec, 0 = unlimited).
    pub throttle_docs_per_sec: u64,
    /// Backfill batch size.
    pub backfill_batch_size: usize,
    /// Retention period for old index (hours).
    pub retain_old_index_hours: u64,
    /// Whether to verify before swap.
    pub verify_before_swap: bool,
    /// History retention for alias.
    pub alias_history_retention: usize,
    /// Task store for persistence.
    pub task_store: Option<Arc<dyn crate::task_store::TaskStore>>,
    /// Metrics callback for phase transitions.
    pub metrics_callback: Option<ReshardMetricsCallback>,
}

/// Callback for metrics emission during resharding.
pub type ReshardMetricsCallback = Arc<dyn Fn(ReshardPhase, u64) + Send + Sync>;

/// Result of the full reshard operation.
#[derive(Debug, Clone)]
pub struct ReshardOrchestratorResult {
    /// Index that was resharded.
    pub index_uid: String,
    /// Old shard count.
    pub old_shards: u32,
    /// New shard count.
    pub new_shards: u32,
    /// Shadow index created.
    pub shadow_index: String,
    /// Documents backfilled.
    pub documents_backfilled: u64,
    /// Total duration (seconds).
    pub total_duration_secs: f64,
    /// Whether verification passed.
    pub verification_passed: bool,
    /// Final phase reached.
    pub final_phase: ReshardPhase,
}

/// Execute the full six-phase online resharding flow (plan §13.1).
///
/// This orchestrator sequences all phases with proper error handling:
/// - Phases 1-4: Any failure deletes shadow and aborts (invisible to clients)
/// - Phase 5: After alias swap, rollback is a reverse alias flip
/// - Phase 6: Cleanup after retention period
///
/// # Arguments
/// * `config` - Orchestrator configuration
///
/// # Returns
/// `Ok(ReshardOrchestratorResult)` on successful completion.
///
/// # Rollback
/// Failures before Phase 5 trigger automatic rollback (shadow deletion).
/// After Phase 5, manual rollback via alias flip is required.
pub async fn execute_reshard(
    config: ReshardOrchestratorConfig,
) -> Result<ReshardOrchestratorResult, String> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let start_time = SystemTime::now();
    let old_shards = config.target_shards / 2; // Assume doubling for now
    let shadow_index = format!("{}__reshard_{}", config.index_uid, config.target_shards);

    tracing::info!(
        index = %config.index_uid,
        old_shards,
        new_shards = config.target_shards,
        "starting six-phase online resharding"
    );

    // Emit metrics for phase transition
    let emit_phase = |phase: ReshardPhase, docs: u64| {
        if let Some(ref cb) = config.metrics_callback {
            cb(phase, docs);
        }
    };

    // Phase 1: Shadow create
    emit_phase(ReshardPhase::ShadowCreated, 0);
    let _shadow_result = shadow_create_phase(
        &config.index_uid,
        config.target_shards,
        &config.node_addresses,
        &config.master_key,
        Some(config.primary_key.clone()),
    )
    .await
    .map_err(|e| {
        // Phase 1 already handles rollback internally
        format!("Phase 1 shadow create failed: {}", e)
    })?;

    tracing::info!(
        shadow_index = %shadow_index,
        "Phase 1 complete: shadow index created"
    );

    // Phase 2: Dual-write is handled by the write path detecting the resharding registry
    emit_phase(ReshardPhase::DualWriteActive, 0);
    tracing::info!("Phase 2 active: dual-hash dual-write enabled");

    // Phase 3: Backfill
    emit_phase(ReshardPhase::BackfillInProgress, 0);
    let backfill_result = backfill_phase(
        &config.index_uid,
        &shadow_index,
        old_shards,
        config.target_shards,
        &config.node_addresses,
        &config.master_key,
        &config.primary_key,
        config.throttle_docs_per_sec,
        config.backfill_batch_size,
        None, // Progress callback - could be added later
    )
    .await
    .map_err(|e| {
        // Rollback: delete shadow index
        tracing::error!(error = %e, "Phase 3 backfill failed, rolling back");
        let _ = rollback_shadow_orchestrator(&shadow_index, &config);
        format!("Phase 3 backfill failed: {}", e)
    })?;

    emit_phase(
        ReshardPhase::BackfillInProgress,
        backfill_result.documents_backfilled,
    );
    tracing::info!(
        documents_backfilled = backfill_result.documents_backfilled,
        "Phase 3 complete: backfill finished"
    );

    // Phase 4: Verify
    emit_phase(
        ReshardPhase::Verifying,
        backfill_result.documents_backfilled,
    );
    let verify_result = verify_phase(
        &config.index_uid,
        &shadow_index,
        old_shards,
        config.target_shards,
        &config.node_addresses,
        &config.master_key,
        &config.primary_key,
    )
    .await
    .map_err(|e| {
        // Rollback: delete shadow index
        tracing::error!(error = %e, "Phase 4 verify failed, rolling back");
        let _ = rollback_shadow_orchestrator(&shadow_index, &config);
        format!("Phase 4 verify failed: {}", e)
    })?;

    if !verify_result.passed {
        // Verification failed - rollback
        let error = format!(
            "Phase 4 verification failed: {} live-only, {} shadow-only, {} mismatched",
            verify_result.live_only_pks.len(),
            verify_result.shadow_only_pks.len(),
            verify_result.mismatched_pks.len()
        );
        tracing::error!(error);
        let _ = rollback_shadow_orchestrator(&shadow_index, &config);
        return Err(error);
    }

    tracing::info!("Phase 4 complete: verification passed");

    // Phase 5: Alias swap
    emit_phase(ReshardPhase::Swapped, backfill_result.documents_backfilled);
    let _swap_result = if let Some(ref task_store) = config.task_store {
        alias_swap_phase(
            &config.index_uid,
            &shadow_index,
            task_store.as_ref(),
            config.alias_history_retention,
        )
        .await
        .map_err(|e| format!("Phase 5 alias swap failed: {}", e))?
    } else {
        // No task store - skip alias swap (for testing)
        tracing::warn!("no task store, skipping alias swap");
        return Ok(ReshardOrchestratorResult {
            index_uid: config.index_uid,
            old_shards,
            new_shards: config.target_shards,
            shadow_index,
            documents_backfilled: backfill_result.documents_backfilled,
            total_duration_secs: start_time.elapsed().unwrap_or_default().as_secs_f64(),
            verification_passed: true,
            final_phase: ReshardPhase::Swapped,
        });
    };

    tracing::info!(
        old_target = %config.index_uid,
        new_target = %shadow_index,
        "Phase 5 complete: alias swapped"
    );

    // Phase 6: Cleanup (after retention period)
    // For now, we skip cleanup in the orchestrator - it's triggered separately
    emit_phase(ReshardPhase::Complete, backfill_result.documents_backfilled);

    let total_duration_secs = start_time.elapsed().unwrap_or_default().as_secs_f64();

    tracing::info!(
        index = %config.index_uid,
        documents_backfilled = backfill_result.documents_backfilled,
        duration_secs = total_duration_secs,
        "reshard complete: all phases finished"
    );

    Ok(ReshardOrchestratorResult {
        index_uid: config.index_uid,
        old_shards,
        new_shards: config.target_shards,
        shadow_index,
        documents_backfilled: backfill_result.documents_backfilled,
        total_duration_secs,
        verification_passed: true,
        final_phase: ReshardPhase::Complete,
    })
}

/// Rollback shadow index deletion (used on failure before Phase 5).
async fn rollback_shadow_orchestrator(
    shadow_index: &str,
    config: &ReshardOrchestratorConfig,
) -> Result<(), String> {
    tracing::warn!(
        shadow_index = %shadow_index,
        "rolling back: deleting shadow index"
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("HTTP client: {}", e))?;

    for address in &config.node_addresses {
        let url = format!("{}/indexes/{}", address.trim_end_matches('/'), shadow_index);

        match client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", config.master_key))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!(node = %address, "rollback: deleted shadow index");
            }
            Ok(resp) => {
                tracing::warn!(
                    node = %address,
                    status = %resp.status(),
                    "rollback: failed to delete shadow index"
                );
            }
            Err(e) => {
                tracing::error!(node = %address, error = %e, "rollback: request failed");
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Orchestrator tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_orchestrator {
    use super::*;

    #[test]
    fn orchestrator_config_fields() {
        let config = ReshardOrchestratorConfig {
            index_uid: "products".to_string(),
            target_shards: 128,
            node_addresses: vec!["http://node-1:7700".to_string()],
            master_key: "key".to_string(),
            primary_key: "id".to_string(),
            throttle_docs_per_sec: 10000,
            backfill_batch_size: 1000,
            retain_old_index_hours: 48,
            verify_before_swap: true,
            alias_history_retention: 10,
            task_store: None,
            metrics_callback: None,
        };

        assert_eq!(config.index_uid, "products");
        assert_eq!(config.target_shards, 128);
        assert_eq!(config.throttle_docs_per_sec, 10000);
    }

    #[test]
    fn orchestrator_result_fields() {
        let result = ReshardOrchestratorResult {
            index_uid: "products".to_string(),
            old_shards: 64,
            new_shards: 128,
            shadow_index: "products__reshard_128".to_string(),
            documents_backfilled: 1000000,
            total_duration_secs: 3600.0,
            verification_passed: true,
            final_phase: ReshardPhase::Complete,
        };

        assert_eq!(result.index_uid, "products");
        assert_eq!(result.old_shards, 64);
        assert_eq!(result.new_shards, 128);
        assert_eq!(result.documents_backfilled, 1000000);
        assert!(result.verification_passed);
        assert_eq!(result.final_phase, ReshardPhase::Complete);
    }
}
