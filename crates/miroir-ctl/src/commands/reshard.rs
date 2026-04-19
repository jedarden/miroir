//! Reshard CLI command: start, status, and schedule window guard.

use clap::Subcommand;
use miroir_core::reshard::{check_window_now, ReshardingConfig, WindowGuardResult};

#[derive(Subcommand, Debug)]
pub enum ReshardSubcommand {
    /// Start an online resharding operation (plan §13.1).
    ///
    /// Creates a shadow index with the new shard count, backfills from the
    /// live index, verifies, and swaps. Refuses to start outside the
    /// configured schedule window unless --force is given.
    Start {
        /// Index UID to reshard.
        #[arg(long)]
        index: String,

        /// Target shard count.
        #[arg(long)]
        new_shards: u32,

        /// Backfill throttle (docs/sec). 0 = unlimited.
        #[arg(long, default_value = "10000")]
        throttle: u64,

        /// Named schedule window (from config). Pass "off-peak" or the
        /// configured window name. The command refuses to start outside
        /// this window unless --force is given.
        #[arg(long)]
        schedule_window: Option<String>,

        /// Override schedule window guard — start resharding regardless
        /// of the current time window.
        #[arg(long)]
        force: bool,

        /// Dry run: show what would happen without starting.
        #[arg(long)]
        dry_run: bool,
    },

    /// Check the status of an ongoing resharding operation.
    Status {
        /// Index UID to check.
        #[arg(long)]
        index: String,
    },
}

pub async fn run(cmd: ReshardSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        ReshardSubcommand::Start {
            index,
            new_shards,
            throttle,
            schedule_window,
            force,
            dry_run,
        } => run_start(index, new_shards, throttle, schedule_window, force, dry_run).await,
        ReshardSubcommand::Status { index } => run_status(index).await,
    }
}

async fn run_start(
    index: String,
    new_shards: u32,
    throttle: u64,
    schedule_window: Option<String>,
    force: bool,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = load_reshard_config()?;

    if !config.enabled {
        return Err("Resharding is disabled in configuration (resharding.enabled = false)".into());
    }

    // Schedule window guard.
    let guard = check_window_now(&config);
    match &guard {
        WindowGuardResult::Denied { utc_now, allowed } => {
            if !force {
                eprintln!("Error: resharding is not allowed at {}.", utc_now);
                eprintln!("Allowed windows: {}", allowed.join(", "));
                eprintln!("Use --force to override (not recommended during peak load).");
                std::process::exit(1);
            }
            eprintln!(
                "Warning: forcing resharding outside allowed window (now: {}, allowed: {})",
                utc_now,
                allowed.join(", ")
            );
        }
        WindowGuardResult::Allowed { window } => {
            eprintln!("Schedule window check: within allowed window ({})", window);
        }
        WindowGuardResult::NoRestriction => {
            eprintln!("Schedule window check: no restriction configured");
        }
    }

    // Validate schedule_window argument against config.
    if let Some(ref window_name) = schedule_window {
        if !config.allowed_windows.is_empty() {
            let found = config.allowed_windows.iter().any(|w| w == window_name);
            if !found {
                eprintln!(
                    "Warning: --schedule-window '{}' not found in config allowed_windows: [{}]",
                    window_name,
                    config.allowed_windows.join(", ")
                );
            }
        }
    }

    if dry_run {
        println!(
            "Dry run: would reshard index '{}' to {} shards",
            index, new_shards
        );
        println!("  throttle: {} docs/sec", throttle);
        println!("  force: {}", force);
        println!("  schedule_window: {:?}", schedule_window);
        println!("  window_guard: {:?}", guard);
        println!(
            "  config.backfill_concurrency: {}",
            config.backfill_concurrency
        );
        println!(
            "  config.backfill_batch_size: {}",
            config.backfill_batch_size
        );
        println!("  config.verify_before_swap: {}", config.verify_before_swap);
        println!(
            "  config.retain_old_index_hours: {}h",
            config.retain_old_index_hours
        );
        println!();
        println!("Phase plan:");
        println!("  1. Shadow create: {index}__reshard_{new_shards}");
        println!("  2. Dual-hash dual-write begins");
        println!(
            "  3. Backfill (throttled to {throttle} docs/sec, concurrency {})",
            config.backfill_concurrency
        );
        println!("  4. Verify PK-set and content hashes");
        println!("  5. Alias swap");
        println!(
            "  6. Cleanup (retain old for {}h)",
            config.retain_old_index_hours
        );
        return Ok(());
    }

    // TODO: Submit reshard job via admin API when proxy is implemented.
    Err("Reshard start requires admin API (not yet connected). Use --dry-run to validate.".into())
}

async fn run_status(index: String) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Query reshard status via admin API when proxy is implemented.
    let _ = index;
    Err("Reshard status requires admin API (not yet connected).".into())
}

/// Load resharding config from the standard config path.
///
/// Looks for `~/.config/miroir/config.toml` and extracts the
/// `[resharding]` section. Returns defaults if no config found.
fn load_reshard_config() -> Result<ReshardingConfig, Box<dyn std::error::Error>> {
    use std::fs;
    use std::path::PathBuf;

    let config_path = dirs::config_dir()
        .map(|d| d.join("miroir").join("config.toml"))
        .unwrap_or_else(|| PathBuf::from("/dev/null"));

    if !config_path.exists() {
        return Ok(ReshardingConfig::default());
    }

    let contents = fs::read_to_string(&config_path)
        .map_err(|e| format!("Failed to read {}: {}", config_path.display(), e))?;

    let full: toml::Value = toml::from_str(&contents)
        .map_err(|e| format!("Invalid TOML in {}: {}", config_path.display(), e))?;

    let resharding = full
        .get("resharding")
        .cloned()
        .unwrap_or(toml::Value::Table(toml::map::Map::new()));

    let config: ReshardingConfig = resharding
        .try_into()
        .map_err(|e| format!("Invalid [resharding] config: {}", e))?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use miroir_core::reshard::{check_window, ReshardingConfig, WindowGuardResult};

    #[test]
    fn start_refused_outside_window() {
        let config = ReshardingConfig {
            allowed_windows: vec!["02:00-06:00".into()],
            ..Default::default()
        };
        let guard = check_window(720, &config);
        assert!(matches!(guard, WindowGuardResult::Denied { .. }));
    }

    #[test]
    fn start_allowed_inside_window() {
        let config = ReshardingConfig {
            allowed_windows: vec!["02:00-06:00".into()],
            ..Default::default()
        };
        let guard = check_window(180, &config);
        assert!(matches!(guard, WindowGuardResult::Allowed { .. }));
    }

    #[test]
    fn start_no_restriction_when_unconfigured() {
        let config = ReshardingConfig::default();
        let guard = check_window(720, &config);
        assert!(matches!(guard, WindowGuardResult::NoRestriction));
    }

    #[test]
    fn wrap_midnight_window() {
        let config = ReshardingConfig {
            allowed_windows: vec!["22:00-06:00".into()],
            ..Default::default()
        };
        assert!(matches!(
            check_window(1380, &config),
            WindowGuardResult::Allowed { .. }
        ));
        assert!(matches!(
            check_window(60, &config),
            WindowGuardResult::Allowed { .. }
        ));
        assert!(matches!(
            check_window(720, &config),
            WindowGuardResult::Denied { .. }
        ));
    }

    #[test]
    fn parse_resharding_config_from_toml() {
        let toml = r#"
enabled = true
backfill_concurrency = 8
backfill_batch_size = 500
throttle_docs_per_sec = 5000
verify_before_swap = true
retain_old_index_hours = 24
allowed_windows = ["02:00-06:00", "22:00-23:30"]
"#;
        let config: ReshardingConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.backfill_concurrency, 8);
        assert_eq!(config.backfill_batch_size, 500);
        assert_eq!(config.throttle_docs_per_sec, 5000);
        assert_eq!(config.retain_old_index_hours, 24);
        assert_eq!(config.allowed_windows.len(), 2);
    }

    #[test]
    fn parse_resharding_config_defaults() {
        let toml = "";
        let config: ReshardingConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.backfill_concurrency, 4);
        assert_eq!(config.backfill_batch_size, 1000);
        assert_eq!(config.throttle_docs_per_sec, 0);
        assert!(config.verify_before_swap);
        assert_eq!(config.retain_old_index_hours, 48);
        assert!(config.allowed_windows.is_empty());
    }
}
