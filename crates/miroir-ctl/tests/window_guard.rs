//! Integration tests for the reshard CLI schedule window guard (P12.OP3).
//!
//! These tests exercise the full CLI binary as a subprocess to confirm that
//! the window guard correctly rejects resharding outside configured windows
//! and that --force overrides the guard.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

fn bin() -> String {
    env!("CARGO_BIN_EXE_miroir-ctl").to_string()
}

/// Write a TOML config with the given resharding section into a temp dir,
/// returning the TempDir (kept alive for the subprocess).
fn write_config(resharding_toml: &str) -> TempDir {
    let tmp = TempDir::new().unwrap();
    let config_dir = tmp.path().join("miroir");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(config_dir.join("config.toml"), resharding_toml).unwrap();
    tmp
}

fn run_reshard(tmp: &TempDir, extra_args: &[&str]) -> std::process::Output {
    Command::new(bin())
        .env("XDG_CONFIG_HOME", tmp.path())
        .env("MIROIR_ADMIN_API_KEY", "test-key")
        .args([
            "reshard",
            "start",
            "--index",
            "test-idx",
            "--new-shards",
            "128",
            "--dry-run",
        ])
        .args(extra_args)
        .output()
        .unwrap()
}

#[test]
fn rejected_outside_configured_window() {
    // Use a 1-minute window in the middle of the night — very unlikely to match now.
    let tmp = write_config(
        r#"[resharding]
enabled = true
allowed_windows = ["03:42-03:43"]"#,
    );

    let output = run_reshard(&tmp, &[]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "CLI should fail outside window. stderr: {stderr}"
    );
    assert!(
        stderr.contains("not allowed") || stderr.contains("Error"),
        "stderr should mention rejection: {stderr}"
    );
}

#[test]
fn force_overrides_window_guard() {
    let tmp = write_config(
        r#"[resharding]
enabled = true
allowed_windows = ["03:42-03:43"]"#,
    );

    let output = run_reshard(&tmp, &["--force"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "CLI should succeed with --force. stderr: {stderr}"
    );
    assert!(
        stderr.contains("forcing resharding outside"),
        "stderr should warn about force override: {stderr}"
    );
}

#[test]
fn no_windows_allows_any_time() {
    let tmp = write_config(
        r#"[resharding]
enabled = true"#,
    );

    let output = run_reshard(&tmp, &[]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "CLI should succeed when no windows configured. stderr: {stderr}"
    );
    assert!(
        stderr.contains("no restriction"),
        "stderr should note no restriction: {stderr}"
    );
    assert!(
        stdout.contains("Dry run"),
        "stdout should show dry run plan: {stdout}"
    );
}

#[test]
fn disabled_config_rejects_even_with_no_windows() {
    let tmp = write_config(
        r#"[resharding]
enabled = false"#,
    );

    let output = run_reshard(&tmp, &[]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "CLI should fail when resharding disabled. stderr: {stderr}"
    );
    assert!(
        stderr.contains("disabled"),
        "stderr should mention disabled: {stderr}"
    );
}
