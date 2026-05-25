//! Rebalancing management commands.
//!
//! Implements Phase 4 rebalancing operations for cluster topology changes.

use clap::Subcommand;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep as tokio_sleep;

#[derive(Subcommand, Debug)]
#[command(
    about = "Manage rebalancing operations",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/rebalance.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub enum RebalanceSubcommand {
    /// Show rebalancing status
    Status {
        /// Watch mode: continuously refresh status
        #[arg(short, long)]
        watch: bool,
    },
    /// Start a manual rebalance operation
    Start,
    /// Cancel an active rebalance operation
    Cancel {
        /// Operation ID to cancel
        operation_id: u64,

        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Deserialize)]
struct MigrationStatus {
    id: u64,
    new_node: String,
    replica_group: u32,
    phase: String,
    shards_count: usize,
    completed_count: usize,
}

#[derive(Debug, Deserialize)]
struct TopologyOperation {
    id: u64,
    #[serde(rename = "op_type")]
    op_type: String,
    status: String,
    target_node: Option<String>,
    target_group: Option<u32>,
    migrations: Vec<serde_json::Value>,
    started_at: Option<u64>,
    completed_at: Option<u64>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RebalanceStatusResponse {
    in_progress: bool,
    operations: Vec<TopologyOperation>,
    migrations: serde_json::Value,
}

pub async fn run(
    cmd: RebalanceSubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    match cmd {
        RebalanceSubcommand::Status { watch } => {
            if watch {
                watch_status(client, admin_key, api_url).await
            } else {
                show_status(client, admin_key, api_url).await
            }
        }
        RebalanceSubcommand::Start => start_rebalance(client, admin_key, api_url).await,
        RebalanceSubcommand::Cancel { operation_id, yes } => {
            cancel_rebalance(client, operation_id, yes, admin_key, api_url).await
        }
    }
}

async fn show_status(
    client: Client,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}/_miroir/rebalance/status", api_url.trim_end_matches('/'));

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", admin_key))
        .header("X-Admin-Key", admin_key)
        .send()
        .await
        .map_err(|e| format!("Failed to get rebalance status: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Rebalance status failed: HTTP {} — {}", status, text).into());
    }

    let result: RebalanceStatusResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid response: {}", e))?;

    print_status(&result);

    Ok(())
}

async fn watch_status(
    client: Client,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{self, Write};

    let url = format!("{}/_miroir/rebalance/status", api_url.trim_end_matches('/'));

    loop {
        // Clear screen
        print!("\x1b[2J\x1b[H");
        io::stdout().flush()?;

        let resp = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", admin_key))
            .header("X-Admin-Key", admin_key)
            .send()
            .await
            .map_err(|e| format!("Failed to get rebalance status: {}", e))?;

        if resp.status().is_success() {
            let result: RebalanceStatusResponse = resp
                .json()
                .await
                .map_err(|e| format!("Invalid response: {}", e))?;

            print_status(&result);
            println!("\nRefreshing every 2 seconds (Ctrl+C to exit)...");
        } else {
            println!("Error fetching status");
        }

        tokio_sleep(Duration::from_secs(2)).await;
    }
}

fn print_status(result: &RebalanceStatusResponse) {
    println!("=== Rebalance Status ===");
    println!();

    if result.in_progress {
        println!("Status: Rebalance in progress");
    } else {
        println!("Status: No rebalance in progress");
    }

    println!();

    if result.operations.is_empty() {
        println!("No operations recorded.");
    } else {
        println!("Operations:");
        for op in &result.operations {
            let status_emoji = match op.status.as_str() {
                "pending" => "⏳",
                "in_progress" => "▶",
                "complete" => "✓",
                "failed" => "✗",
                "cancelled" => "⊘",
                _ => "?",
            };

            println!("  {} Operation {} ({})", status_emoji, op.id, op.op_type);

            if let Some(ref node) = op.target_node {
                println!("    Target node: {}", node);
            }
            if let Some(group) = op.target_group {
                println!("    Target group: {}", group);
            }

            println!("    Status: {}", op.status);
            println!("    Migrations: {}", op.migrations.len());

            if let Some(ref error) = op.error {
                println!("    Error: {}", error);
            }

            if let Some(started) = op.started_at {
                if let Some(completed) = op.completed_at {
                    let duration_secs = (completed - started) / 1000;
                    println!("    Duration: {}s", duration_secs);
                }
            }
        }
    }

    println!();
}

async fn start_rebalance(
    _client: Client,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Note: Rebalancing is triggered automatically by topology changes
    // (add_node, drain_node, etc.). This command is for documentation
    // and future manual rebalance support.
    println!("Rebalancing is triggered automatically by topology changes:");
    println!("  - miroir-ctl node add       : triggers rebalance to move shards");
    println!("  - miroir-ctl node drain     : triggers rebalance to migrate data");
    println!("  - miroir-ctl node remove    : removes drained node");
    println!();
    println!("To check rebalance status, use:");
    println!("  miroir-ctl rebalance status");
    Ok(())
}

async fn cancel_rebalance(
    client: Client,
    operation_id: u64,
    yes: bool,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !yes {
        print!("Cancel rebalance operation {}? [y/N] ", operation_id);
        use std::io::Write;
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Note: The admin API doesn't have a cancel endpoint yet
    // This is a placeholder for future implementation
    println!("Cancel operation not yet implemented via API.");
    println!();
    println!("To stop a rebalance operation:");
    println!("  1. Let the current migrations complete (they are safe)");
    println!("  2. Or restart the proxy to cancel pending operations");
    println!();
    println!("Operation ID: {}", operation_id);

    let _ = (client, admin_key, api_url);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_status_deserialization() {
        let json = r#"{
            "in_progress": true,
            "operations": [
                {
                    "id": 1,
                    "op_type": "add_node",
                    "status": "in_progress",
                    "target_node": "node-4",
                    "target_group": 0,
                    "migrations": [],
                    "started_at": 1700000000000,
                    "completed_at": null,
                    "error": null
                }
            ],
            "migrations": {}
        }"#;

        let _status: RebalanceStatusResponse = serde_json::from_str(json).unwrap();
    }
}
