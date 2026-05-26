use clap::Parser;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep as tokio_sleep;

#[derive(Parser, Debug)]
#[command(
    about = "Show cluster status and health",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/status.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub struct StatusSubcommand {
    /// Watch mode: continuously refresh status
    #[arg(short, long)]
    watch: bool,
}

#[derive(Debug, Deserialize)]
struct NodeInfo {
    id: String,
    #[allow(dead_code)]
    address: String,
    status: String,
    shard_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopologyResponse {
    shards: u32,
    replication_factor: u32,
    nodes: Vec<NodeInfo>,
    degraded_node_count: u32,
    rebalance_in_progress: bool,
    fully_covered: bool,
}

pub async fn run(
    cmd: StatusSubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;

    if cmd.watch {
        watch_status(client, admin_key, api_url).await
    } else {
        show_status(client, admin_key, api_url).await
    }
}

async fn show_status(
    client: Client,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}/_miroir/topology", api_url.trim_end_matches('/'));

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {admin_key}"))
        .header("X-Admin-Key", admin_key)
        .send()
        .await
        .map_err(|e| format!("Failed to get cluster status: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Status check failed: HTTP {status} — {text}").into());
    }

    let topo: TopologyResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid response: {e}"))?;

    print_cluster_status(&topo);
    Ok(())
}

async fn watch_status(
    client: Client,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{self, Write};

    let url = format!("{}/_miroir/topology", api_url.trim_end_matches('/'));

    loop {
        print!("\x1b[2J\x1b[H");
        io::stdout().flush()?;

        let resp = client
            .get(&url)
            .header("Authorization", format!("Bearer {admin_key}"))
            .header("X-Admin-Key", admin_key)
            .send()
            .await
            .map_err(|e| format!("Failed to get cluster status: {e}"))?;

        if resp.status().is_success() {
            let topo: TopologyResponse = resp
                .json()
                .await
                .map_err(|e| format!("Invalid response: {e}"))?;

            print_cluster_status(&topo);
            println!("\nRefreshing every 2 seconds (Ctrl+C to exit)...");
        } else {
            println!("Error fetching status");
        }

        tokio_sleep(Duration::from_secs(2)).await;
    }
}

fn print_cluster_status(topo: &TopologyResponse) {
    println!("=== Miroir Cluster Status ===");
    println!();
    println!("Configuration:");
    println!("  Shards: {}", topo.shards);
    println!("  Replication Factor: {}", topo.replication_factor);
    println!();
    println!("Health:");
    println!("  Fully Covered: {}", topo.fully_covered);
    println!("  Degraded Nodes: {}", topo.degraded_node_count);
    println!("  Rebalance In Progress: {}", topo.rebalance_in_progress);
    println!();
    println!("Nodes:");

    if topo.nodes.is_empty() {
        println!("  (none)");
    } else {
        let max_id_len = topo.nodes.iter().map(|n| n.id.len()).max().unwrap_or(0);

        for node in &topo.nodes {
            let status_emoji = match node.status.as_str() {
                "active" | "healthy" => "✓",
                "joining" => "→",
                "draining" => "↓",
                "failed" => "✗",
                "degraded" => "⚠",
                _ => "?",
            };

            println!(
                "  {} {:id_width$}  {}  shards: {}",
                status_emoji,
                node.id,
                node.status,
                node.shard_count,
                id_width = max_id_len
            );

            if let Some(ref error) = node.error {
                println!("    └─ error: {error}");
            }
        }
    }
}
