//! Node management commands.
//!
//! Implements Phase 4 topology operations for adding, removing, and listing nodes.

use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;

#[derive(Subcommand, Debug)]
pub enum NodeSubcommand {
    /// Add a new node to the cluster
    Add(AddNodeArgs),
    /// Remove a node from the cluster
    Remove(RemoveNodeArgs),
    /// Drain a node (prepare for removal)
    Drain(DrainNodeArgs),
    /// List all nodes in the cluster
    List,
}

#[derive(Parser, Debug)]
pub struct AddNodeArgs {
    /// Node ID (unique identifier)
    #[arg(long)]
    id: String,

    /// Node address (e.g., http://node-4:7700)
    #[arg(long)]
    address: String,

    /// Replica group ID to join
    #[arg(long)]
    replica_group: u32,
}

#[derive(Parser, Debug)]
pub struct RemoveNodeArgs {
    /// Node ID to remove
    node_id: String,

    /// Force removal without draining (dangerous)
    #[arg(long)]
    force: bool,

    /// Skip confirmation prompt
    #[arg(long)]
    yes: bool,
}

#[derive(Parser, Debug)]
pub struct DrainNodeArgs {
    /// Node ID to drain
    node_id: String,
}

#[derive(Debug, Deserialize)]
struct NodeInfo {
    id: String,
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

#[derive(Debug, Deserialize)]
struct AddNodeResponse {
    operation_id: u64,
    message: String,
    migrations_count: usize,
}

#[derive(Debug, Deserialize)]
struct RemoveNodeResponse {
    operation_id: u64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct DrainNodeResponse {
    operation_id: u64,
    message: String,
    migrations_count: usize,
}

pub async fn run(
    cmd: NodeSubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    match cmd {
        NodeSubcommand::Add(args) => add_node(client, args, admin_key, api_url).await,
        NodeSubcommand::Remove(args) => remove_node(client, args, admin_key, api_url).await,
        NodeSubcommand::Drain(args) => drain_node(client, args, admin_key, api_url).await,
        NodeSubcommand::List => list_nodes(client, admin_key, api_url).await,
    }
}

async fn add_node(
    client: Client,
    args: AddNodeArgs,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}/_miroir/nodes", api_url.trim_end_matches('/'));

    let body = json!({
        "id": args.id,
        "address": args.address,
        "replica_group": args.replica_group,
    });

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", admin_key))
        .header("X-Admin-Key", admin_key)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Failed to add node: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Add node failed: HTTP {} — {}", status, text).into());
    }

    let result: AddNodeResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid response: {}", e))?;

    println!("{}", result.message);
    println!("Operation ID: {}", result.operation_id);
    println!("Migrations started: {}", result.migrations_count);
    println!("\nTrack progress with: miroir-ctl rebalance status");

    Ok(())
}

async fn remove_node(
    client: Client,
    args: RemoveNodeArgs,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !args.yes {
        println!("Removing node {} from the cluster", args.node_id);
        if args.force {
            println!("WARNING: --force flag is set. Node will be removed immediately without draining.");
        }
        print!("Continue? [y/N] ");
        use std::io::Write;
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let url = format!(
        "{}/_miroir/nodes/{}",
        api_url.trim_end_matches('/'),
        args.node_id
    );

    let body = json!({
        "force": args.force,
    });

    let resp = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", admin_key))
        .header("X-Admin-Key", admin_key)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Failed to remove node: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Remove node failed: HTTP {} — {}", status, text).into());
    }

    let result: RemoveNodeResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid response: {}", e))?;

    println!("{}", result.message);
    println!("Operation ID: {}", result.operation_id);

    Ok(())
}

async fn drain_node(
    client: Client,
    args: DrainNodeArgs,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!(
        "{}/_miroir/nodes/{}/drain",
        api_url.trim_end_matches('/'),
        args.node_id
    );

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", admin_key))
        .header("X-Admin-Key", admin_key)
        .send()
        .await
        .map_err(|e| format!("Failed to drain node: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Drain node failed: HTTP {} — {}", status, text).into());
    }

    let result: DrainNodeResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid response: {}", e))?;

    println!("{}", result.message);
    println!("Operation ID: {}", result.operation_id);
    println!("Migrations started: {}", result.migrations_count);
    println!("\nTrack progress with: miroir-ctl rebalance status");
    println!("After drain completes, remove the node with: miroir-ctl node remove {}", args.node_id);

    Ok(())
}

async fn list_nodes(
    client: Client,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}/_miroir/topology", api_url.trim_end_matches('/'));

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", admin_key))
        .header("X-Admin-Key", admin_key)
        .send()
        .await
        .map_err(|e| format!("Failed to list nodes: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("List nodes failed: HTTP {} — {}", status, text).into());
    }

    let topo: TopologyResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid response: {}", e))?;

    println!("=== Miroir Cluster Topology ===");
    println!();
    println!("Shards: {}", topo.shards);
    println!("Replication Factor: {}", topo.replication_factor);
    println!("Degraded Nodes: {}", topo.degraded_node_count);
    println!("Rebalance In Progress: {}", topo.rebalance_in_progress);
    println!("Fully Covered: {}", topo.fully_covered);
    println!();
    println!("Nodes:");

    if topo.nodes.is_empty() {
        println!("  (none)");
    } else {
        let max_id_len = topo.nodes.iter().map(|n| n.id.len()).max().unwrap_or(0);
        let max_addr_len = topo.nodes.iter().map(|n| n.address.len()).max().unwrap_or(0);

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
                "  {} {:id_width$}  {:addr_width$}  {}  shards: {}",
                status_emoji,
                node.id,
                node.address,
                node.status,
                node.shard_count,
                id_width = max_id_len,
                addr_width = max_addr_len
            );

            if let Some(ref error) = node.error {
                println!("    └─ error: {}", error);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_response_deserialization() {
        let json = r#"{
            "shards": 64,
            "replication_factor": 2,
            "nodes": [
                {
                    "id": "node-0",
                    "address": "http://node-0:7700",
                    "status": "active",
                    "shard_count": 32,
                    "last_seen_ms": 100
                }
            ],
            "degraded_node_count": 0,
            "rebalance_in_progress": false,
            "fully_covered": true
        }"#;

        let _topo: TopologyResponse = serde_json::from_str(json).unwrap();
    }
}
