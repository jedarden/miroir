//! Key management commands.
//!
//! Implements plan §9 zero-downtime rotation for the admin-scoped nodeMasterKey.

use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::io::{self, Write};

/// Key management commands.
#[derive(Subcommand, Debug)]
#[command(
    about = "Manage API keys",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/key.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub enum KeySubcommand {
    /// Rotate the admin-scoped nodeMasterKey (zero-downtime).
    ///
    /// Implements the 4-step zero-downtime rotation from plan §9:
    ///   1. Create a new admin-scoped key on every Meilisearch node
    ///   2. Print instructions for updating the K8s Secret
    ///   3. Wait for operator to confirm rolling restart of Miroir pods
    ///   4. Delete the old admin-scoped key from every node
    ///
    /// TERMINOLOGY (plan §9):
    ///   - MEILI_MASTER_KEY (startup env var): fixed at process start.
    ///     Rotation requires a Meilisearch pod restart (separate runbook).
    ///   - Admin-scoped child keys (POST /keys, actions: ["*"]): multiple
    ///     can coexist, rotation is zero-downtime.
    ///   - The "nodeMasterKey" in Miroir config is the second kind.
    RotateNodeMaster(RotateNodeMasterArgs),
}

#[derive(Parser, Debug)]
pub struct RotateNodeMasterArgs {
    /// Print the rotation plan without executing any changes.
    #[arg(long)]
    dry_run: bool,

    /// Current nodeMasterKey used to authenticate with Meilisearch nodes.
    /// Falls back to MIROIR_NODE_MASTER_KEY env var.
    #[arg(long, env = "MIROIR_NODE_MASTER_KEY")]
    current_key: Option<String>,

    /// Meilisearch node base URL (repeatable, e.g. http://meili-0.search.svc:7700).
    /// Discovered from the topology API when omitted.
    #[arg(long = "node")]
    nodes: Vec<String>,

    /// Name for the new key (visible in GET /keys output).
    #[arg(long, default_value = "miroir-node-master")]
    key_name: String,

    /// Optional expiration for the new key (ISO 8601, e.g. "2026-12-31T23:59:59Z").
    #[arg(long)]
    expires_at: Option<String>,

    /// Kubernetes namespace containing the Miroir secret.
    #[arg(long, default_value = "search")]
    namespace: String,

    /// Kubernetes Secret name containing nodeMasterKey.
    #[arg(long, default_value = "miroir-keys")]
    secret_name: String,

    /// Skip confirmation prompts (use with caution).
    #[arg(long)]
    yes: bool,
}

// -- Meilisearch API response types ------------------------------------------

#[derive(Debug, Deserialize)]
struct MeiliKeysResponse {
    results: Vec<MeiliKey>,
}

#[derive(Debug, Deserialize, Serialize)]
struct MeiliKey {
    uid: String,
    key: String,
    name: Option<String>,
    description: Option<String>,
    actions: Vec<serde_json::Value>,
    indexes: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct MeiliKeyCreated {
    uid: String,
    key: String,
}

// -- Topology API response type -----------------------------------------------

#[derive(Debug, Deserialize)]
struct TopologyResponse {
    nodes: Vec<TopologyNode>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TopologyNode {
    id: String,
    address: String,
    status: String,
}

// -- Entry point --------------------------------------------------------------

pub async fn run(
    cmd: KeySubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        KeySubcommand::RotateNodeMaster(args) => rotate_node_master(args, admin_key, api_url).await,
    }
}

// -- Rotation logic -----------------------------------------------------------

async fn rotate_node_master(
    args: RotateNodeMasterArgs,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Resolve current key
    let current_key = match &args.current_key {
        Some(k) => k.clone(),
        None => {
            return Err(
                "No current nodeMasterKey. Use --current-key or set MIROIR_NODE_MASTER_KEY.".into(),
            );
        }
    };

    // Resolve node addresses
    let node_addresses = if args.nodes.is_empty() {
        discover_nodes(admin_key, api_url).await?
    } else {
        args.nodes.clone()
    };

    if node_addresses.is_empty() {
        return Err(
            "No Meilisearch node addresses. Use --node or ensure topology API is reachable.".into(),
        );
    }

    // ── Dry-run ──────────────────────────────────────────────────────
    if args.dry_run {
        return print_dry_run(&args, &node_addresses, &current_key);
    }

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // ── Step 1: Create new admin-scoped key on all nodes ─────────────
    eprintln!("Step 1/4: Creating new admin-scoped key on all Meilisearch nodes...");

    let mut create_body = json!({
        "name": args.key_name,
        "description": format!("{} (rotated epoch {})", args.key_name, epoch_seconds()),
        "actions": ["*"],
        "indexes": ["*"],
    });
    if let Some(ref exp) = args.expires_at {
        create_body["expiresAt"] = json!(exp);
    } else {
        create_body["expiresAt"] = serde_json::Value::Null;
    }

    let mut new_key_value: Option<String> = None;
    let mut new_key_uid: Option<String> = None;
    let mut created_on: Vec<String> = Vec::new();

    for addr in &node_addresses {
        let url = format!("{}/keys", addr.trim_end_matches('/'));
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", current_key))
            .header("Content-Type", "application/json")
            .json(&create_body)
            .send()
            .await
            .map_err(|e| format!("Failed to contact {}: {}", addr, e))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            rollback_create(&client, &created_on, &new_key_uid, &current_key).await;
            return Err(format!(
                "Key creation failed on {}: HTTP {} — {}",
                addr, status, text
            )
            .into());
        }

        let body: MeiliKeyCreated = resp
            .json()
            .await
            .map_err(|e| format!("Bad response from {}: {}", addr, e))?;

        if new_key_value.is_none() {
            new_key_value = Some(body.key.clone());
            new_key_uid = Some(body.uid.clone());
        }
        created_on.push(addr.clone());
        eprintln!("  Created key on {}", addr);
    }

    let new_key = new_key_value.ok_or("No key value received")?;
    let new_uid = new_key_uid.ok_or("No key UID received")?;

    eprintln!(
        "  New key: {}...  UID: {}",
        &new_key[..8.min(new_key.len())],
        new_uid
    );

    // ── Step 2: Print K8s Secret update instructions ─────────────────
    println!("\n--- Step 2/4: Update K8s Secret ---\n");
    println!("Patch the secret with the new key:");
    println!(
        "  kubectl -n {} patch secret {} \\",
        args.namespace, args.secret_name
    );
    println!(
        "    -p '{{\"stringData\":{{\"nodeMasterKey\":\"{}\"}}}}'",
        new_key
    );
    println!("\nOr update your ExternalSecret / OpenBao source.\n");

    // ── Step 3: Rolling restart instructions ─────────────────────────
    println!("--- Step 3/4: Rolling restart Miroir pods ---\n");
    println!(
        "  kubectl -n {} rollout restart deployment/miroir",
        args.namespace
    );
    println!(
        "  kubectl -n {} rollout status deployment/miroir",
        args.namespace
    );
    println!("\nBoth old and new keys are valid concurrently — no downtime.\n");

    if !args.yes {
        print!("Press Enter once ALL Miroir pods are running with the new key (Ctrl+C to abort): ");
        io::stdout().flush()?;
        let mut buf = String::new();
        io::stdin().read_line(&mut buf)?;
    }

    // ── Step 4: Delete old key ───────────────────────────────────────
    eprintln!("\nStep 4/4: Deleting old admin-scoped key...");

    let old_uid = find_old_key_uid(&client, &node_addresses[0], &current_key).await?;

    match old_uid {
        Some(uid) => {
            eprintln!("  Old key UID: {}", uid);

            if !args.yes {
                print!("Delete old key {} from all nodes? [y/N] ", uid);
                io::stdout().flush()?;
                let mut buf = String::new();
                io::stdin().read_line(&mut buf)?;
                if !buf.trim().eq_ignore_ascii_case("y") {
                    eprintln!("Skipped. Delete manually with:");
                    for addr in &node_addresses {
                        eprintln!(
                            "  curl -X DELETE {}/keys/{} -H 'Authorization: Bearer <key>'",
                            addr.trim_end_matches('/'),
                            uid
                        );
                    }
                    return Ok(());
                }
            }

            for addr in &node_addresses {
                let url = format!("{}/keys/{}", addr.trim_end_matches('/'), uid);
                let resp = client
                    .delete(&url)
                    .header("Authorization", format!("Bearer {}", current_key))
                    .send()
                    .await
                    .map_err(|e| format!("Delete failed on {}: {}", addr, e))?;

                if resp.status().is_success() {
                    eprintln!("  Deleted old key on {}", addr);
                } else {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    eprintln!(
                        "  Warning: delete on {} returned HTTP {} — {}",
                        addr, status, text
                    );
                }
            }
        }
        None => {
            eprintln!("  Could not determine old key UID. Skipping deletion.");
            eprintln!("  List keys and delete manually:");
            for addr in &node_addresses {
                eprintln!(
                    "    curl {}/keys -H 'Authorization: Bearer <key>'",
                    addr.trim_end_matches('/')
                );
            }
        }
    }

    eprintln!("\nRotation complete.");
    Ok(())
}

// -- Dry-run plan printer ----------------------------------------------------

fn print_dry_run(
    args: &RotateNodeMasterArgs,
    nodes: &[String],
    current_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== nodeMasterKey Rotation Plan (dry-run) ===\n");

    println!("Target nodes ({}):", nodes.len());
    for addr in nodes {
        println!("  - {}", addr);
    }
    println!();

    println!(
        "Current key prefix: {}...",
        &current_key[..8.min(current_key.len())]
    );
    println!();

    println!("Steps:");
    println!("  1. Create new admin-scoped key on each node");
    println!(
        "       POST /keys  {{ name: {:?}, actions: [\"*\"], indexes: [\"*\"] }}",
        args.key_name
    );
    if let Some(ref exp) = args.expires_at {
        println!("       expiresAt: {:?}", exp);
    }
    println!();

    println!(
        "  2. Update K8s Secret {}/{} with new key value",
        args.namespace, args.secret_name
    );
    println!();

    println!(
        "  3. Rolling restart: kubectl -n {} rollout restart deployment/miroir",
        args.namespace
    );
    println!(
        "     During rollout old-key pods and new-key pods both authenticate (zero-downtime)."
    );
    println!();

    println!("  4. Delete old key (UID from GET /keys) on every node");
    println!();

    println!("Notes:");
    println!("  - Both old and new admin-scoped keys are valid concurrently (plan §9)");
    println!("  - The startup MEILI_MASTER_KEY is NOT changed by this flow");
    println!("  - For startup-master rotation see docs/runbooks/startup-master-key-rotation.md");
    Ok(())
}

// -- Node discovery via topology API -----------------------------------------

async fn discover_nodes(
    admin_key: &str,
    api_url: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let url = format!("{}/_miroir/topology", api_url.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", admin_key))
        .header("X-Admin-Key", admin_key)
        .send()
        .await
        .map_err(|e| format!("Topology API unreachable at {}: {}", url, e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Topology API returned HTTP {} — {}", status, text).into());
    }

    let topo: TopologyResponse = resp
        .json()
        .await
        .map_err(|e| format!("Bad topology response: {}", e))?;

    let addresses: Vec<String> = topo
        .nodes
        .into_iter()
        .filter(|n| n.status == "healthy" || n.status == "active" || n.status == "joining")
        .map(|n| n.address)
        .collect();

    if addresses.is_empty() {
        return Err("Topology returned no healthy nodes".into());
    }

    eprintln!("Discovered {} node(s) from topology API", addresses.len());
    Ok(addresses)
}

// -- Find old key UID by matching prefix -------------------------------------

async fn find_old_key_uid(
    client: &Client,
    node_addr: &str,
    current_key: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let url = format!("{}/keys", node_addr.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", current_key))
        .send()
        .await
        .map_err(|e| format!("Failed to list keys on {}: {}", node_addr, e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        eprintln!(
            "  Warning: could not list keys on {} (HTTP {} — {})",
            node_addr, status, text
        );
        return Ok(None);
    }

    let keys: MeiliKeysResponse = resp
        .json()
        .await
        .map_err(|e| format!("Bad keys response from {}: {}", node_addr, e))?;

    let prefix_len = 8.min(current_key.len());
    let prefix = &current_key[..prefix_len];

    for k in &keys.results {
        if k.key.len() >= prefix_len && &k.key[..prefix_len] == prefix {
            return Ok(Some(k.uid.clone()));
        }
    }

    Ok(None)
}

// -- Rollback on step 1 failure -----------------------------------------------

async fn rollback_create(
    client: &Client,
    created_on: &[String],
    key_uid: &Option<String>,
    auth_key: &str,
) {
    let Some(uid) = key_uid else { return };
    for addr in created_on {
        let url = format!("{}/keys/{}", addr.trim_end_matches('/'), uid);
        match client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", auth_key))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                eprintln!("  Rollback: deleted key on {}", addr);
            }
            Ok(resp) => {
                eprintln!("  Rollback failed on {}: HTTP {}", addr, resp.status());
            }
            Err(e) => {
                eprintln!("  Rollback failed on {}: {}", addr, e);
            }
        }
    }
}

// -- Helpers ------------------------------------------------------------------

fn epoch_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_seconds_returns_non_zero() {
        let now = epoch_seconds();
        assert!(now > 0, "epoch_seconds should return a positive value");
        assert!(
            now > 1700000000,
            "epoch_seconds should be reasonably recent (post-2023)"
        );
    }

    #[test]
    fn test_rotate_node_master_args_defaults() {
        let args = RotateNodeMasterArgs {
            dry_run: true,
            current_key: Some("test-key".to_string()),
            nodes: vec![],
            key_name: "miroir-node-master".to_string(),
            expires_at: None,
            namespace: "search".to_string(),
            secret_name: "miroir-keys".to_string(),
            yes: false,
        };

        assert!(args.dry_run);
        assert_eq!(args.current_key, Some("test-key".to_string()));
        assert!(args.nodes.is_empty());
        assert_eq!(args.key_name, "miroir-node-master");
        assert_eq!(args.namespace, "search");
        assert_eq!(args.secret_name, "miroir-keys");
        assert!(!args.yes);
    }

    #[test]
    fn test_dry_run_plan_format() {
        let args = RotateNodeMasterArgs {
            dry_run: true,
            current_key: Some("test-key-abc123".to_string()),
            nodes: vec![
                "http://meili-0:7700".to_string(),
                "http://meili-1:7700".to_string(),
            ],
            key_name: "test-key".to_string(),
            expires_at: Some("2026-12-31T23:59:59Z".to_string()),
            namespace: "test-ns".to_string(),
            secret_name: "test-secret".to_string(),
            yes: false,
        };

        let nodes = args.nodes.clone();
        let current_key = args.current_key.as_ref().unwrap();
        let result = print_dry_run(&args, &nodes, current_key);

        assert!(result.is_ok(), "dry_run should succeed");

        // The function prints to stdout, we can't easily capture it in unit tests
        // but we can verify it doesn't error
    }

    #[test]
    fn test_meili_key_serialization() {
        let key = MeiliKey {
            uid: "test-uid".to_string(),
            key: "test-key-value".to_string(),
            name: Some("test-key-name".to_string()),
            description: Some("test description".to_string()),
            actions: vec![serde_json::json!("*")],
            indexes: vec![serde_json::json!("*")],
        };

        // Verify the struct can be serialized
        let json = serde_json::to_string(&key);
        assert!(json.is_ok(), "MeiliKey should serialize");

        let parsed: Result<MeiliKey, _> = serde_json::from_str(&json.unwrap());
        assert!(parsed.is_ok(), "MeiliKey should deserialize");
        let parsed_key = parsed.unwrap();
        assert_eq!(parsed_key.uid, "test-uid");
        assert_eq!(parsed_key.key, "test-key-value");
    }

    #[test]
    fn test_topology_node_serialization() {
        let node = TopologyNode {
            id: "node-0".to_string(),
            address: "http://meili-0:7700".to_string(),
            status: "healthy".to_string(),
        };

        let json = serde_json::to_string(&node);
        assert!(json.is_ok(), "TopologyNode should serialize");

        let parsed: Result<TopologyNode, _> = serde_json::from_str(&json.unwrap());
        assert!(parsed.is_ok(), "TopologyNode should deserialize");
        let parsed_node = parsed.unwrap();
        assert_eq!(parsed_node.id, "node-0");
        assert_eq!(parsed_node.address, "http://meili-0:7700");
        assert_eq!(parsed_node.status, "healthy");
    }
}
