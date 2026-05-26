//! Index alias management commands.
//!
//! Implements Phase 13.7 alias operations for creating, deleting, and listing aliases.

use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;

#[derive(Subcommand, Debug)]
#[command(
    about = "Manage index aliases",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/alias.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub enum AliasSubcommand {
    /// Create a new alias
    Create(CreateAliasArgs),
    /// Delete an alias
    Delete(DeleteAliasArgs),
    /// List all aliases
    List,
    /// Show detailed info for a specific alias
    Show(ShowAliasArgs),
}

#[derive(Parser, Debug)]
pub struct CreateAliasArgs {
    /// Alias name
    name: String,

    /// Target index UID (for single-target alias)
    #[arg(long)]
    target: Option<String>,

    /// Target index UIDs (for multi-target alias, comma-separated)
    #[arg(long, value_delimiter = ',')]
    targets: Option<Vec<String>>,
}

#[derive(Parser, Debug)]
pub struct DeleteAliasArgs {
    /// Alias name to delete
    name: String,

    /// Skip confirmation prompt
    #[arg(long)]
    yes: bool,
}

#[derive(Parser, Debug)]
pub struct ShowAliasArgs {
    /// Alias name to show
    name: String,
}

#[derive(Debug, Deserialize)]
struct AliasInfo {
    name: String,
    kind: String,
    current_uid: Option<String>,
    target_uids: Option<Vec<String>>,
    version: u64,
}

#[derive(Debug, Deserialize)]
struct GetAliasResponse {
    name: String,
    kind: String,
    current_uid: Option<String>,
    target_uids: Option<Vec<String>>,
    version: u64,
    #[allow(dead_code)]
    created_at: u64,
    history: Vec<AliasHistoryEntry>,
}

#[derive(Debug, Deserialize)]
struct AliasHistoryEntry {
    uid: String,
    flipped_at: u64,
}

#[derive(Debug, Deserialize)]
struct ListAliasesResponse {
    aliases: Vec<AliasInfo>,
}

pub async fn run(
    cmd: AliasSubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    match cmd {
        AliasSubcommand::Create(args) => create_alias(client, args, admin_key, api_url).await,
        AliasSubcommand::Delete(args) => delete_alias(client, args, admin_key, api_url).await,
        AliasSubcommand::List => list_aliases(client, admin_key, api_url).await,
        AliasSubcommand::Show(args) => show_alias(client, args, admin_key, api_url).await,
    }
}

async fn create_alias(
    client: Client,
    args: CreateAliasArgs,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!(
        "{}/_miroir/aliases/{}",
        api_url.trim_end_matches('/'),
        args.name
    );

    // Determine request body based on whether target or targets is provided
    let body = if let Some(target) = args.target {
        json!({
            "target": target,
        })
    } else if let Some(targets) = args.targets {
        json!({
            "targets": targets,
        })
    } else {
        return Err("Must provide either --target or --targets".into());
    };

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {admin_key}"))
        .header("X-Admin-Key", admin_key)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Failed to create alias: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Create alias failed: HTTP {status} — {text}").into());
    }

    println!("Alias '{}' created successfully", args.name);
    Ok(())
}

async fn delete_alias(
    client: Client,
    args: DeleteAliasArgs,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !args.yes {
        println!("Deleting alias '{}'", args.name);
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
        "{}/_miroir/aliases/{}",
        api_url.trim_end_matches('/'),
        args.name
    );

    let resp = client
        .delete(&url)
        .header("Authorization", format!("Bearer {admin_key}"))
        .header("X-Admin-Key", admin_key)
        .send()
        .await
        .map_err(|e| format!("Failed to delete alias: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Delete alias failed: HTTP {status} — {text}").into());
    }

    println!("Alias '{}' deleted successfully", args.name);
    Ok(())
}

async fn list_aliases(
    client: Client,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}/_miroir/aliases", api_url.trim_end_matches('/'));

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {admin_key}"))
        .header("X-Admin-Key", admin_key)
        .send()
        .await
        .map_err(|e| format!("Failed to list aliases: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("List aliases failed: HTTP {status} — {text}").into());
    }

    let result: ListAliasesResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid response: {e}"))?;

    println!("=== Index Aliases ===");
    println!();

    if result.aliases.is_empty() {
        println!("(none)");
    } else {
        let max_name_len = result
            .aliases
            .iter()
            .map(|a| a.name.len())
            .max()
            .unwrap_or(0);

        for alias in result.aliases {
            let kind_display = match alias.kind.as_str() {
                "single" => "single",
                "multi" => "multi (ILM)",
                _ => &alias.kind,
            };

            let manager = if alias.kind == "multi" {
                "ILM"
            } else {
                "operator"
            };

            println!(
                "{:width$}  {:8}  {:10}  v{}",
                alias.name,
                kind_display,
                manager,
                alias.version,
                width = max_name_len
            );

            // Show targets
            if let Some(ref target) = alias.current_uid {
                println!("  └─ target: {target}");
            } else if let Some(ref targets) = alias.target_uids {
                if targets.len() == 1 {
                    println!("  └─ target: {}", targets[0]);
                } else {
                    println!("  └─ targets ({}):", targets.len());
                    for target in targets {
                        println!("       └─ {target}");
                    }
                }
            }
        }
    }

    Ok(())
}

async fn show_alias(
    client: Client,
    args: ShowAliasArgs,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!(
        "{}/_miroir/aliases/{}",
        api_url.trim_end_matches('/'),
        args.name
    );

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {admin_key}"))
        .header("X-Admin-Key", admin_key)
        .send()
        .await
        .map_err(|e| format!("Failed to get alias: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Get alias failed: HTTP {status} — {text}").into());
    }

    let alias: GetAliasResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid response: {e}"))?;

    println!("=== Alias Details ===");
    println!();
    println!("Name: {}", alias.name);
    println!("Kind: {}", alias.kind);
    println!("Version: {}", alias.version);

    let manager = if alias.kind == "multi" {
        "ILM (read-only, use ILM policy to modify)"
    } else {
        "operator (writable)"
    };
    println!("Manager: {manager}");

    println!();
    println!("Targets:");

    if let Some(ref target) = alias.current_uid {
        println!("  {target}");
    } else if let Some(ref targets) = alias.target_uids {
        for target in targets {
            println!("  {target}");
        }
    }

    if !alias.history.is_empty() {
        println!();
        println!("History (last {}):", alias.history.len());
        for entry in &alias.history {
            // Format timestamp as ISO 8601
            let flipped_at = format_timestamp(entry.flipped_at);
            println!("  {} -> {}", entry.uid, flipped_at);
        }
    }

    Ok(())
}

/// Format a UNIX timestamp as ISO 8601 string.
fn format_timestamp(timestamp_ms: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    let duration = Duration::from_millis(timestamp_ms);
    if let Some(datetime) = UNIX_EPOCH.checked_add(duration) {
        // Use debug format which gives ISO 8601-like output
        return format!("{datetime:?}");
    }

    // Fallback: just show the raw value
    format!("{timestamp_ms} ms")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alias_info_deserialization() {
        let json = r#"{
            "name": "products",
            "kind": "single",
            "current_uid": "products_v3",
            "target_uids": null,
            "version": 5
        }"#;

        let _alias: AliasInfo = serde_json::from_str(json).unwrap();
    }

    #[test]
    fn test_get_alias_response_deserialization() {
        let json = r#"{
            "name": "products",
            "kind": "single",
            "current_uid": "products_v3",
            "target_uids": null,
            "version": 5,
            "created_at": 1704067200,
            "history": [
                {
                    "uid": "products_v2",
                    "flipped_at": 1703980800
                }
            ]
        }"#;

        let _alias: GetAliasResponse = serde_json::from_str(json).unwrap();
    }
}
