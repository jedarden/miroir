use clap::Subcommand;
use reqwest::Client;

#[derive(Subcommand, Debug)]
#[command(
    about = "Manage multi-tenancy",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/tenant.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub enum TenantSubcommand {
    /// Add a tenant mapping (api_key mode)
    Add {
        /// API key to map to tenant (will be hashed)
        #[arg(long)]
        api_key: String,

        /// Tenant identifier
        #[arg(long)]
        tenant_id: String,

        /// Replica group ID to pin this tenant to (optional for hash-based routing)
        #[arg(long)]
        group: Option<u32>,
    },
    /// List all tenant mappings
    List,
    /// Delete a tenant mapping by API key
    Remove {
        /// API key to delete (will be hashed for lookup)
        #[arg(long)]
        api_key: String,
    },
}

pub async fn run(
    cmd: TenantSubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let api_url = api_url.trim_end_matches('/');
    let base_url = format!("{api_url}/_miroir");

    match cmd {
        TenantSubcommand::Add {
            api_key,
            tenant_id,
            group,
        } => {
            let url = format!("{base_url}/tenants");
            let body = serde_json::json!({
                "api_key": api_key,
                "tenant_id": tenant_id,
                "group_id": group,
            });

            let response = client
                .post(&url)
                .header("Authorization", format!("Bearer {admin_key}"))
                .json(&body)
                .send()
                .await?;

            if response.status().is_success() {
                let result: serde_json::Value = response.json().await?;
                println!("Tenant mapping created:");
                println!("  tenant_id: {}", result["tenant_id"]);
                if let Some(g) = result.get("group_id") {
                    println!("  group_id: {g}");
                }
            } else {
                let error = response.text().await?;
                return Err(format!("Failed to add tenant mapping: {error}").into());
            }
        }
        TenantSubcommand::List => {
            let url = format!("{base_url}/tenants");

            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {admin_key}"))
                .send()
                .await?;

            if response.status().is_success() {
                let result: serde_json::Value = response.json().await?;
                if let Some(mappings) = result.get("mappings").and_then(|v| v.as_array()) {
                    if mappings.is_empty() {
                        println!("No tenant mappings configured.");
                    } else {
                        println!("Tenant mappings:");
                        for mapping in mappings {
                            println!("  tenant_id: {}", mapping["tenant_id"]);
                            if let Some(g) = mapping.get("group_id") {
                                println!("  group_id: {g}");
                            } else {
                                println!("  group_id: (hash-based routing)");
                            }
                            println!("  api_key_hash_prefix: {}", mapping["api_key_hash_prefix"]);
                            println!();
                        }
                    }
                }
            } else {
                let error = response.text().await?;
                return Err(format!("Failed to list tenant mappings: {error}").into());
            }
        }
        TenantSubcommand::Remove { api_key } => {
            let url = format!("{base_url}/tenants");
            let body = serde_json::json!({
                "api_key": api_key,
            });

            let response = client
                .delete(&url)
                .header("Authorization", format!("Bearer {admin_key}"))
                .json(&body)
                .send()
                .await?;

            if response.status().is_success() {
                println!("Tenant mapping deleted.");
            } else if response.status() == 404 {
                println!("Tenant mapping not found.");
            } else {
                let error = response.text().await?;
                return Err(format!("Failed to delete tenant mapping: {error}").into());
            }
        }
    }

    Ok(())
}
