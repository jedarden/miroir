use clap::Subcommand;
use serde_json::{json, Value};

#[derive(Subcommand, Debug)]
#[command(
    about = "Explain query plans",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/explain.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub enum ExplainSubcommand {
    /// Explain a search query plan (plan §13.20)
    Query {
        /// Index name or alias to explain
        index: String,
        /// Query string (q parameter)
        #[arg(short, long)]
        q: Option<String>,
        /// Filter expression
        #[arg(short, long)]
        filter: Option<String>,
        /// Offset
        #[arg(short, long, default_value = "0")]
        offset: usize,
        /// Limit
        #[arg(short, long, default_value = "20")]
        limit: usize,
        /// Tenant ID for affinity
        #[arg(long)]
        tenant_id: Option<String>,
        /// Execute the query and show results
        #[arg(long)]
        execute: bool,
        /// Show full JSON output
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(
    cmd: ExplainSubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        ExplainSubcommand::Query {
            index,
            q,
            filter,
            offset,
            limit,
            tenant_id,
            execute,
            json,
        } => {
            explain_query(
                index, q, filter, offset, limit, tenant_id, execute, json, admin_key, api_url,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn explain_query(
    index: String,
    q: Option<String>,
    filter: Option<String>,
    offset: usize,
    limit: usize,
    tenant_id: Option<String>,
    execute: bool,
    json_output: bool,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Build the request body
    let mut body = json!({
        "q": q,
        "offset": offset,
        "limit": limit,
    });

    if let Some(f) = filter {
        // Parse filter as JSON if it looks like JSON, otherwise treat as string
        if f.trim().starts_with('{') || f.trim().starts_with('[') {
            if let Ok(value) = serde_json::from_str::<Value>(&f) {
                body["filter"] = value;
            } else {
                body["filter"] = json!(f);
            }
        } else {
            body["filter"] = json!(f);
        }
    }

    if let Some(ref tenant) = tenant_id {
        body["tenantId"] = json!(tenant);
    }

    // Build the URL with ?execute=true if needed
    let mut url = format!("{api_url}/indexes/{index}/explain");
    if execute {
        url.push_str("?execute=true");
    }

    // Send the request
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {admin_key}"))
        .json(&body)
        .send()
        .await?;

    let status = response.status();
    let response_text = response.text().await?;

    if !status.is_success() {
        return Err(format!("Explain request failed with status {status}: {response_text}").into());
    }

    // Parse the response
    let result: Value = serde_json::from_str(&response_text)?;

    if json_output {
        // Pretty print JSON
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        // Human-readable output
        print_explain_result(&result);
    }

    Ok(())
}

fn print_explain_result(result: &Value) {
    println!("=== Query Explanation ===\n");

    // Resolved UID
    if let Some(uid) = result.get("resolvedUid") {
        println!("Index: {}", uid.as_str().unwrap_or("?"));
    }

    // Plan section
    if let Some(plan) = result.get("plan") {
        println!("\n--- Execution Plan ---");

        // Alias resolution
        if let Some(alias) = plan.get("aliasResolution").and_then(|v| v.as_object()) {
            println!(
                "Alias: {} → {} (version: {})",
                alias.get("from").and_then(|v| v.as_str()).unwrap_or("?"),
                alias.get("to").and_then(|v| v.as_str()).unwrap_or("?"),
                alias.get("version").and_then(|v| v.as_u64()).unwrap_or(0)
            );
        }

        // Narrowing
        if let Some(narrowed) = plan.get("narrowed").and_then(|v| v.as_bool()) {
            if narrowed {
                println!("Narrowed: true");
                if let Some(reason) = plan.get("narrowingReason").and_then(|v| v.as_str()) {
                    println!("  Reason: {reason}");
                }
                if let Some(shards) = plan.get("targetShards").and_then(|v| v.as_array()) {
                    let shard_ids: Vec<String> = shards
                        .iter()
                        .filter_map(|v| v.as_u64().map(|id| id.to_string()))
                        .collect();
                    println!("  Target shards: [{}]", shard_ids.join(", "));
                }
            } else {
                println!("Narrowed: false (all shards)");
            }
        }

        // Chosen group
        if let Some(group) = plan.get("chosenGroup").and_then(|v| v.as_object()) {
            println!(
                "Group: {} ({})",
                group.get("id").and_then(|v| v.as_u64()).unwrap_or(0),
                group.get("reason").and_then(|v| v.as_str()).unwrap_or("?")
            );
        }

        // Target nodes
        if let Some(nodes) = plan.get("targetNodes").and_then(|v| v.as_object()) {
            println!("Target nodes:");
            for (shard, node) in nodes.iter().take(10) {
                println!("  Shard {} → {}", shard, node.as_str().unwrap_or("?"));
            }
            if nodes.len() > 10 {
                println!("  ... and {} more", nodes.len() - 10);
            }
        }

        // Hedging
        if let Some(hedging) = plan.get("hedgingArmed").and_then(|v| v.as_bool()) {
            if hedging {
                if let Some(trigger) = plan.get("hedgeTriggerMs").and_then(|v| v.as_u64()) {
                    println!("Hedging: armed (trigger: {trigger}ms)");
                } else {
                    println!("Hedging: armed");
                }
            } else {
                println!("Hedging: disabled");
            }
        }

        // Coalescing
        if let Some(coalescing) = plan.get("coalescingEligible").and_then(|v| v.as_bool()) {
            println!(
                "Coalescing: {}",
                if coalescing {
                    "eligible"
                } else {
                    "not eligible"
                }
            );
        }

        // Cache candidate
        if let Some(cache) = plan.get("cacheCandidate").and_then(|v| v.as_bool()) {
            println!("Cache candidate: {}", if cache { "yes" } else { "no" });
        }

        // Tenant affinity
        if let Some(affinity) = plan.get("tenantAffinityPinned").and_then(|v| v.as_object()) {
            println!(
                "Tenant affinity: {} → group {}",
                affinity
                    .get("tenant")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?"),
                affinity.get("group").and_then(|v| v.as_u64()).unwrap_or(0)
            );
        }

        // Estimated latency
        if let Some(latency) = plan.get("estimatedP95Ms").and_then(|v| v.as_f64()) {
            println!("Estimated p95 latency: {latency:.1}ms");
        }

        // Settings version
        if let Some(version) = plan.get("settingsVersion").and_then(|v| v.as_u64()) {
            println!("Settings version: {version}");
        }

        // Broadcast pending
        if let Some(pending) = plan.get("broadcastPending").and_then(|v| v.as_object()) {
            println!(
                "Broadcast pending: {} (commit in: {})",
                pending
                    .get("fingerprint")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?"),
                pending
                    .get("commitIn")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
            );
        }
    }

    // Warnings
    if let Some(warnings) = result.get("warnings").and_then(|v| v.as_array()) {
        if !warnings.is_empty() {
            println!("\n--- Warnings ---");
            for warning in warnings {
                if let Some(obj) = warning.as_object() {
                    if let Some(warning_type) = obj.get("type").and_then(|v| v.as_str()) {
                        match warning_type {
                            "UnfilterableAttribute" => {
                                println!(
                                    "  • Unfilterable attribute: {} ({})",
                                    obj.get("attribute").and_then(|v| v.as_str()).unwrap_or("?"),
                                    obj.get("suggestion")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("?")
                                );
                            }
                            "LargeOffsetLimit" => {
                                println!(
                                    "  • Large offset+limit: offset={}, limit={}, total={} ({})",
                                    obj.get("offset").and_then(|v| v.as_u64()).unwrap_or(0),
                                    obj.get("limit").and_then(|v| v.as_u64()).unwrap_or(0),
                                    obj.get("total").and_then(|v| v.as_u64()).unwrap_or(0),
                                    obj.get("suggestion")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("?")
                                );
                            }
                            "UnboundedWildcard" => {
                                println!(
                                    "  • Unbounded wildcard query: {}",
                                    obj.get("query").and_then(|v| v.as_str()).unwrap_or("?")
                                );
                            }
                            "SettingsDrift" => {
                                println!(
                                    "  • Settings drift detected: {}",
                                    obj.get("index").and_then(|v| v.as_str()).unwrap_or("?")
                                );
                            }
                            "TenantAffinityMismatch" => {
                                println!(
                                    "  • Tenant affinity mismatch: tenant={}, expected={}, actual={}",
                                    obj.get("tenant").and_then(|v| v.as_str()).unwrap_or("?"),
                                    obj.get("expected_group").and_then(|v| v.as_u64()).unwrap_or(0),
                                    obj.get("actual_group").and_then(|v| v.as_u64()).unwrap_or(0)
                                );
                            }
                            "NarrowingNotPossible" => {
                                println!(
                                    "  • Narrowing not possible: {}",
                                    obj.get("reason").and_then(|v| v.as_str()).unwrap_or("?")
                                );
                            }
                            "SettingsBroadcastInFlight" => {
                                println!(
                                    "  • Settings broadcast in flight: commit in {}",
                                    obj.get("commitIn").and_then(|v| v.as_str()).unwrap_or("?")
                                );
                            }
                            _ => {
                                println!(
                                    "  • {}",
                                    serde_json::to_string(warning).unwrap_or_default()
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    // Result (if execute=true)
    if let Some(result_value) = result.get("result") {
        println!("\n--- Execution Result ---");
        println!(
            "{}",
            serde_json::to_string_pretty(result_value).unwrap_or_default()
        );
    }

    println!();
}
