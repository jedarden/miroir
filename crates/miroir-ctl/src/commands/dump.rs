use clap::Subcommand;
use reqwest::Client;

#[derive(Subcommand, Debug)]
pub enum DumpSubcommand {
    /// Import a Meilisearch dump file into Miroir
    ///
    /// Imports use streaming mode by default, routing documents via the public API.
    /// Falls back to broadcast mode for incompatible dump variants.
    ///
    /// See compatibility matrix: docs/dump-import/compatibility-matrix.md
    Import {
        /// Path to the .dump file
        #[arg(short, long)]
        file: String,

        /// Target index UID (required for single-index dumps)
        #[arg(short, long)]
        index: String,

        /// Primary key field name
        #[arg(short, long)]
        primary_key: String,

        /// Number of shards for the index
        #[arg(long, default_value = "64")]
        shard_count: u32,

        /// Import mode: 'streaming' (default) or 'broadcast' (legacy)
        ///
        /// Streaming routes documents per-shard for optimal storage distribution.
        /// Broadcast sends all documents to all nodes, requiring post-import rebalance.
        #[arg(short, long, default_value = "streaming")]
        mode: String,

        /// Batch size for document streaming (documents per POST per target node)
        #[arg(long, default_value = "1000")]
        batch_size: usize,

        /// Maximum concurrent in-flight POSTs across target nodes
        #[arg(long, default_value = "8")]
        parallel_writes: usize,
    },

    /// Export data from Miroir to a dump file
    ///
    /// Creates a Meilisearch-compatible dump by fan-out collection and merge.
    Export {
        /// Output file path (.dump extension recommended)
        #[arg(short, long)]
        output: String,

        /// Index UID to export (omit for all indexes)
        #[arg(short, long)]
        index: Option<String>,

        /// Include task history in dump
        #[arg(long, default_value = "false")]
        include_tasks: bool,
    },

    /// Analyze a dump file for compatibility with streaming import mode
    ///
    /// Scans the dump and reports whether streaming mode can fully reconstruct it,
    /// or if broadcast fallback is required. References the compatibility matrix.
    Analyze {
        /// Path to the .dump file to analyze
        #[arg(short, long)]
        file: String,
    },

    /// Show the status of a dump import
    Status {
        /// Import task ID
        #[arg(short, long)]
        id: String,
    },
}

pub async fn run(
    cmd: DumpSubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        DumpSubcommand::Import {
            file,
            index,
            primary_key,
            shard_count,
            mode,
            ..
        } => {
            let client = Client::new();

            // Read the dump file
            let dump_data = std::fs::read_to_string(&file)?;

            // Build the request
            let request_body = serde_json::json!({
                "index_uid": index,
                "primary_key": primary_key,
                "shard_count": shard_count,
                "dump_data": dump_data,
            });

            let url = format!("{}/_miroir/dumps/import", api_url.trim_end_matches('/'));

            let response = client
                .post(&url)
                .header("Authorization", format!("Bearer {}", admin_key))
                .json(&request_body)
                .send()
                .await?;

            if response.status().is_success() {
                let result: serde_json::Value = response.json().await?;
                let task_id = result["miroir_task_id"]
                    .as_str()
                    .ok_or("missing miroir_task_id")?;

                println!("Dump import started successfully!");
                println!("Import ID: {}", task_id);
                println!(
                    "Status URL: {}/_miroir/dumps/import/{}/status",
                    api_url.trim_end_matches('/'),
                    task_id
                );
                println!("\nMode: {}", mode);
                println!("Index: {}", index);
                println!("Primary key: {}", primary_key);
                println!("Shard count: {}", shard_count);
                println!("\nTo check status, run:");
                println!("  miroir-ctl dump status --id {}", task_id);
            } else {
                let error = response.text().await?;
                Err(format!("Dump import failed: {}", error).into())
            }
        }

        DumpSubcommand::Export { output, index, .. } => Err(format!(
            "Dump export is not yet implemented. See bead miroir-qon for tracking.\n\n\
                 Requested:\n\
                 Output: {}\n\
                 Index: {:?}",
            output, index
        )
        .into()),

        DumpSubcommand::Analyze { file } => Err(format!(
            "Dump analysis is not yet implemented. See bead miroir-zc2.5 for tracking.\n\n\
                 This command will analyze {} and report:\n\
                 - Whether streaming mode can reconstruct the dump\n\
                 - Any field conflicts (e.g., existing `_miroir_shard`)\n\
                 - Meilisearch version compatibility\n\
                 - Recommended import mode\n\n\
                 See compatibility matrix:\n\
                 docs/dump-import/compatibility-matrix.md",
            file
        )
        .into()),

        DumpSubcommand::Status { id } => {
            let client = Client::new();

            let url = format!(
                "{}/_miroir/dumps/import/{}/status",
                api_url.trim_end_matches('/'),
                id
            );

            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", admin_key))
                .send()
                .await?;

            if response.status().is_success() {
                let status: serde_json::Value = response.json().await?;

                println!("Dump Import Status");
                println!("==================");
                println!(
                    "ID: {}",
                    status["id"].as_str().unwrap_or(&serde_json::Value::Null)
                );
                println!(
                    "Index: {}",
                    status["index_uid"]
                        .as_str()
                        .unwrap_or(&serde_json::Value::Null)
                );
                println!(
                    "Phase: {}",
                    status["phase"].as_str().unwrap_or(&serde_json::Value::Null)
                );
                println!(
                    "Documents Processed: {}",
                    status["documents_processed"].as_u64().unwrap_or(0)
                );
                println!(
                    "Total Documents: {}",
                    status["total_documents"].as_u64().unwrap_or(0)
                );
                println!("Bytes Read: {}", status["bytes_read"].as_u64().unwrap_or(0));

                if let Some(error) = status["error"].as_str() {
                    println!("Error: {}", error);
                }

                Ok(())
            } else {
                let error = response.text().await?;
                Err(format!("Failed to get status: {}", error).into())
            }
        }
    }
}
