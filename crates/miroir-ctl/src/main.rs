use clap::{Parser, Subcommand};
use credentials::load_admin_key;

mod commands;
mod credentials;

#[derive(Parser)]
#[command(name = "miroir-ctl")]
#[command(author, version, about)]
#[command(
    long_about = "Miroir management CLI

Runbook documentation for each subcommand is available at:
  https://github.com/jedarden/miroir/tree/main/docs/ctl/

For local docs, see docs/ctl/*.md in the repository.",
    long_version = option_env!("GIT_VERSION").unwrap_or_else(|| env!("CARGO_PKG_VERSION"))
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Admin API key for authentication.
    ///
    /// WARNING: This flag's value is visible in your shell history and the process list
    /// (ps, top, etc.). For production use, prefer the MIROIR_ADMIN_API_KEY environment
    /// variable or ~/.config/miroir/credentials file.
    #[arg(long, global = true)]
    admin_key: Option<String>,

    /// API endpoint URL (default: http://localhost:8080)
    #[arg(long, global = true)]
    api_url: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show cluster status and health
    Status(commands::status::StatusSubcommand),

    /// Manage cluster nodes
    #[command(subcommand)]
    Node(commands::node::NodeSubcommand),

    /// Manage rebalancing operations
    #[command(subcommand)]
    Rebalance(commands::rebalance::RebalanceSubcommand),

    /// Manage resharding operations
    #[command(subcommand)]
    Reshard(commands::reshard::ReshardSubcommand),

    /// Verify data integrity
    #[command(subcommand)]
    Verify(commands::verify::VerifySubcommand),

    /// Monitor and manage background tasks
    #[command(subcommand)]
    Task(commands::task::TaskSubcommand),

    /// Dump and inspect data
    #[command(subcommand)]
    Dump(commands::dump::DumpSubcommand),

    /// Manage key aliases
    #[command(subcommand)]
    Alias(commands::alias::AliasSubcommand),

    /// Manage canary deployments
    #[command(subcommand)]
    Canary(commands::canary::CanarySubcommand),

    /// Manage TTL policies
    #[command(subcommand)]
    Ttl(commands::ttl::TtlSubcommand),

    /// Manage change data capture
    #[command(subcommand)]
    Cdc(commands::cdc::CdcSubcommand),

    /// Manage shadow indexing
    #[command(subcommand)]
    Shadow(commands::shadow::ShadowSubcommand),

    /// Launch the web UI
    #[command(subcommand)]
    Ui(commands::ui::UiSubcommand),

    /// Manage multi-tenancy
    #[command(subcommand)]
    Tenant(commands::tenant::TenantSubcommand),

    /// Explain query plans and operations
    #[command(subcommand)]
    Explain(commands::explain::ExplainSubcommand),

    /// Manage Meilisearch keys
    #[command(subcommand)]
    Key(commands::key::KeySubcommand),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Load admin API key following priority order:
    // 1. MIROIR_ADMIN_API_KEY env var
    // 2. ~/.config/miroir/credentials
    // 3. --admin-key flag
    let admin_key =
        load_admin_key(cli.admin_key).map_err(|e| format!("Failed to load credentials: {e}"))?;

    if admin_key.is_none() {
        eprintln!("Error: No admin API key found.");
        eprintln!("Set one of:");
        eprintln!("  1. MIROIR_ADMIN_API_KEY environment variable");
        eprintln!("  2. ~/.config/miroir/credentials file with [default].admin_api_key");
        eprintln!("  3. --admin-key flag (WARNING: visible in process list)");
        std::process::exit(1);
    }

    let admin_key = admin_key.unwrap();
    let api_url = cli
        .api_url
        .unwrap_or_else(|| "http://localhost:8080".to_string());

    match cli.command {
        Commands::Status(cmd) => commands::status::run(cmd, &admin_key, &api_url).await,
        Commands::Node(cmd) => commands::node::run(cmd, &admin_key, &api_url).await,
        Commands::Rebalance(cmd) => commands::rebalance::run(cmd, &admin_key, &api_url).await,
        Commands::Reshard(cmd) => commands::reshard::run(cmd, &admin_key, &api_url).await,
        Commands::Verify(cmd) => commands::verify::run(cmd, &admin_key, &api_url).await,
        Commands::Task(cmd) => commands::task::run(cmd, &admin_key, &api_url).await,
        Commands::Dump(cmd) => commands::dump::run(cmd, &admin_key, &api_url).await,
        Commands::Alias(cmd) => commands::alias::run(cmd, &admin_key, &api_url).await,
        Commands::Canary(cmd) => commands::canary::run(cmd, &admin_key, &api_url).await,
        Commands::Ttl(cmd) => commands::ttl::run(cmd, &admin_key, &api_url).await,
        Commands::Cdc(cmd) => commands::cdc::run(cmd, &admin_key, &api_url).await,
        Commands::Shadow(cmd) => commands::shadow::run(cmd, &admin_key, &api_url).await,
        Commands::Ui(cmd) => commands::ui::run(cmd, &admin_key, &api_url).await,
        Commands::Tenant(cmd) => commands::tenant::run(cmd, &admin_key, &api_url).await,
        Commands::Explain(cmd) => commands::explain::run(cmd, &admin_key, &api_url).await,
        Commands::Key(cmd) => commands::key::run(cmd, &admin_key, &api_url).await,
    }
}
