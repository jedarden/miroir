use clap::{Parser, Subcommand};
use credentials::load_admin_key;

mod commands;
mod credentials;

#[derive(Parser)]
#[command(name = "miroir-ctl")]
#[command(about = "Miroir management CLI", long_about = None)]
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Load admin API key following priority order:
    // 1. MIROIR_ADMIN_API_KEY env var
    // 2. ~/.config/miroir/credentials
    // 3. --admin-key flag
    let admin_key =
        load_admin_key(cli.admin_key).map_err(|e| format!("Failed to load credentials: {}", e))?;

    if admin_key.is_none() {
        eprintln!("Error: No admin API key found.");
        eprintln!("Set one of:");
        eprintln!("  1. MIROIR_ADMIN_API_KEY environment variable");
        eprintln!("  2. ~/.config/miroir/credentials file with [default].admin_api_key");
        eprintln!("  3. --admin-key flag (WARNING: visible in process list)");
        std::process::exit(1);
    }

    // TODO: Use admin_key for API authentication when commands are implemented
    let _admin_key = admin_key.unwrap();

    match cli.command {
        Commands::Status(cmd) => commands::status::run(cmd).await,
        Commands::Node(cmd) => commands::node::run(cmd).await,
        Commands::Rebalance(cmd) => commands::rebalance::run(cmd).await,
        Commands::Reshard(cmd) => commands::reshard::run(cmd).await,
        Commands::Verify(cmd) => commands::verify::run(cmd).await,
        Commands::Task(cmd) => commands::task::run(cmd).await,
        Commands::Dump(cmd) => commands::dump::run(cmd).await,
        Commands::Alias(cmd) => commands::alias::run(cmd).await,
        Commands::Canary(cmd) => commands::canary::run(cmd).await,
        Commands::Ttl(cmd) => commands::ttl::run(cmd).await,
        Commands::Cdc(cmd) => commands::cdc::run(cmd).await,
        Commands::Shadow(cmd) => commands::shadow::run(cmd).await,
        Commands::Ui(cmd) => commands::ui::run(cmd).await,
        Commands::Tenant(cmd) => commands::tenant::run(cmd).await,
        Commands::Explain(cmd) => commands::explain::run(cmd).await,
    }
}
