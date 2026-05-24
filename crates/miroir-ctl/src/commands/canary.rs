use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum CanarySubcommand {
    /// Create a canary deployment
    Create,
    /// Promote a canary to primary
    Promote,
    /// Rollback a canary
    Rollback,
    /// Show canary status
    Status,
}

pub async fn run(
    _cmd: CanarySubcommand,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
