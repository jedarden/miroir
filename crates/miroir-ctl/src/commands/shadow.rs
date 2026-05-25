use clap::Subcommand;

#[derive(Subcommand, Debug)]
#[command(about = "Manage shadow indexing", after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/shadow.md\n\nSee `miroir-ctl help` for a list of all subcommands.")]
pub enum ShadowSubcommand {
    /// Create a shadow index
    Create,
    /// Promote a shadow index to primary
    Promote,
    /// Delete a shadow index
    Delete,
    /// Show shadow index status
    Status,
}

pub async fn run(
    _cmd: ShadowSubcommand,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
