use clap::Subcommand;

#[derive(Subcommand, Debug)]
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

pub async fn run(_cmd: ShadowSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
