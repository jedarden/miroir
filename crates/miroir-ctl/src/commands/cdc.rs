use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum CdcSubcommand {
    /// Create a CDC subscription
    Create,
    /// List CDC subscriptions
    List,
    /// Delete a CDC subscription
    Delete,
}

pub async fn run(_cmd: CdcSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
