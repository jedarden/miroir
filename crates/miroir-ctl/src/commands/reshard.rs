use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum ReshardSubcommand {
    /// Start a reshard operation
    Start,
    /// Show reshard status
    Status,
    /// Cancel an active reshard
    Cancel,
}

pub async fn run(_cmd: ReshardSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
