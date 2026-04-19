use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum TtlSubcommand {
    /// Set a TTL policy
    Set,
    /// Get TTL policy for a key
    Get,
    /// Remove a TTL policy
    Remove,
}

pub async fn run(_cmd: TtlSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
