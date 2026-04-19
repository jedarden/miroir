use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum AliasSubcommand {
    /// Create a new alias
    Create,
    /// Delete an alias
    Delete,
    /// List all aliases
    List,
}

pub async fn run(_cmd: AliasSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
