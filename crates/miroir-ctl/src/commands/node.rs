use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum NodeSubcommand {
    /// Add a new node to the cluster
    Add,
    /// Remove a node from the cluster
    Remove,
    /// List all nodes
    List,
}

pub async fn run(_cmd: NodeSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
