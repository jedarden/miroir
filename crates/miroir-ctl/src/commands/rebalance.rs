use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum RebalanceSubcommand {
    /// Show rebalancing status
    Status {
        /// Watch mode: continuously refresh status
        #[arg(short, long)]
        watch: bool,
    },
    /// Start a rebalance operation
    Start,
    /// Cancel an active rebalance
    Cancel,
}

pub async fn run(_cmd: RebalanceSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
