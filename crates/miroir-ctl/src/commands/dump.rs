use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum DumpSubcommand {
    /// Dump data for a key or prefix
    Keys {
        /// Key or prefix to dump
        #[arg(short, long)]
        prefix: Option<String>,
    },
}

pub async fn run(_cmd: DumpSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
