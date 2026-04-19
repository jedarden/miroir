use clap::Parser;

#[derive(Parser, Debug)]
pub struct StatusSubcommand {
    /// Watch mode: continuously refresh status
    #[arg(short, long)]
    watch: bool,
}

pub async fn run(_cmd: StatusSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
