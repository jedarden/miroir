use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum UiSubcommand {
    /// Launch the web UI
    Launch {
        /// Port to listen on
        #[arg(short, long, default_value = "3000")]
        port: u16,
    },
}

pub async fn run(_cmd: UiSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
