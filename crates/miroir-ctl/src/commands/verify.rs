use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum VerifySubcommand {
    /// Verify data integrity for a key prefix
    Check {
        /// Key prefix to verify
        #[arg(short, long)]
        prefix: Option<String>,
    },
}

pub async fn run(
    _cmd: VerifySubcommand,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
