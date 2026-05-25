use clap::Subcommand;

#[derive(Subcommand, Debug)]
#[command(
    about = "Manage change data capture",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/cdc.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub enum CdcSubcommand {
    /// Create a CDC subscription
    Create,
    /// List CDC subscriptions
    List,
    /// Delete a CDC subscription
    Delete,
}

pub async fn run(
    _cmd: CdcSubcommand,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
