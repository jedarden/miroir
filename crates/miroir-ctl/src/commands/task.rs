use clap::Subcommand;

#[derive(Subcommand, Debug)]
#[command(about = "Monitor and manage background tasks", after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/task.md\n\nSee `miroir-ctl help` for a list of all subcommands.")]
pub enum TaskSubcommand {
    /// Show all background tasks
    List,
    /// Show task status
    Status {
        /// Task ID
        #[arg(short, long)]
        id: Option<String>,
    },
    /// Cancel a task
    Cancel {
        /// Task ID
        id: String,
    },
}

pub async fn run(
    _cmd: TaskSubcommand,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
