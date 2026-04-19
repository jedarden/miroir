use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum ExplainSubcommand {
    /// Explain a query plan or operation
    Query {
        /// Query or operation to explain
        query: String,
    },
}

pub async fn run(_cmd: ExplainSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
