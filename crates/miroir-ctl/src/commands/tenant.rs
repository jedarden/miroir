use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum TenantSubcommand {
    /// Create a new tenant
    Create,
    /// List all tenants
    List,
    /// Delete a tenant
    Delete,
    /// Set tenant quota
    SetQuota,
}

pub async fn run(
    _cmd: TenantSubcommand,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
}
