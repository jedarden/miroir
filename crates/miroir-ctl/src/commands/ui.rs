//! Search UI management commands.
//!
//! Implements plan §9 JWT signing-secret rotation via `rotate-jwt-secret`.

use clap::Subcommand;
use std::io::{self, Write};
use std::process::Command;

#[derive(Subcommand, Debug)]
#[command(
    about = "Launch the web UI",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/ui.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub enum UiSubcommand {
    /// Launch the web UI
    Launch {
        /// Port to listen on
        #[arg(short, long, default_value = "3000")]
        port: u16,
    },

    /// Rotate the JWT signing secret (zero-downtime, plan §9).
    ///
    /// Implements the 5-step dual-secret overlap rotation:
    ///   1. Generate a new 64-byte random secret
    ///   2. Set PREVIOUS = current primary, PRIMARY = new
    ///   3. Rolling restart — both secrets active during overlap
    ///   4. Wait session_ttl + buffer (default 20 min)
    ///   5. Remove PREVIOUS and rolling restart
    RotateJwtSecret(RotateJwtSecretArgs),
}

#[derive(clap::Parser, Debug)]
pub struct RotateJwtSecretArgs {
    /// Print the rotation plan without executing any changes.
    #[arg(long)]
    dry_run: bool,

    /// Kubernetes namespace containing the Miroir deployment.
    #[arg(long, default_value = "search")]
    namespace: String,

    /// Kubernetes Secret name containing JWT secrets.
    #[arg(long, default_value = "miroir-keys")]
    secret_name: String,

    /// Secret key for the primary JWT secret.
    #[arg(long, default_value = "searchUiJwtSecret")]
    primary_key: String,

    /// Secret key for the previous JWT secret.
    #[arg(long, default_value = "searchUiJwtSecretPrevious")]
    previous_key: String,

    /// Deployment name for rolling restart.
    #[arg(long, default_value = "miroir")]
    deployment_name: String,

    /// Session TTL in seconds (wait time before clearing PREVIOUS).
    #[arg(long, default_value = "900")]
    session_ttl_s: u64,

    /// Buffer seconds added to session TTL for the overlap wait.
    #[arg(long, default_value = "300")]
    buffer_s: u64,

    /// Skip confirmation prompts.
    #[arg(long)]
    yes: bool,
}

pub async fn run(
    cmd: UiSubcommand,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = (_admin_key, _api_url);
    match cmd {
        UiSubcommand::Launch { .. } => {
            Err("This command is not yet implemented. See bead miroir-qon for tracking.".into())
        }
        UiSubcommand::RotateJwtSecret(args) => rotate_jwt_secret(args).await,
    }
}

async fn rotate_jwt_secret(args: RotateJwtSecretArgs) -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Generate new secret
    let new_secret = generate_secret()?;
    eprintln!(
        "Generated new secret: {}...",
        &new_secret[..8.min(new_secret.len())]
    );

    // Step 2: Read current primary from K8s Secret
    let current_secret = read_secret_key(&args.namespace, &args.secret_name, &args.primary_key)?;

    if current_secret.is_empty() {
        return Err(format!(
            "Current JWT secret is empty in {}/{} key {} — cannot rotate",
            args.namespace, args.secret_name, args.primary_key
        )
        .into());
    }

    eprintln!(
        "Current primary: {}...",
        &current_secret[..8.min(current_secret.len())]
    );

    if args.dry_run {
        return print_dry_run(&args, &current_secret, &new_secret);
    }

    if !args.yes {
        print!("Proceed with JWT secret rotation? [y/N] ");
        io::stdout().flush()?;
        let mut buf = String::new();
        io::stdin().read_line(&mut buf)?;
        if !buf.trim().eq_ignore_ascii_case("y") {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    // Step 2: Set PREVIOUS = current primary, PRIMARY = new
    eprintln!("\nStep 2/5: Updating K8s Secret...");
    patch_secret_key(
        &args.namespace,
        &args.secret_name,
        &args.previous_key,
        &current_secret,
    )?;
    patch_secret_key(
        &args.namespace,
        &args.secret_name,
        &args.primary_key,
        &new_secret,
    )?;
    eprintln!("  Set {} = current primary (overlap)", args.previous_key);
    eprintln!("  Set {} = new secret", args.primary_key);

    // Step 3: Rolling restart — both secrets active
    eprintln!("\nStep 3/5: Triggering rolling restart...");
    rollout_restart(&args.namespace, &args.deployment_name)?;
    eprintln!("  Waiting for rollout to complete...");
    rollout_status(&args.namespace, &args.deployment_name)?;

    // Step 4: Wait session_ttl + buffer
    let wait_s = args.session_ttl_s + args.buffer_s;
    eprintln!(
        "\nStep 4/5: Waiting {}s (session_ttl={} + buffer={}) for old tokens to expire...",
        wait_s, args.session_ttl_s, args.buffer_s
    );
    tokio::time::sleep(std::time::Duration::from_secs(wait_s)).await;

    // Step 5: Remove PREVIOUS and rolling restart
    eprintln!(
        "\nStep 5/5: Clearing {} and restarting...",
        args.previous_key
    );
    remove_secret_key(&args.namespace, &args.secret_name, &args.previous_key)?;
    rollout_restart(&args.namespace, &args.deployment_name)?;
    eprintln!("  Waiting for rollout to complete...");
    rollout_status(&args.namespace, &args.deployment_name)?;

    eprintln!("\nJWT secret rotation complete.");
    Ok(())
}

fn generate_secret() -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("openssl")
        .args(["rand", "-base64", "64"])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "openssl rand failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn read_secret_key(
    namespace: &str,
    secret_name: &str,
    key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "get",
            "secret",
            secret_name,
            "-n",
            namespace,
            "-o",
            &format!("jsonpath={{.data.{key}}}"),
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // If the key doesn't exist, return empty
        if stderr.contains("not found") || output.stdout.is_empty() {
            return Ok(String::new());
        }
        return Err(format!("kubectl get secret failed: {stderr}").into());
    }
    let b64 = String::from_utf8(output.stdout)?;
    if b64.is_empty() {
        return Ok(String::new());
    }
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    let decoded = BASE64.decode(b64.trim())?;
    Ok(String::from_utf8(decoded)?)
}

fn patch_secret_key(
    namespace: &str,
    secret_name: &str,
    key: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let patch = format!("{{\"stringData\":{{\"{key}\":\"{value}\"}}}}");
    let output = Command::new("kubectl")
        .args([
            "patch",
            "secret",
            secret_name,
            "-n",
            namespace,
            "-p",
            &patch,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl patch secret failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

fn remove_secret_key(
    namespace: &str,
    secret_name: &str,
    key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let patch = format!("{{\"stringData\":{{\"{key}\":null}}}}");
    let output = Command::new("kubectl")
        .args([
            "patch",
            "secret",
            secret_name,
            "-n",
            namespace,
            "--type",
            "merge",
            "-p",
            &patch,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl patch secret (remove) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

fn rollout_restart(namespace: &str, deployment: &str) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "rollout",
            "restart",
            "deployment",
            deployment,
            "-n",
            namespace,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl rollout restart failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

fn rollout_status(namespace: &str, deployment: &str) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "rollout",
            "status",
            "deployment",
            deployment,
            "-n",
            namespace,
            "--timeout",
            "300s",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl rollout status failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

fn print_dry_run(
    args: &RotateJwtSecretArgs,
    current_secret: &str,
    new_secret: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== JWT Secret Rotation Plan (dry-run) ===\n");
    println!("Secret: {}/{}", args.namespace, args.secret_name);
    println!(
        "Current primary: {}...",
        &current_secret[..8.min(current_secret.len())]
    );
    println!(
        "New primary:     {}...",
        &new_secret[..8.min(new_secret.len())]
    );
    println!("\nSteps:");
    println!("  1. Generated new 64-byte random secret (done)");
    println!(
        "  2. kubectl patch secret {}/{}:",
        args.namespace, args.secret_name
    );
    println!(
        "       {} = current primary (overlap window)",
        args.previous_key
    );
    println!("       {} = new secret", args.primary_key);
    println!(
        "  3. kubectl -n {} rollout restart deployment/{}",
        args.namespace, args.deployment_name
    );
    println!("     Both old and new tokens validate — zero downtime");
    println!(
        "  4. Wait {}s (session_ttl={} + buffer={})",
        args.session_ttl_s + args.buffer_s,
        args.session_ttl_s,
        args.buffer_s
    );
    println!(
        "  5. Remove {} from secret, rollout restart again",
        args.previous_key
    );
    println!("\nLeak response (manual):");
    println!(
        "  kubectl patch secret {}/{} -p '{{\\\"stringData\\\":{{\\\"{}\\\":\\\"\\\"}}}}'",
        args.namespace, args.secret_name, args.previous_key
    );
    println!(
        "  kubectl -n {} rollout restart deployment/{}",
        args.namespace, args.deployment_name
    );
    println!("  This immediately invalidates all old tokens.");
    Ok(())
}
