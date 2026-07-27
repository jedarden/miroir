use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs;

/// Doctor subcommands for diagnosing Miroir deployment and runtime issues
#[derive(Subcommand, Debug)]
#[command(
    about = "Diagnose deployment and runtime issues",
    after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/doctor.md\n\nSee `miroir-ctl help` for a list of all subcommands."
)]
pub enum DoctorSubcommand {
    #[command(
        about = "Run pre-flight checks before deploying (checks chart source, secrets, and task store)",
        after_help = "Runbooks: https://github.com/jedarden/miroir/blob/main/docs/ctl/doctor.md#deploy-preflight\n\nSee `miroir-ctl help` for a list of all subcommands."
    )]
    DeployPreflight(DeployPreflightArgs),
}

/// Arguments for the deploy-preflight check
#[derive(Parser, Debug)]
pub struct DeployPreflightArgs {
    /// Path to environment configuration file (Helm values.yaml or ArgoCD Application YAML)
    #[arg(short, long)]
    config: PathBuf,

    /// Kubernetes context to use for secret checks (defaults to current context)
    #[arg(long)]
    context: Option<String>,

    /// Kubernetes namespace to check for secrets
    #[arg(short, long)]
    namespace: Option<String>,

    /// Exit with non-zero status on any check failure
    #[arg(short, long)]
    strict: bool,
}

/// Result of a single pre-flight check
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckResult {
    check_name: String,
    status: CheckStatus,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

impl CheckResult {
    fn pass(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            check_name: name.into(),
            status: CheckStatus::Pass,
            message: message.into(),
            detail: None,
        }
    }

    fn fail(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            check_name: name.into(),
            status: CheckStatus::Fail,
            message: message.into(),
            detail: None,
        }
    }

    fn warn(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            check_name: name.into(),
            status: CheckStatus::Warn,
            message: message.into(),
            detail: None,
        }
    }

    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CheckStatus {
    Pass,
    Fail,
    Warn,
}

impl CheckStatus {
    fn emoji(self) -> &'static str {
        match self {
            CheckStatus::Pass => "✓",
            CheckStatus::Fail => "✗",
            CheckStatus::Warn => "⚠",
        }
    }

    fn color(self) -> &'static str {
        match self {
            CheckStatus::Pass => "green",
            CheckStatus::Fail => "red",
            CheckStatus::Warn => "yellow",
        }
    }
}

/// Environment configuration extracted from Helm values or ArgoCD Application
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnvConfig {
    #[serde(default)]
    miroir: Option<MiroirValues>,
    #[serde(default)]
    task_store: Option<TaskStoreValues>,
    #[serde(default)]
    eso: Option<EsoValues>,
    #[serde(default)]
    existing_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MiroirValues {
    #[serde(default)]
    existing_secret: Option<String>,
    #[serde(default)]
    replicas: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskStoreValues {
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EsoValues {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    secret_store_ref: Option<SecretStoreRef>,
    #[serde(default)]
    secret_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SecretStoreRef {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    kind: Option<String>,
}

/// ArgoCD Application spec
#[derive(Debug, Deserialize)]
struct ArgoCDApplication {
    spec: ApplicationSpec,
}

#[derive(Debug, Deserialize)]
struct ApplicationSpec {
    source: ApplicationSource,
    #[serde(default)]
    destination: Option<ApplicationDestination>,
}

#[derive(Debug, Deserialize)]
struct ApplicationSource {
    #[serde(default)]
    repo_url: Option<String>,
    #[serde(default)]
    chart: Option<String>,
    #[serde(default)]
    target_revision: Option<String>,
    #[serde(default)]
    helm: Option<ApplicationHelm>,
}

#[derive(Debug, Deserialize)]
struct ApplicationHelm {
    #[serde(default)]
    values: Option<serde_yaml::Value>,
    #[serde(default)]
    values_object: Option<EnvConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApplicationDestination {
    #[serde(default)]
    namespace: Option<String>,
}

pub async fn run(
    cmd: DoctorSubcommand,
    admin_key: &str,
    api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        DoctorSubcommand::DeployPreflight(args) => {
            run_deploy_preflight(args, admin_key, api_url).await
        }
    }
}

async fn run_deploy_preflight(
    args: DeployPreflightArgs,
    _admin_key: &str,
    _api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    println!("=== Miroir Deploy Preflight Checks ===");
    println!();

    // Load the configuration file
    let config_content = fs::read_to_string(&args.config).await
        .map_err(|e| format!("Failed to read config file {}: {}", args.config.display(), e))?;

    // Determine if it's an ArgoCD Application or Helm values file
    let (env_config, chart_source_info, namespace) = if config_content.contains("kind: Application")
        && config_content.contains("argoproj.io/v1alpha1") {
        // It's an ArgoCD Application
        let app: ArgoCDApplication = serde_yaml::from_str(&config_content)
            .map_err(|e| format!("Failed to parse ArgoCD Application: {}", e))?;

        let ns = args.namespace
            .or_else(|| app.spec.destination.as_ref().and_then(|d| d.namespace.clone()))
            .clone()
            .unwrap_or_else(|| "miroir".to_string());

        let chart_info = if let Some(chart) = app.spec.source.chart {
            let repo_url = app.spec.source.repo_url
                .unwrap_or_else(|| "unknown".to_string());
            let version = app.spec.source.target_revision
                .unwrap_or_else(|| "latest".to_string());
            Some(ChartSourceInfo::HelmRepo {
                repo_url,
                chart_name: chart,
                version,
            })
        } else {
            None
        };

        let env_cfg = app.spec.source.helm
            .and_then(|h| h.values_object)
            .unwrap_or_else(|| EnvConfig {
                miroir: None,
                task_store: None,
                eso: None,
                existing_secret: None,
            });

        (env_cfg, chart_info, ns)
    } else {
        // Assume it's a Helm values file
        let env_cfg: EnvConfig = serde_yaml::from_str(&config_content)
            .map_err(|e| format!("Failed to parse Helm values: {}", e))?;

        let ns = args.namespace
            .clone()
            .unwrap_or_else(|| "miroir".to_string());

        (env_cfg, None, ns)
    };

    let mut results = vec![];

    // Check 1: Chart source reachability
    println!("Check 1: Chart Source");
    println!("---------------------");
    if let Some(chart_info) = chart_source_info {
        match check_chart_source(&http_client, &chart_info).await {
            Ok(result) => {
                println!("  {} {}", result.status.emoji(), result.message);
                if let Some(detail) = &result.detail {
                    println!("     {}", detail);
                }
                results.push(result);
            }
            Err(e) => {
                let result = CheckResult::fail("chart_source",
                    format!("Failed to check chart source: {}", e));
                println!("  {} {}", result.status.emoji(), result.message);
                results.push(result);
            }
        }
    } else {
        let result = CheckResult::warn("chart_source",
            "No chart source information found in config (skipped)");
        println!("  {} {}", result.status.emoji(), result.message);
        results.push(result);
    }
    println!();

    // Check 2: Secret/ExternalSecret sync status
    println!("Check 2: Secret Sync Status");
    println!("----------------------------");
    let secret_name = env_config.miroir
        .as_ref()
        .and_then(|m| m.existing_secret.clone())
        .or_else(|| env_config.existing_secret.clone())
        .unwrap_or_else(|| "miroir-keys".to_string());

    if env_config.eso.as_ref().and_then(|e| e.enabled).unwrap_or(false) {
        // Check ExternalSecret
        match check_external_secret_sync(&secret_name, &args.context, &namespace).await {
            Ok(result) => {
                println!("  {} {}", result.status.emoji(), result.message);
                if let Some(detail) = &result.detail {
                    println!("     {}", detail);
                }
                results.push(result);
            }
            Err(e) => {
                let result = CheckResult::fail("external_secret_sync",
                    format!("Failed to check ExternalSecret: {}", e));
                println!("  {} {}", result.status.emoji(), result.message);
                results.push(result);
            }
        }
    } else {
        // Check regular Secret
        match check_secret_exists(&secret_name, &args.context, &namespace).await {
            Ok(result) => {
                println!("  {} {}", result.status.emoji(), result.message);
                if let Some(detail) = &result.detail {
                    println!("     {}", detail);
                }
                results.push(result);
            }
            Err(e) => {
                let result = CheckResult::fail("secret_exists",
                    format!("Failed to check Secret: {}", e));
                println!("  {} {}", result.status.emoji(), result.message);
                results.push(result);
            }
        }
    }
    println!();

    // Check 3: Task store reachability
    println!("Check 3: Task Store Reachability");
    println!("---------------------------------");
    let task_store = env_config.task_store.as_ref();
    match check_task_store(task_store).await {
        Ok(result) => {
            println!("  {} {}", result.status.emoji(), result.message);
            if let Some(detail) = &result.detail {
                println!("     {}", detail);
            }
            results.push(result);
        }
        Err(e) => {
            let result = CheckResult::fail("task_store",
                format!("Failed to check task store: {}", e));
            println!("  {} {}", result.status.emoji(), result.message);
            results.push(result);
        }
    }
    println!();

    // Print summary
    println!("=== Summary ===");
    let passed = results.iter().filter(|r| r.status == CheckStatus::Pass).count();
    let failed = results.iter().filter(|r| r.status == CheckStatus::Fail).count();
    let warned = results.iter().filter(|r| r.status == CheckStatus::Warn).count();
    let total = results.len();

    println!("Total checks: {}", total);
    println!("  Passed: {}", passed);
    println!("  Failed: {}", failed);
    println!("  Warnings: {}", warned);
    println!();

    if args.strict && failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

#[derive(Debug)]
enum ChartSourceInfo {
    HelmRepo {
        repo_url: String,
        chart_name: String,
        version: String,
    },
    Oci {
        registry: String,
        chart_name: String,
        version: String,
    },
}

async fn check_chart_source(
    client: &Client,
    chart_info: &ChartSourceInfo,
) -> Result<CheckResult, Box<dyn std::error::Error>> {
    match chart_info {
        ChartSourceInfo::HelmRepo { repo_url, chart_name, version } => {
            // Try to fetch chart metadata from Helm repo
            let index_url = format!("{}/index.yaml", repo_url.trim_end_matches('/'));

            let response = client.get(&index_url).send().await?;

            if response.status().is_success() {
                let body = response.text().await?;
                // Parse index.yaml and check for chart/version
                if body.contains(chart_name) && body.contains(version) {
                    Ok(CheckResult::pass("chart_source",
                        format!("Helm repo reachable, chart {} version {} found", chart_name, version)))
                } else if body.contains(chart_name) {
                    Ok(CheckResult::fail("chart_source",
                        format!("Chart {} found but version {} not available in repo", chart_name, version)))
                } else {
                    Ok(CheckResult::fail("chart_source",
                        format!("Chart {} not found in repo {}", chart_name, repo_url)))
                }
            } else {
                Ok(CheckResult::fail("chart_source",
                    format!("Failed to reach Helm repo {}: HTTP {}", repo_url, response.status())))
            }
        }
        ChartSourceInfo::Oci { registry, chart_name, version } => {
            // For OCI, we'd typically use `helm pull` or `crane` commands
            // For now, check if the registry is reachable
            let check_url = format!("{}/v2/", registry.trim_end_matches('/'));

            let response = client.get(&check_url).send().await?;

            if response.status().is_success() {
                Ok(CheckResult::pass("chart_source",
                    format!("OCI registry {} reachable (chart {} version {} not verified - requires helm/crane)",
                            registry, chart_name, version)))
            } else {
                Ok(CheckResult::fail("chart_source",
                    format!("Failed to reach OCI registry {}: HTTP {}", registry, response.status())))
            }
        }
    }
}

async fn check_secret_exists(
    secret_name: &str,
    _context: &Option<String>,
    namespace: &str,
) -> Result<CheckResult, Box<dyn std::error::Error>> {
    // Use kubectl to check if secret exists
    // Note: This requires kubectl to be installed and configured
    let output = tokio::process::Command::new("kubectl")
        .args([
            "get", "secret", secret_name,
            "-n", namespace,
            "--ignore-not-found",
        ])
        .output()
        .await?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.contains(secret_name) {
            Ok(CheckResult::pass("secret_exists",
                format!("Secret {} exists in namespace {}", secret_name, namespace)))
        } else {
            Ok(CheckResult::fail("secret_exists",
                format!("Secret {} not found in namespace {}", secret_name, namespace)))
        }
    } else {
        Ok(CheckResult::fail("secret_exists",
            format!("Failed to check secret {} in namespace {}: kubectl error", secret_name, namespace)))
    }
}

async fn check_external_secret_sync(
    secret_name: &str,
    _context: &Option<String>,
    namespace: &str,
) -> Result<CheckResult, Box<dyn std::error::Error>> {
    // First check if ExternalSecret exists
    let eso_output = tokio::process::Command::new("kubectl")
        .args([
            "get", "externalsecret", secret_name,
            "-n", namespace,
            "--ignore-not-found",
        ])
        .output()
        .await?;

    if !eso_output.status.success() || !String::from_utf8_lossy(&eso_output.stdout).contains(secret_name) {
        return Ok(CheckResult::fail("external_secret_sync",
            format!("ExternalSecret {} not found in namespace {}", secret_name, namespace)));
    }

    // Check the synced Secret status
    let secret_output = tokio::process::Command::new("kubectl")
        .args([
            "get", "secret", secret_name,
            "-n", namespace,
            "--ignore-not-found",
        ])
        .output()
        .await?;

    if secret_output.status.success() {
        let stdout = String::from_utf8_lossy(&secret_output.stdout);
        if stdout.contains(secret_name) {
            Ok(CheckResult::pass("external_secret_sync",
                format!("ExternalSecret {} synced successfully in namespace {}", secret_name, namespace)))
        } else {
            Ok(CheckResult::fail("external_secret_sync",
                format!("ExternalSecret {} exists but target Secret not synced in namespace {}", secret_name, namespace)))
        }
    } else {
        Ok(CheckResult::warn("external_secret_sync",
            format!("ExternalSecret {} exists but sync status unknown in namespace {}", secret_name, namespace)))
    }
}

async fn check_task_store(
    task_store: Option<&TaskStoreValues>,
) -> Result<CheckResult, Box<dyn std::error::Error>> {
    let store = match task_store {
        Some(s) => s,
        None => {
            return Ok(CheckResult::warn("task_store", "No task_store configuration found"))
        }
    };

    let backend = store.backend.as_deref().unwrap_or("sqlite");

    match backend {
        "redis" => {
            let url = match &store.url {
                Some(u) => u,
                None => {
                    return Ok(CheckResult::fail("task_store", "Redis backend configured but no URL provided"))
                }
            };

            // Try to connect to Redis
            // Parse Redis URL to extract host and port
            let url_parts: Vec<&str> = url.split("://").collect();
            if url_parts.len() < 2 {
                return Ok(CheckResult::fail("task_store",
                    format!("Invalid Redis URL format: {}", url)));
            }

            let host_port = url_parts[1].split('/').next().unwrap_or("");
            let (host, port) = if host_port.contains(':') {
                let parts: Vec<&str> = host_port.split(':').collect();
                (parts[0], parts[1])
            } else {
                (host_port, "6379")
            };

            // Try a simple TCP connection to verify reachability
            use tokio::net::TcpStream;
            match TcpStream::connect((host, port.parse::<u16>().unwrap_or(6379))).await {
                Ok(_) => {
                    Ok(CheckResult::pass("task_store",
                        format!("Redis at {} reachable (authentication not verified)", url)))
                }
                Err(e) => {
                    Ok(CheckResult::fail("task_store",
                        format!("Redis at {} not reachable: {}", url, e)))
                }
            }
        }
        "sqlite" => {
            let path = match &store.path {
                Some(p) => p,
                None => {
                    return Ok(CheckResult::fail("task_store", "SQLite backend configured but no path provided"))
                }
            };

            // Check if the directory exists and is writable
            let path_obj = std::path::Path::new(path);
            if let Some(parent) = path_obj.parent() {
                if parent.exists() {
                    // Check if writable by trying to create a temp file
                    let test_path = parent.join(".miroir-doctor-test");
                    match std::fs::File::create(&test_path) {
                        Ok(_) => {
                            std::fs::remove_file(&test_path).ok();
                            Ok(CheckResult::pass("task_store",
                                format!("SQLite path {} directory exists and is writable", path)))
                        }
                        Err(e) => {
                            Ok(CheckResult::fail("task_store",
                                format!("SQLite path {} directory exists but not writable: {}", path, e)))
                        }
                    }
                } else {
                    Ok(CheckResult::fail("task_store",
                        format!("SQLite path {} directory does not exist", path)))
                }
            } else {
                Ok(CheckResult::fail("task_store",
                    format!("Invalid SQLite path: {}", path)))
            }
        }
        _ => {
            Ok(CheckResult::warn("task_store",
                format!("Unknown task store backend: {}", backend)))
        }
    }
}
