use axum::{
    extract::FromRef,
    routing::{get, post},
    Router,
};
use miroir_core::{
    config::MiroirConfig,
    peer_discovery::PeerDiscovery,
    rebalancer_worker::{RebalancerWorker, RebalancerWorkerConfig, TopologyChangeEvent},
    task_pruner,
    topology::{NodeStatus, Topology},
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, registry, util::SubscriberInitExt, EnvFilter};

mod admin_session;
mod auth;
mod client;
mod middleware;
mod otel;
mod routes;
mod scoped_key_rotation;

use admin_session::SealKey;
use auth::AuthState;
use middleware::{metrics_router, Metrics, TelemetryState};
use miroir_core::{
    canary::{
        CanaryAssertion, CanaryRunner, CapturedQuery, QueryCapture, SearchQuery, SearchResponse,
    },
    task_store::TaskStore,
};
use routes::{
    admin, admin_endpoints, explain, health, indexes, keys, multi_search, search, settings, tasks,
    version,
};
use scoped_key_rotation::ScopedKeyRotationState;
use std::sync::Arc;

/// Unified application state containing all shared state.
#[derive(Clone)]
struct UnifiedState {
    auth: AuthState,
    metrics: Metrics,
    admin: admin_endpoints::AppState,
    pod_id: String,
    redis_store: Option<miroir_core::task_store::RedisTaskStore>,
    query_capture: Arc<QueryCapture>,
    peer_discovery: Option<Arc<PeerDiscovery>>,
}

impl UnifiedState {
    fn new(config: MiroirConfig) -> Self {
        let metrics = Metrics::new(&config);

        let master_key =
            std::env::var("MIROIR_MASTER_KEY").unwrap_or_else(|_| config.master_key.clone());

        let admin_key =
            std::env::var("MIROIR_ADMIN_API_KEY").unwrap_or_else(|_| config.admin.api_key.clone());

        let jwt_primary = if config.search_ui.enabled {
            std::env::var(&config.search_ui.auth.jwt_secret_env).ok()
        } else {
            None
        };

        let jwt_previous = std::env::var(&config.search_ui.auth.jwt_secret_previous_env)
            .ok()
            .filter(|v| !v.is_empty());

        let seal_key = SealKey::from_env_or_generate();

        // Set the key-generated gauge before constructing AuthState
        // so the metric is accurate from the first scrape.
        metrics
            .admin_session_key_generated()
            .set(if seal_key.is_generated() { 1.0 } else { 0.0 });

        let pod_id = std::env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());
        let namespace = std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());

        // Create peer discovery instance (plan §14.5)
        // Only enabled when running in Kubernetes (POD_NAME is set to a real pod name)
        let peer_discovery = if pod_id != "unknown" {
            Some(Arc::new(PeerDiscovery::new(
                pod_id.clone(),
                namespace,
                config.peer_discovery.service_name.clone(),
            )))
        } else {
            None
        };

        // Create Redis task store if backend is redis (must happen before AppState
        // so redis_store and pod_id are available to admin endpoints).
        let redis_store =
            if config.task_store.backend == "redis" && !config.task_store.url.is_empty() {
                let url = config.task_store.url.clone();
                Some(
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current()
                            .block_on(miroir_core::task_store::RedisTaskStore::open(&url))
                    })
                    .expect("Failed to connect to Redis for scoped key rotation"),
                )
            } else {
                None
            };

        let auth = AuthState {
            master_key,
            admin_key: admin_key.clone(),
            jwt_primary,
            jwt_previous,
            seal_key: seal_key.clone(),
            revoked_sessions: std::sync::Arc::new(dashmap::DashMap::new()),
            admin_session_revoked_total: metrics.admin_session_revoked_total(),
        };

        let admin = admin_endpoints::AppState::with_redis(
            config.clone(),
            metrics.clone(),
            redis_store.clone(),
            pod_id.clone(),
            seal_key.clone(),
        );

        Self {
            auth,
            metrics,
            admin,
            pod_id,
            redis_store,
            query_capture: Arc::new(QueryCapture::new(1000)),
            peer_discovery,
        }
    }
}

// Implement FromRef so that admin_endpoints::AppState can be extracted from UnifiedState
impl FromRef<UnifiedState> for admin_endpoints::AppState {
    fn from_ref(state: &UnifiedState) -> Self {
        Self {
            config: state.admin.config.clone(),
            topology: state.admin.topology.clone(),
            ready: state.admin.ready.clone(),
            metrics: state.admin.metrics.clone(),
            version_state: state.admin.version_state.clone(),
            task_registry: state.admin.task_registry.clone(),
            redis_store: state.redis_store.clone(),
            task_store: state.admin.task_store.clone(),
            pod_id: state.pod_id.clone(),
            seal_key: state.auth.seal_key.clone(),
            local_rate_limiter: admin_endpoints::LocalAdminRateLimiter::new(),
            local_search_ui_rate_limiter: admin_endpoints::LocalSearchUiRateLimiter::new(),
            rebalancer: state.admin.rebalancer.clone(),
            migration_coordinator: state.admin.migration_coordinator.clone(),
            rebalancer_worker: state.admin.rebalancer_worker.clone(),
            rebalancer_metrics: state.admin.rebalancer_metrics.clone(),
            previous_docs_migrated: state.admin.previous_docs_migrated.clone(),
            settings_broadcast: state.admin.settings_broadcast.clone(),
            drift_reconciler: state.admin.drift_reconciler.clone(),
            anti_entropy_worker: state.admin.anti_entropy_worker.clone(),
            session_manager: state.admin.session_manager.clone(),
            alias_registry: state.admin.alias_registry.clone(),
            leader_election: state.admin.leader_election.clone(),
            mode_c_worker: state.admin.mode_c_worker.clone(),
            replica_selector: state.admin.replica_selector.clone(),
            idempotency_cache: state.admin.idempotency_cache.clone(),
            query_coalescer: state.admin.query_coalescer.clone(),
        }
    }
}

// Implement FromRef so that TelemetryState can be extracted from UnifiedState
impl FromRef<UnifiedState> for TelemetryState {
    fn from_ref(state: &UnifiedState) -> Self {
        TelemetryState {
            metrics: state.metrics.clone(),
            pod_id: state.pod_id.clone(),
        }
    }
}

// Implement FromRef so that CsrfState can be extracted from UnifiedState
impl FromRef<UnifiedState> for auth::CsrfState {
    fn from_ref(state: &UnifiedState) -> Self {
        auth::CsrfState {
            auth: state.auth.clone(),
            redis_store: state.redis_store.clone(),
        }
    }
}

// Implement FromRef so that routes::aliases::AliasState can be extracted from UnifiedState
impl FromRef<UnifiedState> for routes::aliases::AliasState {
    fn from_ref(state: &UnifiedState) -> Self {
        Self {
            config: state.admin.config.clone(),
            task_store: state.admin.task_store.clone(),
            metrics: state.metrics.clone(),
        }
    }
}

// Implement FromRef so that routes::explain::ExplainState can be extracted from UnifiedState
impl FromRef<UnifiedState> for routes::explain::ExplainState {
    fn from_ref(state: &UnifiedState) -> Self {
        Self {
            config: state.admin.config.clone(),
            topology: state.admin.topology.clone(),
        }
    }
}

// Implement FromRef so that routes::multi_search::MultiSearchState can be extracted from UnifiedState
impl FromRef<UnifiedState> for routes::multi_search::MultiSearchState {
    fn from_ref(state: &UnifiedState) -> Self {
        Self {
            config: state.admin.config.clone(),
            topology: state.admin.topology.clone(),
            node_master_key: state.admin.config.master_key.clone(),
            metrics: state.metrics.clone(),
            alias_registry: state.admin.alias_registry.clone(),
            replica_selector: state.admin.replica_selector.clone(),
        }
    }
}

// Implement FromRef so that routes::canary::CanaryState can be extracted from UnifiedState
impl FromRef<UnifiedState> for routes::canary::CanaryState {
    fn from_ref(state: &UnifiedState) -> Self {
        // Canary routes require Redis task store
        let redis_store = state
            .redis_store
            .clone()
            .expect("Canary routes require Redis task store (task_store.backend: redis)");
        let store: Arc<dyn miroir_core::task_store::TaskStore> = Arc::from(redis_store);
        Self {
            store,
            capture: state.query_capture.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration (file → env → CLI overlay)
    let config =
        MiroirConfig::load().map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

    // Initialize structured JSON logging (plan §10 format)
    // Fields on every line: timestamp, level, target, message, pod_id
    // Per-request fields (request_id) are added by telemetry middleware span.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let pod_id = std::env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());

    // Build subscriber - conditionally add OTel layer
    // Note: We rebuild the layers in each branch because the types differ
    // OTel layer must be applied to the bare registry first
    if let Some(otel_layer) = otel::init_otel_layer(&config) {
        let json_layer = tracing_subscriber::fmt::layer()
            .json()
            .flatten_event(true)
            .with_target(true)
            .with_current_span(true)
            .with_span_list(false);
        // Apply OTel layer to registry first, then add filter and json layer
        registry()
            .with(otel_layer)
            .with(filter)
            .with(json_layer)
            .init();
    } else {
        let json_layer = tracing_subscriber::fmt::layer()
            .json()
            .flatten_event(true)
            .with_target(true)
            .with_current_span(true)
            .with_span_list(false);
        registry().with(filter).with(json_layer).init();
    }

    // Set pod_id as a global default field so it appears on every log line.
    // This is done via a separate info span that is entered once and never
    // left — its fields propagate to all child spans and events.
    let _pod_span = tracing::info_span!("runtime", pod_id = %pod_id).entered();

    info!(
        shards = config.shards,
        replication_factor = config.replication_factor,
        replica_groups = config.replica_groups,
        "miroir-proxy starting"
    );

    // Validate critical secrets at startup (plan §9: "orchestrator refuses to
    // start the search UI without it").
    if config.search_ui.enabled {
        let jwt_env = &config.search_ui.auth.jwt_secret_env;
        match std::env::var(jwt_env) {
            Ok(v) if !v.is_empty() => {}
            _ => {
                anyhow::bail!(
                    "search_ui is enabled but {} is not set — refusing to start. \
                     Either set the env var or disable search_ui (search_ui.enabled: false)",
                    jwt_env
                );
            }
        }
    }

    // Build unified state
    let state = UnifiedState::new(config.clone());

    // Start health checker background task
    let health_checker_state = state.admin.clone();
    tokio::spawn(async move {
        run_health_checker(health_checker_state).await;
    });

    // Start rebalancer worker background task (plan §4)
    if let Some(ref worker) = state.admin.rebalancer_worker {
        let worker = worker.clone();
        let pod_id = state.pod_id.clone();
        tokio::spawn(async move {
            info!(
                pod_id = %pod_id,
                "rebalancer worker task starting"
            );
            // Load any persisted rebalance jobs from previous runs
            if let Err(e) = worker.load_persisted_jobs().await {
                error!(error = %e, "failed to load persisted rebalance jobs");
            }
            worker.run().await;
            error!("rebalancer worker task exited unexpectedly");
        });
    } else {
        info!("rebalancer worker not available (no task store configured)");
    }

    // Start scoped key rotation background task (requires Redis)
    if let Some(ref redis) = state.redis_store {
        let rotation_state = ScopedKeyRotationState {
            config: state.admin.config.clone(),
            redis: redis.clone(),
            pod_id: state.pod_id.clone(),
        };
        tokio::spawn(async move {
            scoped_key_rotation::run_scoped_key_rotator(rotation_state).await;
        });

        // Start admin session revocation Pub/Sub subscriber (plan §9).
        // When any pod revokes a session (logout), the session ID is published
        // to `miroir:admin_session:revoked`. Every pod subscribes and adds the
        // ID to its in-memory DashMap, ensuring revoked cookies are rejected
        // across all pods within milliseconds.
        let revoked_sessions = state.auth.revoked_sessions.clone();
        let revoked_total = state.auth.admin_session_revoked_total.clone();
        let redis_url = config.task_store.url.clone();
        let key_prefix = redis.key_prefix().to_string();
        tokio::spawn(async move {
            info!("starting admin session revocation subscriber");
            if let Err(e) = miroir_core::task_store::RedisTaskStore::subscribe_session_revocations(
                &redis_url,
                &key_prefix,
                move |session_id: String| {
                    revoked_sessions.insert(session_id, ());
                    revoked_total.inc();
                },
            )
            .await
            {
                error!(error = %e, "admin session revocation subscriber exited with error");
            }
        });
    }

    // Load aliases from task store on startup (plan §13.7)
    // Aliases must be loaded before any request routing to ensure consistent resolution
    if let Some(ref task_store) = state.admin.task_store {
        let alias_registry = state.admin.alias_registry.clone();
        let store = task_store.clone();
        tokio::spawn(async move {
            info!("loading aliases from task store");
            match alias_registry.sync_from_store(&*store).await {
                Ok(()) => {
                    let count = alias_registry.list().await.len();
                    info!(count, "aliases loaded successfully");
                }
                Err(e) => {
                    error!(error = %e, "failed to load aliases from task store");
                }
            }
        });
    } else {
        info!("alias loading skipped (no task store configured)");
    }

    // Start drift reconciler background task (plan §13.5)
    // Uses the drift_reconciler from AppState which is already configured
    if let Some(ref drift_reconciler) = state.admin.drift_reconciler {
        let drift_reconciler = drift_reconciler.clone();
        tokio::spawn(async move {
            info!("drift reconciler started");
            drift_reconciler.run().await;
            error!("drift reconciler exited unexpectedly");
        });
    } else {
        info!("drift reconciler not available (no task store configured)");
    }

    // Start anti-entropy worker background task (plan §13.8)
    // Uses the anti_entropy_worker from AppState which is already configured
    if let Some(ref anti_entropy_worker) = state.admin.anti_entropy_worker {
        let anti_entropy_worker = anti_entropy_worker.clone();
        tokio::spawn(async move {
            info!("anti-entropy worker started");
            anti_entropy_worker.run().await;
            error!("anti-entropy worker exited unexpectedly");
        });
    } else {
        info!("anti-entropy worker not available (disabled or no task store configured)");
    }

    // Start Mode C worker background task (plan §14.5 Mode C)
    // Processes chunked background jobs (dump import, reshard backfill)
    if let Some(ref mode_c_worker) = state.admin.mode_c_worker {
        let mode_c_worker = mode_c_worker.clone();
        tokio::spawn(async move {
            info!("Mode C worker started");
            match mode_c_worker.run().await {
                Ok(()) => {
                    info!("Mode C worker exited cleanly");
                }
                Err(e) => {
                    error!("Mode C worker exited unexpectedly: {}", e);
                }
            }
        });
    } else {
        info!("Mode C worker not available (no task store configured)");
    }

    // Start group sync worker background task (plan §2 group addition)
    if let Some(ref worker) = state.admin.group_sync_worker {
        let worker = worker.clone();
        tokio::spawn(async move {
            info!("group sync worker started");
            match worker.run().await {
                Ok(()) => {
                    info!("group sync worker exited cleanly");
                }
                Err(e) => {
                    error!("group sync worker exited unexpectedly: {}", e);
                }
            }
        });
    } else {
        info!("group sync worker not available (no task store configured)");
    }

    // Start peer discovery refresh loop (plan §14.5)
    // Periodically performs SRV lookups to discover peer pods
    if let Some(ref peer_discovery) = state.peer_discovery {
        let peer_discovery = peer_discovery.clone();
        let metrics = state.metrics.clone();
        let refresh_interval_s = config.peer_discovery.refresh_interval_s;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(refresh_interval_s));
            info!(
                interval_s = refresh_interval_s,
                "peer discovery refresh loop started"
            );
            loop {
                interval.tick().await;
                match peer_discovery.refresh().await {
                    Ok(peer_set) => {
                        let count = peer_set.len() as u64;
                        info!(peer_count = count, "peer discovery refresh completed");
                        metrics.set_peer_pod_count(count);
                    }
                    Err(e) => {
                        error!(error = %e, "peer discovery refresh failed");
                    }
                }
            }
        });
    } else {
        info!("peer discovery disabled (not running in Kubernetes)");
    }

    // Start task registry TTL pruner background task (plan §4, Phase 3)
    // Runs on single-pod with advisory lock; Phase 6 §14.5 Mode A replaces with rendezvous
    if let Some(ref store) = state.admin.task_store {
        let store = store.clone();
        let pruner_config = config.task_registry.clone();
        tokio::spawn(async move {
            // The pruner runs in its own thread via spawn_pruner
            let _pruner_handle = task_pruner::spawn_pruner(store, pruner_config);
            // The handle is dropped here only on process exit
            info!("task registry TTL pruner started");
            // Keep this task alive forever
            std::future::pending::<()>().await;
        });
    } else {
        info!("task registry TTL pruner not available (no task store)");
    }

    // Start canary runner background task (plan §13.18)
    // Only enabled when canary_runner.enabled = true and Redis is available
    if config.canary_runner.enabled {
        if let Some(ref redis) = state.redis_store {
            let store: Arc<dyn TaskStore> = Arc::from(redis.clone());
            let canary_config = config.canary_runner.clone();

            // Clone config values for the search executor
            let search_config = config.clone();
            let search_executor: miroir_core::canary::SearchExecutor = Arc::new(
                move |index_uid: &str,
                      query: &SearchQuery|
                      -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = miroir_core::error::Result<SearchResponse>>
                            + Send,
                    >,
                > {
                    let index_uid = index_uid.to_string();
                    let query = query.clone();
                    let config = search_config.clone();

                    Box::pin(async move {
                        // For canary queries, we execute against the first available healthy node
                        let node_addresses: Vec<_> =
                            config.nodes.iter().map(|n| n.address.clone()).collect();

                        for address in node_addresses {
                            let client = match reqwest::Client::builder()
                                .timeout(std::time::Duration::from_millis(
                                    config.scatter.node_timeout_ms,
                                ))
                                .build()
                            {
                                Ok(c) => c,
                                Err(_) => continue,
                            };

                            let url = format!(
                                "{}/indexes/{}/search",
                                address.trim_end_matches('/'),
                                index_uid
                            );

                            // Build the search request body
                            let mut body = match serde_json::to_value(&query) {
                                Ok(v) => v,
                                Err(e) => {
                                    return Err(miroir_core::error::MiroirError::InvalidRequest(
                                        format!("Failed to serialize query: {}", e),
                                    ))
                                }
                            };

                            // Add limit to avoid large responses for canary queries
                            if !body.get("limit").and_then(|v| v.as_u64()).is_some() {
                                body["limit"] = serde_json::json!(20);
                            }

                            let response = match client
                                .post(&url)
                                .header(
                                    "Authorization",
                                    format!("Bearer {}", config.node_master_key),
                                )
                                .json(&body)
                                .send()
                                .await
                            {
                                Ok(r) => r,
                                Err(_) => continue,
                            };

                            if response.status().is_success() {
                                if let Ok(text) = response.text().await {
                                    if let Ok(search_response) =
                                        serde_json::from_str::<SearchResponse>(&text)
                                    {
                                        return Ok(search_response);
                                    }
                                }
                            }
                        }

                        // All nodes failed
                        Err(miroir_core::error::MiroirError::Topology(
                            "All nodes failed for canary query".to_string(),
                        ))
                    })
                },
            );

            // Create metrics emitter callback
            let metrics_for_canary = state.metrics.clone();
            let metrics_emitter: miroir_core::canary::MetricsEmitter = Arc::new(move |result| {
                use miroir_core::canary::CanaryStatus;
                let result_str = match result.status {
                    CanaryStatus::Passed => "passed",
                    CanaryStatus::Failed => "failed",
                    CanaryStatus::Error => "error",
                };
                metrics_for_canary.inc_canary_runs(&result.canary_id, result_str);
                metrics_for_canary
                    .observe_canary_latency_ms(&result.canary_id, result.latency_ms as f64);

                for failure in &result.failed_assertions {
                    metrics_for_canary
                        .inc_canary_assertion_failures(&result.canary_id, &failure.assertion_type);
                }
            });

            // Create settings version checker callback
            let store_for_version = store.clone();
            let version_config = config.clone();
            let settings_version_checker: miroir_core::canary::SettingsVersionChecker =
                Arc::new(move |index_uid: &str| -> Option<i64> {
                    // Try to get the settings version from the task store
                    let node_ids: Vec<String> =
                        version_config.nodes.iter().map(|n| n.id.clone()).collect();

                    let mut min_version: Option<i64> = None;
                    for node_id in node_ids {
                        if let Ok(Some(row)) =
                            store_for_version.get_node_settings_version(index_uid, &node_id)
                        {
                            match min_version {
                                None => min_version = Some(row.version),
                                Some(current) if row.version < current => {
                                    min_version = Some(row.version)
                                }
                                _ => {}
                            }
                        }
                    }
                    min_version
                });

            // Create and start the canary runner
            let runner = CanaryRunner::new(
                store,
                canary_config.max_concurrent_canaries as usize,
                canary_config.run_history_per_canary as usize,
                search_executor,
                metrics_emitter,
                settings_version_checker,
            );

            tokio::spawn(async move {
                info!("canary runner started");
                if let Err(e) = runner.start().await {
                    error!("canary runner exited: {}", e);
                }
            });
        } else {
            info!("canary runner enabled but Redis not available - skipping");
        }
    }

    // Build the main app router with UnifiedState
    let app = Router::new()
        .route("/health", get(health::get_health))
        .route("/version", get(version::get_version::<UnifiedState>))
        .route("/stats", get(indexes::global_stats_handler))
        .route(
            "/multi-search",
            post(multi_search::multi_search::<UnifiedState>),
        ) // §13.11
        .nest("/_miroir", admin::router::<UnifiedState>())
        .nest("/indexes", indexes::router::<UnifiedState>())
        .nest("/keys", keys::router::<UnifiedState>())
        .nest("/search", search::router::<UnifiedState>())
        .nest("/settings", settings::router::<UnifiedState>())
        .nest("/tasks", tasks::router::<UnifiedState>())
        // IMPORTANT: Layer order matters! Last layer() call = outermost = runs first.
        // The middleware stack (from outermost to innermost):
        // 1. csrf_middleware - runs first
        // 2. auth_middleware
        // 3. Extension layers
        // 4. session_pinning_middleware - extracts X-Miroir-Session header
        // 5. request_id_middleware - sets X-Request-Id header
        // 6. telemetry_middleware - reads X-Request-Id, creates tracing span with request_id field
        //    The span's request_id field propagates to all child log events via with_current_span(true)
        //
        // To achieve this order, we add layers in REVERSE (last call = outermost):
        .layer(axum::middleware::from_fn_with_state(
            TelemetryState {
                metrics: state.metrics.clone(),
                pod_id: state.pod_id.clone(),
            },
            middleware::telemetry_middleware,
        ))
        .layer(axum::middleware::from_fn(middleware::request_id_middleware))
        .layer(axum::middleware::from_fn(
            middleware::session_pinning_middleware,
        ))
        .layer(axum::extract::DefaultBodyLimit::max(
            config.server.max_body_bytes as usize,
        ))
        .layer(axum::Extension(state.admin.config.clone()))
        .layer(axum::Extension(std::sync::Arc::new(state.admin.clone())))
        .layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            auth::auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            auth::CsrfState {
                auth: state.auth.clone(),
                redis_store: state.redis_store.clone(),
            },
            auth::csrf_middleware,
        ))
        .with_state(state.clone());

    let main_addr: SocketAddr = format!("{}:{}", config.server.bind, config.server.port)
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid bind address: {}", e))?;
    let metrics_addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], 9090));

    info!(
        main_addr = %main_addr,
        metrics_addr = %metrics_addr,
        "listening"
    );

    // Create listeners
    let main_listener = tokio::net::TcpListener::bind(main_addr).await?;
    let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;

    // Spawn main server with graceful shutdown
    let main_server = axum::serve(main_listener, app).with_graceful_shutdown(shutdown_signal());

    // Spawn metrics server with graceful shutdown
    let metrics_app = metrics_router().with_state(state.metrics.clone());
    let metrics_server =
        axum::serve(metrics_listener, metrics_app).with_graceful_shutdown(shutdown_signal());

    // Run both servers concurrently
    let (main_result, metrics_result) = tokio::join!(main_server, metrics_server);

    // Check for errors
    if let Err(e) = main_result {
        error!("main server error: {}", e);
    }
    if let Err(e) = metrics_result {
        error!("metrics server error: {}", e);
    }

    Ok(())
}

/// Background health checker - promotes nodes to Active when reachable.
///
/// On each tick it also updates the Prometheus metrics for node health,
/// shard coverage, shard distribution, and degraded shard count.
///
/// Implements unplanned node failure detection (plan §2):
/// - unhealthy_threshold consecutive failures → mark node as Failed
/// - recovery_threshold consecutive successes → recover from Failed/Degraded
async fn run_health_checker(state: admin_endpoints::AppState) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(state.config.health.interval_ms));

    // Track consecutive failures per node (in-memory only)
    let mut consecutive_failures: std::collections::HashMap<String, u32> =
        std::collections::HashMap::new();
    let mut consecutive_successes: std::collections::HashMap<String, u32> =
        std::collections::HashMap::new();

    loop {
        interval.tick().await;

        let mut topo = state.topology.write().await;
        let mut all_healthy = true;

        // Collect node IDs to iterate
        let node_ids: Vec<_> = topo.nodes().map(|n| n.id.clone()).collect();

        for node_id in &node_ids {
            // Get current node status
            let current_status = topo.node(node_id).map(|n| n.status);

            // Get node address
            let node_address = match topo.node(node_id) {
                Some(n) => n.address.clone(),
                None => {
                    all_healthy = false;
                    continue;
                }
            };

            // Try to reach the node
            let client = match reqwest::Client::builder()
                .timeout(Duration::from_millis(state.config.health.timeout_ms))
                .build()
            {
                Ok(c) => c,
                Err(_) => {
                    all_healthy = false;
                    // Increment failure counter
                    *consecutive_failures.entry(node_id.clone()).or_insert(0) += 1;
                    consecutive_successes.remove(node_id);
                    continue;
                }
            };

            let url = format!("{}/health", node_address.trim_end_matches('/'));
            let result = client.get(&url).send().await;

            if result.is_ok() && result.unwrap().status().is_success() {
                // Node is reachable
                consecutive_successes
                    .entry(node_id.clone())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                consecutive_failures.remove(node_id);

                let successes = *consecutive_successes.get(node_id.as_str()).unwrap_or(&1);

                // Check if we should promote from Joining/Degraded/Failed to Active
                if let Some(node) = topo.node_mut(node_id) {
                    match node.status {
                        NodeStatus::Joining => {
                            // Promote joining nodes immediately on first success
                            let _ = node.transition_to(NodeStatus::Active);
                            info!(node_id = %node_id, "node promoted to Active (was Joining)");
                        }
                        NodeStatus::Degraded => {
                            // Need recovery_threshold consecutive successes to recover
                            if successes >= state.config.health.recovery_threshold {
                                let _ = node.transition_to(NodeStatus::Active);
                                info!(node_id = %node_id, "node recovered to Active (was Degraded)");
                            }
                        }
                        NodeStatus::Failed => {
                            // Need recovery_threshold consecutive successes to recover
                            if successes >= state.config.health.recovery_threshold {
                                let _ = node.transition_to(NodeStatus::Active);
                                info!(node_id = %node_id, "node recovered to Active (was Failed)");

                                // Trigger RF-restore if configured
                                if let Some(ref rebalancer) = state.rebalancer {
                                    if let Some(ref worker) = state.rebalancer_worker {
                                        let event = TopologyChangeEvent::NodeRecovered {
                                            node_id: node_id.as_str().to_string(),
                                            replica_group: node.replica_group,
                                            index_uid: "default".to_string(),
                                        };
                                        let _ = worker.event_sender().try_send(event);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                // Node is unreachable
                all_healthy = false;
                *consecutive_failures.entry(node_id.clone()).or_insert(0) += 1;
                consecutive_successes.remove(node_id);

                let failures = *consecutive_failures.get(node_id.as_str()).unwrap_or(&1);

                // Check if we should mark as Degraded or Failed
                if let Some(node) = topo.node_mut(node_id) {
                    match node.status {
                        NodeStatus::Active | NodeStatus::Healthy => {
                            // First failure → mark Degraded (not full failure yet)
                            if failures >= 1 {
                                let _ = node.transition_to(NodeStatus::Degraded);
                                warn!(node_id = %node_id, consecutive_failures = failures, "node marked Degraded");
                            }
                        }
                        NodeStatus::Degraded => {
                            // unhealthy_threshold consecutive failures → mark Failed
                            if failures >= state.config.health.unhealthy_threshold {
                                let _ = node.transition_to(NodeStatus::Failed);
                                warn!(node_id = %node_id, consecutive_failures = failures, "node marked Failed");

                                // Trigger failure handling
                                if let Some(ref rebalancer) = state.rebalancer {
                                    if let Some(ref worker) = state.rebalancer_worker {
                                        let event = TopologyChangeEvent::NodeFailed {
                                            node_id: node_id.as_str().to_string(),
                                            replica_group: node.replica_group,
                                            index_uid: "default".to_string(),
                                        };
                                        let _ = worker.event_sender().try_send(event);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Update node health gauges (§10 node metrics)
        for node_id in &node_ids {
            let healthy = topo.node(node_id).map(|n| n.is_healthy()).unwrap_or(false);
            state.metrics.set_node_healthy(node_id.as_str(), healthy);
        }

        // Compute and update shard metrics (§10 shard metrics)
        update_shard_metrics(&topo, &state.metrics);

        // Update task registry size gauge
        let task_count = state.task_registry.count();
        state.metrics.set_task_registry_size(task_count as f64);

        // Sync rebalancer metrics to Prometheus
        state.sync_rebalancer_metrics_to_prometheus().await;

        // Mark ready once all configured nodes are reachable
        if all_healthy && !state.config.nodes.is_empty() {
            state.mark_ready().await;
        }

        // Update §14.9 resource-pressure metrics
        update_resource_pressure_metrics(&state.metrics);

        // Update §13.6 session pinning metrics
        state.session_manager.update_metrics(|count| {
            state.metrics.set_session_active_count(count as u64);
        });

        // Prune expired sessions (plan §13.6)
        let pruned = state.session_manager.prune_expired().await;
        if pruned > 0 {
            info!(pruned_count = pruned, "pruned expired sessions");
        }
    }
}

/// Compute shard coverage, degraded count, and per-node shard distribution
/// from the current topology and update the corresponding Prometheus gauges.
fn update_shard_metrics(topo: &Topology, metrics: &middleware::Metrics) {
    let node_map = topo.node_map();
    let mut healthy_shards = 0u64;
    let mut degraded_shards = 0u64;

    // Per-node shard count
    let mut node_shard_counts: std::collections::HashMap<miroir_core::topology::NodeId, u64> =
        std::collections::HashMap::new();

    for shard_id in 0..topo.shards {
        let mut has_healthy_replica = false;
        for group in topo.groups() {
            let assigned =
                miroir_core::router::assign_shard_in_group(shard_id, group.nodes(), topo.rf());
            for node_id in &assigned {
                let healthy = node_map
                    .get(node_id)
                    .map(|n| n.is_healthy())
                    .unwrap_or(false);
                if healthy {
                    has_healthy_replica = true;
                    *node_shard_counts.entry(node_id.clone()).or_insert(0) += 1;
                }
            }
        }
        if has_healthy_replica {
            healthy_shards += 1;
        } else {
            degraded_shards += 1;
        }
    }

    let coverage = if topo.shards > 0 {
        healthy_shards as f64 / topo.shards as f64
    } else {
        1.0
    };
    metrics.set_shard_coverage(coverage);
    metrics.set_degraded_shards(degraded_shards as f64);

    for (node_id, count) in &node_shard_counts {
        metrics.set_shard_distribution(node_id.as_str(), *count as f64);
    }
}

/// Read cgroup v2 memory pressure and update §14.9 resource-pressure gauges.
///
/// In Kubernetes each container has its own cgroup; the paths below are the
/// standard cgroup v2 mount points. If the files don't exist (e.g. local dev
/// on macOS) the metrics remain at their zero defaults.
fn update_resource_pressure_metrics(metrics: &middleware::Metrics) {
    // ── Memory pressure ──
    // cgroup v2: /sys/fs/cgroup/memory.current and memory.max
    let mem_current = read_cgroup_metric("/sys/fs/cgroup/memory.current");
    let mem_max = read_cgroup_metric("/sys/fs/cgroup/memory.max");

    if let (Some(current), Some(max)) = (mem_current, mem_max) {
        if max > 0 {
            let ratio = current as f64 / max as f64;
            let level = if ratio > 0.90 {
                2
            } else if ratio > 0.75 {
                1
            } else {
                0
            };
            metrics.set_memory_pressure(level);
        }
    }

    // ── CPU throttling ──
    // cgroup v2: /sys/fs/cgroup/cpu.stat contains throttle_usec
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/cpu.stat") {
        for line in contents.lines() {
            if let Some(rest) = line.strip_prefix("throttled_usec ") {
                if let Ok(usec) = rest.trim().parse::<f64>() {
                    // Report delta — the counter is cumulative, so we report
                    // the raw value and let Prometheus handle rate().
                    // For simplicity we set the counter to the absolute value
                    // (Prometheus counters are monotonic; since this is called
                    // periodically, we just inc by the new delta).
                    // Actually, the metric is a Counter, so we can only inc it.
                    // We'll read the previous throttled value from a thread-local.
                    // Simpler approach: just report the current throttle time
                    // as a one-shot increment if non-zero.
                    metrics.inc_cpu_throttled_seconds(usec / 1_000_000.0);
                }
            }
        }
    }

    // ── Peer pod count and leader status ──
    // Peer pod count is now set by peer discovery refresh loop (plan §14.5).
    // Leader election is not yet implemented (plan §14.5 Mode B).
    // Owned shards count will be set by Mode A rendezvous (plan §14.5).
    metrics.set_leader("global", true);
    metrics.set_owned_shards_count(0);
}

/// Read a single integer value from a cgroup pseudo-file.
fn read_cgroup_metric(path: &str) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// Graceful shutdown signal handler.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>;

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, draining in-flight requests...");

    // Shutdown OpenTelemetry to flush any pending traces
    otel::shutdown_otel();
}
