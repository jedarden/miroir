//! OpenTelemetry tracing layer setup (plan §10).
//!
//! When `tracing.enabled: false`, this module is a no-op — zero overhead.
//! When enabled, it initializes an OTLP exporter with head-based sampling.

use miroir_core::config::MiroirConfig;

#[cfg(feature = "tracing")]
use opentelemetry::trace::TracerProvider;

/// Initialize the OpenTelemetry tracing layer if enabled in config.
///
/// Returns `Some(layer)` when tracing is enabled, `None` otherwise.
/// The caller is responsible for adding the layer to the subscriber.
#[cfg(feature = "tracing")]
pub fn init_otel_layer(
    config: &MiroirConfig,
) -> Option<
    tracing_opentelemetry::OpenTelemetryLayer<
        tracing_subscriber::Registry,
        opentelemetry_sdk::trace::Tracer,
    >,
> {
    if !config.tracing.enabled {
        return None;
    }

    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::{
        trace::{Sampler, Tracer, TracerProvider as SdkTracerProvider},
        Resource,
    };
    use tracing_opentelemetry::OpenTelemetryLayer;

    // Set global propagator for distributed tracing
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // Build resource attributes (service.name, service.version, host.name)
    let pod_name = std::env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());
    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.tracing.service_name.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("host.name", pod_name),
    ]);

    // Create OTLP exporter with tonic (gRPC) transport
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.tracing.endpoint)
        .build()
        .map_err(|e| {
            eprintln!("Failed to create OTLP exporter: {}", e);
            e
        })
        .ok()?;

    // Head-based sampler: sample_rate fraction of traces
    let sampler = Sampler::TraceIdRatioBased(config.tracing.sample_rate);

    // Build provider with exporter, sampler, and resource
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    // Get the tracer from the provider
    let tracer = provider.tracer("miroir-proxy");

    // Set global tracer provider
    let _ = opentelemetry::global::set_tracer_provider(provider);

    // Create and return the tracing layer
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);

    Some(layer)
}

/// Shutdown the OpenTelemetry tracer provider, flushing any pending spans.
///
/// This should be called during graceful shutdown to ensure all in-flight
/// traces are exported before the process exits.
#[cfg(feature = "tracing")]
pub fn shutdown_otel() {
    use opentelemetry::global;
    // Flush any remaining traces
    let _ = global::shutdown_tracer_provider();
}

/// No-op implementation when tracing feature is disabled.
#[cfg(not(feature = "tracing"))]
pub fn init_otel_layer(_config: &MiroirConfig) -> Option<tracing_subscriber::layer::Identity> {
    None
}

/// No-op shutdown when tracing feature is disabled.
#[cfg(not(feature = "tracing"))]
pub fn shutdown_otel() {
    // No-op
}
