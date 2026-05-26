//! Shared test helpers for Miroir integration tests.
//!
//! Provides consistent skip patterns for:
//! - Docker availability (testcontainers)
//! - Redis availability
//! - External service availability

use std::path::Path;

/// Check if Docker is available for testcontainers.
///
/// Returns Ok(()) if Docker is available, Err(message) if not.
/// The error message is suitable for printing as a skip reason.
pub fn check_docker_available() -> Result<(), String> {
    // Check explicit skip flag first
    if std::env::var("MIROIR_TEST_SKIP_DOCKER").is_ok() {
        return Err("Docker tests skipped via MIROIR_TEST_SKIP_DOCKER. \
             Unset MIROIR_TEST_SKIP_DOCKER and ensure Docker is available."
            .to_string());
    }

    // Check for Docker socket
    let docker_sock = Path::new("/var/run/docker.sock");
    if !docker_sock.exists() {
        return Err("Docker socket not found at /var/run/docker.sock. \
             Set MIROIR_TEST_SKIP_DOCKER=1 to skip, or ensure Docker is running."
            .to_string());
    }

    // Try to connect to the socket
    if let Err(e) = std::fs::metadata(docker_sock) {
        return Err(format!(
            "Cannot access Docker socket: {e}. \
             Set MIROIR_TEST_SKIP_DOCKER=1 to skip, or ensure Docker is running."
        ));
    }

    Ok(())
}

/// Check if Redis is available for integration tests.
///
/// Returns Ok(url) if Redis is available, Err(message) if not.
/// Reads MIROIR_TEST_REDIS_URL or defaults to redis://localhost:6379.
pub fn check_redis_available() -> Result<Option<String>, String> {
    // Check explicit skip flag
    if std::env::var("MIROIR_TEST_SKIP_REDIS").is_ok() {
        return Err("Redis tests skipped via MIROIR_TEST_SKIP_REDIS. \
             Unset MIROIR_TEST_SKIP_REDIS and ensure Redis is available."
            .to_string());
    }

    // Use explicit URL if provided
    if let Ok(url) = std::env::var("MIROIR_TEST_REDIS_URL") {
        return Ok(Some(url));
    }

    // Try to detect Redis via testcontainers if available
    // For now, just return None to indicate "use default"
    Ok(None)
}

/// Macro to skip a test if Docker is unavailable.
///
/// Usage:
/// ```rust
/// #[tokio::test]
/// async fn my_test() {
///     skip_if_no_docker!();
///     // ... test code using testcontainers
/// }
/// ```
#[macro_export]
macro_rules! skip_if_no_docker {
    () => {
        match $crate::test_helper::check_docker_available() {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Skipping test: {e}");
                return;
            }
        }
    };
}

/// Macro to skip a test if Redis is unavailable.
///
/// Usage:
/// ```rust
/// #[tokio::test]
/// async fn my_test() {
///     let url = skip_if_no_redis!();
///     // ... test code using Redis URL
/// }
/// ```
#[macro_export]
macro_rules! skip_if_no_redis {
    () => {{
        match $crate::test_helper::check_redis_available() {
            Ok(url) => url,
            Err(e) => {
                eprintln!("Skipping test: {e}");
                return;
            }
        }
    }};
}
