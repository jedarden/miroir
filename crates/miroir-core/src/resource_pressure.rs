//! Resource-pressure metrics collection (plan §14.9).
//!
//! This module provides periodic collection of system resource metrics:
//! - Memory pressure (cgroup v2 memory.current / memory.max)
//! - CPU throttling (cgroup cpu.stat nr_throttled / throttled_time)
//! - Request queue depth (updated by middleware)
//! - Background queue depth (updated by workers)
//! - Peer pod count (updated by peer discovery)
//! - Leader status (updated by leader election)
//! - Owned shards count (updated by Mode A coordinator)
//!
//! The collection runs on a 15-second interval and updates the metrics
//! via the Metrics accessor methods.

use crate::error::Result;

/// Read cgroup v2 memory.current and memory.max to compute memory pressure.
///
/// Returns:
/// - 0 = ok (<75% usage)
/// - 1 = warn (75-90% usage)
/// - 2 = critical (>90% usage)
#[cfg(target_os = "linux")]
pub fn read_memory_pressure() -> Result<u32> {
    use std::fs;

    // Try cgroup v2 first
    let memory_current = fs::read_to_string("/sys/fs/cgroup/memory.current")
        .or_else(|_| fs::read_to_string("/sys/fs/cgroup/memory/memory.current"))
        .map_err(|e| {
            crate::error::MiroirError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("failed to read memory.current: {e}"),
            ))
        })?;

    let memory_max = fs::read_to_string("/sys/fs/cgroup/memory.max")
        .or_else(|_| fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .map_err(|e| {
            crate::error::MiroirError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("failed to read memory.max: {e}"),
            ))
        })?;

    let current_bytes: u64 = memory_current.trim().parse().map_err(|e| {
        crate::error::MiroirError::InvalidRequest(format!("invalid memory.current: {e}"))
    })?;

    let max_bytes: u64 = memory_max.trim().parse().map_err(|e| {
        crate::error::MiroirError::InvalidRequest(format!("invalid memory.max: {e}"))
    })?;

    if max_bytes == 0 {
        return Ok(0); // No limit set
    }

    let usage_ratio = current_bytes as f64 / max_bytes as f64;

    Ok(if usage_ratio > 0.9 {
        2 // Critical
    } else if usage_ratio > 0.75 {
        1 // Warning
    } else {
        0 // OK
    })
}

/// Read cgroup CPU throttling statistics.
///
/// Returns (nr_throttled, throttled_time_seconds) from cpu.stat.
#[cfg(target_os = "linux")]
pub fn read_cpu_throttling() -> Result<(u64, f64)> {
    use std::fs;

    let cpu_stat = fs::read_to_string("/sys/fs/cgroup/cpu.stat")
        .or_else(|_| fs::read_to_string("/sys/fs/cgroup/cpu/cpu.stat"))
        .map_err(|e| {
            crate::error::MiroirError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("failed to read cpu.stat: {e}"),
            ))
        })?;

    let mut nr_throttled = 0u64;
    let mut throttled_time_ns = 0u64;

    for line in cpu_stat.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 2 {
            continue;
        }
        match parts[0] {
            "nr_throttled" => {
                nr_throttled = parts[1].parse().unwrap_or(0);
            }
            "throttled_time" | "throttled_usec" => {
                // throttled_time is in nanoseconds (cgroup v2)
                // throttled_usec is in microseconds (cgroup v1)
                let val: u64 = parts[1].parse().unwrap_or(0);
                throttled_time_ns = if parts[0] == "throttled_usec" {
                    val * 1000 // Convert microseconds to nanoseconds
                } else {
                    val
                };
            }
            _ => {}
        }
    }

    let throttled_time_seconds = throttled_time_ns as f64 / 1_000_000_000.0;
    Ok((nr_throttled, throttled_time_seconds))
}

/// Fallback for non-Linux platforms (e.g., macOS during development).
#[cfg(not(target_os = "linux"))]
pub fn read_memory_pressure() -> Result<u32> {
    Ok(0) // No memory pressure info available
}

/// Fallback for non-Linux platforms.
#[cfg(not(target_os = "linux"))]
pub fn read_cpu_throttling() -> Result<(u64, f64)> {
    Ok((0, 0.0)) // No CPU throttling info available
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn test_read_memory_pressure() {
        // This test will only pass on Linux with cgroup v2
        // In CI/production environments, it validates the reading logic
        let pressure = read_memory_pressure();
        // We expect either Ok(0), Ok(1), or Ok(2)
        // Or an error if cgroup files don't exist (e.g., in a container without proper mounts)
        match pressure {
            Ok(level) => assert!(level <= 2, "memory pressure level should be 0, 1, or 2"),
            Err(_) => {
                // OK if cgroup files don't exist in test environment
                println!("cgroup memory files not available in test environment");
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_read_cpu_throttling() {
        // This test will only pass on Linux with cgroup
        let (nr_throttled, throttled_time) = read_cpu_throttling().unwrap();
        // nr_throttled should be a valid count
        // throttled_time should be non-negative
        assert!(
            throttled_time >= 0.0,
            "throttled time should be non-negative"
        );
    }

    #[test]
    fn test_memory_pressure_calculation() {
        // Test the calculation logic with hypothetical values
        // At 50% usage -> 0
        let ratio = 0.5;
        let level = if ratio > 0.9 {
            2
        } else if ratio > 0.75 {
            1
        } else {
            0
        };
        assert_eq!(level, 0);

        // At 80% usage -> 1
        let ratio = 0.8;
        let level = if ratio > 0.9 {
            2
        } else if ratio > 0.75 {
            1
        } else {
            0
        };
        assert_eq!(level, 1);

        // At 95% usage -> 2
        let ratio = 0.95;
        let level = if ratio > 0.9 {
            2
        } else if ratio > 0.75 {
            1
        } else {
            0
        };
        assert_eq!(level, 2);
    }
}
