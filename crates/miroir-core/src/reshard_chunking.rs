//! Reshard backfill chunking for Mode C coordinator (plan §13.1 + §14.5).
//!
//! Splits reshard backfill work by shard-id ranges.
//! Each chunk can process a range of old shards independently.

use crate::mode_c_coordinator::JobChunk;

/// Chunk specification for a reshard backfill.
#[derive(Debug, Clone)]
pub struct ReshardChunkSpec {
    /// Chunk index (0-based).
    pub index: u32,
    /// Total number of chunks.
    pub total: u32,
    /// Starting old shard ID (inclusive).
    pub start_shard: u32,
    /// Ending old shard ID (exclusive).
    pub end_shard: u32,
    /// Number of shards in this chunk.
    pub shard_count: u32,
}

/// Split a reshard backfill into chunks by shard-id ranges.
///
/// Returns a vector of chunk specifications. Each chunk contains a range
/// of old shards to backfill to the new shard configuration.
///
/// # Arguments
/// * `old_shards` - Number of shards in the old configuration
/// * `target_shards` - Number of shards in the new configuration
/// * `shards_per_chunk` - Target number of old shards per chunk
///
/// # Returns
/// A vector of chunk specifications
pub fn split_reshard_into_chunks(
    old_shards: u32,
    target_shards: u32,
    shards_per_chunk: u32,
) -> Vec<ReshardChunkSpec> {
    if old_shards == 0 {
        return Vec::new();
    }

    // If we have fewer shards than the chunk size, return a single chunk
    if old_shards <= shards_per_chunk {
        return vec![ReshardChunkSpec {
            index: 0,
            total: 1,
            start_shard: 0,
            end_shard: old_shards,
            shard_count: old_shards,
        }];
    }

    let mut chunks = Vec::new();
    let mut current_shard = 0u32;
    let mut chunk_index = 0u32;

    while current_shard < old_shards {
        let end_shard = (current_shard + shards_per_chunk).min(old_shards);
        let shard_count = end_shard - current_shard;

        chunks.push(ReshardChunkSpec {
            index: chunk_index,
            total: 0, // Will be filled in later
            start_shard: current_shard,
            end_shard,
            shard_count,
        });

        current_shard = end_shard;
        chunk_index += 1;
    }

    // Update the total count for all chunks
    let total = chunks.len() as u32;
    for chunk in &mut chunks {
        chunk.total = total;
    }

    chunks
}

/// Convert reshard chunk specs to job chunks for the Mode C coordinator.
pub fn reshard_specs_to_job_chunks(specs: Vec<ReshardChunkSpec>) -> Vec<JobChunk> {
    specs
        .into_iter()
        .map(|spec| JobChunk {
            index: spec.index,
            total: spec.total,
            start: spec.start_shard.to_string(),
            end: spec.end_shard.to_string(),
            size_bytes: spec.shard_count as u64, // Use shard count as the size metric
        })
        .collect()
}

/// Parse a reshard chunk from a job chunk.
///
/// Returns the shard range for the chunk.
pub fn parse_reshard_chunk(chunk: &JobChunk) -> Result<(u32, u32), String> {
    let start_shard = chunk
        .start
        .parse::<u32>()
        .map_err(|e| format!("invalid start shard: {e}"))?;
    let end_shard = chunk
        .end
        .parse::<u32>()
        .map_err(|e| format!("invalid end shard: {e}"))?;

    Ok((start_shard, end_shard))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_old_shards() {
        let chunks = split_reshard_into_chunks(0, 128, 16);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_single_chunk() {
        let chunks = split_reshard_into_chunks(16, 32, 32);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[0].total, 1);
        assert_eq!(chunks[0].start_shard, 0);
        assert_eq!(chunks[0].end_shard, 16);
        assert_eq!(chunks[0].shard_count, 16);
    }

    #[test]
    fn test_multiple_chunks() {
        let chunks = split_reshard_into_chunks(64, 128, 16);
        assert_eq!(chunks.len(), 4);

        // Verify first chunk
        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[0].total, 4);
        assert_eq!(chunks[0].start_shard, 0);
        assert_eq!(chunks[0].end_shard, 16);
        assert_eq!(chunks[0].shard_count, 16);

        // Verify second chunk
        assert_eq!(chunks[1].index, 1);
        assert_eq!(chunks[1].start_shard, 16);
        assert_eq!(chunks[1].end_shard, 32);
        assert_eq!(chunks[1].shard_count, 16);

        // Verify last chunk
        assert_eq!(chunks[3].index, 3);
        assert_eq!(chunks[3].start_shard, 48);
        assert_eq!(chunks[3].end_shard, 64);
        assert_eq!(chunks[3].shard_count, 16);
    }

    #[test]
    fn test_partial_final_chunk() {
        // 65 shards with 16 per chunk = 4 full chunks + 1 partial
        let chunks = split_reshard_into_chunks(65, 128, 16);
        assert_eq!(chunks.len(), 5);

        // First 4 chunks should have 16 shards each
        for i in 0..4 {
            assert_eq!(chunks[i].shard_count, 16);
        }

        // Last chunk should have 1 shard
        assert_eq!(chunks[4].shard_count, 1);
        assert_eq!(chunks[4].start_shard, 64);
        assert_eq!(chunks[4].end_shard, 65);
    }

    #[test]
    fn test_chunks_cover_full_range() {
        let old_shards = 100;
        let chunks = split_reshard_into_chunks(old_shards, 200, 15);

        let mut total_shards = 0u32;
        for chunk in &chunks {
            total_shards += chunk.shard_count;
        }

        assert_eq!(total_shards, old_shards);
    }

    #[test]
    fn test_specs_to_job_chunks() {
        let specs = vec![
            ReshardChunkSpec {
                index: 0,
                total: 2,
                start_shard: 0,
                end_shard: 32,
                shard_count: 32,
            },
            ReshardChunkSpec {
                index: 1,
                total: 2,
                start_shard: 32,
                end_shard: 64,
                shard_count: 32,
            },
        ];

        let job_chunks = reshard_specs_to_job_chunks(specs);
        assert_eq!(job_chunks.len(), 2);
        assert_eq!(job_chunks[0].index, 0);
        assert_eq!(job_chunks[0].total, 2);
        assert_eq!(job_chunks[0].start, "0");
        assert_eq!(job_chunks[0].end, "32");
        assert_eq!(job_chunks[1].index, 1);
        assert_eq!(job_chunks[1].start, "32");
        assert_eq!(job_chunks[1].end, "64");
    }

    #[test]
    fn test_parse_reshard_chunk() {
        let job_chunk = JobChunk {
            index: 0,
            total: 1,
            start: "16".to_string(),
            end: "32".to_string(),
            size_bytes: 16,
        };

        let (start, end) = parse_reshard_chunk(&job_chunk).unwrap();
        assert_eq!(start, 16);
        assert_eq!(end, 32);
    }

    #[test]
    fn test_parse_reshard_chunk_invalid() {
        let job_chunk = JobChunk {
            index: 0,
            total: 1,
            start: "invalid".to_string(),
            end: "32".to_string(),
            size_bytes: 16,
        };

        assert!(parse_reshard_chunk(&job_chunk).is_err());
    }

    #[test]
    fn test_large_reshard() {
        // Simulate resharding from 64 to 128 shards
        let chunks = split_reshard_into_chunks(64, 128, 8);
        assert_eq!(chunks.len(), 8);

        // Verify sequential coverage
        let mut last_end = 0;
        for chunk in &chunks {
            assert_eq!(chunk.start_shard, last_end);
            assert!(chunk.end_shard > chunk.start_shard);
            last_end = chunk.end_shard;
        }

        assert_eq!(last_end, 64);
    }

    #[test]
    fn test_uneven_chunk_distribution() {
        // 50 shards with 12 per chunk = 4 chunks (12, 12, 12, 14)
        let chunks = split_reshard_into_chunks(50, 100, 12);
        assert_eq!(chunks.len(), 5);

        assert_eq!(chunks[0].shard_count, 12);
        assert_eq!(chunks[1].shard_count, 12);
        assert_eq!(chunks[2].shard_count, 12);
        assert_eq!(chunks[3].shard_count, 12);
        assert_eq!(chunks[4].shard_count, 2);
    }
}
