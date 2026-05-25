//! Dump import chunking for Mode C coordinator (plan §13.9 + §14.5).
//!
//! Splits large NDJSON dumps into chunks on line boundaries.
//! Each chunk can be processed independently by any pod.

use crate::mode_c_coordinator::JobChunk;
use std::io::{BufRead, BufReader, Cursor};

/// Chunk specification for a dump import.
#[derive(Debug, Clone)]
pub struct DumpChunkSpec {
    /// Chunk index (0-based).
    pub index: u32,
    /// Total number of chunks.
    pub total: u32,
    /// Starting byte offset.
    pub start_offset: u64,
    /// Ending byte offset.
    pub end_offset: u64,
    /// Estimated size in bytes.
    pub size_bytes: u64,
}

/// Split dump data into chunks on line boundaries.
///
/// Returns a vector of chunk specifications. Each chunk contains the
/// byte offsets for processing that chunk of the dump.
///
/// # Arguments
/// * `data` - The full dump data (NDJSON)
/// * `chunk_size_bytes` - Target chunk size in bytes
///
/// # Returns
/// A vector of chunk specifications
pub fn split_dump_into_chunks(data: &[u8], chunk_size_bytes: u64) -> Vec<DumpChunkSpec> {
    if data.is_empty() {
        return Vec::new();
    }

    let total_size = data.len() as u64;

    // If the data is smaller than the chunk size, return a single chunk
    if total_size <= chunk_size_bytes {
        return vec![DumpChunkSpec {
            index: 0,
            total: 1,
            start_offset: 0,
            end_offset: total_size,
            size_bytes: total_size,
        }];
    }

    let mut chunks = Vec::new();
    let mut current_offset: u64 = 0;
    let mut chunk_index = 0u32;

    // Use a cursor to read through the data
    let cursor = Cursor::new(data);
    let reader = BufReader::new(cursor);

    // Track line boundaries for chunking
    let line_start = 0u64;
    let mut last_line_end = 0u64;

    for line_result in reader.lines() {
        match line_result {
            Ok(line) => {
                let line_bytes = line.len() as u64 + 1; // +1 for newline
                let line_end = last_line_end + line_bytes;

                // Check if we've exceeded the chunk size since the last chunk start
                if line_end - current_offset >= chunk_size_bytes && current_offset < last_line_end {
                    // Create a chunk up to the previous line end
                    chunks.push(DumpChunkSpec {
                        index: chunk_index,
                        total: 0, // Will be filled in later
                        start_offset: current_offset,
                        end_offset: last_line_end,
                        size_bytes: last_line_end - current_offset,
                    });

                    chunk_index += 1;
                    current_offset = last_line_end;
                }

                last_line_end = line_end;
            }
            Err(_) => break,
        }
    }

    // Add the final chunk
    if current_offset < total_size {
        chunks.push(DumpChunkSpec {
            index: chunk_index,
            total: 0, // Will be filled in later
            start_offset: current_offset,
            end_offset: total_size,
            size_bytes: total_size - current_offset,
        });
    }

    // Update the total count for all chunks
    let total = chunks.len() as u32;
    for chunk in &mut chunks {
        chunk.total = total;
    }

    chunks
}

/// Convert dump chunk specs to job chunks for the Mode C coordinator.
pub fn dump_specs_to_job_chunks(specs: Vec<DumpChunkSpec>) -> Vec<JobChunk> {
    specs
        .into_iter()
        .map(|spec| JobChunk {
            index: spec.index,
            total: spec.total,
            start: spec.start_offset.to_string(),
            end: spec.end_offset.to_string(),
            size_bytes: spec.size_bytes,
        })
        .collect()
}

/// Extract a chunk of data from the full dump.
///
/// Returns the byte slice for the specified chunk.
pub fn extract_chunk_data<'a>(data: &'a [u8], chunk: &DumpChunkSpec) -> &'a [u8] {
    let start = chunk.start_offset as usize;
    let end = chunk.end_offset as usize;
    &data[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_data(lines: usize, line_size: usize) -> Vec<u8> {
        let mut data = Vec::new();
        for i in 0..lines {
            let line = format!(
                "{{\"id\": {}, \"data\": \"{}\"}}\n",
                i,
                "x".repeat(line_size)
            );
            data.extend_from_slice(line.as_bytes());
        }
        data
    }

    #[test]
    fn test_empty_data() {
        let data = Vec::new();
        let chunks = split_dump_into_chunks(&data, 1024);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_small_data() {
        let data = b"{\"id\": 1}\n{\"id\": 2}\n".to_vec();
        let chunks = split_dump_into_chunks(&data, 1024);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start_offset, 0);
        assert_eq!(chunks[0].end_offset, data.len() as u64);
    }

    #[test]
    fn test_single_chunk() {
        let data = create_test_data(10, 50);
        let chunks = split_dump_into_chunks(&data, 10_000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[0].total, 1);
    }

    #[test]
    fn test_multiple_chunks() {
        // Create data that will split into multiple chunks
        // Each line is about 70 bytes, so 100 lines = ~7KB
        let data = create_test_data(100, 50);
        let chunk_size = 2_000; // Should get ~3-4 chunks
        let chunks = split_dump_into_chunks(&data, chunk_size);

        assert!(chunks.len() > 1);

        // Verify chunks are sequential and cover the full range
        let mut last_end = 0;
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.index, i as u32);
            assert_eq!(chunk.start_offset, last_end);
            assert!(chunk.end_offset > chunk.start_offset);
            last_end = chunk.end_offset;
        }

        // Last chunk should end at the data size
        assert_eq!(chunks.last().unwrap().end_offset, data.len() as u64);
    }

    #[test]
    fn test_chunk_boundaries_on_lines() {
        let data = create_test_data(20, 50);
        let chunk_size = 500;
        let chunks = split_dump_into_chunks(&data, chunk_size);

        // Verify each chunk starts and ends on line boundaries
        for chunk in &chunks {
            let chunk_data = extract_chunk_data(&data, chunk);

            // Should start with valid JSON
            assert!(chunk_data.starts_with(b"{"));
            assert!(chunk_data.ends_with(b"\n") || chunk_data.ends_with(b"}"));
        }
    }

    #[test]
    fn test_extract_chunk() {
        let data = b"line1\nline2\nline3\n".to_vec();
        let chunks = split_dump_into_chunks(&data, 5);

        for chunk in &chunks {
            let chunk_data = extract_chunk_data(&data, chunk);
            // Verify the chunk data is within bounds
            assert!(chunk_data.len() <= (chunk.end_offset - chunk.start_offset) as usize);
        }
    }

    #[test]
    fn test_specs_to_job_chunks() {
        let specs = vec![
            DumpChunkSpec {
                index: 0,
                total: 2,
                start_offset: 0,
                end_offset: 100,
                size_bytes: 100,
            },
            DumpChunkSpec {
                index: 1,
                total: 2,
                start_offset: 100,
                end_offset: 200,
                size_bytes: 100,
            },
        ];

        let job_chunks = dump_specs_to_job_chunks(specs);
        assert_eq!(job_chunks.len(), 2);
        assert_eq!(job_chunks[0].index, 0);
        assert_eq!(job_chunks[0].total, 2);
        assert_eq!(job_chunks[0].start, "0");
        assert_eq!(job_chunks[0].end, "100");
        assert_eq!(job_chunks[1].index, 1);
        assert_eq!(job_chunks[1].start, "100");
        assert_eq!(job_chunks[1].end, "200");
    }

    #[test]
    fn test_large_file_chunking() {
        // Simulate a 1GB file split into 256MB chunks
        let line_size = 100;
        let lines_per_chunk = (256 * 1024 * 1024) / line_size;
        let total_lines = lines_per_chunk * 4; // 4 chunks

        let data = create_test_data(total_lines as usize, line_size - 20);
        let chunks = split_dump_into_chunks(&data, 256 * 1024 * 1024);

        // Should get approximately 4 chunks
        assert!(chunks.len() >= 3 && chunks.len() <= 5);

        // Verify total coverage
        let total_covered: u64 = chunks.iter().map(|c| c.size_bytes).sum();
        assert_eq!(total_covered, data.len() as u64);
    }

    #[test]
    fn test_chunks_cover_full_data() {
        let data = create_test_data(1000, 100);
        let chunks = split_dump_into_chunks(&data, 50_000);

        let mut total_size = 0u64;
        for chunk in &chunks {
            total_size += chunk.size_bytes;
        }

        assert_eq!(total_size, data.len() as u64);
    }
}
