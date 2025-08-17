//! Encoding utilities for compact storage of graph data
//!
//! Implements efficient encoding schemes for neighbor lists and other graph structures.

use crate::utils::elias_fano::{EliasFanoConfig, PartitionedEliasFano};
use crate::{AsterError, Result, VertexId};
use std::collections::BTreeSet;

/// High-performance encode a list of vertex IDs using optimized delta compression
/// Vertices are sorted and stored as deltas from the previous value
pub fn encode_neighbors(neighbors: &[VertexId]) -> Vec<u8> {
    if neighbors.is_empty() {
        return Vec::new();
    }

    // Pre-allocate with reasonable capacity to reduce reallocations
    let mut sorted_neighbors: Vec<u64> = Vec::with_capacity(neighbors.len());
    sorted_neighbors.extend(neighbors.iter().map(|v| v.as_u64()));

    // Use unstable sort for better performance when order of equal elements doesn't matter
    sorted_neighbors.sort_unstable();

    // Remove duplicates efficiently
    sorted_neighbors.dedup();

    // Pre-allocate output buffer with estimated size
    // Each varint can be up to 10 bytes, but most will be much smaller
    let estimated_size = 1 + sorted_neighbors.len() * 3; // Conservative estimate
    let mut encoded = Vec::with_capacity(estimated_size);

    // First, write the count
    encode_varint(sorted_neighbors.len() as u64, &mut encoded);

    // Optimized delta encoding - batch process when possible
    if sorted_neighbors.len() >= 8 {
        // For larger lists, process in chunks for better cache locality
        let mut prev = 0u64;

        // Process the bulk in chunks of 8 for potential SIMD optimization
        let chunks = sorted_neighbors.chunks(8);
        for chunk in chunks {
            for &vertex_id in chunk {
                let delta = vertex_id - prev;
                encode_varint(delta, &mut encoded);
                prev = vertex_id;
            }
        }
    } else {
        // For small lists, use simple sequential processing
        let mut prev = 0u64;
        for &vertex_id in &sorted_neighbors {
            let delta = vertex_id - prev;
            encode_varint(delta, &mut encoded);
            prev = vertex_id;
        }
    }

    // Shrink to fit to reduce memory usage for long-term storage
    encoded.shrink_to_fit();
    encoded
}

/// Decode a list of vertex IDs from delta-compressed format
pub fn decode_neighbors(data: &[u8]) -> Result<Vec<VertexId>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut cursor = 0;

    // Read the count
    let (count, new_cursor) = decode_varint(data, cursor)?;
    cursor = new_cursor;

    let mut neighbors = Vec::with_capacity(count as usize);
    let mut prev = 0u64;

    // Decode the deltas
    for _ in 0..count {
        let (delta, new_cursor) = decode_varint(data, cursor)?;
        cursor = new_cursor;

        prev += delta;
        neighbors.push(VertexId::from_u64(prev));
    }

    Ok(neighbors)
}

/// Encode an unsigned integer using variable-length encoding (LEB128)
fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

/// Decode an unsigned integer from variable-length encoding
fn decode_varint(data: &[u8], start: usize) -> Result<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;
    let mut cursor = start;

    loop {
        if cursor >= data.len() {
            return Err(AsterError::storage(
                "Unexpected end of data while decoding varint",
            ));
        }

        let byte = data[cursor];
        cursor += 1;

        value |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            return Err(AsterError::storage("Varint overflow"));
        }
    }

    Ok((value, cursor))
}

/// Merge two encoded neighbor lists, removing duplicates
pub fn merge_encoded_neighbors(encoded1: &[u8], encoded2: &[u8]) -> Result<Vec<u8>> {
    let neighbors1 = decode_neighbors(encoded1)?;
    let neighbors2 = decode_neighbors(encoded2)?;

    // Use BTreeSet to automatically handle sorting and deduplication
    let mut merged: BTreeSet<u64> = neighbors1.iter().map(|v| v.as_u64()).collect();
    for vertex in neighbors2 {
        merged.insert(vertex.as_u64());
    }

    // Convert back to VertexId vector
    let result: Vec<VertexId> = merged.into_iter().map(VertexId::from_u64).collect();
    Ok(encode_neighbors(&result))
}

/// Remove vertices from an encoded neighbor list
pub fn remove_from_encoded_neighbors(encoded: &[u8], to_remove: &[VertexId]) -> Result<Vec<u8>> {
    let mut neighbors = decode_neighbors(encoded)?;
    let remove_set: BTreeSet<u64> = to_remove.iter().map(|v| v.as_u64()).collect();

    neighbors.retain(|v| !remove_set.contains(&v.as_u64()));
    Ok(encode_neighbors(&neighbors))
}

/// Check if a vertex exists in an encoded neighbor list (binary search)
pub fn contains_neighbor(encoded: &[u8], target: VertexId) -> Result<bool> {
    let neighbors = decode_neighbors(encoded)?;
    let target_id = target.as_u64();

    // Binary search since neighbors are sorted
    Ok(neighbors
        .binary_search_by_key(&target_id, |v| v.as_u64())
        .is_ok())
}

/// Get the number of neighbors in an encoded list without full decoding
pub fn count_neighbors(data: &[u8]) -> Result<usize> {
    if data.is_empty() {
        return Ok(0);
    }

    let (count, _) = decode_varint(data, 0)?;
    Ok(count as usize)
}

/// Encode a single vertex ID with optional timestamp for versioning
pub fn encode_vertex_entry(vertex_id: VertexId, timestamp: Option<u64>) -> Vec<u8> {
    let mut encoded = Vec::new();

    // Vertex ID
    encode_varint(vertex_id.as_u64(), &mut encoded);

    // Optional timestamp
    if let Some(ts) = timestamp {
        encoded.push(1); // Has timestamp
        encode_varint(ts, &mut encoded);
    } else {
        encoded.push(0); // No timestamp
    }

    encoded
}

/// Decode a vertex entry
pub fn decode_vertex_entry(data: &[u8]) -> Result<(VertexId, Option<u64>)> {
    if data.is_empty() {
        return Err(AsterError::storage("Empty vertex entry data"));
    }

    let mut cursor = 0;

    // Decode vertex ID
    let (vertex_id, new_cursor) = decode_varint(data, cursor)?;
    cursor = new_cursor;

    if cursor >= data.len() {
        return Err(AsterError::storage("Incomplete vertex entry data"));
    }

    // Check for timestamp
    let has_timestamp = data[cursor];
    cursor += 1;

    let timestamp = if has_timestamp == 1 {
        if cursor >= data.len() {
            return Err(AsterError::storage("Missing timestamp in vertex entry"));
        }
        let (ts, _) = decode_varint(data, cursor)?;
        Some(ts)
    } else {
        None
    };

    Ok((VertexId::from_u64(vertex_id), timestamp))
}

/// Enhanced encoding using Partitioned Elias-Fano compression
/// Provides better compression than basic delta encoding for large neighbor lists
pub fn encode_neighbors_compressed(neighbors: &[VertexId]) -> Result<Vec<u8>> {
    if neighbors.is_empty() {
        return Ok(Vec::new());
    }

    // Use Partitioned Elias-Fano for better compression on large lists
    let config = EliasFanoConfig {
        segment_count: calculate_optimal_segments(neighbors.len()),
        prefix_length: 0, // Auto-calculate
        max_segment_size: 4096,
        min_segment_size: 64,
        use_optimal_allocation: true,
    };

    let elias_fano = PartitionedEliasFano::encode(neighbors, config)?;

    // Serialize to bytes
    let mut encoded = Vec::new();

    // Write header indicating this uses Elias-Fano encoding
    encoded.push(0xFF); // Magic byte for Elias-Fano format

    // Serialize the compressed structure
    let serialized = bincode::serialize(&elias_fano)
        .map_err(|e| AsterError::storage(&format!("Failed to serialize Elias-Fano: {}", e)))?;

    encoded.extend(serialized);
    Ok(encoded)
}

/// Decode neighbors encoded with Partitioned Elias-Fano compression
pub fn decode_neighbors_compressed(data: &[u8]) -> Result<Vec<VertexId>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    // Check for Elias-Fano magic byte
    if data[0] == 0xFF {
        let elias_fano: PartitionedEliasFano = bincode::deserialize(&data[1..]).map_err(|e| {
            AsterError::storage(&format!("Failed to deserialize Elias-Fano: {}", e))
        })?;

        return elias_fano.decode();
    }

    // Fall back to basic encoding for backward compatibility
    decode_neighbors(data)
}

/// Adaptive encoding that chooses between basic and compressed encoding
/// based on the size and characteristics of the neighbor list
pub fn encode_neighbors_adaptive(neighbors: &[VertexId]) -> Result<Vec<u8>> {
    if neighbors.is_empty() {
        return Ok(Vec::new());
    }

    // For small lists, use basic encoding to avoid compression overhead
    if neighbors.len() < 32 {
        return Ok(encode_neighbors(neighbors));
    }

    // For larger lists, try both encodings and use the better one
    let basic_encoded = encode_neighbors(neighbors);
    let compressed_encoded = encode_neighbors_compressed(neighbors)?;

    if compressed_encoded.len() < basic_encoded.len() {
        Ok(compressed_encoded)
    } else {
        Ok(basic_encoded)
    }
}

/// Decode neighbors with automatic format detection
pub fn decode_neighbors_adaptive(data: &[u8]) -> Result<Vec<VertexId>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    // Check for Elias-Fano format
    if data[0] == 0xFF {
        decode_neighbors_compressed(data)
    } else {
        decode_neighbors(data)
    }
}

/// Calculate optimal number of segments for Partitioned Elias-Fano encoding
fn calculate_optimal_segments(neighbor_count: usize) -> usize {
    // Balance between compression and access speed
    // More segments = faster access but less compression
    match neighbor_count {
        0..=32 => 1,
        33..=128 => 4,
        129..=512 => 8,
        513..=2048 => 16,
        _ => 32.min(neighbor_count / 64), // At least 64 items per segment
    }
}

/// Check if compressed neighbors contain a specific vertex (optimized)
pub fn contains_neighbor_compressed(data: &[u8], target: VertexId) -> Result<bool> {
    if data.is_empty() {
        return Ok(false);
    }

    if data[0] == 0xFF {
        let elias_fano: PartitionedEliasFano = bincode::deserialize(&data[1..]).map_err(|e| {
            AsterError::storage(&format!("Failed to deserialize Elias-Fano: {}", e))
        })?;

        Ok(elias_fano.contains(target))
    } else {
        contains_neighbor(data, target)
    }
}

/// Get compression statistics for encoded neighbor list
pub fn get_encoding_stats(original_neighbors: &[VertexId], encoded_data: &[u8]) -> EncodingStats {
    let original_size = original_neighbors.len() * 8; // 8 bytes per u64
    let compressed_size = encoded_data.len();

    EncodingStats {
        original_count: original_neighbors.len(),
        original_size_bytes: original_size,
        compressed_size_bytes: compressed_size,
        compression_ratio: if original_size > 0 {
            compressed_size as f64 / original_size as f64
        } else {
            0.0
        },
        encoding_type: if !encoded_data.is_empty() && encoded_data[0] == 0xFF {
            "Partitioned Elias-Fano".to_string()
        } else {
            "Delta Compression".to_string()
        },
    }
}

/// Statistics about neighbor list encoding
#[derive(Debug, Clone)]
pub struct EncodingStats {
    pub original_count: usize,
    pub original_size_bytes: usize,
    pub compressed_size_bytes: usize,
    pub compression_ratio: f64,
    pub encoding_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        let test_values = vec![0, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX as u64];

        for &value in &test_values {
            let mut encoded = Vec::new();
            encode_varint(value, &mut encoded);

            let (decoded, cursor) = decode_varint(&encoded, 0).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(cursor, encoded.len());
        }
    }

    #[test]
    fn test_neighbor_encoding() {
        let neighbors = vec![
            VertexId::from_u64(100),
            VertexId::from_u64(50), // Will be sorted
            VertexId::from_u64(200),
            VertexId::from_u64(50), // Duplicate will be removed
        ];

        let encoded = encode_neighbors(&neighbors);
        let decoded = decode_neighbors(&encoded).unwrap();

        // Should have 3 unique neighbors, sorted
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].as_u64(), 50);
        assert_eq!(decoded[1].as_u64(), 100);
        assert_eq!(decoded[2].as_u64(), 200);
    }

    #[test]
    fn test_empty_neighbors() {
        let neighbors = vec![];
        let encoded = encode_neighbors(&neighbors);
        let decoded = decode_neighbors(&encoded).unwrap();

        assert!(encoded.is_empty());
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_merge_neighbors() {
        let neighbors1 = vec![VertexId::from_u64(10), VertexId::from_u64(30)];
        let neighbors2 = vec![VertexId::from_u64(20), VertexId::from_u64(30)]; // 30 is duplicate

        let encoded1 = encode_neighbors(&neighbors1);
        let encoded2 = encode_neighbors(&neighbors2);
        let merged = merge_encoded_neighbors(&encoded1, &encoded2).unwrap();
        let decoded = decode_neighbors(&merged).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].as_u64(), 10);
        assert_eq!(decoded[1].as_u64(), 20);
        assert_eq!(decoded[2].as_u64(), 30);
    }

    #[test]
    fn test_remove_neighbors() {
        let neighbors = vec![
            VertexId::from_u64(10),
            VertexId::from_u64(20),
            VertexId::from_u64(30),
        ];
        let to_remove = vec![VertexId::from_u64(20)];

        let encoded = encode_neighbors(&neighbors);
        let after_removal = remove_from_encoded_neighbors(&encoded, &to_remove).unwrap();
        let decoded = decode_neighbors(&after_removal).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].as_u64(), 10);
        assert_eq!(decoded[1].as_u64(), 30);
    }

    #[test]
    fn test_contains_neighbor() {
        let neighbors = vec![
            VertexId::from_u64(10),
            VertexId::from_u64(20),
            VertexId::from_u64(30),
        ];

        let encoded = encode_neighbors(&neighbors);

        assert!(contains_neighbor(&encoded, VertexId::from_u64(20)).unwrap());
        assert!(!contains_neighbor(&encoded, VertexId::from_u64(25)).unwrap());
    }

    #[test]
    fn test_count_neighbors() {
        let neighbors = vec![
            VertexId::from_u64(10),
            VertexId::from_u64(20),
            VertexId::from_u64(30),
        ];

        let encoded = encode_neighbors(&neighbors);
        assert_eq!(count_neighbors(&encoded).unwrap(), 3);

        let empty = encode_neighbors(&[]);
        assert_eq!(count_neighbors(&empty).unwrap(), 0);
    }

    #[test]
    fn test_vertex_entry_encoding() {
        let vertex_id = VertexId::from_u64(123);
        let timestamp = Some(456u64);

        let encoded = encode_vertex_entry(vertex_id, timestamp);
        let (decoded_id, decoded_ts) = decode_vertex_entry(&encoded).unwrap();

        assert_eq!(decoded_id, vertex_id);
        assert_eq!(decoded_ts, timestamp);

        // Test without timestamp
        let encoded_no_ts = encode_vertex_entry(vertex_id, None);
        let (decoded_id_no_ts, decoded_ts_no_ts) = decode_vertex_entry(&encoded_no_ts).unwrap();

        assert_eq!(decoded_id_no_ts, vertex_id);
        assert_eq!(decoded_ts_no_ts, None);
    }

    #[test]
    fn test_compressed_encoding() {
        let neighbors = vec![
            VertexId::from_u64(1000),
            VertexId::from_u64(1010),
            VertexId::from_u64(1020),
            VertexId::from_u64(1030),
            VertexId::from_u64(1040),
        ];

        let encoded = encode_neighbors_compressed(&neighbors).unwrap();
        let decoded = decode_neighbors_compressed(&encoded).unwrap();

        assert_eq!(neighbors, decoded);
        assert_eq!(encoded[0], 0xFF); // Should have Elias-Fano magic byte
    }

    #[test]
    fn test_adaptive_encoding() {
        // Small list should use basic encoding
        let small_neighbors = vec![VertexId::from_u64(10), VertexId::from_u64(20)];

        let encoded_small = encode_neighbors_adaptive(&small_neighbors).unwrap();
        let decoded_small = decode_neighbors_adaptive(&encoded_small).unwrap();
        assert_eq!(small_neighbors, decoded_small);
        assert_ne!(encoded_small[0], 0xFF); // Should not use Elias-Fano

        // Large list might use compressed encoding
        let mut large_neighbors = Vec::new();
        for i in 0..100 {
            large_neighbors.push(VertexId::from_u64(i * 10));
        }

        let encoded_large = encode_neighbors_adaptive(&large_neighbors).unwrap();
        let decoded_large = decode_neighbors_adaptive(&encoded_large).unwrap();
        assert_eq!(large_neighbors, decoded_large);
    }

    #[test]
    fn test_contains_neighbor_compressed() {
        let neighbors = vec![
            VertexId::from_u64(1000),
            VertexId::from_u64(2000),
            VertexId::from_u64(3000),
        ];

        let encoded = encode_neighbors_compressed(&neighbors).unwrap();

        assert!(contains_neighbor_compressed(&encoded, VertexId::from_u64(2000)).unwrap());
        assert!(!contains_neighbor_compressed(&encoded, VertexId::from_u64(2500)).unwrap());
    }

    #[test]
    fn test_encoding_stats() {
        let neighbors = vec![
            VertexId::from_u64(1000000),
            VertexId::from_u64(1000001),
            VertexId::from_u64(1000002),
            VertexId::from_u64(1000003),
        ];

        let encoded = encode_neighbors_compressed(&neighbors).unwrap();
        let stats = get_encoding_stats(&neighbors, &encoded);

        assert_eq!(stats.original_count, 4);
        assert_eq!(stats.original_size_bytes, 32); // 4 * 8 bytes
        assert_eq!(stats.encoding_type, "Partitioned Elias-Fano");
        assert!(stats.compression_ratio > 0.0);
    }
}
