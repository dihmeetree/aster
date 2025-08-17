//! Partitioned Elias-Fano encoding for efficient neighbor list compression
//!
//! Implements the two-level compression structure described in the Aster paper:
//! - First level: Starting ID of each segment  
//! - Second level: Elias-Fano encoding within segments
//!
//! Space requirement: 2 + log₂(N_j/t) bits per element
//! where N_j is the number of elements and t is the segment count

use crate::{AsterError, Result, VertexId};
use serde::{Deserialize, Serialize};

/// Configuration for Partitioned Elias-Fano encoding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EliasFanoConfig {
    /// Number of segments to partition the data into
    pub segment_count: usize,
    /// Prefix length for high bits (automatically calculated if 0)
    pub prefix_length: usize,
    /// Maximum segment size (paper-specified optimization)
    pub max_segment_size: usize,
    /// Minimum segment size for efficient encoding
    pub min_segment_size: usize,
    /// Use paper-specified optimal bit allocation
    pub use_optimal_allocation: bool,
}

impl EliasFanoConfig {
    /// Create configuration with paper-specified parameters
    pub fn paper_specification() -> Self {
        Self {
            segment_count: 16,            // t = 16 segments (paper default)
            prefix_length: 0,             // Auto-calculate based on data
            max_segment_size: 4096,       // Align with block size (B = 4KB)
            min_segment_size: 64,         // Minimum for efficient encoding
            use_optimal_allocation: true, // Use paper's optimal bit allocation
        }
    }

    /// Create configuration optimized for small neighbor lists
    pub fn small_lists() -> Self {
        Self {
            segment_count: 4,
            prefix_length: 0,
            max_segment_size: 1024,
            min_segment_size: 16,
            use_optimal_allocation: true,
        }
    }

    /// Create configuration optimized for large neighbor lists
    pub fn large_lists() -> Self {
        Self {
            segment_count: 32,
            prefix_length: 0,
            max_segment_size: 8192,
            min_segment_size: 128,
            use_optimal_allocation: true,
        }
    }
}

impl Default for EliasFanoConfig {
    fn default() -> Self {
        Self::paper_specification()
    }
}

/// Partitioned Elias-Fano encoded neighbor list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionedEliasFano {
    /// Configuration used for encoding
    config: EliasFanoConfig,
    /// Starting vertex ID for each segment
    segment_starts: Vec<u64>,
    /// Elias-Fano encoded segments
    segments: Vec<EliasFanoSegment>,
    /// Total number of vertices in the list
    total_count: usize,
    /// Compression statistics
    compression_stats: CompressionStats,
}

/// Statistics about the compression performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    /// Original size in bytes (uncompressed)
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Compression ratio (original/compressed)
    pub compression_ratio: f64,
    /// Average bits per vertex ID
    pub bits_per_vertex: f64,
    /// Number of segments created
    pub num_segments: usize,
    /// Average segment size
    pub avg_segment_size: f64,
    /// Space overhead from segmentation
    pub segmentation_overhead: usize,
}

/// Single Elias-Fano encoded segment
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EliasFanoSegment {
    /// High bits (prefix) for each element
    high_bits: Vec<u8>,
    /// Low bits for each element
    low_bits: Vec<u8>,
    /// Number of bits per low value
    low_bits_per_value: u8,
    /// Number of elements in this segment
    count: usize,
    /// Base value (minimum value in segment)
    base_value: u64,
}

/// Information about segment boundaries for optimal partitioning
#[derive(Debug, Clone)]
struct SegmentInfo {
    /// Starting index in the vertex array
    start_index: usize,
    /// Ending index in the vertex array
    end_index: usize,
    /// Estimated compressed size
    estimated_size: usize,
}

impl PartitionedEliasFano {
    /// Encode a sorted list of vertex IDs using Partitioned Elias-Fano
    pub fn encode(vertices: &[VertexId], config: EliasFanoConfig) -> Result<Self> {
        if vertices.is_empty() {
            return Ok(Self {
                config,
                segment_starts: Vec::new(),
                segments: Vec::new(),
                total_count: 0,
                compression_stats: CompressionStats {
                    original_size: 0,
                    compressed_size: 0,
                    compression_ratio: 1.0,
                    bits_per_vertex: 0.0,
                    num_segments: 0,
                    avg_segment_size: 0.0,
                    segmentation_overhead: 0,
                },
            });
        }

        // Convert VertexIds to u64 for processing
        let mut vertex_ids: Vec<u64> = vertices.iter().map(|v| v.as_u64()).collect();
        vertex_ids.sort_unstable();
        vertex_ids.dedup();

        let total_count = vertex_ids.len();

        // Use paper-specified adaptive segmentation
        let segments_info = Self::calculate_optimal_segments(&vertex_ids, &config);

        let mut segment_starts = Vec::new();
        let mut segments = Vec::new();

        // Create segments based on optimal partitioning
        for segment_info in segments_info {
            let chunk = &vertex_ids[segment_info.start_index..segment_info.end_index];
            if chunk.is_empty() {
                continue;
            }

            segment_starts.push(chunk[0]);
            let segment = Self::encode_segment(chunk, &config)?;
            segments.push(segment);
        }

        // Calculate compression statistics
        let compression_stats =
            Self::calculate_compression_stats(&vertex_ids, &segments, &segment_starts, &config);

        Ok(Self {
            config,
            segment_starts,
            segments,
            total_count,
            compression_stats,
        })
    }

    /// Calculate optimal segment boundaries based on paper specifications
    fn calculate_optimal_segments(
        vertex_ids: &[u64],
        config: &EliasFanoConfig,
    ) -> Vec<SegmentInfo> {
        let total_count = vertex_ids.len();
        let mut segments = Vec::new();

        if total_count <= config.min_segment_size {
            // Single segment for small lists
            segments.push(SegmentInfo {
                start_index: 0,
                end_index: total_count,
                estimated_size: total_count,
            });
            return segments;
        }

        // Calculate base segment size
        let mut base_segment_size = (total_count + config.segment_count - 1) / config.segment_count;
        base_segment_size =
            base_segment_size.clamp(config.min_segment_size, config.max_segment_size);

        let mut current_index = 0;
        while current_index < total_count {
            let end_index = std::cmp::min(current_index + base_segment_size, total_count);

            // Adjust segment boundary to avoid splitting dense clusters
            let adjusted_end = if end_index < total_count {
                Self::find_optimal_boundary(vertex_ids, current_index, end_index)
            } else {
                end_index
            };

            segments.push(SegmentInfo {
                start_index: current_index,
                end_index: adjusted_end,
                estimated_size: adjusted_end - current_index,
            });

            current_index = adjusted_end;
        }

        segments
    }

    /// Find optimal segment boundary to minimize compression overhead
    fn find_optimal_boundary(vertex_ids: &[u64], _start: usize, suggested_end: usize) -> usize {
        if suggested_end >= vertex_ids.len() {
            return vertex_ids.len();
        }

        // Look for gaps in the vertex ID sequence
        let search_window = std::cmp::min(32, vertex_ids.len() - suggested_end);
        let mut best_boundary = suggested_end;
        let mut max_gap = 0;

        for i in 0..search_window {
            let idx = suggested_end + i;
            if idx >= vertex_ids.len() - 1 {
                break;
            }

            let gap = vertex_ids[idx + 1] - vertex_ids[idx];
            if gap > max_gap {
                max_gap = gap;
                best_boundary = idx + 1;
            }
        }

        best_boundary
    }

    /// Calculate comprehensive compression statistics
    fn calculate_compression_stats(
        vertex_ids: &[u64],
        segments: &[EliasFanoSegment],
        segment_starts: &[u64],
        config: &EliasFanoConfig,
    ) -> CompressionStats {
        // Original size: 8 bytes per vertex ID
        let original_size = vertex_ids.len() * 8;

        // Compressed size: sum of all segment sizes plus metadata
        let mut compressed_size = 0;
        for segment in segments {
            compressed_size += segment.high_bits.len();
            compressed_size += segment.low_bits.len();
            compressed_size += 1; // low_bits_per_value
            compressed_size += 8; // base_value
            compressed_size += 8; // count (stored as usize, approximately 8 bytes)
        }

        // Add segment starts overhead
        let segmentation_overhead = segment_starts.len() * 8; // 8 bytes per u64
        compressed_size += segmentation_overhead;

        // Add configuration overhead
        compressed_size += std::mem::size_of::<EliasFanoConfig>();

        let compression_ratio = if compressed_size > 0 {
            original_size as f64 / compressed_size as f64
        } else {
            1.0
        };

        let bits_per_vertex = if vertex_ids.len() > 0 {
            (compressed_size * 8) as f64 / vertex_ids.len() as f64
        } else {
            0.0
        };

        let avg_segment_size = if segments.len() > 0 {
            vertex_ids.len() as f64 / segments.len() as f64
        } else {
            0.0
        };

        CompressionStats {
            original_size,
            compressed_size,
            compression_ratio,
            bits_per_vertex,
            num_segments: segments.len(),
            avg_segment_size,
            segmentation_overhead,
        }
    }

    /// Get compression statistics
    pub fn compression_stats(&self) -> &CompressionStats {
        &self.compression_stats
    }

    /// Get the theoretical compression ratio from the paper
    /// Formula: 2 + log₂(N_j/t) bits per element
    pub fn theoretical_compression(&self) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }

        let n_j = self.total_count as f64;
        let t = self.config.segment_count as f64;

        // Paper formula: 2 + log₂(N_j/t) bits per element
        let bits_per_element = 2.0 + (n_j / t).log2();

        // Original is 64 bits per element (8 bytes * 8 bits)
        64.0 / bits_per_element
    }

    /// Encode a single segment using Elias-Fano
    fn encode_segment(vertices: &[u64], config: &EliasFanoConfig) -> Result<EliasFanoSegment> {
        if vertices.is_empty() {
            return Ok(EliasFanoSegment {
                high_bits: Vec::new(),
                low_bits: Vec::new(),
                low_bits_per_value: 0,
                count: 0,
                base_value: 0,
            });
        }

        let count = vertices.len();
        let base_value = vertices[0];
        let max_value = vertices[vertices.len() - 1];

        // Calculate delta values (difference from base)
        let deltas: Vec<u64> = vertices.iter().map(|&v| v - base_value).collect();
        let max_delta = max_value - base_value;

        // Calculate optimal bit split using paper-specified algorithm
        let low_bits_per_value = if max_delta == 0 {
            0
        } else if config.use_optimal_allocation {
            // Paper-specified optimal allocation: minimize expected space
            // Formula: k = log₂(N_j/t) where N_j is segment size and t is total segments
            let segment_size = vertices.len() as f64;
            let expected_k = (segment_size / config.segment_count as f64).log2();
            let optimal_k = expected_k.round() as u8;

            // Ensure k is reasonable for the actual data range
            let max_bits = if max_delta == 0 {
                0
            } else {
                (max_delta as f64).log2().ceil() as u8
            };
            std::cmp::min(optimal_k, std::cmp::min(max_bits, 32)) // Cap at 32 bits
        } else {
            // Simple bit split for compatibility
            let total_bits = (max_delta as f64).log2().ceil() as u8;
            std::cmp::min(total_bits / 2, 16)
        };

        let low_mask = if low_bits_per_value >= 64 {
            !0
        } else {
            (1u64 << low_bits_per_value) - 1
        };

        // Split into high and low bits
        let mut high_values = Vec::new();
        let mut low_values = Vec::new();

        for &delta in &deltas {
            let high = if low_bits_per_value >= 64 {
                0
            } else {
                delta >> low_bits_per_value
            };
            let low = delta & low_mask;

            high_values.push(high);
            low_values.push(low);
        }

        // Encode high bits using unary encoding with select support
        let high_bits = Self::encode_high_bits(&high_values)?;

        // Pack low bits efficiently
        let low_bits = Self::pack_low_bits(&low_values, low_bits_per_value)?;

        Ok(EliasFanoSegment {
            high_bits,
            low_bits,
            low_bits_per_value,
            count,
            base_value,
        })
    }

    /// Encode high bits using unary encoding optimized for select operations
    fn encode_high_bits(high_values: &[u64]) -> Result<Vec<u8>> {
        let mut bits = Vec::new();
        let mut current_byte = 0u8;
        let mut bit_pos = 0;

        for &high in high_values {
            // Write 'high' zero bits followed by one 1 bit
            for _ in 0..high {
                if bit_pos >= 8 {
                    bits.push(current_byte);
                    current_byte = 0;
                    bit_pos = 0;
                }
                // Zero bit is already in place
                bit_pos += 1;
            }

            // Write the 1 bit
            if bit_pos >= 8 {
                bits.push(current_byte);
                current_byte = 0;
                bit_pos = 0;
            }
            current_byte |= 1 << bit_pos;
            bit_pos += 1;
        }

        if bit_pos > 0 {
            bits.push(current_byte);
        }

        Ok(bits)
    }

    /// Pack low bits efficiently into bytes
    fn pack_low_bits(low_values: &[u64], bits_per_value: u8) -> Result<Vec<u8>> {
        if bits_per_value == 0 {
            return Ok(Vec::new());
        }

        let total_bits = low_values.len() * bits_per_value as usize;
        let mut packed = vec![0u8; (total_bits + 7) / 8];

        for (i, &low_value) in low_values.iter().enumerate() {
            let start_bit = i * bits_per_value as usize;

            for bit in 0..bits_per_value {
                if (low_value >> bit) & 1 != 0 {
                    let bit_index = start_bit + bit as usize;
                    let byte_index = bit_index / 8;
                    let bit_in_byte = bit_index % 8;
                    packed[byte_index] |= 1 << bit_in_byte;
                }
            }
        }

        Ok(packed)
    }

    /// Decode the compressed neighbor list back to vertex IDs
    pub fn decode(&self) -> Result<Vec<VertexId>> {
        let mut result = Vec::with_capacity(self.total_count);

        for (i, segment) in self.segments.iter().enumerate() {
            let base = self.segment_starts[i];
            let decoded_segment = Self::decode_segment(segment, base)?;
            result.extend(decoded_segment.into_iter().map(VertexId::from_u64));
        }

        Ok(result)
    }

    /// Decode a single segment
    fn decode_segment(segment: &EliasFanoSegment, base: u64) -> Result<Vec<u64>> {
        if segment.count == 0 {
            return Ok(Vec::new());
        }

        let high_values = Self::decode_high_bits(&segment.high_bits, segment.count)?;
        let low_values =
            Self::unpack_low_bits(&segment.low_bits, segment.low_bits_per_value, segment.count)?;

        let mut result = Vec::with_capacity(segment.count);
        for i in 0..segment.count {
            let high = high_values[i];
            let low = low_values[i];
            let delta = if segment.low_bits_per_value >= 64 {
                high
            } else {
                (high << segment.low_bits_per_value) | low
            };
            result.push(base + delta);
        }

        Ok(result)
    }

    /// Decode high bits from unary encoding
    fn decode_high_bits(encoded: &[u8], count: usize) -> Result<Vec<u64>> {
        let mut result = Vec::with_capacity(count);
        let mut current_value = 0u64;
        let mut values_found = 0;

        for &byte in encoded {
            for bit in 0..8 {
                if values_found >= count {
                    break;
                }

                if (byte >> bit) & 1 != 0 {
                    // Found a 1 bit, this completes the current value
                    result.push(current_value);
                    current_value = 0;
                    values_found += 1;
                } else {
                    // Found a 0 bit, increment current value
                    current_value += 1;
                }
            }

            if values_found >= count {
                break;
            }
        }

        if result.len() != count {
            return Err(AsterError::storage("Invalid Elias-Fano high bits encoding"));
        }

        Ok(result)
    }

    /// Unpack low bits from packed representation
    fn unpack_low_bits(packed: &[u8], bits_per_value: u8, count: usize) -> Result<Vec<u64>> {
        if bits_per_value == 0 {
            return Ok(vec![0; count]);
        }

        let mut result = Vec::with_capacity(count);

        for i in 0..count {
            let start_bit = i * bits_per_value as usize;
            let mut value = 0u64;

            for bit in 0..bits_per_value {
                let bit_index = start_bit + bit as usize;
                let byte_index = bit_index / 8;
                let bit_in_byte = bit_index % 8;

                if byte_index < packed.len() && (packed[byte_index] >> bit_in_byte) & 1 != 0 {
                    value |= 1u64 << bit;
                }
            }

            result.push(value);
        }

        Ok(result)
    }

    /// Get the compressed size in bytes
    pub fn compressed_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        size += self.segment_starts.len() * std::mem::size_of::<u64>();

        for segment in &self.segments {
            size += segment.high_bits.len();
            size += segment.low_bits.len();
            size += std::mem::size_of::<EliasFanoSegment>();
        }

        size
    }

    /// Check if the list contains a specific vertex ID (optimized lookup)
    pub fn contains(&self, vertex_id: VertexId) -> bool {
        let target = vertex_id.as_u64();

        // Binary search for the appropriate segment
        match self.segment_starts.binary_search(&target) {
            Ok(_) => true, // Exact match in segment starts
            Err(insert_pos) => {
                if insert_pos == 0 {
                    return false; // Target is smaller than all segments
                }

                let segment_index = insert_pos - 1;
                if segment_index >= self.segments.len() {
                    return false;
                }

                // Search within the segment
                self.segment_contains(
                    &self.segments[segment_index],
                    target,
                    self.segment_starts[segment_index],
                )
            }
        }
    }

    /// Check if a segment contains the target value
    fn segment_contains(&self, segment: &EliasFanoSegment, target: u64, base: u64) -> bool {
        if target < base {
            return false;
        }

        let delta = target - base;

        // For small segments, just decode and search
        if segment.count <= 32 {
            if let Ok(decoded) = Self::decode_segment(segment, base) {
                return decoded.binary_search(&target).is_ok();
            }
        }

        // TODO: Implement efficient binary search without full decoding
        // This would require implementing select operations on the Elias-Fano encoding
        false
    }

    /// Get statistics about the encoding
    pub fn stats(&self) -> EliasFanoStats {
        let original_size = self.total_count * 8; // 8 bytes per u64
        let compressed_size = self.compressed_size();

        EliasFanoStats {
            total_vertices: self.total_count,
            segment_count: self.segments.len(),
            original_size_bytes: original_size,
            compressed_size_bytes: compressed_size,
            compression_ratio: if original_size > 0 {
                compressed_size as f64 / original_size as f64
            } else {
                0.0
            },
        }
    }
}

/// Statistics about Partitioned Elias-Fano encoding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EliasFanoStats {
    pub total_vertices: usize,
    pub segment_count: usize,
    pub original_size_bytes: usize,
    pub compressed_size_bytes: usize,
    pub compression_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_encoding_decoding() {
        let vertices = vec![
            VertexId::from_u64(10),
            VertexId::from_u64(15),
            VertexId::from_u64(20),
            VertexId::from_u64(25),
            VertexId::from_u64(30),
        ];

        let config = EliasFanoConfig::default();
        let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();
        let decoded = encoded.decode().unwrap();

        assert_eq!(vertices, decoded);
    }

    #[test]
    fn test_empty_list() {
        let vertices = vec![];
        let config = EliasFanoConfig::default();

        let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();
        let decoded = encoded.decode().unwrap();

        assert_eq!(vertices, decoded);
    }

    #[test]
    fn test_single_vertex() {
        let vertices = vec![VertexId::from_u64(42)];
        let config = EliasFanoConfig::default();

        let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();
        let decoded = encoded.decode().unwrap();

        assert_eq!(vertices, decoded);
    }

    #[test]
    fn test_duplicate_removal() {
        let vertices = vec![
            VertexId::from_u64(10),
            VertexId::from_u64(10),
            VertexId::from_u64(20),
            VertexId::from_u64(15),
            VertexId::from_u64(20),
        ];

        let expected = vec![
            VertexId::from_u64(10),
            VertexId::from_u64(15),
            VertexId::from_u64(20),
        ];

        let config = EliasFanoConfig::default();
        let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();
        let decoded = encoded.decode().unwrap();

        assert_eq!(expected, decoded);
    }

    #[test]
    fn test_large_list() {
        let mut vertices = Vec::new();
        for i in 0..1000 {
            vertices.push(VertexId::from_u64(i * 2)); // Even numbers
        }

        let config = EliasFanoConfig {
            segment_count: 10,
            prefix_length: 0,
            max_segment_size: 4096,
            min_segment_size: 64,
            use_optimal_allocation: true,
        };

        let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();
        let decoded = encoded.decode().unwrap();

        assert_eq!(vertices, decoded);

        // Check compression
        let stats = encoded.stats();
        assert!(stats.compression_ratio < 1.0); // Should be compressed
        assert_eq!(stats.total_vertices, 1000);
        assert_eq!(stats.segment_count, 10);
    }

    #[test]
    fn test_contains_lookup() {
        let vertices = vec![
            VertexId::from_u64(10),
            VertexId::from_u64(20),
            VertexId::from_u64(30),
            VertexId::from_u64(40),
            VertexId::from_u64(50),
        ];

        let config = EliasFanoConfig::default();
        let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();

        assert!(encoded.contains(VertexId::from_u64(10)));
        assert!(encoded.contains(VertexId::from_u64(30)));
        assert!(encoded.contains(VertexId::from_u64(50)));
        assert!(!encoded.contains(VertexId::from_u64(5)));
        assert!(!encoded.contains(VertexId::from_u64(25)));
        assert!(!encoded.contains(VertexId::from_u64(60)));
    }

    #[test]
    fn test_compression_ratio() {
        // Test with skewed data (should compress well)
        let mut vertices = Vec::new();

        // Dense cluster around 1000000
        for i in 1000000..1000100 {
            vertices.push(VertexId::from_u64(i));
        }

        let config = EliasFanoConfig {
            segment_count: 4,
            prefix_length: 0,
            max_segment_size: 4096,
            min_segment_size: 64,
            use_optimal_allocation: true,
        };

        let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();
        let stats = encoded.stats();

        // Should achieve good compression on clustered data
        assert!(stats.compression_ratio < 0.8);
        assert_eq!(stats.total_vertices, 100);
    }

    #[test]
    fn test_paper_specified_configuration() {
        let vertices: Vec<VertexId> = (1..5000).step_by(7).map(VertexId::from_u64).collect();

        let config = EliasFanoConfig::paper_specification();
        let encoded = PartitionedEliasFano::encode(&vertices, config.clone()).unwrap();

        // Verify paper-specified parameters
        let stats = encoded.compression_stats();
        assert_eq!(config.segment_count, 16);
        assert_eq!(config.max_segment_size, 4096);
        assert_eq!(config.min_segment_size, 64);
        assert!(config.use_optimal_allocation);

        // Test compression performance
        assert!(stats.compression_ratio > 1.0, "Should achieve compression");
        assert!(
            stats.bits_per_vertex < 64.0,
            "Should use fewer than 64 bits per vertex"
        );

        // Compare with theoretical prediction
        let theoretical = encoded.theoretical_compression();
        println!("Paper-specified compression:");
        println!("  Actual ratio: {:.2}x", stats.compression_ratio);
        println!("  Theoretical ratio: {:.2}x", theoretical);
        println!("  Bits per vertex: {:.2}", stats.bits_per_vertex);
        println!("  Segments: {}", stats.num_segments);

        // Verify decoding works
        let decoded = encoded.decode().unwrap();
        assert_eq!(vertices.len(), decoded.len());
    }

    #[test]
    fn test_optimal_bit_allocation() {
        let vertices: Vec<VertexId> = (100..2000).step_by(5).map(VertexId::from_u64).collect();

        // Test with optimal allocation enabled
        let config_optimal = EliasFanoConfig {
            segment_count: 16,
            use_optimal_allocation: true,
            max_segment_size: 4096,
            min_segment_size: 64,
            prefix_length: 0,
        };
        let encoded_optimal = PartitionedEliasFano::encode(&vertices, config_optimal).unwrap();

        // Test with simple allocation
        let config_simple = EliasFanoConfig {
            segment_count: 16,
            use_optimal_allocation: false,
            max_segment_size: 4096,
            min_segment_size: 64,
            prefix_length: 0,
        };
        let encoded_simple = PartitionedEliasFano::encode(&vertices, config_simple).unwrap();

        let stats_optimal = encoded_optimal.compression_stats();
        let stats_simple = encoded_simple.compression_stats();

        println!("Bit allocation comparison:");
        println!(
            "  Optimal: {:.2} bits/vertex",
            stats_optimal.bits_per_vertex
        );
        println!("  Simple: {:.2} bits/vertex", stats_simple.bits_per_vertex);

        // Both should work and decode correctly
        let decoded_optimal = encoded_optimal.decode().unwrap();
        let decoded_simple = encoded_simple.decode().unwrap();

        assert_eq!(vertices.len(), decoded_optimal.len());
        assert_eq!(vertices.len(), decoded_simple.len());

        // Both methods should provide valid compression (less than 64 bits per vertex)
        assert!(
            stats_optimal.bits_per_vertex < 64.0,
            "Optimal allocation should compress data"
        );
        assert!(
            stats_simple.bits_per_vertex < 64.0,
            "Simple allocation should compress data"
        );

        // Both methods should achieve some compression
        assert!(
            stats_optimal.compression_ratio > 1.0,
            "Optimal should achieve compression"
        );
        assert!(
            stats_simple.compression_ratio > 1.0,
            "Simple should achieve compression"
        );
    }

    #[test]
    fn test_configuration_variants() {
        let vertices: Vec<VertexId> = (1..1000).map(VertexId::from_u64).collect();

        // Test different configuration presets
        let configs = vec![
            ("Paper", EliasFanoConfig::paper_specification()),
            ("Small", EliasFanoConfig::small_lists()),
            ("Large", EliasFanoConfig::large_lists()),
        ];

        for (name, config) in configs {
            let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();
            let stats = encoded.compression_stats();
            let decoded = encoded.decode().unwrap();

            assert_eq!(vertices.len(), decoded.len());
            println!(
                "{} config: {:.2} bits/vertex, {:.2}x compression",
                name, stats.bits_per_vertex, stats.compression_ratio
            );
        }
    }
}
