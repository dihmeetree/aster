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
}

impl Default for EliasFanoConfig {
    fn default() -> Self {
        Self {
            segment_count: 16, // Good balance between compression and access speed
            prefix_length: 0,  // Auto-calculate
        }
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

impl PartitionedEliasFano {
    /// Encode a sorted list of vertex IDs using Partitioned Elias-Fano
    pub fn encode(vertices: &[VertexId], config: EliasFanoConfig) -> Result<Self> {
        if vertices.is_empty() {
            return Ok(Self {
                config,
                segment_starts: Vec::new(),
                segments: Vec::new(),
                total_count: 0,
            });
        }

        // Convert VertexIds to u64 for processing
        let mut vertex_ids: Vec<u64> = vertices.iter().map(|v| v.as_u64()).collect();
        vertex_ids.sort_unstable();
        vertex_ids.dedup();

        let total_count = vertex_ids.len();
        let segment_size = (total_count + config.segment_count - 1) / config.segment_count;

        let mut segment_starts = Vec::new();
        let mut segments = Vec::new();

        // Partition vertices into segments
        for chunk in vertex_ids.chunks(segment_size) {
            if chunk.is_empty() {
                continue;
            }

            segment_starts.push(chunk[0]);
            let segment = Self::encode_segment(chunk)?;
            segments.push(segment);
        }

        Ok(Self {
            config,
            segment_starts,
            segments,
            total_count,
        })
    }

    /// Encode a single segment using Elias-Fano
    fn encode_segment(vertices: &[u64]) -> Result<EliasFanoSegment> {
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

        // Calculate optimal bit split
        let low_bits_per_value = if max_delta == 0 {
            0
        } else {
            // Use log₂(max_delta) but ensure at least some bits for low
            let total_bits = (max_delta as f64).log2().ceil() as u8;
            std::cmp::min(total_bits / 2, 16) // Cap at 16 bits for practical reasons
        };

        let high_mask = if low_bits_per_value >= 64 {
            0
        } else {
            !((1u64 << low_bits_per_value) - 1)
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
        };

        let encoded = PartitionedEliasFano::encode(&vertices, config).unwrap();
        let stats = encoded.stats();

        // Should achieve good compression on clustered data
        assert!(stats.compression_ratio < 0.8);
        assert_eq!(stats.total_vertices, 100);
    }
}
