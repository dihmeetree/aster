//! Bloom filter implementation for LSM-tree levels
//!
//! Used to quickly check if a key might exist in an SSTable without reading the actual data.

use ahash::AHasher;
use std::hash::{Hash, Hasher};

/// Simple Bloom filter implementation
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hash_functions: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with the given capacity and bits per key
    pub fn new(capacity: usize, bits_per_key: usize) -> Self {
        let num_bits = capacity * bits_per_key;
        let num_hash_functions = Self::optimal_hash_functions(bits_per_key);

        // Round up to nearest multiple of 64 for efficient storage
        let num_u64s = (num_bits + 63) / 64;

        Self {
            bits: vec![0u64; num_u64s],
            num_bits: num_u64s * 64,
            num_hash_functions,
        }
    }

    /// Create a Bloom filter optimized for vertex IDs with automatic sizing
    pub fn for_vertices(estimated_vertex_count: usize, target_fpr: f64) -> Self {
        // Calculate optimal bits per key for target false positive rate
        // m = -n * ln(p) / (ln(2)^2) where p is target FPR
        let bits_per_key = (-target_fpr.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let bits_per_key = bits_per_key.max(4).min(20); // Reasonable bounds

        Self::new(estimated_vertex_count, bits_per_key)
    }

    /// Create an empty Bloom filter (all queries return false)
    pub fn empty() -> Self {
        Self {
            bits: Vec::new(),
            num_bits: 0,
            num_hash_functions: 0,
        }
    }

    /// Calculate optimal number of hash functions for given bits per key
    fn optimal_hash_functions(bits_per_key: usize) -> usize {
        // k = ln(2) * (m/n) where m is bits and n is capacity
        // For simplicity, use a good approximation
        ((bits_per_key as f64 * 0.693).round() as usize)
            .max(1)
            .min(8)
    }

    /// Add an item to the Bloom filter
    pub fn insert<T: Hash>(&mut self, item: &T) {
        if self.bits.is_empty() {
            return;
        }

        let hashes = self.hash_item(item);

        for i in 0..self.num_hash_functions {
            let bit_index = self.get_bit_index(hashes.0, hashes.1, i);
            self.set_bit(bit_index);
        }
    }

    /// Check if an item might be in the set (may have false positives)
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        if self.bits.is_empty() {
            return false;
        }

        let hashes = self.hash_item(item);

        for i in 0..self.num_hash_functions {
            let bit_index = self.get_bit_index(hashes.0, hashes.1, i);
            if !self.get_bit(bit_index) {
                return false;
            }
        }

        true
    }

    /// Get two hash values for double hashing
    fn hash_item<T: Hash>(&self, item: &T) -> (u64, u64) {
        let mut hasher1 = AHasher::default();
        let mut hasher2 = AHasher::default();

        // Use different seeds for the two hash functions
        hasher1.write_u64(0x123456789ABCDEF0);
        item.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        hasher2.write_u64(0xFEDCBA9876543210);
        item.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        (hash1, hash2)
    }

    /// Get bit index using double hashing: hash1 + i * hash2
    fn get_bit_index(&self, hash1: u64, hash2: u64, i: usize) -> usize {
        let combined = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
        (combined as usize) % self.num_bits
    }

    /// Set a bit at the given index
    fn set_bit(&mut self, index: usize) {
        let word_index = index / 64;
        let bit_index = index % 64;

        if word_index < self.bits.len() {
            self.bits[word_index] |= 1u64 << bit_index;
        }
    }

    /// Get a bit at the given index
    fn get_bit(&self, index: usize) -> bool {
        let word_index = index / 64;
        let bit_index = index % 64;

        if word_index < self.bits.len() {
            (self.bits[word_index] >> bit_index) & 1 == 1
        } else {
            false
        }
    }

    /// Get the size in bytes
    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Get the number of bits
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Get the number of hash functions
    pub fn num_hash_functions(&self) -> usize {
        self.num_hash_functions
    }

    /// Serialize the Bloom filter to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write metadata
        bytes.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.num_hash_functions as u32).to_le_bytes());

        // Write bit data
        for &word in &self.bits {
            bytes.extend_from_slice(&word.to_le_bytes());
        }

        bytes
    }

    /// Deserialize a Bloom filter from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }

        let num_bits = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let num_hash_functions =
            u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;

        let expected_words = (num_bits + 63) / 64;
        let expected_size = 8 + expected_words * 8;

        if bytes.len() != expected_size {
            return None;
        }

        let mut bits = Vec::with_capacity(expected_words);
        let mut offset = 8;

        for _ in 0..expected_words {
            let word = u64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);
            bits.push(word);
            offset += 8;
        }

        Some(Self {
            bits,
            num_bits,
            num_hash_functions,
        })
    }

    /// Estimate the false positive rate
    pub fn false_positive_rate(&self, num_inserted: usize) -> f64 {
        if self.num_bits == 0 || num_inserted == 0 {
            return 0.0;
        }

        // FPR ≈ (1 - e^(-k*n/m))^k
        let k = self.num_hash_functions as f64;
        let n = num_inserted as f64;
        let m = self.num_bits as f64;

        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Specialized insert for vertex IDs with better hash distribution
    pub fn insert_vertex_id(&mut self, vertex_id: u64) {
        if self.bits.is_empty() {
            return;
        }

        // Use the vertex ID directly for better distribution in graph workloads
        let hash1 = vertex_id;
        let hash2 = vertex_id.wrapping_mul(0x9E3779B97F4A7C15); // Golden ratio multiplier

        for i in 0..self.num_hash_functions {
            let bit_index = self.get_bit_index(hash1, hash2, i);
            self.set_bit(bit_index);
        }
    }

    /// Specialized contains check for vertex IDs
    pub fn contains_vertex_id(&self, vertex_id: u64) -> bool {
        if self.bits.is_empty() {
            return false;
        }

        let hash1 = vertex_id;
        let hash2 = vertex_id.wrapping_mul(0x9E3779B97F4A7C15);

        for i in 0..self.num_hash_functions {
            let bit_index = self.get_bit_index(hash1, hash2, i);
            if !self.get_bit(bit_index) {
                return false;
            }
        }

        true
    }

    /// Batch insert multiple vertex IDs efficiently
    pub fn insert_vertex_batch(&mut self, vertex_ids: &[u64]) {
        for &vertex_id in vertex_ids {
            self.insert_vertex_id(vertex_id);
        }
    }

    /// Check if any of the given vertex IDs might be present
    pub fn contains_any_vertex(&self, vertex_ids: &[u64]) -> bool {
        for &vertex_id in vertex_ids {
            if self.contains_vertex_id(vertex_id) {
                return true;
            }
        }
        false
    }

    /// Get fill ratio (percentage of bits set)
    pub fn fill_ratio(&self) -> f64 {
        if self.bits.is_empty() {
            return 0.0;
        }

        let total_bits = self.num_bits as f64;
        let set_bits = self
            .bits
            .iter()
            .map(|&word| word.count_ones() as f64)
            .sum::<f64>();

        set_bits / total_bits
    }

    /// Reset the filter (clear all bits)
    pub fn clear(&mut self) {
        for word in &mut self.bits {
            *word = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(1000, 10);

        // Insert some items
        filter.insert(&"hello");
        filter.insert(&"world");
        filter.insert(&123);

        // Check contains
        assert!(filter.contains(&"hello"));
        assert!(filter.contains(&"world"));
        assert!(filter.contains(&123));

        // These should not be present (but might give false positives)
        // We can't guarantee they return false due to the probabilistic nature
    }

    #[test]
    fn test_bloom_filter_empty() {
        let filter = BloomFilter::empty();

        // Empty filter should never contain anything
        assert!(!filter.contains(&"hello"));
        assert!(!filter.contains(&123));
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut filter = BloomFilter::new(100, 8);
        filter.insert(&"test1");
        filter.insert(&"test2");

        let bytes = filter.to_bytes();
        let deserialized = BloomFilter::from_bytes(&bytes).unwrap();

        assert_eq!(filter.num_bits(), deserialized.num_bits());
        assert_eq!(
            filter.num_hash_functions(),
            deserialized.num_hash_functions()
        );

        // Should contain the same items
        assert!(deserialized.contains(&"test1"));
        assert!(deserialized.contains(&"test2"));
    }

    #[test]
    fn test_optimal_hash_functions() {
        assert_eq!(BloomFilter::optimal_hash_functions(1), 1);
        assert_eq!(BloomFilter::optimal_hash_functions(10), 7);
        assert_eq!(BloomFilter::optimal_hash_functions(20), 8); // capped at 8
    }

    #[test]
    fn test_false_positive_rate() {
        let mut filter = BloomFilter::new(1000, 10);

        // Insert some items
        for i in 0..100 {
            filter.insert(&i);
        }

        let fpr = filter.false_positive_rate(100);
        assert!(fpr > 0.0 && fpr < 1.0);

        // With good parameters, FPR should be reasonably low
        assert!(fpr < 0.1); // Less than 10%
    }

    #[test]
    fn test_clear() {
        let mut filter = BloomFilter::new(100, 8);
        filter.insert(&"test");

        assert!(filter.contains(&"test"));

        filter.clear();

        // After clearing, should not contain anything
        // (though false positives are theoretically possible, they're very unlikely with cleared filter)
        let mut empty_count = 0;
        for i in 0..100 {
            if !filter.contains(&format!("test{}", i)) {
                empty_count += 1;
            }
        }

        // Most queries should return false on a cleared filter
        assert!(empty_count > 90);
    }

    #[test]
    fn test_vertex_id_operations() {
        let mut filter = BloomFilter::new(1000, 8);

        // Test vertex ID insertion and lookup
        filter.insert_vertex_id(12345);
        filter.insert_vertex_id(67890);
        filter.insert_vertex_id(99999);

        assert!(filter.contains_vertex_id(12345));
        assert!(filter.contains_vertex_id(67890));
        assert!(filter.contains_vertex_id(99999));

        // Test batch operations
        let vertex_ids = vec![1, 2, 3, 4, 5];
        filter.insert_vertex_batch(&vertex_ids);

        assert!(filter.contains_any_vertex(&vertex_ids));
        assert!(filter.contains_any_vertex(&[3, 100, 200])); // 3 should be found
    }

    #[test]
    fn test_fill_ratio() {
        let mut filter = BloomFilter::new(100, 8);

        // Empty filter should have 0% fill ratio
        assert_eq!(filter.fill_ratio(), 0.0);

        // Add some items
        for i in 0..10 {
            filter.insert_vertex_id(i);
        }

        // Should have some bits set
        let ratio = filter.fill_ratio();
        assert!(ratio > 0.0 && ratio < 1.0);
    }
}
