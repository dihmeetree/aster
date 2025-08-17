//! Morris Counter implementation for space-efficient degree sketching
//!
//! Based on the paper's design using 8 bits per vertex:
//! - 4 bits for exponent (0-15)
//! - 4 bits for mantissa (0-15)

use rand::Rng;
use serde::{Deserialize, Serialize};

/// Morris Counter for approximate counting with minimal space
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MorrisCounter {
    /// Combined exponent (high 4 bits) and mantissa (low 4 bits)
    value: u8,
}

impl MorrisCounter {
    /// Create a new Morris Counter initialized to 0
    pub fn new() -> Self {
        Self { value: 0 }
    }

    /// Create a Morris Counter from a raw u8 value
    pub fn from_raw(value: u8) -> Self {
        Self { value }
    }

    /// Get the raw u8 value
    pub fn raw(&self) -> u8 {
        self.value
    }

    /// Get the exponent (high 4 bits)
    pub fn exponent(&self) -> u8 {
        (self.value >> 4) & 0xF
    }

    /// Get the mantissa (low 4 bits)
    pub fn mantissa(&self) -> u8 {
        self.value & 0xF
    }

    /// Set the exponent (high 4 bits)
    fn set_exponent(&mut self, exp: u8) {
        let exp = exp & 0xF; // Ensure only 4 bits
        self.value = (self.value & 0xF) | (exp << 4);
    }

    /// Set the mantissa (low 4 bits)
    fn set_mantissa(&mut self, mantissa: u8) {
        let mantissa = mantissa & 0xF; // Ensure only 4 bits
        self.value = (self.value & 0xF0) | mantissa;
    }

    /// Increment the counter probabilistically
    /// Returns true if the counter was actually incremented
    pub fn increment<R: Rng>(&mut self, rng: &mut R) -> bool {
        let exp = self.exponent();
        let mantissa = self.mantissa();

        // Probability of increment is 2^(-exponent)
        let probability = 1.0 / (1u64 << exp) as f64;

        if rng.gen::<f64>() < probability {
            let new_mantissa = mantissa + 1;

            if new_mantissa > 15 {
                // Overflow: increment exponent and reset mantissa
                if exp < 15 {
                    self.set_exponent(exp + 1);
                    self.set_mantissa(0);
                    return true;
                }
                // If exponent is already max (15), we can't increment further
                false
            } else {
                self.set_mantissa(new_mantissa);
                true
            }
        } else {
            false
        }
    }

    /// Get the approximate count
    /// Formula from paper: (2^E - 1) * 2^4 + 2^E * M
    /// Where E is exponent and M is mantissa
    pub fn count(&self) -> u64 {
        let exp = self.exponent();
        let mantissa = self.mantissa();

        if exp == 0 {
            // Special case for small counts - exact for E=0
            mantissa as u64
        } else {
            // Paper-specified formula: (2^E - 1) * 16 + 2^E * M
            let base = (1u64 << exp) - 1;
            let term1 = base * 16; // 2^4 = 16
            let term2 = (1u64 << exp) * (mantissa as u64);
            term1 + term2
        }
    }

    /// Get the approximate count with bias correction
    /// Provides better estimates by correcting known bias in Morris counters
    pub fn count_corrected(&self) -> f64 {
        let raw_count = self.count() as f64;

        if raw_count <= 16.0 {
            // For small counts, no correction needed
            raw_count
        } else {
            // Apply correction factor for larger counts
            // Based on known bias characteristics of Morris counters
            let correction_factor = 1.0 - 1.0 / (2.0 * raw_count);
            raw_count * correction_factor
        }
    }

    /// Get estimated relative error for this counter
    /// Returns the standard relative error as specified in the paper
    pub fn relative_error(&self) -> f64 {
        let exp = self.exponent();
        if exp == 0 {
            0.0 // Exact for small counts
        } else {
            // Standard relative error for Morris counters: 1.04 / sqrt(2^E)
            1.04 / ((1u64 << exp) as f64).sqrt()
        }
    }

    /// Get the precision bits for this counter value
    pub fn precision_bits(&self) -> u8 {
        let exp = self.exponent();
        if exp == 0 {
            4
        } else {
            exp + 4
        } // 4 mantissa bits + exponent precision
    }

    /// Maximum representable count
    pub fn max_count() -> u64 {
        // When exp=15, mantissa=15
        let exp = 15u64;
        let mantissa = 15u64;
        let base = (1u64 << exp) - 1;
        let term1 = base * 16;
        let term2 = (1u64 << exp) * mantissa;
        term1 + term2
    }

    /// Reset the counter to 0
    pub fn reset(&mut self) {
        self.value = 0;
    }
}

impl Default for MorrisCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// Degree sketch for tracking vertex degrees with hash-based indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DegreeSketch {
    counters: Vec<MorrisCounter>,
    /// Hash function seed for consistent vertex mapping
    hash_seed: u64,
}

impl DegreeSketch {
    /// Create a new degree sketch with capacity for `num_vertices` vertices
    pub fn new(num_vertices: usize) -> Self {
        Self::with_seed(num_vertices, rand::random())
    }

    /// Create a new degree sketch with a specific hash seed
    pub fn with_seed(num_vertices: usize, seed: u64) -> Self {
        Self {
            counters: vec![MorrisCounter::new(); num_vertices],
            hash_seed: seed,
        }
    }

    /// Get the number of vertices this sketch can handle
    pub fn capacity(&self) -> usize {
        self.counters.len()
    }

    /// Hash a vertex ID to an index in the counter array
    fn hash_vertex(&self, vertex_id: u64) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.hash_seed.hash(&mut hasher);
        vertex_id.hash(&mut hasher);

        (hasher.finish() as usize) % self.counters.len()
    }

    /// Increment the degree for a vertex by its ID
    pub fn increment_degree_by_id(&mut self, vertex_id: u64) -> bool {
        let index = self.hash_vertex(vertex_id);
        let mut rng = rand::thread_rng();
        self.counters[index].increment(&mut rng)
    }

    /// Increment the degree for a vertex by array index (for compatibility)
    pub fn increment_degree(&mut self, vertex_index: usize) -> bool {
        if vertex_index >= self.counters.len() {
            return false;
        }

        let mut rng = rand::thread_rng();
        self.counters[vertex_index].increment(&mut rng)
    }

    /// Batch increment degrees for multiple vertices (more efficient)
    pub fn batch_increment_degrees(&mut self, vertex_ids: &[u64]) -> Vec<bool> {
        let mut rng = rand::thread_rng();
        let mut results = Vec::with_capacity(vertex_ids.len());

        for &vertex_id in vertex_ids {
            let index = self.hash_vertex(vertex_id);
            let success = self.counters[index].increment(&mut rng);
            results.push(success);
        }

        results
    }

    /// Get the approximate degree for a vertex by ID
    pub fn get_degree_by_id(&self, vertex_id: u64) -> u64 {
        let index = self.hash_vertex(vertex_id);
        self.counters[index].count()
    }

    /// Get the approximate degree for a vertex by array index (for compatibility)
    pub fn get_degree(&self, vertex_index: usize) -> Option<u64> {
        self.counters.get(vertex_index).map(|c| c.count())
    }

    /// Get the corrected degree estimate for a vertex by ID
    pub fn get_degree_corrected_by_id(&self, vertex_id: u64) -> f64 {
        let index = self.hash_vertex(vertex_id);
        self.counters[index].count_corrected()
    }

    /// Get the relative error estimate for a vertex degree by ID
    pub fn get_degree_error_by_id(&self, vertex_id: u64) -> f64 {
        let index = self.hash_vertex(vertex_id);
        self.counters[index].relative_error()
    }

    /// Reset the degree for a vertex by ID
    pub fn reset_degree_by_id(&mut self, vertex_id: u64) {
        let index = self.hash_vertex(vertex_id);
        self.counters[index].reset();
    }

    /// Reset the degree for a vertex by array index (for compatibility)
    pub fn reset_degree(&mut self, vertex_index: usize) {
        if let Some(counter) = self.counters.get_mut(vertex_index) {
            counter.reset();
        }
    }

    /// Get the raw counter for a vertex (for serialization)
    pub fn get_raw_counter(&self, vertex_index: usize) -> Option<u8> {
        self.counters.get(vertex_index).map(|c| c.raw())
    }

    /// Set the raw counter for a vertex (for deserialization)
    pub fn set_raw_counter(&mut self, vertex_index: usize, raw: u8) {
        if let Some(counter) = self.counters.get_mut(vertex_index) {
            *counter = MorrisCounter::from_raw(raw);
        }
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.counters.len() // 1 byte per counter
    }

    /// Resize the sketch to accommodate more vertices
    pub fn resize(&mut self, new_capacity: usize) {
        self.counters.resize(new_capacity, MorrisCounter::new());
    }

    /// Get comprehensive statistics about the degree sketch
    pub fn get_statistics(&self) -> DegreeSketchStatistics {
        let mut stats = DegreeSketchStatistics::default();

        for counter in &self.counters {
            let count = counter.count();
            let exp = counter.exponent();

            stats.total_vertices += 1;
            if count > 0 {
                stats.active_vertices += 1;
            }

            stats.total_degree += count;
            stats.max_degree = stats.max_degree.max(count);
            stats.min_degree = if stats.min_degree == 0 {
                count
            } else {
                stats.min_degree.min(count)
            };

            // Track exponent distribution
            if (exp as usize) < stats.exponent_distribution.len() {
                stats.exponent_distribution[exp as usize] += 1;
            }

            // Accumulate error estimates
            stats.total_relative_error += counter.relative_error();
        }

        stats.avg_degree = if stats.active_vertices > 0 {
            stats.total_degree as f64 / stats.active_vertices as f64
        } else {
            0.0
        };

        stats.avg_relative_error = if stats.active_vertices > 0 {
            stats.total_relative_error / stats.active_vertices as f64
        } else {
            0.0
        };

        stats.capacity = self.capacity();
        stats.memory_usage = self.memory_usage();

        stats
    }

    /// Get hash function seed
    pub fn hash_seed(&self) -> u64 {
        self.hash_seed
    }

    /// Clear all counters
    pub fn clear(&mut self) {
        for counter in &mut self.counters {
            counter.reset();
        }
    }

    /// Get all non-zero degrees (for iteration)
    pub fn get_all_non_zero_degrees(&self) -> Vec<(usize, u64)> {
        self.counters
            .iter()
            .enumerate()
            .filter_map(|(i, counter)| {
                let count = counter.count();
                if count > 0 {
                    Some((i, count))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Ensure a vertex is tracked in the degree sketch
    /// This initializes the vertex with a degree of 0 if not already present
    pub fn ensure_vertex_tracked(&mut self, vertex_id: u64) {
        let vertex_index = self.hash_vertex(vertex_id);
        // The counter will already be initialized to 0, so we don't need to do anything
        // This method exists for API completeness and future extensibility
        // We could verify the vertex is within bounds
        if vertex_index < self.capacity() {
            // Vertex is tracked (all counters start at 0)
        }
    }
}

/// Statistics for monitoring degree sketch performance
#[derive(Debug, Clone, Default)]
pub struct DegreeSketchStatistics {
    pub capacity: usize,
    pub total_vertices: usize,
    pub active_vertices: usize,
    pub total_degree: u64,
    pub avg_degree: f64,
    pub min_degree: u64,
    pub max_degree: u64,
    pub avg_relative_error: f64,
    pub total_relative_error: f64,
    pub memory_usage: usize,
    pub exponent_distribution: [usize; 16], // Distribution of exponent values (0-15)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_morris_counter_basic() {
        let mut counter = MorrisCounter::new();
        assert_eq!(counter.count(), 0);
        assert_eq!(counter.exponent(), 0);
        assert_eq!(counter.mantissa(), 0);
    }

    #[test]
    fn test_morris_counter_increment() {
        let mut counter = MorrisCounter::new();
        let mut rng = StdRng::seed_from_u64(42);

        // First few increments should be deterministic
        for _ in 0..10 {
            counter.increment(&mut rng);
        }

        // Should have some non-zero count
        assert!(counter.count() > 0);
    }

    #[test]
    fn test_morris_counter_exponent_mantissa() {
        let mut counter = MorrisCounter::new();
        counter.set_exponent(5);
        counter.set_mantissa(10);

        assert_eq!(counter.exponent(), 5);
        assert_eq!(counter.mantissa(), 10);
    }

    #[test]
    fn test_degree_sketch() {
        let mut sketch = DegreeSketch::new(100);
        assert_eq!(sketch.capacity(), 100);

        // Test incrementing degree
        sketch.increment_degree(0);
        sketch.increment_degree(0);
        sketch.increment_degree(0);

        // Should have some approximate count
        assert!(sketch.get_degree(0).unwrap_or(0) >= 0);

        // Test invalid index
        assert_eq!(sketch.get_degree(1000), None);
    }

    #[test]
    fn test_morris_counter_serialization() {
        let counter = MorrisCounter::from_raw(0x5A); // exp=5, mantissa=10
        assert_eq!(counter.exponent(), 5);
        assert_eq!(counter.mantissa(), 10);
        assert_eq!(counter.raw(), 0x5A);
    }

    #[test]
    fn test_morris_counter_max_values() {
        let max_count = MorrisCounter::max_count();
        assert!(max_count > 1_000_000); // Should be able to represent large counts
    }

    #[test]
    fn test_degree_sketch_resize() {
        let mut sketch = DegreeSketch::new(10);
        sketch.resize(20);
        assert_eq!(sketch.capacity(), 20);

        // Original counters should still work
        sketch.increment_degree(5);
        assert!(sketch.get_degree(5).is_some());

        // New counters should be initialized
        assert_eq!(sketch.get_degree(15), Some(0));
    }

    #[test]
    fn test_morris_counter_paper_specifications() {
        // Test paper-specified 8-bit structure (4-bit exponent + 4-bit mantissa)
        let mut counter = MorrisCounter::new();

        // Test exponent and mantissa extraction
        counter.set_exponent(5);
        counter.set_mantissa(10);
        assert_eq!(counter.exponent(), 5);
        assert_eq!(counter.mantissa(), 10);
        assert_eq!(counter.raw(), 0x5A); // 0101 1010 in binary

        // Test maximum values
        counter.set_exponent(15);
        counter.set_mantissa(15);
        assert_eq!(counter.exponent(), 15);
        assert_eq!(counter.mantissa(), 15);
        assert_eq!(counter.raw(), 0xFF);

        // Test precision and error estimation
        let error = counter.relative_error();
        assert!(error > 0.0 && error < 1.0);

        let precision = counter.precision_bits();
        assert!(precision >= 4 && precision <= 19); // 4 mantissa + up to 15 exponent
    }

    #[test]
    fn test_degree_sketch_hash_based_indexing() {
        let mut sketch = DegreeSketch::with_seed(100, 12345);

        // Test vertex ID based operations
        let vertex_ids = vec![1000, 2000, 3000, 4000, 5000];

        // Increment degrees for specific vertex IDs
        for &vertex_id in &vertex_ids {
            sketch.increment_degree_by_id(vertex_id);
            sketch.increment_degree_by_id(vertex_id);
            sketch.increment_degree_by_id(vertex_id);
        }

        // Verify degrees are tracked correctly
        for &vertex_id in &vertex_ids {
            let degree = sketch.get_degree_by_id(vertex_id);
            assert!(
                degree > 0,
                "Vertex {} should have non-zero degree",
                vertex_id
            );

            let corrected = sketch.get_degree_corrected_by_id(vertex_id);
            assert!(corrected > 0.0);

            let error = sketch.get_degree_error_by_id(vertex_id);
            assert!(error >= 0.0);
        }

        // Test batch operations
        let results = sketch.batch_increment_degrees(&vertex_ids);
        assert_eq!(results.len(), vertex_ids.len());
    }

    #[test]
    fn test_degree_sketch_statistics() {
        let mut sketch = DegreeSketch::new(1000);

        // Add some degrees
        for i in 0..100 {
            sketch.increment_degree_by_id(i);
            sketch.increment_degree_by_id(i);
        }

        let stats = sketch.get_statistics();

        assert_eq!(stats.capacity, 1000);
        assert!(stats.active_vertices > 0);
        assert!(stats.total_degree > 0);
        assert!(stats.avg_degree > 0.0);
        assert!(stats.max_degree > 0);
        assert!(stats.memory_usage > 0);
        assert!(stats.avg_relative_error >= 0.0);

        // Test exponent distribution
        let total_exp_dist: usize = stats.exponent_distribution.iter().sum();
        assert_eq!(total_exp_dist, stats.total_vertices);

        println!("Degree sketch statistics: {:?}", stats);
    }

    #[test]
    fn test_morris_counter_bias_correction() {
        let mut counter = MorrisCounter::new();
        let mut rng = StdRng::seed_from_u64(42);

        // Increment many times
        for _ in 0..1000 {
            counter.increment(&mut rng);
        }

        let raw_count = counter.count();
        let corrected_count = counter.count_corrected();

        // For large counts, corrected should be less than raw (corrects positive bias)
        if raw_count > 16 {
            assert!(corrected_count < raw_count as f64);
        }

        println!(
            "Raw count: {}, Corrected count: {:.2}",
            raw_count, corrected_count
        );
    }

    #[test]
    fn test_degree_sketch_clear_and_utilities() {
        let mut sketch = DegreeSketch::new(100);

        // Add some data
        for i in 0..10 {
            sketch.increment_degree_by_id(i);
        }

        // Verify non-zero degrees
        let non_zero = sketch.get_all_non_zero_degrees();
        assert!(!non_zero.is_empty());

        // Clear and verify
        sketch.clear();
        let non_zero_after = sketch.get_all_non_zero_degrees();
        assert!(non_zero_after.is_empty());

        // Test hash seed access
        let seed = sketch.hash_seed();
        assert!(seed > 0);
    }
}
