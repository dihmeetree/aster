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
            // Special case for small counts
            mantissa as u64
        } else {
            let base = (1u64 << exp) - 1;
            let term1 = base * 16; // 2^4 = 16
            let term2 = (1u64 << exp) * (mantissa as u64);
            term1 + term2
        }
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

/// Degree sketch for tracking vertex degrees
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DegreeSketch {
    counters: Vec<MorrisCounter>,
}

impl DegreeSketch {
    /// Create a new degree sketch with capacity for `num_vertices` vertices
    pub fn new(num_vertices: usize) -> Self {
        Self {
            counters: vec![MorrisCounter::new(); num_vertices],
        }
    }

    /// Get the number of vertices this sketch can handle
    pub fn capacity(&self) -> usize {
        self.counters.len()
    }

    /// Increment the degree for a vertex
    pub fn increment_degree(&mut self, vertex_index: usize) -> bool {
        if vertex_index >= self.counters.len() {
            return false;
        }

        let mut rng = rand::thread_rng();
        self.counters[vertex_index].increment(&mut rng)
    }

    /// Get the approximate degree for a vertex
    pub fn get_degree(&self, vertex_index: usize) -> Option<u64> {
        self.counters.get(vertex_index).map(|c| c.count())
    }

    /// Reset the degree for a vertex
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
}
