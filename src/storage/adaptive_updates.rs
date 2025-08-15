//! Adaptive update mechanism for Poly-LSM
//!
//! Implements the cost model from the paper to determine whether to use
//! delta updates (edge-based) or pivot updates (vertex-based) for each operation.

use crate::types::PolyLSMConfig;
use crate::{Result, VertexId};
use std::collections::HashMap;

/// Update method selection for edge operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateMethod {
    /// Delta update: store edge as separate entry (edge-based)
    Delta,
    /// Pivot update: read-modify-write adjacency list (vertex-based)
    Pivot,
}

/// Cost model for adaptive update selection
#[derive(Debug, Clone)]
pub struct CostModel {
    /// Configuration parameters
    config: PolyLSMConfig,
    /// Current workload statistics
    stats: WorkloadStats,
}

/// Workload statistics for cost calculation
#[derive(Debug, Clone)]
struct WorkloadStats {
    /// Ratio of lookup operations (θ_L in the paper)
    lookup_ratio: f64,
    /// Ratio of update operations (θ_U in the paper)  
    update_ratio: f64,
    /// Average degree of vertices in the graph
    average_degree: f64,
}

impl CostModel {
    /// Create a new cost model with the given configuration
    pub fn new(config: PolyLSMConfig) -> Self {
        let stats = WorkloadStats {
            lookup_ratio: config.lookup_ratio,
            update_ratio: 1.0 - config.lookup_ratio,
            average_degree: config.average_degree,
        };

        Self { config, stats }
    }

    /// Calculate the cost of a delta update for a vertex with given degree
    /// Based on Equation 3 from the paper:
    /// C_D = (2I·T·L)/B + (θ_L·d)/(θ_U·(T-1))
    fn delta_update_cost(&self, vertex_degree: u64) -> f64 {
        let i = 8.0; // Size of vertex ID in bytes (u64)
        let t = self.config.level_size_ratio as f64;
        let l = 4.0; // Number of levels (typical for most workloads)
        let b = self.config.block_size as f64;
        let theta_l = self.stats.lookup_ratio;
        let theta_u = self.stats.update_ratio;
        let d = self.stats.average_degree;

        // Write I/O cost
        let write_cost = (2.0 * i * t * l) / b;

        // Prospective read I/O cost
        let read_cost = (theta_l * d) / (theta_u * (t - 1.0));

        write_cost + read_cost
    }

    /// Calculate the cost of a pivot update for a vertex with given degree
    /// Based on Equation 4 from the paper:
    /// C_P = 2 + ((d(u)+1)·I)/B + ((d(u)+2)·I·T·L)/B
    fn pivot_update_cost(&self, vertex_degree: u64) -> f64 {
        let i = 8.0; // Size of vertex ID in bytes
        let t = self.config.level_size_ratio as f64;
        let l = 4.0; // Number of levels
        let b = self.config.block_size as f64;
        let d_u = vertex_degree as f64;

        // Lookup cost for vertex u
        let lookup_cost = 2.0 + ((d_u + 1.0) * i) / b;

        // Rewrite I/O cost
        let rewrite_cost = ((d_u + 2.0) * i * t * l) / b;

        lookup_cost + rewrite_cost
    }

    /// Calculate the degree threshold where delta becomes better than pivot
    /// Based on Equation 8 from the paper
    fn calculate_degree_threshold(&self) -> u64 {
        let i = 8.0; // Size of vertex ID in bytes
        let t = self.config.level_size_ratio as f64;
        let l = 4.0; // Number of levels
        let b = self.config.block_size as f64;
        let theta_l = self.stats.lookup_ratio;
        let theta_u = self.stats.update_ratio;
        let d = self.stats.average_degree;

        let numerator = (theta_l * d * b) / (theta_u * i * (t - 1.0) * (t * l + 1.0));
        let term2 = (2.0 * b) / (i * (t * l + 1.0));
        let term3 = 1.0 / (t * l + 1.0);

        let threshold = numerator - term2 - term3;
        threshold.max(0.0) as u64
    }

    /// Select the optimal update method for a vertex with given degree
    pub fn select_update_method(&self, vertex_degree: u64) -> UpdateMethod {
        let threshold = self.calculate_degree_threshold();

        if vertex_degree >= threshold {
            UpdateMethod::Delta
        } else {
            UpdateMethod::Pivot
        }
    }

    /// Update workload statistics based on recent operations
    pub fn update_stats(&mut self, recent_lookups: usize, recent_updates: usize) {
        let total_ops = recent_lookups + recent_updates;
        if total_ops > 0 {
            self.stats.lookup_ratio = recent_lookups as f64 / total_ops as f64;
            self.stats.update_ratio = recent_updates as f64 / total_ops as f64;
        }
    }

    /// Update average degree estimate
    pub fn update_average_degree(&mut self, new_average: f64) {
        // Use exponential moving average for smoothing
        let alpha = 0.1; // Smoothing factor
        self.stats.average_degree = alpha * new_average + (1.0 - alpha) * self.stats.average_degree;
    }

    /// Get current degree threshold for debugging/monitoring
    pub fn current_threshold(&self) -> u64 {
        self.calculate_degree_threshold()
    }

    /// Get detailed cost analysis for a vertex degree
    pub fn analyze_costs(&self, vertex_degree: u64) -> CostAnalysis {
        let delta_cost = self.delta_update_cost(vertex_degree);
        let pivot_cost = self.pivot_update_cost(vertex_degree);
        let selected_method = self.select_update_method(vertex_degree);

        CostAnalysis {
            vertex_degree,
            delta_cost,
            pivot_cost,
            selected_method,
            cost_difference: (delta_cost - pivot_cost).abs(),
            threshold: self.calculate_degree_threshold(),
        }
    }
}

/// Detailed cost analysis for a specific vertex degree
#[derive(Debug, Clone)]
pub struct CostAnalysis {
    pub vertex_degree: u64,
    pub delta_cost: f64,
    pub pivot_cost: f64,
    pub selected_method: UpdateMethod,
    pub cost_difference: f64,
    pub threshold: u64,
}

/// Adaptive update strategy that combines cost model with degree sketching
#[derive(Debug)]
pub struct AdaptiveUpdateStrategy {
    cost_model: CostModel,
    /// Cache of recent decisions to avoid repeated calculations
    decision_cache: HashMap<u64, UpdateMethod>,
    /// Statistics for monitoring
    stats: AdaptiveStats,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AdaptiveStats {
    pub delta_updates: u64,
    pub pivot_updates: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl AdaptiveUpdateStrategy {
    /// Create a new adaptive update strategy
    pub fn new(config: PolyLSMConfig) -> Self {
        Self {
            cost_model: CostModel::new(config),
            decision_cache: HashMap::new(),
            stats: AdaptiveStats::default(),
        }
    }

    /// Select update method for a vertex, using cache when possible
    pub fn select_update_method(
        &mut self,
        vertex_id: VertexId,
        vertex_degree: u64,
    ) -> UpdateMethod {
        // Check cache first
        let degree_bucket = vertex_degree / 10; // Group similar degrees
        if let Some(&cached_method) = self.decision_cache.get(&degree_bucket) {
            self.stats.cache_hits += 1;
            self.update_method_stats(cached_method);
            return cached_method;
        }

        // Calculate using cost model
        self.stats.cache_misses += 1;
        let method = self.cost_model.select_update_method(vertex_degree);

        // Cache the decision
        self.decision_cache.insert(degree_bucket, method);

        // Limit cache size
        if self.decision_cache.len() > 1000 {
            self.decision_cache.clear();
        }

        self.update_method_stats(method);
        method
    }

    /// Update internal statistics
    fn update_method_stats(&mut self, method: UpdateMethod) {
        match method {
            UpdateMethod::Delta => self.stats.delta_updates += 1,
            UpdateMethod::Pivot => self.stats.pivot_updates += 1,
        }
    }

    /// Update workload statistics
    pub fn update_workload_stats(&mut self, recent_lookups: usize, recent_updates: usize) {
        self.cost_model.update_stats(recent_lookups, recent_updates);
        // Clear cache when workload changes significantly
        self.decision_cache.clear();
    }

    /// Update average degree
    pub fn update_average_degree(&mut self, new_average: f64) {
        self.cost_model.update_average_degree(new_average);
        // Clear cache when degree distribution changes
        self.decision_cache.clear();
    }

    /// Get current statistics
    pub fn get_stats(&self) -> &AdaptiveStats {
        &self.stats
    }

    /// Get cost analysis for debugging
    pub fn analyze_costs(&self, vertex_degree: u64) -> CostAnalysis {
        self.cost_model.analyze_costs(vertex_degree)
    }

    /// Get current degree threshold
    pub fn current_threshold(&self) -> u64 {
        self.cost_model.current_threshold()
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = AdaptiveStats::default();
        self.decision_cache.clear();
    }
}

impl AdaptiveStats {
    /// Get total number of updates
    pub fn total_updates(&self) -> u64 {
        self.delta_updates + self.pivot_updates
    }

    /// Get delta update ratio
    pub fn delta_ratio(&self) -> f64 {
        let total = self.total_updates();
        if total > 0 {
            self.delta_updates as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Get cache hit ratio
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total > 0 {
            self.cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PolyLSMConfig;

    #[test]
    fn test_cost_model_basic() {
        let config = PolyLSMConfig::default();
        let cost_model = CostModel::new(config);

        // Test cost calculations
        let low_degree_cost = cost_model.pivot_update_cost(5);
        let high_degree_cost = cost_model.pivot_update_cost(100);

        // Higher degree should have higher pivot cost
        assert!(high_degree_cost > low_degree_cost);

        // Delta cost should be relatively stable
        let delta_cost_1 = cost_model.delta_update_cost(5);
        let delta_cost_2 = cost_model.delta_update_cost(100);
        assert!((delta_cost_1 - delta_cost_2).abs() < 1.0); // Should be similar
    }

    #[test]
    fn test_update_method_selection() {
        let config = PolyLSMConfig::default();
        let cost_model = CostModel::new(config);

        // Low degree vertices should prefer pivot updates
        let low_degree_method = cost_model.select_update_method(1);
        assert_eq!(low_degree_method, UpdateMethod::Pivot);

        // High degree vertices should prefer delta updates
        let high_degree_method = cost_model.select_update_method(1000);
        assert_eq!(high_degree_method, UpdateMethod::Delta);
    }

    #[test]
    fn test_threshold_calculation() {
        let config = PolyLSMConfig::default();
        let cost_model = CostModel::new(config);

        let threshold = cost_model.calculate_degree_threshold();
        assert!(threshold > 0);
        assert!(threshold < 1000); // Should be reasonable
    }

    #[test]
    fn test_adaptive_strategy() {
        let config = PolyLSMConfig::default();
        let mut strategy = AdaptiveUpdateStrategy::new(config);

        let vertex_id = VertexId::from_u64(1);

        // Test method selection
        let method1 = strategy.select_update_method(vertex_id, 10);
        let method2 = strategy.select_update_method(vertex_id, 10); // Should hit cache

        assert_eq!(method1, method2);

        let stats = strategy.get_stats();
        assert!(stats.cache_hits > 0 || stats.cache_misses > 0);
    }

    #[test]
    fn test_workload_adaptation() {
        let config = PolyLSMConfig::default();
        let mut cost_model = CostModel::new(config);

        let initial_threshold = cost_model.current_threshold();

        // Simulate lookup-heavy workload
        cost_model.update_stats(900, 100); // 90% lookups
        let lookup_heavy_threshold = cost_model.current_threshold();

        // Simulate update-heavy workload
        cost_model.update_stats(100, 900); // 10% lookups
        let update_heavy_threshold = cost_model.current_threshold();

        // Thresholds should adapt to workload
        // Lookup-heavy should prefer pivot updates (higher threshold)
        // Update-heavy should prefer delta updates (lower threshold)
        assert!(lookup_heavy_threshold > update_heavy_threshold);
    }

    #[test]
    fn test_cost_analysis() {
        let config = PolyLSMConfig::default();
        let cost_model = CostModel::new(config);

        let analysis = cost_model.analyze_costs(50);

        assert_eq!(analysis.vertex_degree, 50);
        assert!(analysis.delta_cost > 0.0);
        assert!(analysis.pivot_cost > 0.0);
        assert!(analysis.cost_difference >= 0.0);
        assert!(matches!(
            analysis.selected_method,
            UpdateMethod::Delta | UpdateMethod::Pivot
        ));
    }
}
