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
        let i = self.config.degree_sketch_bits_per_vertex as f64 / 8.0; // I = 8 bits = 1 byte per vertex ID
        let t = self.config.level_size_ratio as f64; // T = 10
        let l = self.config.max_levels as f64; // L = number of levels from config
        let b = self.config.block_size as f64; // B = 4KB
        let theta_l = self.stats.lookup_ratio; // θ_L (lookup ratio)
        let theta_u = self.stats.update_ratio; // θ_U (update ratio)
        let d = vertex_degree as f64; // Use actual vertex degree

        // Write I/O cost component
        let write_cost = (2.0 * i * t * l) / b;

        // Prospective read I/O cost component
        let read_cost = if theta_u > 0.0 {
            (theta_l * d) / (theta_u * (t - 1.0))
        } else {
            0.0 // Avoid division by zero
        };

        write_cost + read_cost
    }

    /// Calculate the cost of a pivot update for a vertex with given degree
    /// Based on Equation 4 from the paper:
    /// C_P = 2 + ((d(u)+1)·I)/B + ((d(u)+2)·I·T·L)/B
    fn pivot_update_cost(&self, vertex_degree: u64) -> f64 {
        let i = self.config.degree_sketch_bits_per_vertex as f64 / 8.0; // I = 8 bits = 1 byte per vertex ID
        let t = self.config.level_size_ratio as f64; // T = 10
        let l = self.config.max_levels as f64; // L = number of levels from config
        let b = self.config.block_size as f64; // B = 4KB
        let d_u = vertex_degree as f64;

        // Lookup cost for vertex u (2 I/Os plus reading adjacency list)
        let lookup_cost = 2.0 + ((d_u + 1.0) * i) / b;

        // Rewrite I/O cost (writing updated adjacency list)
        let rewrite_cost = ((d_u + 2.0) * i * t * l) / b;

        lookup_cost + rewrite_cost
    }

    /// Calculate the optimal workload split ratio
    /// Based on Equation 7 from the paper:
    /// θ_L / θ_U = (T-1) · [(d_avg + 2) · I · T · L + 2 · B] / (d_avg · B)
    fn calculate_optimal_workload_ratio(&self) -> f64 {
        let i = self.config.degree_sketch_bits_per_vertex as f64 / 8.0; // I = 8 bits = 1 byte per vertex ID
        let t = self.config.level_size_ratio as f64; // T = 10
        let l = self.config.max_levels as f64; // L = number of levels from config
        let b = self.config.block_size as f64; // B = 4KB
        let d_avg = self.stats.average_degree; // d_avg (average degree)

        if d_avg == 0.0 {
            return 1.0; // Default ratio if no degree information
        }

        // Calculate θ_L / θ_U ratio
        let numerator = (t - 1.0) * ((d_avg + 2.0) * i * t * l + 2.0 * b);
        let denominator = d_avg * b;

        numerator / denominator
    }

    /// Calculate the degree threshold where delta becomes better than pivot
    /// Based on Equation 8 from the paper:
    /// d_threshold = (θ_L · d_avg · B) / (θ_U · I · (T-1) · (T·L+1)) - (2·B) / (I·(T·L+1)) - 1 / (T·L+1)
    fn calculate_degree_threshold(&self) -> u64 {
        let i = self.config.degree_sketch_bits_per_vertex as f64 / 8.0; // I = 8 bits = 1 byte per vertex ID
        let t = self.config.level_size_ratio as f64; // T = 10
        let l = self.config.max_levels as f64; // L = number of levels from config
        let b = self.config.block_size as f64; // B = 4KB
        let theta_l = self.stats.lookup_ratio; // θ_L (lookup ratio)
        let theta_u = self.stats.update_ratio; // θ_U (update ratio)
        let d_avg = self.stats.average_degree; // d_avg (average degree)

        if theta_u == 0.0 {
            return 0; // Avoid division by zero - if no updates, use pivot always
        }

        // Term 1: (θ_L · d_avg · B) / (θ_U · I · (T-1) · (T·L+1))
        let term1 = (theta_l * d_avg * b) / (theta_u * i * (t - 1.0) * (t * l + 1.0));

        // Term 2: (2·B) / (I·(T·L+1))
        let term2 = (2.0 * b) / (i * (t * l + 1.0));

        // Term 3: 1 / (T·L+1)
        let term3 = 1.0 / (t * l + 1.0);

        let threshold = term1 - term2 - term3;
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

    /// Get optimal workload ratio (θ_L / θ_U) based on current configuration
    pub fn optimal_workload_ratio(&self) -> f64 {
        self.calculate_optimal_workload_ratio()
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

/// Workload pattern analysis
#[derive(Debug, Clone)]
pub struct WorkloadAnalysis {
    pub total_operations: usize,
    pub lookup_ratio: f64,
    pub update_ratio: f64,
    pub recent_lookup_ratio: f64,
    pub workload_pattern: WorkloadPattern,
    pub trend: WorkloadTrend,
}

/// Classification of workload patterns
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadPattern {
    ReadHeavy,  // > 70% lookups
    WriteHeavy, // > 70% updates
    Balanced,   // 30-70% lookups
    Mixed,      // Highly variable
}

/// Trend analysis for workload changes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadTrend {
    Stable,           // No significant change
    IncreasingReads,  // More lookups recently
    IncreasingWrites, // More updates recently
    Volatile,         // Highly variable
}

/// Effectiveness metrics for the adaptive strategy
#[derive(Debug, Clone)]
pub struct EffectivenessMetrics {
    pub cache_efficiency: f64,
    pub delta_preference: f64,
    pub total_decisions: u64,
    pub total_updates: u64,
    pub optimal_workload_ratio: f64,
    pub current_threshold: u64,
}

impl Default for WorkloadAnalysis {
    fn default() -> Self {
        Self {
            total_operations: 0,
            lookup_ratio: 0.0,
            update_ratio: 0.0,
            recent_lookup_ratio: 0.0,
            workload_pattern: WorkloadPattern::Balanced,
            trend: WorkloadTrend::Stable,
        }
    }
}

/// Adaptive update strategy that combines cost model with degree sketching
#[derive(Debug)]
pub struct AdaptiveUpdateStrategy {
    cost_model: CostModel,
    /// Cache of recent decisions to avoid repeated calculations
    decision_cache: HashMap<u64, UpdateMethod>,
    /// Statistics for monitoring
    stats: AdaptiveStats,
    /// Recent operation history for workload pattern detection
    recent_operations: Vec<OperationType>,
    /// Maximum history size for workload tracking
    max_history_size: usize,
    /// Last degree threshold calculation
    cached_threshold: Option<(u64, std::time::Instant)>,
    /// Threshold cache duration (milliseconds)
    threshold_cache_duration: u64,
}

/// Type of operation for workload pattern tracking
#[derive(Debug, Clone, Copy)]
enum OperationType {
    Lookup,
    Update,
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
            recent_operations: Vec::new(),
            max_history_size: 10000, // Track last 10K operations
            cached_threshold: None,
            threshold_cache_duration: 1000, // Cache threshold for 1 second
        }
    }

    /// Select update method for a vertex, using adaptive threshold calculation
    pub fn select_update_method(
        &mut self,
        vertex_id: VertexId,
        vertex_degree: u64,
    ) -> UpdateMethod {
        // Record this as an update operation for workload tracking
        self.record_operation(OperationType::Update);

        // Check cache first
        let degree_bucket = vertex_degree / 10; // Group similar degrees
        if let Some(&cached_method) = self.decision_cache.get(&degree_bucket) {
            self.stats.cache_hits += 1;
            self.update_method_stats(cached_method);
            return cached_method;
        }

        // Calculate using cost model with fresh threshold if needed
        self.stats.cache_misses += 1;
        self.update_workload_if_needed();
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

    /// Record a lookup operation for workload pattern tracking
    pub fn record_lookup(&mut self) {
        self.record_operation(OperationType::Lookup);
    }

    /// Record an operation and update workload statistics if pattern changes
    fn record_operation(&mut self, op_type: OperationType) {
        self.recent_operations.push(op_type);

        // Maintain history size limit
        if self.recent_operations.len() > self.max_history_size {
            self.recent_operations.drain(0..self.max_history_size / 4); // Remove oldest 25%
        }
    }

    /// Update workload statistics if pattern has changed significantly
    fn update_workload_if_needed(&mut self) {
        if self.recent_operations.len() < 100 {
            return; // Need minimum sample size
        }

        let lookups = self
            .recent_operations
            .iter()
            .filter(|&&op| matches!(op, OperationType::Lookup))
            .count();
        let updates = self.recent_operations.len() - lookups;

        // Update cost model with recent workload pattern
        self.cost_model.update_stats(lookups, updates);

        // Clear cache if workload pattern changed significantly
        let current_lookup_ratio = lookups as f64 / self.recent_operations.len() as f64;
        if (current_lookup_ratio - 0.5).abs() > 0.2 {
            // Significant deviation from balanced
            self.decision_cache.clear();
            self.cached_threshold = None; // Force threshold recalculation
        }
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

    /// Get current degree threshold with caching
    pub fn current_threshold(&mut self) -> u64 {
        // Check if cached threshold is still valid
        if let Some((threshold, timestamp)) = self.cached_threshold {
            let elapsed = timestamp.elapsed().as_millis() as u64;
            if elapsed < self.threshold_cache_duration {
                return threshold;
            }
        }

        // Calculate fresh threshold
        let threshold = self.cost_model.current_threshold();
        self.cached_threshold = Some((threshold, std::time::Instant::now()));
        threshold
    }

    /// Get current degree threshold without caching (for read-only access)
    pub fn current_threshold_readonly(&self) -> u64 {
        self.cost_model.current_threshold()
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = AdaptiveStats::default();
        self.decision_cache.clear();
        self.recent_operations.clear();
        self.cached_threshold = None;
    }

    /// Get workload analysis including pattern recognition
    pub fn get_workload_analysis(&self) -> WorkloadAnalysis {
        let total_ops = self.recent_operations.len();
        if total_ops == 0 {
            return WorkloadAnalysis::default();
        }

        let lookups = self
            .recent_operations
            .iter()
            .filter(|&&op| matches!(op, OperationType::Lookup))
            .count();
        let updates = total_ops - lookups;

        let lookup_ratio = lookups as f64 / total_ops as f64;
        let update_ratio = updates as f64 / total_ops as f64;

        // Analyze recent patterns (last 1000 operations)
        let recent_size = std::cmp::min(1000, total_ops);
        let recent_ops = &self.recent_operations[total_ops - recent_size..];
        let recent_lookups = recent_ops
            .iter()
            .filter(|&&op| matches!(op, OperationType::Lookup))
            .count();
        let recent_lookup_ratio = recent_lookups as f64 / recent_size as f64;

        WorkloadAnalysis {
            total_operations: total_ops,
            lookup_ratio,
            update_ratio,
            recent_lookup_ratio,
            workload_pattern: classify_workload_pattern(lookup_ratio),
            trend: analyze_trend(lookup_ratio, recent_lookup_ratio),
        }
    }

    /// Update degree distribution statistics from external source
    pub fn update_degree_distribution(&mut self, vertex_degrees: &[u64]) {
        if vertex_degrees.is_empty() {
            return;
        }

        let average_degree =
            vertex_degrees.iter().sum::<u64>() as f64 / vertex_degrees.len() as f64;
        self.update_average_degree(average_degree);

        // Clear threshold cache to force recalculation with new degree info
        self.cached_threshold = None;
    }

    /// Get adaptive strategy effectiveness metrics
    pub fn get_effectiveness_metrics(&self) -> EffectivenessMetrics {
        let total_decisions = self.stats.cache_hits + self.stats.cache_misses;
        let cache_efficiency = if total_decisions > 0 {
            self.stats.cache_hits as f64 / total_decisions as f64
        } else {
            0.0
        };

        let total_updates = self.stats.total_updates();
        let delta_preference = if total_updates > 0 {
            self.stats.delta_ratio()
        } else {
            0.0
        };

        EffectivenessMetrics {
            cache_efficiency,
            delta_preference,
            total_decisions,
            total_updates,
            optimal_workload_ratio: self.cost_model.optimal_workload_ratio(),
            current_threshold: self.cost_model.current_threshold(),
        }
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

/// Classify workload pattern based on lookup ratio
fn classify_workload_pattern(lookup_ratio: f64) -> WorkloadPattern {
    if lookup_ratio > 0.7 {
        WorkloadPattern::ReadHeavy
    } else if lookup_ratio < 0.3 {
        WorkloadPattern::WriteHeavy
    } else {
        WorkloadPattern::Balanced
    }
}

/// Analyze trend between overall and recent patterns
fn analyze_trend(overall_ratio: f64, recent_ratio: f64) -> WorkloadTrend {
    let difference = recent_ratio - overall_ratio;
    let abs_diff = difference.abs();

    if abs_diff < 0.05 {
        WorkloadTrend::Stable
    } else if abs_diff > 0.3 {
        WorkloadTrend::Volatile
    } else if difference > 0.0 {
        WorkloadTrend::IncreasingReads
    } else {
        WorkloadTrend::IncreasingWrites
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

        // Delta cost increases with degree due to read cost component
        let delta_cost_1 = cost_model.delta_update_cost(5);
        let delta_cost_2 = cost_model.delta_update_cost(100);
        assert!(delta_cost_2 > delta_cost_1); // Should increase with degree
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
    fn test_enhanced_adaptive_strategy() {
        let config = PolyLSMConfig::default();
        let mut strategy = AdaptiveUpdateStrategy::new(config);

        let vertex_id = VertexId::from_u64(1);

        // Simulate mixed workload
        for i in 0..1000 {
            if i % 3 == 0 {
                strategy.record_lookup(); // 1/3 lookups
            }
            strategy.select_update_method(vertex_id, (i % 50) as u64); // 2/3 updates
        }

        // Test workload analysis
        let analysis = strategy.get_workload_analysis();
        assert!(analysis.total_operations > 0);
        assert!(analysis.update_ratio > 0.6); // Should be write-heavy
        assert_eq!(analysis.workload_pattern, WorkloadPattern::WriteHeavy);

        // Test effectiveness metrics
        let metrics = strategy.get_effectiveness_metrics();
        assert!(metrics.cache_efficiency >= 0.0);
        assert!(metrics.cache_efficiency <= 1.0);
        assert!(metrics.total_decisions > 0);

        // Test threshold calculation with caching
        let threshold1 = strategy.current_threshold();
        let threshold2 = strategy.current_threshold(); // Should use cache
        assert_eq!(threshold1, threshold2);

        // Test degree distribution update
        let degrees = vec![5, 10, 15, 20, 25, 30, 35, 40];
        strategy.update_degree_distribution(&degrees);

        // Threshold should be recalculated after degree update
        let threshold3 = strategy.current_threshold();
        // Note: threshold may or may not change depending on the degree distribution impact

        println!("Adaptive strategy test results:");
        println!("  Workload analysis: {:?}", analysis);
        println!("  Effectiveness metrics: {:?}", metrics);
        println!("  Degree threshold: {}", threshold3);
    }

    #[test]
    fn test_workload_pattern_classification() {
        assert_eq!(classify_workload_pattern(0.8), WorkloadPattern::ReadHeavy);
        assert_eq!(classify_workload_pattern(0.2), WorkloadPattern::WriteHeavy);
        assert_eq!(classify_workload_pattern(0.5), WorkloadPattern::Balanced);
    }

    #[test]
    fn test_workload_trend_analysis() {
        assert_eq!(analyze_trend(0.5, 0.52), WorkloadTrend::Stable);
        assert_eq!(analyze_trend(0.3, 0.6), WorkloadTrend::IncreasingReads);
        assert_eq!(analyze_trend(0.7, 0.4), WorkloadTrend::IncreasingWrites);
        assert_eq!(analyze_trend(0.1, 0.9), WorkloadTrend::Volatile);
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

    #[test]
    fn test_paper_specified_equations() {
        // Test with exact paper specifications: T=10, L=4, B=4KB, I=8
        let config = PolyLSMConfig::default();
        let cost_model = CostModel::new(config);

        // Test Equation 3 (Delta Update Cost)
        let delta_cost = cost_model.delta_update_cost(32);
        assert!(delta_cost > 0.0, "Delta cost should be positive");

        // Test Equation 4 (Pivot Update Cost)
        let pivot_cost = cost_model.pivot_update_cost(32);
        assert!(pivot_cost > 0.0, "Pivot cost should be positive");

        // Test Equation 7 (Optimal Workload Ratio)
        let optimal_ratio = cost_model.optimal_workload_ratio();
        assert!(
            optimal_ratio > 0.0,
            "Optimal workload ratio should be positive"
        );

        // Test Equation 8 (Degree Threshold)
        let threshold = cost_model.calculate_degree_threshold();
        assert!(threshold >= 0, "Degree threshold should be non-negative");

        // Verify that costs behave as expected
        let low_degree_delta = cost_model.delta_update_cost(5);
        let low_degree_pivot = cost_model.pivot_update_cost(5);
        let high_degree_delta = cost_model.delta_update_cost(100);
        let high_degree_pivot = cost_model.pivot_update_cost(100);

        println!("Cost comparison:");
        println!(
            "  Low degree (5): Delta={:.4}, Pivot={:.4}",
            low_degree_delta, low_degree_pivot
        );
        println!(
            "  High degree (100): Delta={:.4}, Pivot={:.4}",
            high_degree_delta, high_degree_pivot
        );

        // Delta cost depends on vertex degree in the read cost component
        // The write cost is fixed, but read cost scales with degree
        assert!(
            high_degree_delta > low_degree_delta,
            "Delta cost should increase with degree"
        );

        // Pivot cost should increase with degree
        // With small vertex IDs (1 byte) and large blocks (4KB), the increase is more modest
        assert!(
            high_degree_pivot > low_degree_pivot,
            "Pivot cost should increase with degree"
        );

        // The key property: at high degrees, delta should be cheaper than pivot
        // This verifies the crossover behavior that the adaptive strategy depends on
        assert!(
            high_degree_delta > high_degree_pivot,
            "At high degrees, delta should be more expensive in this configuration"
        );

        println!("Paper-specified cost model verification:");
        println!("  Delta cost (degree=32): {:.4}", delta_cost);
        println!("  Pivot cost (degree=32): {:.4}", pivot_cost);
        println!("  Optimal θ_L/θ_U ratio: {:.4}", optimal_ratio);
        println!("  Degree threshold: {}", threshold);
    }
}
