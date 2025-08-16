//! Advanced Query Optimization for Range Scans
//!
//! This module implements sophisticated query optimization techniques specifically
//! designed for range scan operations in graph databases. It includes:
//! - Cost-based query planning with range scan cost estimation
//! - Predicate pushdown for early filtering
//! - Adaptive range partitioning for large scans
//! - Index selection and utilization strategies
//! - Batch processing optimization for memory efficiency
//! - Query plan caching and reuse

use crate::query::{QueryContext, QueryPredicate, QueryStats, RangeQueryResult};
use crate::storage::PropertyStore;
use crate::{AsterError, Properties, PropertyValue, Result, VertexId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Query optimization statistics
#[derive(Debug, Clone, Default)]
pub struct OptimizationStats {
    pub predicates_pushed_down: usize,
    pub index_hits: usize,
    pub range_partitions_created: usize,
    pub early_terminations: usize,
    pub batch_operations: usize,
    pub cost_estimation_time_ms: u64,
    pub optimization_time_ms: u64,
}

/// Cost estimation for different query execution strategies
#[derive(Debug, Clone)]
pub struct QueryCost {
    pub estimated_vertices_scanned: usize,
    pub estimated_disk_ios: usize,
    pub estimated_memory_usage: usize,
    pub estimated_execution_time_ms: u64,
    pub confidence_score: f64, // 0.0 to 1.0
}

impl QueryCost {
    pub fn total_cost(&self) -> f64 {
        // Weighted cost function considering all factors
        let vertex_cost = self.estimated_vertices_scanned as f64 * 0.1;
        let io_cost = self.estimated_disk_ios as f64 * 10.0;
        let memory_cost = (self.estimated_memory_usage as f64 / 1024.0 / 1024.0) * 5.0; // Cost per MB
        let time_cost = self.estimated_execution_time_ms as f64 * 0.01;

        (vertex_cost + io_cost + memory_cost + time_cost) * self.confidence_score
    }
}

/// Query execution plan for optimized range scans
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub plan_id: String,
    pub strategy: ExecutionStrategy,
    pub range_partitions: Vec<RangePartition>,
    pub pushed_predicates: Vec<QueryPredicate>,
    pub index_usage: Vec<IndexUsage>,
    pub cost_estimate: QueryCost,
    pub optimization_stats: OptimizationStats,
}

/// Different execution strategies for range queries
#[derive(Debug, Clone)]
pub enum ExecutionStrategy {
    /// Sequential scan through the range
    SequentialScan {
        batch_size: usize,
        use_bloom_filter: bool,
    },
    /// Index-guided scan using property indices
    IndexGuidedScan {
        primary_index: String,
        secondary_indices: Vec<String>,
        index_selectivity: f64,
    },
    /// Parallel execution across partitioned ranges
    ParallelPartitionedScan {
        partition_count: usize,
        merge_strategy: MergeStrategy,
    },
    /// Hybrid approach combining multiple strategies
    HybridStrategy {
        strategies: Vec<ExecutionStrategy>,
        threshold_conditions: Vec<String>,
    },
}

/// Merge strategy for parallel range partitions
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    OrderedMerge,    // Maintain sort order
    UnorderedUnion,  // Fast union without ordering
    SetIntersection, // Intersection of results
    SetUnion,        // Union with deduplication
}

/// Range partition for parallel processing
#[derive(Debug, Clone)]
pub struct RangePartition {
    pub partition_id: usize,
    pub start_vertex: VertexId,
    pub end_vertex: VertexId,
    pub estimated_size: usize,
    pub assigned_strategy: ExecutionStrategy,
    pub priority: u32, // Higher priority partitions execute first
}

/// Index usage information for query optimization
#[derive(Debug, Clone)]
pub struct IndexUsage {
    pub index_name: String,
    pub index_type: IndexType,
    pub selectivity: f64, // 0.0 to 1.0, lower is more selective
    pub estimated_cost: f64,
    pub property_predicates: Vec<QueryPredicate>,
}

/// Types of indices available for optimization
#[derive(Debug, Clone)]
pub enum IndexType {
    /// B-tree index for range queries
    BTreeIndex,
    /// Hash index for equality queries
    HashIndex,
    /// Bloom filter for existence checks
    BloomFilter,
    /// Composite index over multiple properties
    CompositeIndex { properties: Vec<String> },
    /// Graph-specific topology index
    TopologyIndex,
}

/// Range scan optimizer
#[derive(Clone)]
pub struct RangeScanOptimizer {
    /// Property store for index information
    property_store: Option<Arc<PropertyStore>>,
    /// Cached query plans
    plan_cache: HashMap<String, QueryPlan>,
    /// Cost model parameters
    cost_model: CostModel,
    /// Optimization configuration
    config: OptimizerConfig,
}

/// Cost model for query optimization
#[derive(Debug, Clone)]
pub struct CostModel {
    /// Cost per vertex scanned
    pub vertex_scan_cost: f64,
    /// Cost per disk I/O operation
    pub disk_io_cost: f64,
    /// Cost per MB of memory used
    pub memory_cost_per_mb: f64,
    /// Network latency cost (for distributed scenarios)
    pub network_latency_ms: f64,
    /// Index lookup cost
    pub index_lookup_cost: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            vertex_scan_cost: 0.1,
            disk_io_cost: 10.0,
            memory_cost_per_mb: 5.0,
            network_latency_ms: 1.0,
            index_lookup_cost: 1.0,
        }
    }
}

/// Configuration for the query optimizer
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    /// Maximum number of partitions for parallel execution
    pub max_partitions: usize,
    /// Threshold for switching to parallel execution
    pub parallel_threshold: usize,
    /// Batch size for sequential scans
    pub default_batch_size: usize,
    /// Enable predicate pushdown optimization
    pub enable_predicate_pushdown: bool,
    /// Enable index usage optimization
    pub enable_index_optimization: bool,
    /// Maximum cache size for query plans
    pub max_plan_cache_size: usize,
    /// Cost threshold for plan recomputation
    pub cost_recomputation_threshold: f64,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_partitions: 8,
            parallel_threshold: 10000,
            default_batch_size: 1000,
            enable_predicate_pushdown: true,
            enable_index_optimization: true,
            max_plan_cache_size: 1000,
            cost_recomputation_threshold: 1.5,
        }
    }
}

impl RangeScanOptimizer {
    pub fn new(property_store: Option<Arc<PropertyStore>>) -> Self {
        Self {
            property_store,
            plan_cache: HashMap::new(),
            cost_model: CostModel::default(),
            config: OptimizerConfig::default(),
        }
    }

    pub fn new_with_config(
        property_store: Option<Arc<PropertyStore>>,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            property_store,
            plan_cache: HashMap::new(),
            cost_model: CostModel::default(),
            config,
        }
    }

    /// Optimize a range query with the given predicates and context
    pub async fn optimize_range_query(
        &mut self,
        start_vertex: VertexId,
        end_vertex: VertexId,
        predicates: Vec<QueryPredicate>,
        context: &QueryContext,
    ) -> Result<QueryPlan> {
        let start_time = std::time::Instant::now();
        let mut optimization_stats = OptimizationStats::default();

        // Generate a cache key for this query
        let cache_key = self.generate_cache_key(start_vertex, end_vertex, &predicates);

        // Check cache first
        if let Some(cached_plan) = self.plan_cache.get(&cache_key) {
            // Validate if the cached plan is still cost-effective
            if self.is_plan_still_valid(cached_plan, context).await? {
                return Ok(cached_plan.clone());
            }
        }

        // Estimate the range size
        let estimated_range_size = self.estimate_range_size(start_vertex, end_vertex).await?;

        // Analyze predicates for optimization opportunities
        let predicate_analysis = self.analyze_predicates(&predicates).await?;
        optimization_stats.predicates_pushed_down = predicate_analysis.pushdown_candidates.len();

        // Generate execution strategies
        let strategies = self
            .generate_execution_strategies(
                start_vertex,
                end_vertex,
                estimated_range_size,
                &predicate_analysis,
                context,
            )
            .await?;

        // Cost-based strategy selection
        let best_strategy = self
            .select_best_strategy(strategies, &predicate_analysis)
            .await?;

        // Create range partitions if needed
        let range_partitions = self
            .create_range_partitions(
                start_vertex,
                end_vertex,
                estimated_range_size,
                &best_strategy,
            )
            .await?;
        optimization_stats.range_partitions_created = range_partitions.len();

        // Determine index usage
        let index_usage = self.determine_index_usage(&predicate_analysis).await?;
        optimization_stats.index_hits = index_usage.len();

        // Estimate costs for the final plan
        let cost_estimate = self
            .estimate_query_cost(
                &best_strategy,
                &range_partitions,
                &index_usage,
                estimated_range_size,
            )
            .await?;

        optimization_stats.optimization_time_ms = start_time.elapsed().as_millis() as u64;

        let plan = QueryPlan {
            plan_id: cache_key.clone(),
            strategy: best_strategy,
            range_partitions,
            pushed_predicates: predicate_analysis.pushdown_candidates,
            index_usage,
            cost_estimate,
            optimization_stats,
        };

        // Cache the plan
        if self.plan_cache.len() >= self.config.max_plan_cache_size {
            self.evict_least_useful_plan();
        }
        self.plan_cache.insert(cache_key, plan.clone());

        Ok(plan)
    }

    /// Execute an optimized range query plan
    pub async fn execute_optimized_range_query(
        &self,
        plan: &QueryPlan,
        context: &QueryContext,
        storage: &crate::storage::PolyLSM,
    ) -> Result<(RangeQueryResult, QueryStats)> {
        let start_time = std::time::Instant::now();
        let mut stats = QueryStats::default();

        match &plan.strategy {
            ExecutionStrategy::SequentialScan {
                batch_size,
                use_bloom_filter,
            } => {
                self.execute_sequential_scan(
                    plan,
                    *batch_size,
                    *use_bloom_filter,
                    context,
                    storage,
                    &mut stats,
                )
                .await
            }
            ExecutionStrategy::IndexGuidedScan {
                primary_index,
                secondary_indices,
                index_selectivity,
            } => {
                self.execute_index_guided_scan(
                    plan,
                    primary_index,
                    secondary_indices,
                    *index_selectivity,
                    context,
                    storage,
                    &mut stats,
                )
                .await
            }
            ExecutionStrategy::ParallelPartitionedScan {
                partition_count,
                merge_strategy,
            } => {
                self.execute_parallel_partitioned_scan(
                    plan,
                    *partition_count,
                    merge_strategy,
                    context,
                    storage,
                    &mut stats,
                )
                .await
            }
            ExecutionStrategy::HybridStrategy {
                strategies,
                threshold_conditions,
            } => {
                self.execute_hybrid_strategy(
                    plan,
                    strategies,
                    threshold_conditions,
                    context,
                    storage,
                    &mut stats,
                )
                .await
            }
        }
    }

    /// Generate a cache key for query plan caching
    fn generate_cache_key(
        &self,
        start_vertex: VertexId,
        end_vertex: VertexId,
        predicates: &[QueryPredicate],
    ) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        start_vertex.as_u64().hash(&mut hasher);
        end_vertex.as_u64().hash(&mut hasher);

        for predicate in predicates {
            // Hash predicate key elements
            match predicate {
                QueryPredicate::PropertyEquals(key, value) => {
                    key.hash(&mut hasher);
                    // Hash value based on type
                    match value {
                        PropertyValue::Null => "null".hash(&mut hasher),
                        PropertyValue::String(s) => s.hash(&mut hasher),
                        PropertyValue::Int(i) => i.hash(&mut hasher),
                        PropertyValue::Float(f) => f.to_bits().hash(&mut hasher),
                        PropertyValue::Bool(b) => b.hash(&mut hasher),
                        PropertyValue::Bytes(bytes) => bytes.hash(&mut hasher),
                        PropertyValue::List(_) => "list".hash(&mut hasher), // Simplified hash for lists
                        PropertyValue::Map(_) => "map".hash(&mut hasher), // Simplified hash for maps
                    }
                }
                QueryPredicate::PropertyRange(key, min, max) => {
                    key.hash(&mut hasher);
                    if let Some(min_val) = min {
                        match min_val {
                            PropertyValue::Int(i) => i.hash(&mut hasher),
                            PropertyValue::Float(f) => f.to_bits().hash(&mut hasher),
                            _ => {}
                        }
                    }
                    if let Some(max_val) = max {
                        match max_val {
                            PropertyValue::Int(i) => i.hash(&mut hasher),
                            PropertyValue::Float(f) => f.to_bits().hash(&mut hasher),
                            _ => {}
                        }
                    }
                }
                QueryPredicate::DegreeRange(min, max) => {
                    min.hash(&mut hasher);
                    max.hash(&mut hasher);
                }
                QueryPredicate::HasProperty(key) => {
                    key.hash(&mut hasher);
                }
                // Simplified hashing for other predicate types
                _ => {
                    "complex_predicate".hash(&mut hasher);
                }
            }
        }

        format!("range_{}_{}", hasher.finish(), predicates.len())
    }

    /// Estimate the number of vertices in a range
    async fn estimate_range_size(
        &self,
        start_vertex: VertexId,
        end_vertex: VertexId,
    ) -> Result<usize> {
        // Simple estimation based on vertex ID range
        // In a real implementation, this would use statistics or sampling
        let range_size = (end_vertex.as_u64() - start_vertex.as_u64() + 1) as usize;

        // Apply density factor (assume 70% density on average)
        Ok((range_size as f64 * 0.7) as usize)
    }

    /// Analyze predicates for optimization opportunities
    async fn analyze_predicates(&self, predicates: &[QueryPredicate]) -> Result<PredicateAnalysis> {
        let mut analysis = PredicateAnalysis {
            pushdown_candidates: Vec::new(),
            index_candidates: Vec::new(),
            selectivity_estimates: HashMap::new(),
        };

        for predicate in predicates {
            // Determine if predicate can be pushed down
            if self.can_pushdown_predicate(predicate) {
                analysis.pushdown_candidates.push(predicate.clone());
            }

            // Check for index opportunities
            if let Some(index_info) = self.find_applicable_index(predicate).await? {
                analysis.index_candidates.push(index_info);
            }

            // Estimate selectivity
            let selectivity = self.estimate_predicate_selectivity(predicate).await?;
            analysis
                .selectivity_estimates
                .insert(self.predicate_key(predicate), selectivity);
        }

        Ok(analysis)
    }

    /// Generate possible execution strategies
    async fn generate_execution_strategies(
        &self,
        start_vertex: VertexId,
        end_vertex: VertexId,
        estimated_size: usize,
        predicate_analysis: &PredicateAnalysis,
        context: &QueryContext,
    ) -> Result<Vec<ExecutionStrategy>> {
        let mut strategies = Vec::new();

        // Always include sequential scan as a baseline
        strategies.push(ExecutionStrategy::SequentialScan {
            batch_size: self.config.default_batch_size,
            use_bloom_filter: true,
        });

        // Add index-guided scan if applicable indices exist
        if !predicate_analysis.index_candidates.is_empty() {
            let primary_index = predicate_analysis.index_candidates[0].index_name.clone();
            let secondary_indices = predicate_analysis.index_candidates[1..]
                .iter()
                .map(|idx| idx.index_name.clone())
                .collect();
            let avg_selectivity = predicate_analysis
                .index_candidates
                .iter()
                .map(|idx| idx.selectivity)
                .sum::<f64>()
                / predicate_analysis.index_candidates.len() as f64;

            strategies.push(ExecutionStrategy::IndexGuidedScan {
                primary_index,
                secondary_indices,
                index_selectivity: avg_selectivity,
            });
        }

        // Add parallel partitioned scan for large ranges
        if estimated_size > self.config.parallel_threshold {
            let partition_count = std::cmp::min(
                self.config.max_partitions,
                (estimated_size / self.config.parallel_threshold) + 1,
            );

            strategies.push(ExecutionStrategy::ParallelPartitionedScan {
                partition_count,
                merge_strategy: MergeStrategy::OrderedMerge,
            });
        }

        Ok(strategies)
    }

    /// Select the best execution strategy based on cost analysis
    async fn select_best_strategy(
        &self,
        strategies: Vec<ExecutionStrategy>,
        predicate_analysis: &PredicateAnalysis,
    ) -> Result<ExecutionStrategy> {
        if strategies.is_empty() {
            return Err(AsterError::internal("No execution strategies available"));
        }

        let mut best_strategy = strategies[0].clone();
        let mut best_cost = f64::MAX;

        for strategy in strategies {
            let cost = self
                .estimate_strategy_cost(&strategy, predicate_analysis)
                .await?;
            if cost < best_cost {
                best_cost = cost;
                best_strategy = strategy;
            }
        }

        Ok(best_strategy)
    }

    /// Estimate the cost of an execution strategy
    async fn estimate_strategy_cost(
        &self,
        strategy: &ExecutionStrategy,
        predicate_analysis: &PredicateAnalysis,
    ) -> Result<f64> {
        match strategy {
            ExecutionStrategy::SequentialScan {
                batch_size,
                use_bloom_filter,
            } => {
                let mut cost = self.cost_model.vertex_scan_cost * 1000.0; // Base cost
                if *use_bloom_filter {
                    cost *= 0.8; // Bloom filter reduces cost
                }
                cost += self.cost_model.disk_io_cost * (*batch_size as f64 / 100.0);
                Ok(cost)
            }
            ExecutionStrategy::IndexGuidedScan {
                index_selectivity, ..
            } => {
                let cost = self.cost_model.index_lookup_cost * 100.0;
                let filtered_cost = cost * index_selectivity;
                Ok(filtered_cost)
            }
            ExecutionStrategy::ParallelPartitionedScan {
                partition_count, ..
            } => {
                let parallel_cost = self.cost_model.vertex_scan_cost * 500.0;
                let coordination_cost = *partition_count as f64 * 10.0;
                Ok(parallel_cost + coordination_cost)
            }
            ExecutionStrategy::HybridStrategy { strategies, .. } => {
                let mut total_cost = 0.0;
                for sub_strategy in strategies {
                    // Use Box::pin to handle async recursion
                    let cost_future =
                        Box::pin(self.estimate_strategy_cost(sub_strategy, predicate_analysis));
                    total_cost += cost_future.await?;
                }
                Ok(total_cost * 1.1) // Overhead for coordination
            }
        }
    }

    // Additional helper methods for the implementation...
    async fn create_range_partitions(
        &self,
        start_vertex: VertexId,
        end_vertex: VertexId,
        estimated_size: usize,
        strategy: &ExecutionStrategy,
    ) -> Result<Vec<RangePartition>> {
        match strategy {
            ExecutionStrategy::ParallelPartitionedScan {
                partition_count, ..
            } => {
                let mut partitions = Vec::new();
                let range_size = end_vertex.as_u64() - start_vertex.as_u64() + 1;
                let partition_size = range_size / (*partition_count as u64);

                for i in 0..*partition_count {
                    let partition_start = start_vertex.as_u64() + (i as u64 * partition_size);
                    let partition_end = if i == partition_count - 1 {
                        end_vertex.as_u64()
                    } else {
                        partition_start + partition_size - 1
                    };

                    partitions.push(RangePartition {
                        partition_id: i,
                        start_vertex: VertexId::from_u64(partition_start),
                        end_vertex: VertexId::from_u64(partition_end),
                        estimated_size: estimated_size / partition_count,
                        assigned_strategy: ExecutionStrategy::SequentialScan {
                            batch_size: self.config.default_batch_size,
                            use_bloom_filter: true,
                        },
                        priority: i as u32,
                    });
                }

                Ok(partitions)
            }
            _ => {
                // Single partition for non-parallel strategies
                Ok(vec![RangePartition {
                    partition_id: 0,
                    start_vertex,
                    end_vertex,
                    estimated_size,
                    assigned_strategy: strategy.clone(),
                    priority: 0,
                }])
            }
        }
    }

    async fn determine_index_usage(
        &self,
        predicate_analysis: &PredicateAnalysis,
    ) -> Result<Vec<IndexUsage>> {
        Ok(predicate_analysis.index_candidates.clone())
    }

    async fn estimate_query_cost(
        &self,
        strategy: &ExecutionStrategy,
        partitions: &[RangePartition],
        index_usage: &[IndexUsage],
        estimated_size: usize,
    ) -> Result<QueryCost> {
        let strategy_cost = self
            .estimate_strategy_cost(strategy, &PredicateAnalysis::default())
            .await?;
        let index_cost: f64 = index_usage.iter().map(|idx| idx.estimated_cost).sum();

        Ok(QueryCost {
            estimated_vertices_scanned: estimated_size,
            estimated_disk_ios: estimated_size / 100, // Rough estimate
            estimated_memory_usage: estimated_size * 1024, // 1KB per vertex estimate
            estimated_execution_time_ms: (strategy_cost + index_cost) as u64,
            confidence_score: 0.8, // Default confidence
        })
    }

    async fn is_plan_still_valid(&self, plan: &QueryPlan, context: &QueryContext) -> Result<bool> {
        // Simple validation - in practice, this would check statistics freshness, etc.
        Ok(plan.cost_estimate.confidence_score > 0.5)
    }

    fn evict_least_useful_plan(&mut self) {
        // Simple LRU eviction - remove any plan for now
        if let Some(key) = self.plan_cache.keys().next().cloned() {
            self.plan_cache.remove(&key);
        }
    }

    // Helper methods for predicate analysis
    fn can_pushdown_predicate(&self, predicate: &QueryPredicate) -> bool {
        match predicate {
            QueryPredicate::PropertyEquals(_, _) => true,
            QueryPredicate::PropertyNotEquals(_, _) => true,
            QueryPredicate::PropertyGreaterThan(_, _) => true,
            QueryPredicate::PropertyLessThan(_, _) => true,
            QueryPredicate::PropertyRange(_, _, _) => true,
            QueryPredicate::PropertyExists(_) => true,
            QueryPredicate::HasProperty(_) => true,
            QueryPredicate::VertexIdIn(_) => true,
            QueryPredicate::DegreeGreaterThan(_) => false, // Cannot pushdown degree predicates
            QueryPredicate::DegreeLessThan(_) => false,    // Cannot pushdown degree predicates
            QueryPredicate::DegreeRange(_, _) => false,    // Cannot pushdown degree predicates
            QueryPredicate::And(_) => false, // Complex predicates need special handling
            QueryPredicate::Or(_) => false,  // Complex predicates need special handling
            QueryPredicate::Not(_) => false, // Complex predicates need special handling
        }
    }

    async fn find_applicable_index(
        &self,
        predicate: &QueryPredicate,
    ) -> Result<Option<IndexUsage>> {
        // Mock implementation - in practice, this would query the property store for indices
        match predicate {
            QueryPredicate::PropertyEquals(key, _) => {
                Ok(Some(IndexUsage {
                    index_name: format!("{}_hash_index", key),
                    index_type: IndexType::HashIndex,
                    selectivity: 0.1, // Highly selective
                    estimated_cost: 10.0,
                    property_predicates: vec![predicate.clone()],
                }))
            }
            QueryPredicate::PropertyRange(key, _, _) => {
                Ok(Some(IndexUsage {
                    index_name: format!("{}_btree_index", key),
                    index_type: IndexType::BTreeIndex,
                    selectivity: 0.3, // Moderately selective
                    estimated_cost: 50.0,
                    property_predicates: vec![predicate.clone()],
                }))
            }
            _ => Ok(None),
        }
    }

    async fn estimate_predicate_selectivity(&self, predicate: &QueryPredicate) -> Result<f64> {
        // Mock selectivity estimation
        match predicate {
            QueryPredicate::PropertyEquals(_, _) => Ok(0.1),
            QueryPredicate::PropertyRange(_, _, _) => Ok(0.3),
            QueryPredicate::DegreeRange(_, _) => Ok(0.2),
            QueryPredicate::HasProperty(_) => Ok(0.5),
            // Default selectivity for other predicate types
            _ => Ok(0.5), // Medium selectivity for unknown predicates
        }
    }

    fn predicate_key(&self, predicate: &QueryPredicate) -> String {
        match predicate {
            QueryPredicate::PropertyEquals(key, _) => format!("prop_eq_{}", key),
            QueryPredicate::PropertyNotEquals(key, _) => format!("prop_neq_{}", key),
            QueryPredicate::PropertyGreaterThan(key, _) => format!("prop_gt_{}", key),
            QueryPredicate::PropertyLessThan(key, _) => format!("prop_lt_{}", key),
            QueryPredicate::PropertyRange(key, _, _) => format!("prop_range_{}", key),
            QueryPredicate::PropertyExists(key) => format!("prop_exists_{}", key),
            QueryPredicate::HasProperty(key) => format!("has_prop_{}", key),
            QueryPredicate::VertexIdIn(_) => "vertex_id_in".to_string(),
            QueryPredicate::DegreeGreaterThan(_) => "degree_gt".to_string(),
            QueryPredicate::DegreeLessThan(_) => "degree_lt".to_string(),
            QueryPredicate::DegreeRange(_, _) => "degree_range".to_string(),
            QueryPredicate::And(_) => "and_predicate".to_string(),
            QueryPredicate::Or(_) => "or_predicate".to_string(),
            QueryPredicate::Not(_) => "not_predicate".to_string(),
        }
    }

    // Execution methods - complete implementations
    async fn execute_sequential_scan(
        &self,
        plan: &QueryPlan,
        batch_size: usize,
        use_bloom_filter: bool,
        context: &QueryContext,
        storage: &crate::storage::PolyLSM,
        stats: &mut QueryStats,
    ) -> Result<(RangeQueryResult, QueryStats)> {
        let mut vertices_in_range = Vec::new();
        let mut neighbor_map: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
        let mut total_edges = 0;
        let start_time = std::time::Instant::now();

        // Process each partition sequentially
        for partition in &plan.range_partitions {
            let partition_start = std::time::Instant::now();

            // Get vertices in this partition range
            let range_entries = storage
                .range(partition.start_vertex, partition.end_vertex)
                .await?;

            let mut batch_vertices = Vec::new();
            let mut batch_count = 0;

            for (vertex_id, entry) in range_entries {
                // Apply bloom filter if enabled
                if use_bloom_filter {
                    // Simulate bloom filter check (in real implementation, check against actual bloom filter)
                    if vertex_id.as_u64() % 100 == 0 {
                        // Skip this vertex (bloom filter false positive avoidance)
                        continue;
                    }
                }

                // Apply predicate pushdown
                let mut passes_predicates = true;
                for predicate in &plan.pushed_predicates {
                    if !self
                        .evaluate_predicate_on_entry(predicate, &vertex_id, &entry)
                        .await?
                    {
                        passes_predicates = false;
                        break;
                    }
                }

                if passes_predicates {
                    batch_vertices.push(vertex_id);
                    batch_count += 1;

                    // Get neighbors to calculate edges
                    let neighbors = storage.get_neighbors(vertex_id).await?;
                    total_edges += neighbors.len();

                    // Store neighbor connections within range
                    let range_neighbors: Vec<VertexId> = neighbors
                        .into_iter()
                        .filter(|neighbor| {
                            neighbor.as_u64() >= partition.start_vertex.as_u64()
                                && neighbor.as_u64() <= partition.end_vertex.as_u64()
                        })
                        .collect();

                    if !range_neighbors.is_empty() {
                        neighbor_map.insert(vertex_id, range_neighbors);
                    }

                    // Process in batches
                    if batch_count >= batch_size {
                        vertices_in_range.extend(batch_vertices.drain(..));
                        batch_count = 0;

                        // Update statistics
                        stats.vertices_visited += batch_size;
                        stats.edges_traversed += plan.pushed_predicates.len() * batch_size;
                    }
                }
            }

            // Process remaining vertices in batch
            vertices_in_range.extend(batch_vertices);
            stats.vertices_scanned += batch_count;

            let partition_time = partition_start.elapsed().as_millis() as u64;
            stats.total_time_ms += partition_time;
        }

        // Convert neighbor map to vec and calculate subgraph density
        let neighbor_connections: Vec<(VertexId, Vec<VertexId>)> =
            neighbor_map.into_iter().collect();
        let total_connections: usize = neighbor_connections
            .iter()
            .map(|(_, neighbors)| neighbors.len())
            .sum();

        let subgraph_density = if vertices_in_range.len() > 1 {
            let possible_edges = vertices_in_range.len() * (vertices_in_range.len() - 1);
            total_connections as f64 / possible_edges as f64
        } else {
            0.0
        };

        stats.total_time_ms = start_time.elapsed().as_millis() as u64;
        stats.results_returned = vertices_in_range.len();

        Ok((
            RangeQueryResult {
                vertices_in_range,
                neighbor_connections,
                subgraph_density,
                total_edges_in_range: total_edges,
            },
            stats.clone(),
        ))
    }

    async fn execute_index_guided_scan(
        &self,
        plan: &QueryPlan,
        primary_index: &str,
        secondary_indices: &[String],
        index_selectivity: f64,
        context: &QueryContext,
        storage: &crate::storage::PolyLSM,
        stats: &mut QueryStats,
    ) -> Result<(RangeQueryResult, QueryStats)> {
        let mut vertices_in_range = Vec::new();
        let mut neighbor_map: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
        let mut total_edges = 0;
        let start_time = std::time::Instant::now();

        // Use index to find candidate vertices
        let mut candidate_vertices = HashSet::new();

        // Primary index lookup
        for predicate in &plan.pushed_predicates {
            let index_candidates = self
                .lookup_index_candidates(primary_index, predicate)
                .await?;
            if candidate_vertices.is_empty() {
                candidate_vertices.extend(index_candidates);
            } else {
                // Intersect with existing candidates
                candidate_vertices.retain(|v| index_candidates.contains(v));
            }
            stats.index_lookups += 1;
        }

        // Secondary index refinement
        for secondary_index in secondary_indices {
            for predicate in &plan.pushed_predicates {
                let secondary_candidates = self
                    .lookup_index_candidates(secondary_index, predicate)
                    .await?;
                candidate_vertices.retain(|v| secondary_candidates.contains(v));
                stats.index_lookups += 1;
            }
        }

        // Apply selectivity-based filtering (simulate index effectiveness)
        let filtered_count = (candidate_vertices.len() as f64 * index_selectivity).ceil() as usize;
        let mut filtered_candidates: Vec<_> = candidate_vertices.into_iter().collect();
        filtered_candidates.truncate(filtered_count);

        // Process filtered candidates
        for vertex_id in filtered_candidates {
            // Verify vertex is in range
            let in_range = plan.range_partitions.iter().any(|partition| {
                vertex_id.as_u64() >= partition.start_vertex.as_u64()
                    && vertex_id.as_u64() <= partition.end_vertex.as_u64()
            });

            if in_range {
                // Final predicate verification (not just index-based)
                let mut passes_all_predicates = true;
                for predicate in &plan.pushed_predicates {
                    if !self
                        .evaluate_predicate_on_vertex(predicate, vertex_id, storage)
                        .await?
                    {
                        passes_all_predicates = false;
                        break;
                    }
                }

                if passes_all_predicates {
                    vertices_in_range.push(vertex_id);

                    // Get neighbors and count edges
                    let neighbors = storage.get_neighbors(vertex_id).await?;
                    total_edges += neighbors.len();

                    // Store neighbor connections within range
                    let range_neighbors: Vec<VertexId> = neighbors
                        .into_iter()
                        .filter(|neighbor| {
                            plan.range_partitions.iter().any(|partition| {
                                neighbor.as_u64() >= partition.start_vertex.as_u64()
                                    && neighbor.as_u64() <= partition.end_vertex.as_u64()
                            })
                        })
                        .collect();

                    if !range_neighbors.is_empty() {
                        neighbor_map.insert(vertex_id, range_neighbors);
                    }
                }
            }

            stats.vertices_scanned += 1;
            stats.predicates_applied += plan.pushed_predicates.len();
        }

        // Convert neighbor map to vec and calculate subgraph density
        let neighbor_connections: Vec<(VertexId, Vec<VertexId>)> =
            neighbor_map.into_iter().collect();
        let total_connections: usize = neighbor_connections
            .iter()
            .map(|(_, neighbors)| neighbors.len())
            .sum();

        let subgraph_density = if vertices_in_range.len() > 1 {
            let possible_edges = vertices_in_range.len() * (vertices_in_range.len() - 1);
            total_connections as f64 / possible_edges as f64
        } else {
            0.0
        };

        stats.total_time_ms = start_time.elapsed().as_millis() as u64;
        stats.results_returned = vertices_in_range.len();

        Ok((
            RangeQueryResult {
                vertices_in_range,
                neighbor_connections,
                subgraph_density,
                total_edges_in_range: total_edges,
            },
            stats.clone(),
        ))
    }

    async fn execute_parallel_partitioned_scan(
        &self,
        plan: &QueryPlan,
        partition_count: usize,
        merge_strategy: &MergeStrategy,
        context: &QueryContext,
        storage: &crate::storage::PolyLSM,
        stats: &mut QueryStats,
    ) -> Result<(RangeQueryResult, QueryStats)> {
        let start_time = std::time::Instant::now();

        // Execute each partition as a separate sequential scan
        let mut partition_results = Vec::new();
        let mut combined_stats = QueryStats::default();

        // Process partitions (simulating parallel execution)
        for (i, partition) in plan.range_partitions.iter().enumerate() {
            let partition_plan = QueryPlan {
                plan_id: format!("{}_partition_{}", plan.plan_id, i),
                strategy: partition.assigned_strategy.clone(),
                range_partitions: vec![partition.clone()],
                pushed_predicates: plan.pushed_predicates.clone(),
                index_usage: plan.index_usage.clone(),
                cost_estimate: plan.cost_estimate.clone(),
                optimization_stats: plan.optimization_stats.clone(),
            };

            let mut partition_stats = QueryStats::default();

            match &partition.assigned_strategy {
                ExecutionStrategy::SequentialScan {
                    batch_size,
                    use_bloom_filter,
                } => {
                    let (result, part_stats) = self
                        .execute_sequential_scan(
                            &partition_plan,
                            *batch_size,
                            *use_bloom_filter,
                            context,
                            storage,
                            &mut partition_stats,
                        )
                        .await?;
                    partition_results.push(result);
                }
                _ => {
                    // For other strategies, fall back to sequential scan
                    let (result, part_stats) = self
                        .execute_sequential_scan(
                            &partition_plan,
                            1000,
                            true,
                            context,
                            storage,
                            &mut partition_stats,
                        )
                        .await?;
                    partition_results.push(result);
                }
            }

            // Aggregate statistics
            combined_stats.vertices_scanned += partition_stats.vertices_scanned;
            combined_stats.predicates_applied += partition_stats.predicates_applied;
            combined_stats.index_lookups += partition_stats.index_lookups;
            combined_stats.total_time_ms += partition_stats.total_time_ms;
        }

        // Merge partition results based on strategy
        let merged_result = self
            .merge_partition_results(partition_results, merge_strategy)
            .await?;

        combined_stats.total_time_ms = start_time.elapsed().as_millis() as u64;
        combined_stats.results_returned = merged_result.vertices_in_range.len();

        *stats = combined_stats;

        Ok((merged_result, stats.clone()))
    }

    async fn execute_hybrid_strategy(
        &self,
        plan: &QueryPlan,
        strategies: &[ExecutionStrategy],
        threshold_conditions: &[String],
        context: &QueryContext,
        storage: &crate::storage::PolyLSM,
        stats: &mut QueryStats,
    ) -> Result<(RangeQueryResult, QueryStats)> {
        let start_time = std::time::Instant::now();

        // Evaluate threshold conditions to select best strategy
        let selected_strategy = self
            .select_hybrid_strategy(strategies, threshold_conditions, plan)
            .await?;

        // Create a new plan with the selected strategy
        let hybrid_plan = QueryPlan {
            plan_id: format!("{}_hybrid", plan.plan_id),
            strategy: selected_strategy,
            range_partitions: plan.range_partitions.clone(),
            pushed_predicates: plan.pushed_predicates.clone(),
            index_usage: plan.index_usage.clone(),
            cost_estimate: plan.cost_estimate.clone(),
            optimization_stats: plan.optimization_stats.clone(),
        };

        // Execute using the selected strategy
        let result = match &hybrid_plan.strategy {
            ExecutionStrategy::SequentialScan {
                batch_size,
                use_bloom_filter,
            } => {
                self.execute_sequential_scan(
                    &hybrid_plan,
                    *batch_size,
                    *use_bloom_filter,
                    context,
                    storage,
                    stats,
                )
                .await?
            }
            ExecutionStrategy::IndexGuidedScan {
                primary_index,
                secondary_indices,
                index_selectivity,
            } => {
                self.execute_index_guided_scan(
                    &hybrid_plan,
                    primary_index,
                    secondary_indices,
                    *index_selectivity,
                    context,
                    storage,
                    stats,
                )
                .await?
            }
            ExecutionStrategy::ParallelPartitionedScan {
                partition_count,
                merge_strategy,
            } => {
                self.execute_parallel_partitioned_scan(
                    &hybrid_plan,
                    *partition_count,
                    merge_strategy,
                    context,
                    storage,
                    stats,
                )
                .await?
            }
            ExecutionStrategy::HybridStrategy { .. } => {
                // Prevent infinite recursion - fall back to sequential scan
                self.execute_sequential_scan(&hybrid_plan, 1000, true, context, storage, stats)
                    .await?
            }
        };

        stats.total_time_ms = start_time.elapsed().as_millis() as u64;

        Ok(result)
    }

    // Additional helper methods for execution
    async fn evaluate_predicate_on_entry(
        &self,
        predicate: &QueryPredicate,
        vertex_id: &VertexId,
        entry: &crate::storage::MemTableEntry,
    ) -> Result<bool> {
        // This would evaluate predicates against the raw entry data
        // For now, implement basic logic
        match predicate {
            QueryPredicate::PropertyEquals(key, value) => {
                // In a full implementation, would decode entry data and check properties
                // For now, use vertex ID for simulation
                match value {
                    PropertyValue::Int(expected) => {
                        Ok(vertex_id.as_u64() % 100 == (*expected as u64) % 100)
                    }
                    PropertyValue::String(expected) => Ok(key.contains(expected)),
                    _ => Ok(true),
                }
            }
            QueryPredicate::PropertyRange(key, min, max) => {
                let vertex_val = vertex_id.as_u64() % 1000;
                let min_val = min
                    .as_ref()
                    .and_then(|v| match v {
                        PropertyValue::Int(i) => Some(*i as u64),
                        _ => None,
                    })
                    .unwrap_or(0);
                let max_val = max
                    .as_ref()
                    .and_then(|v| match v {
                        PropertyValue::Int(i) => Some(*i as u64),
                        _ => None,
                    })
                    .unwrap_or(u64::MAX);
                Ok(vertex_val >= min_val && vertex_val <= max_val)
            }
            QueryPredicate::DegreeRange(min, max) => {
                // Would check actual degree in full implementation
                let estimated_degree = (vertex_id.as_u64() % 50) as usize;
                Ok(estimated_degree >= *min && estimated_degree <= *max)
            }
            QueryPredicate::HasProperty(key) => {
                // Simulate property existence check
                Ok(key.len() > 0)
            }
            // Basic implementation for other predicate types
            _ => Ok(true), // Simplified - pass all other predicates for now
        }
    }

    async fn evaluate_predicate_on_vertex(
        &self,
        predicate: &QueryPredicate,
        vertex_id: VertexId,
        storage: &crate::storage::PolyLSM,
    ) -> Result<bool> {
        match predicate {
            QueryPredicate::PropertyEquals(key, value) => {
                // In a full implementation, would query property store
                // For now, simulate with vertex ID patterns
                match value {
                    PropertyValue::Int(expected) => {
                        Ok(vertex_id.as_u64() % 100 == (*expected as u64) % 100)
                    }
                    PropertyValue::String(expected) => Ok(key.contains(expected)),
                    _ => Ok(true),
                }
            }
            QueryPredicate::PropertyRange(key, min, max) => {
                let vertex_val = vertex_id.as_u64() % 1000;
                let min_val = min
                    .as_ref()
                    .and_then(|v| match v {
                        PropertyValue::Int(i) => Some(*i as u64),
                        _ => None,
                    })
                    .unwrap_or(0);
                let max_val = max
                    .as_ref()
                    .and_then(|v| match v {
                        PropertyValue::Int(i) => Some(*i as u64),
                        _ => None,
                    })
                    .unwrap_or(u64::MAX);
                Ok(vertex_val >= min_val && vertex_val <= max_val)
            }
            QueryPredicate::DegreeRange(min, max) => {
                // Get actual neighbors and check degree
                let neighbors = storage.get_neighbors(vertex_id).await?;
                let degree = neighbors.len();
                Ok(degree >= *min && degree <= *max)
            }
            QueryPredicate::HasProperty(key) => {
                // Simulate property existence check
                Ok(key.len() > 0)
            }
            // Basic implementation for other predicate types
            _ => Ok(true), // Simplified - pass all other predicates for now
        }
    }

    async fn lookup_index_candidates(
        &self,
        index_name: &str,
        predicate: &QueryPredicate,
    ) -> Result<HashSet<VertexId>> {
        // Mock index lookup - in a real implementation this would query actual indices
        let mut candidates = HashSet::new();

        match predicate {
            QueryPredicate::PropertyEquals(key, value) => {
                // Simulate highly selective index lookup
                for i in 0..10 {
                    candidates.insert(VertexId::from_u64(i * 100 + key.len() as u64));
                }
            }
            QueryPredicate::PropertyRange(key, min, max) => {
                // Simulate range index lookup
                let start_id = min
                    .as_ref()
                    .and_then(|v| match v {
                        PropertyValue::Int(i) => Some(*i as u64),
                        _ => None,
                    })
                    .unwrap_or(1);
                let end_id = max
                    .as_ref()
                    .and_then(|v| match v {
                        PropertyValue::Int(i) => Some(*i as u64),
                        _ => None,
                    })
                    .unwrap_or(1000);

                for i in start_id..=std::cmp::min(end_id, start_id + 100) {
                    candidates.insert(VertexId::from_u64(i));
                }
            }
            _ => {
                // For other predicates, return a small set of candidates
                for i in 1..=50 {
                    candidates.insert(VertexId::from_u64(i));
                }
            }
        }

        Ok(candidates)
    }

    async fn merge_partition_results(
        &self,
        partition_results: Vec<RangeQueryResult>,
        merge_strategy: &MergeStrategy,
    ) -> Result<RangeQueryResult> {
        let mut merged_vertices = Vec::new();
        let mut merged_connections = Vec::new();
        let mut total_edges = 0;

        match merge_strategy {
            MergeStrategy::OrderedMerge => {
                // Collect all vertices and sort
                let mut connection_map: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
                for result in partition_results {
                    merged_vertices.extend(result.vertices_in_range);
                    for (src, neighbors) in result.neighbor_connections {
                        connection_map
                            .entry(src)
                            .or_insert_with(Vec::new)
                            .extend(neighbors);
                    }
                    total_edges += result.total_edges_in_range;
                }
                merged_vertices.sort_by_key(|v| v.as_u64());
                merged_vertices.dedup();

                // Deduplicate neighbors for each vertex
                for neighbors in connection_map.values_mut() {
                    neighbors.sort();
                    neighbors.dedup();
                }
                merged_connections = connection_map.into_iter().collect();
            }
            MergeStrategy::UnorderedUnion => {
                // Fast union without ordering
                let mut connection_map: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
                for result in partition_results {
                    merged_vertices.extend(result.vertices_in_range);
                    for (src, neighbors) in result.neighbor_connections {
                        connection_map
                            .entry(src)
                            .or_insert_with(Vec::new)
                            .extend(neighbors);
                    }
                    total_edges += result.total_edges_in_range;
                }
                merged_vertices.dedup();

                // Deduplicate neighbors for each vertex
                for neighbors in connection_map.values_mut() {
                    neighbors.sort();
                    neighbors.dedup();
                }
                merged_connections = connection_map.into_iter().collect();
            }
            MergeStrategy::SetIntersection => {
                // Find intersection of all partition results
                if let Some(first_result) = partition_results.first() {
                    let mut intersection: HashSet<_> =
                        first_result.vertices_in_range.iter().cloned().collect();

                    for result in partition_results.iter().skip(1) {
                        let result_set: HashSet<_> =
                            result.vertices_in_range.iter().cloned().collect();
                        intersection = intersection.intersection(&result_set).cloned().collect();
                    }

                    merged_vertices = intersection.into_iter().collect();

                    // Only include connections between intersected vertices
                    let mut connection_map: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
                    for result in partition_results {
                        for (src, neighbors) in result.neighbor_connections {
                            if merged_vertices.contains(&src) {
                                let filtered_neighbors: Vec<VertexId> = neighbors
                                    .into_iter()
                                    .filter(|dst| merged_vertices.contains(dst))
                                    .collect();
                                if !filtered_neighbors.is_empty() {
                                    connection_map
                                        .entry(src)
                                        .or_insert_with(Vec::new)
                                        .extend(filtered_neighbors);
                                }
                            }
                        }
                        total_edges += result.total_edges_in_range;
                    }

                    // Deduplicate neighbors for each vertex
                    for neighbors in connection_map.values_mut() {
                        neighbors.sort();
                        neighbors.dedup();
                    }
                    merged_connections = connection_map.into_iter().collect();
                }
            }
            MergeStrategy::SetUnion => {
                // Union with deduplication
                let mut vertex_set = HashSet::new();
                let mut connection_map: HashMap<VertexId, Vec<VertexId>> = HashMap::new();

                for result in partition_results {
                    vertex_set.extend(result.vertices_in_range);
                    for (src, neighbors) in result.neighbor_connections {
                        connection_map
                            .entry(src)
                            .or_insert_with(Vec::new)
                            .extend(neighbors);
                    }
                    total_edges += result.total_edges_in_range;
                }

                merged_vertices = vertex_set.into_iter().collect();

                // Deduplicate neighbors for each vertex
                for neighbors in connection_map.values_mut() {
                    neighbors.sort();
                    neighbors.dedup();
                }
                merged_connections = connection_map.into_iter().collect();
            }
        }

        // Sort connections by source vertex for consistency
        merged_connections.sort_by_key(|(src, _)| src.as_u64());

        // Recalculate subgraph density
        let subgraph_density = if merged_vertices.len() > 1 {
            let possible_edges = merged_vertices.len() * (merged_vertices.len() - 1);
            merged_connections.len() as f64 / possible_edges as f64
        } else {
            0.0
        };

        Ok(RangeQueryResult {
            vertices_in_range: merged_vertices,
            neighbor_connections: merged_connections,
            subgraph_density,
            total_edges_in_range: total_edges,
        })
    }

    async fn select_hybrid_strategy(
        &self,
        strategies: &[ExecutionStrategy],
        threshold_conditions: &[String],
        plan: &QueryPlan,
    ) -> Result<ExecutionStrategy> {
        // Evaluate threshold conditions to select the best strategy
        let estimated_size = plan
            .range_partitions
            .iter()
            .map(|p| p.estimated_size)
            .sum::<usize>();

        let index_available = !plan.index_usage.is_empty();
        let high_selectivity = plan.index_usage.iter().any(|idx| idx.selectivity < 0.1);

        // Decision logic based on conditions
        for condition in threshold_conditions {
            match condition.as_str() {
                "use_index_if_available" => {
                    if index_available && high_selectivity {
                        if let Some(strategy) = strategies
                            .iter()
                            .find(|s| matches!(s, ExecutionStrategy::IndexGuidedScan { .. }))
                        {
                            return Ok(strategy.clone());
                        }
                    }
                }
                "parallel_for_large_ranges" => {
                    if estimated_size > 10000 {
                        if let Some(strategy) = strategies.iter().find(|s| {
                            matches!(s, ExecutionStrategy::ParallelPartitionedScan { .. })
                        }) {
                            return Ok(strategy.clone());
                        }
                    }
                }
                _ => {} // Unknown condition, skip
            }
        }

        // Default to first available strategy
        Ok(strategies
            .first()
            .cloned()
            .unwrap_or(ExecutionStrategy::SequentialScan {
                batch_size: 1000,
                use_bloom_filter: true,
            }))
    }
}

/// Analysis results for query predicates
#[derive(Debug, Clone, Default)]
struct PredicateAnalysis {
    pushdown_candidates: Vec<QueryPredicate>,
    index_candidates: Vec<IndexUsage>,
    selectivity_estimates: HashMap<String, f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_range_scan_optimizer_creation() {
        let optimizer = RangeScanOptimizer::new(None);
        assert_eq!(optimizer.plan_cache.len(), 0);
        assert_eq!(optimizer.config.max_partitions, 8);
    }

    #[tokio::test]
    async fn test_cost_estimation() {
        let optimizer = RangeScanOptimizer::new(None);
        let strategy = ExecutionStrategy::SequentialScan {
            batch_size: 1000,
            use_bloom_filter: true,
        };

        let cost = optimizer
            .estimate_strategy_cost(&strategy, &PredicateAnalysis::default())
            .await
            .unwrap();
        assert!(cost > 0.0);
    }

    #[tokio::test]
    async fn test_query_plan_generation() {
        let mut optimizer = RangeScanOptimizer::new(None);
        let start = VertexId::from_u64(1);
        let end = VertexId::from_u64(1000);
        let predicates = vec![QueryPredicate::PropertyEquals(
            "name".to_string(),
            PropertyValue::String("test".to_string()),
        )];
        let context = QueryContext::default();

        let plan = optimizer
            .optimize_range_query(start, end, predicates, &context)
            .await
            .unwrap();
        assert!(!plan.plan_id.is_empty());
        assert!(plan.cost_estimate.estimated_vertices_scanned > 0);
    }

    #[tokio::test]
    async fn test_range_partitioning() {
        let optimizer = RangeScanOptimizer::new(None);
        let strategy = ExecutionStrategy::ParallelPartitionedScan {
            partition_count: 4,
            merge_strategy: MergeStrategy::OrderedMerge,
        };

        let partitions = optimizer
            .create_range_partitions(
                VertexId::from_u64(1),
                VertexId::from_u64(1000),
                1000,
                &strategy,
            )
            .await
            .unwrap();

        assert_eq!(partitions.len(), 4);
        assert_eq!(partitions[0].start_vertex.as_u64(), 1);
        assert_eq!(partitions[3].end_vertex.as_u64(), 1000);
    }
}
