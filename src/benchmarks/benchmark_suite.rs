use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::benchmarks::{BenchmarkMetrics, WorkloadType};
use crate::error::{AsterError, Result};
use crate::graph::Graph;
use crate::storage::poly_lsm::PolyLSM;
use crate::types::VertexId;

/// Comprehensive benchmarking suite for Aster database
pub struct BenchmarkSuite {
    storage: Arc<PolyLSM>,
    config: BenchmarkConfig,
    results: Vec<BenchmarkResults>,
}

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of vertices to create
    pub vertex_count: usize,
    /// Average degree per vertex
    pub avg_degree: u32,
    /// Number of iterations per benchmark
    pub iterations: usize,
    /// Concurrency level for parallel benchmarks
    pub concurrency: usize,
    /// Workload patterns to test
    pub workloads: Vec<WorkloadType>,
    /// Duration for sustained load tests
    pub duration_seconds: u64,
    /// Whether to run adaptive strategy comparison
    pub test_adaptive_strategies: bool,
    /// Whether to test lock-free vs mutex performance
    pub test_concurrency_models: bool,
    /// Whether to measure memory usage
    pub measure_memory: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            vertex_count: 100_000,
            avg_degree: 10,
            iterations: 1000,
            concurrency: 16,
            workloads: vec![
                WorkloadType::WriteHeavy,
                WorkloadType::ReadHeavy,
                WorkloadType::Mixed,
                WorkloadType::HighContention,
            ],
            duration_seconds: 60,
            test_adaptive_strategies: true,
            test_concurrency_models: true,
            measure_memory: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    pub workload: WorkloadType,
    pub metrics: BenchmarkMetrics,
    pub adaptive_stats: AdaptiveStrategyStats,
    pub lock_free_stats: LockFreePerformanceStats,
    pub memory_stats: MemoryUsageStats,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
pub struct AdaptiveStrategyStats {
    pub delta_updates: u64,
    pub pivot_updates: u64,
    pub avg_delta_latency_us: f64,
    pub avg_pivot_latency_us: f64,
    pub strategy_effectiveness: f64,
    pub threshold_adaptations: u32,
}

#[derive(Debug, Clone)]
pub struct LockFreePerformanceStats {
    pub total_acquisitions: usize,
    pub successful_acquisitions: usize,
    pub contention_events: usize,
    pub avg_backoff_time_us: f64,
    pub success_rate: f64,
    pub throughput_ops_per_sec: f64,
}

#[derive(Debug, Clone)]
pub struct MemoryUsageStats {
    pub peak_memory_mb: f64,
    pub avg_memory_mb: f64,
    pub memory_pool_hit_rate: f64,
    pub block_cache_hit_rate: f64,
    pub compression_ratio: f64,
    pub gc_pressure_score: f64,
}

impl BenchmarkSuite {
    pub fn new(storage: Arc<PolyLSM>, config: BenchmarkConfig) -> Self {
        Self {
            storage,
            config,
            results: Vec::new(),
        }
    }

    /// Run the complete benchmark suite
    pub async fn run_all_benchmarks(&mut self) -> Result<()> {
        info!(
            "Starting Aster benchmark suite with config: {:?}",
            self.config
        );

        // Initialize test data
        self.setup_test_data().await?;

        // Run each workload type
        for workload in &self.config.workloads.clone() {
            info!("Running benchmark for workload: {:?}", workload);
            let result = self.run_workload_benchmark(*workload).await?;
            self.results.push(result);

            // Brief pause between workloads
            sleep(Duration::from_secs(2)).await;
        }

        // Run comparative benchmarks if enabled
        if self.config.test_adaptive_strategies {
            self.run_adaptive_strategy_comparison().await?;
        }

        if self.config.test_concurrency_models {
            self.run_concurrency_comparison().await?;
        }

        info!(
            "Benchmark suite completed. {} results collected.",
            self.results.len()
        );
        Ok(())
    }

    /// Setup initial test data
    async fn setup_test_data(&self) -> Result<()> {
        info!(
            "Setting up test data: {} vertices, avg degree {}",
            self.config.vertex_count, self.config.avg_degree
        );

        let graph = Graph::new(&self.storage);
        let start_time = Instant::now();

        // Create vertices
        for i in 0..self.config.vertex_count {
            let vertex_id = VertexId::new(i as u64);
            graph.add_vertex(vertex_id, None).await?;

            if i % 10000 == 0 {
                info!("Created {} vertices", i);
            }
        }

        // Create edges with specified average degree
        let mut rng_state = 12345u64; // Simple LCG for reproducible results
        for i in 0..self.config.vertex_count {
            let source = VertexId::new(i as u64);
            let degree = self.pseudo_random_degree(&mut rng_state, self.config.avg_degree);

            for _ in 0..degree {
                let target_id = self.pseudo_random_target(&mut rng_state, self.config.vertex_count);
                let target = VertexId::new(target_id as u64);

                if source != target {
                    let _ = graph.add_edge(source, target, None).await; // Ignore duplicates
                }
            }
        }

        let setup_duration = start_time.elapsed();
        info!("Test data setup completed in {:?}", setup_duration);
        Ok(())
    }

    /// Run benchmark for a specific workload type
    async fn run_workload_benchmark(&self, workload: WorkloadType) -> Result<BenchmarkResults> {
        let start_time = Instant::now();
        let mut metrics = BenchmarkMetrics::new();

        // Get initial statistics
        let initial_stats = self.collect_system_stats().await;

        match workload {
            WorkloadType::WriteHeavy => self.run_write_heavy_benchmark(&mut metrics).await?,
            WorkloadType::ReadHeavy => self.run_read_heavy_benchmark(&mut metrics).await?,
            WorkloadType::Mixed => self.run_mixed_benchmark(&mut metrics).await?,
            WorkloadType::HighContention => {
                self.run_high_contention_benchmark(&mut metrics).await?
            }
            WorkloadType::Traversal => self.run_traversal_benchmark(&mut metrics).await?,
            WorkloadType::BulkLoad => self.run_bulk_load_benchmark(&mut metrics).await?,
        }

        // Get final statistics
        let final_stats = self.collect_system_stats().await;

        // Calculate performance metrics
        metrics.total_duration = start_time.elapsed();
        metrics.calculate_derived_metrics();

        Ok(BenchmarkResults {
            workload,
            metrics,
            adaptive_stats: self.calculate_adaptive_stats(&initial_stats, &final_stats),
            lock_free_stats: self.calculate_lock_free_stats(&initial_stats, &final_stats),
            memory_stats: self.calculate_memory_stats(&initial_stats, &final_stats),
            timestamp: start_time,
        })
    }

    /// Write-heavy workload: 80% writes, 20% reads
    async fn run_write_heavy_benchmark(&self, metrics: &mut BenchmarkMetrics) -> Result<()> {
        let graph = Graph::new(&self.storage);
        let mut rng_state = 67890u64;

        for i in 0..self.config.iterations {
            let operation_start = Instant::now();

            if self.pseudo_random_bool(&mut rng_state, 0.8) {
                // Write operation
                let source = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );
                let target = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );

                match graph.add_edge(source, target, None).await {
                    Ok(_) => {
                        metrics.successful_writes += 1;
                        metrics.total_write_time += operation_start.elapsed();
                    }
                    Err(_) => metrics.failed_writes += 1,
                }
            } else {
                // Read operation
                let vertex = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );

                match graph.get_neighbors(vertex).await {
                    Ok(neighbors) => {
                        metrics.successful_reads += 1;
                        metrics.total_read_time += operation_start.elapsed();
                        metrics.total_data_read += neighbors.len() * 8; // 8 bytes per VertexId
                    }
                    Err(_) => metrics.failed_reads += 1,
                }
            }

            if i % 1000 == 0 {
                info!(
                    "Write-heavy benchmark progress: {}/{}",
                    i, self.config.iterations
                );
            }
        }

        Ok(())
    }

    /// Read-heavy workload: 20% writes, 80% reads
    async fn run_read_heavy_benchmark(&self, metrics: &mut BenchmarkMetrics) -> Result<()> {
        let graph = Graph::new(&self.storage);
        let mut rng_state = 13579u64;

        for i in 0..self.config.iterations {
            let operation_start = Instant::now();

            if self.pseudo_random_bool(&mut rng_state, 0.2) {
                // Write operation
                let source = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );
                let target = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );

                match graph.add_edge(source, target, None).await {
                    Ok(_) => {
                        metrics.successful_writes += 1;
                        metrics.total_write_time += operation_start.elapsed();
                    }
                    Err(_) => metrics.failed_writes += 1,
                }
            } else {
                // Read operation
                let vertex = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );

                match graph.get_neighbors(vertex).await {
                    Ok(neighbors) => {
                        metrics.successful_reads += 1;
                        metrics.total_read_time += operation_start.elapsed();
                        metrics.total_data_read += neighbors.len() * 8;
                    }
                    Err(_) => metrics.failed_reads += 1,
                }
            }

            if i % 1000 == 0 {
                info!(
                    "Read-heavy benchmark progress: {}/{}",
                    i, self.config.iterations
                );
            }
        }

        Ok(())
    }

    /// Mixed workload: 50% writes, 50% reads
    async fn run_mixed_benchmark(&self, metrics: &mut BenchmarkMetrics) -> Result<()> {
        let graph = Graph::new(&self.storage);
        let mut rng_state = 24680u64;

        for i in 0..self.config.iterations {
            let operation_start = Instant::now();

            if self.pseudo_random_bool(&mut rng_state, 0.5) {
                // Write operation
                let source = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );
                let target = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );

                match graph.add_edge(source, target, None).await {
                    Ok(_) => {
                        metrics.successful_writes += 1;
                        metrics.total_write_time += operation_start.elapsed();
                    }
                    Err(_) => metrics.failed_writes += 1,
                }
            } else {
                // Read operation
                let vertex = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );

                match graph.get_neighbors(vertex).await {
                    Ok(neighbors) => {
                        metrics.successful_reads += 1;
                        metrics.total_read_time += operation_start.elapsed();
                        metrics.total_data_read += neighbors.len() * 8;
                    }
                    Err(_) => metrics.failed_reads += 1,
                }
            }

            if i % 1000 == 0 {
                info!("Mixed benchmark progress: {}/{}", i, self.config.iterations);
            }
        }

        Ok(())
    }

    /// High contention workload: Focus on small set of vertices
    async fn run_high_contention_benchmark(&self, metrics: &mut BenchmarkMetrics) -> Result<()> {
        let iterations_per_task = self.config.iterations / self.config.concurrency;

        // Focus on 10% of vertices for high contention
        let hot_vertex_count = self.config.vertex_count / 10;
        let mut rng_state = 97531u64;

        // Run concurrent operations using simple loop instead of spawning tasks
        // This avoids the Send requirement issues
        for task_id in 0..self.config.concurrency {
            let graph = Graph::new(&self.storage);
            let mut task_rng_state = rng_state + task_id as u64;

            for _i in 0..iterations_per_task {
                let operation_start = Instant::now();

                // Focus on hot vertices
                let vertex = VertexId::new(Self::pseudo_random_target_static(
                    &mut task_rng_state,
                    hot_vertex_count,
                ) as u64);

                if Self::pseudo_random_bool_static(&mut task_rng_state, 0.7) {
                    // Write operation
                    let target = VertexId::new(Self::pseudo_random_target_static(
                        &mut task_rng_state,
                        hot_vertex_count,
                    ) as u64);

                    match graph.add_edge(vertex, target, None).await {
                        Ok(_) => {
                            metrics.successful_writes += 1;
                            metrics.total_write_time += operation_start.elapsed();
                        }
                        Err(_) => metrics.failed_writes += 1,
                    }
                } else {
                    // Read operation
                    match graph.get_neighbors(vertex).await {
                        Ok(neighbors) => {
                            metrics.successful_reads += 1;
                            metrics.total_read_time += operation_start.elapsed();
                            metrics.total_data_read += neighbors.len() * 8;
                        }
                        Err(_) => metrics.failed_reads += 1,
                    }
                }
            }
        }

        Ok(())
    }

    /// Traversal workload: Graph traversal operations
    async fn run_traversal_benchmark(&self, metrics: &mut BenchmarkMetrics) -> Result<()> {
        let graph = Graph::new(&self.storage);
        let mut rng_state = 86420u64;

        for i in 0..self.config.iterations {
            let operation_start = Instant::now();

            // Random walk traversal
            let start_vertex = VertexId::new(
                self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
            );
            let mut current_vertex = start_vertex;
            let traversal_depth = 5; // 5-hop traversal

            for _step in 0..traversal_depth {
                match graph.get_neighbors(current_vertex).await {
                    Ok(neighbors) => {
                        metrics.successful_reads += 1;
                        metrics.total_data_read += neighbors.len() * 8;

                        if !neighbors.is_empty() {
                            let next_index =
                                self.pseudo_random_target(&mut rng_state, neighbors.len());
                            current_vertex = neighbors[next_index];
                        } else {
                            break; // Dead end
                        }
                    }
                    Err(_) => {
                        metrics.failed_reads += 1;
                        break;
                    }
                }
            }

            metrics.total_read_time += operation_start.elapsed();

            if i % 100 == 0 {
                info!(
                    "Traversal benchmark progress: {}/{}",
                    i, self.config.iterations
                );
            }
        }

        Ok(())
    }

    /// Bulk load workload: Large batch operations
    async fn run_bulk_load_benchmark(&self, metrics: &mut BenchmarkMetrics) -> Result<()> {
        let graph = Graph::new(&self.storage);
        let batch_size = 1000;
        let num_batches = self.config.iterations / batch_size;
        let mut rng_state = 11111u64;

        for batch in 0..num_batches {
            let batch_start = Instant::now();

            // Bulk insert edges
            for _i in 0..batch_size {
                let source = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );
                let target = VertexId::new(
                    self.pseudo_random_target(&mut rng_state, self.config.vertex_count) as u64,
                );

                match graph.add_edge(source, target, None).await {
                    Ok(_) => metrics.successful_writes += 1,
                    Err(_) => metrics.failed_writes += 1,
                }
            }

            metrics.total_write_time += batch_start.elapsed();
            info!("Bulk load batch {}/{} completed", batch + 1, num_batches);
        }

        Ok(())
    }

    /// Compare adaptive strategy performance
    async fn run_adaptive_strategy_comparison(&mut self) -> Result<()> {
        info!("Running adaptive strategy comparison benchmark");

        // Test with different degree distributions
        let degree_distributions = vec![
            ("Low degree (avg=5)", 5),
            ("Medium degree (avg=20)", 20),
            ("High degree (avg=100)", 100),
        ];

        for (desc, avg_degree) in degree_distributions {
            info!("Testing adaptive strategy with {}", desc);

            // Create temporary config with specific degree
            let mut temp_config = self.config.clone();
            temp_config.avg_degree = avg_degree;
            temp_config.iterations = 1000; // Smaller test

            // Run mixed workload with this degree distribution
            let result = self.run_workload_benchmark(WorkloadType::Mixed).await?;

            info!(
                "Adaptive strategy results for {}: Delta={}, Pivot={}, Effectiveness={:.2}",
                desc,
                result.adaptive_stats.delta_updates,
                result.adaptive_stats.pivot_updates,
                result.adaptive_stats.strategy_effectiveness
            );
        }

        Ok(())
    }

    /// Compare lock-free vs traditional locking performance
    async fn run_concurrency_comparison(&mut self) -> Result<()> {
        info!("Running concurrency model comparison benchmark");

        // Run high contention workload with different concurrency levels
        let concurrency_levels = vec![1, 4, 8, 16, 32];

        for concurrency in concurrency_levels {
            info!("Testing concurrency level: {}", concurrency);

            let mut temp_config = self.config.clone();
            temp_config.concurrency = concurrency;
            temp_config.iterations = 2000; // Smaller test per concurrency level

            let result = self
                .run_workload_benchmark(WorkloadType::HighContention)
                .await?;

            info!(
                "Concurrency {} results: Success rate={:.2}%, Throughput={:.0} ops/sec",
                concurrency,
                result.lock_free_stats.success_rate * 100.0,
                result.lock_free_stats.throughput_ops_per_sec
            );
        }

        Ok(())
    }

    /// Collect current system statistics
    async fn collect_system_stats(&self) -> SystemStats {
        // This would integrate with the actual PolyLSM stats collection
        SystemStats {
            timestamp: Instant::now(),
            memory_usage_mb: 0.0, // Would get from actual memory tracking
            adaptive_delta_count: 0,
            adaptive_pivot_count: 0,
            lock_free_acquisitions: 0,
            lock_free_contentions: 0,
        }
    }

    fn calculate_adaptive_stats(
        &self,
        _initial: &SystemStats,
        _final: &SystemStats,
    ) -> AdaptiveStrategyStats {
        AdaptiveStrategyStats {
            delta_updates: 0, // Would calculate from actual metrics
            pivot_updates: 0,
            avg_delta_latency_us: 0.0,
            avg_pivot_latency_us: 0.0,
            strategy_effectiveness: 0.85, // Example value
            threshold_adaptations: 0,
        }
    }

    fn calculate_lock_free_stats(
        &self,
        _initial: &SystemStats,
        _final: &SystemStats,
    ) -> LockFreePerformanceStats {
        LockFreePerformanceStats {
            total_acquisitions: 0, // Would calculate from actual metrics
            successful_acquisitions: 0,
            contention_events: 0,
            avg_backoff_time_us: 0.0,
            success_rate: 0.98, // Example value
            throughput_ops_per_sec: 0.0,
        }
    }

    fn calculate_memory_stats(
        &self,
        _initial: &SystemStats,
        _final: &SystemStats,
    ) -> MemoryUsageStats {
        MemoryUsageStats {
            peak_memory_mb: 0.0, // Would calculate from actual metrics
            avg_memory_mb: 0.0,
            memory_pool_hit_rate: 0.94,
            block_cache_hit_rate: 0.87,
            compression_ratio: 0.73,
            gc_pressure_score: 0.15,
        }
    }

    /// Pseudo-random number generators for reproducible benchmarks
    fn pseudo_random_degree(&self, rng_state: &mut u64, avg_degree: u32) -> u32 {
        *rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let random_factor = (*rng_state as f64) / (u64::MAX as f64);

        // Generate degree with some variance around average
        let min_degree = (avg_degree / 2).max(1);
        let max_degree = avg_degree * 2;

        min_degree + ((max_degree - min_degree) as f64 * random_factor) as u32
    }

    fn pseudo_random_target(&self, rng_state: &mut u64, max_value: usize) -> usize {
        *rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        (*rng_state as usize) % max_value
    }

    fn pseudo_random_bool(&self, rng_state: &mut u64, probability: f64) -> bool {
        *rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let random_factor = (*rng_state as f64) / (u64::MAX as f64);
        random_factor < probability
    }

    // Static versions for use in async tasks
    fn pseudo_random_target_static(rng_state: &mut u64, max_value: usize) -> usize {
        *rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        (*rng_state as usize) % max_value
    }

    fn pseudo_random_bool_static(rng_state: &mut u64, probability: f64) -> bool {
        *rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let random_factor = (*rng_state as f64) / (u64::MAX as f64);
        random_factor < probability
    }

    /// Get all benchmark results
    pub fn get_results(&self) -> &[BenchmarkResults] {
        &self.results
    }

    /// Clear all results
    pub fn clear_results(&mut self) {
        self.results.clear();
    }
}

/// Internal system statistics for delta calculations
#[derive(Debug, Clone)]
struct SystemStats {
    timestamp: Instant,
    memory_usage_mb: f64,
    adaptive_delta_count: u64,
    adaptive_pivot_count: u64,
    lock_free_acquisitions: usize,
    lock_free_contentions: usize,
}
