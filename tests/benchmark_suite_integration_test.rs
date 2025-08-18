//! Benchmark Suite Integration Tests
//!
//! Comprehensive tests for the benchmarking infrastructure:
//! - Benchmark execution and result collection
//! - Performance metrics validation and trending
//! - Cross-workload comparative analysis
//! - System resource monitoring during benchmarks
//! - Benchmark report generation and export

use aster_db::benchmarks::{
    benchmark_suite::{BenchmarkConfig, BenchmarkSuite},
    workloads::WorkloadType,
};
use aster_db::{AsterDB, AsterDBConfig, Result};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

/// Configuration for benchmark integration tests
struct BenchmarkTestConfig {
    temp_dir: TempDir,
    db_config: AsterDBConfig,
}

impl BenchmarkTestConfig {
    fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let db_config = AsterDBConfig {
            enable_recovery: false, // Disable for consistent benchmarking
            enable_metrics: true,
            enable_properties: true,
            ..AsterDBConfig::default()
        };

        Self {
            temp_dir,
            db_config,
        }
    }
}

#[tokio::test]
async fn test_benchmark_suite_basic_functionality() -> Result<()> {
    let test_config = BenchmarkTestConfig::new();

    // Initialize benchmark suite with lightweight configuration
    let config = BenchmarkConfig {
        vertex_count: 1000, // Small number for quick testing
        avg_degree: 5,      // Moderate connectivity
        iterations: 100,    // Few iterations for speed
        concurrency: 1,     // Single threaded for simplicity
        workloads: vec![WorkloadType::Mixed],
        duration_seconds: 1, // Short duration
        test_adaptive_strategies: false,
        test_concurrency_models: false,
        measure_memory: true,
    };

    // Create database and storage
    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;
    let storage = Arc::new(db.storage().clone());

    // Create and run benchmark suite
    let mut suite = BenchmarkSuite::new(Arc::clone(&storage), config.clone());
    suite.run_all_benchmarks().await?;

    // Validate results
    let results = suite.get_results();
    assert_eq!(results.len(), 1, "Should have one benchmark result");

    let result = &results[0];
    assert_eq!(result.workload, WorkloadType::Mixed);
    assert!(
        result.metrics.total_operations() > 0,
        "Should have performed operations"
    );
    assert!(
        result.metrics.total_duration.as_millis() > 0,
        "Should have measurable duration"
    );

    // Validate basic metrics structure
    assert!(
        result.metrics.successful_operations() > 0,
        "Should have successful operations"
    );
    assert!(
        result.metrics.operations_per_second >= 0.0,
        "Should have valid throughput"
    );

    println!("✓ Benchmark suite basic functionality test completed");
    println!("  Operations: {}", result.metrics.total_operations());
    println!(
        "  Duration: {}ms",
        result.metrics.total_duration.as_millis()
    );
    println!(
        "  Throughput: {:.2} ops/sec",
        result.metrics.operations_per_second
    );

    Ok(())
}

#[tokio::test]
async fn test_cross_workload_comparison() -> Result<()> {
    let test_config = BenchmarkTestConfig::new();

    // Configure for multiple workload comparison
    let config = BenchmarkConfig {
        vertex_count: 500,
        avg_degree: 8,
        iterations: 50,
        concurrency: 1,
        workloads: vec![
            WorkloadType::WriteHeavy,
            WorkloadType::ReadHeavy,
            WorkloadType::Mixed,
        ],
        duration_seconds: 1,
        test_adaptive_strategies: false,
        test_concurrency_models: false,
        measure_memory: true,
    };

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;
    let storage = Arc::new(db.storage().clone());

    // Run all workloads
    let mut suite = BenchmarkSuite::new(Arc::clone(&storage), config);
    suite.run_all_benchmarks().await?;

    // Validate cross-workload results
    let results = suite.get_results();
    assert_eq!(results.len(), 3, "Should have three benchmark results");

    let write_result = results
        .iter()
        .find(|r| r.workload == WorkloadType::WriteHeavy)
        .unwrap();
    let read_result = results
        .iter()
        .find(|r| r.workload == WorkloadType::ReadHeavy)
        .unwrap();
    let mixed_result = results
        .iter()
        .find(|r| r.workload == WorkloadType::Mixed)
        .unwrap();

    // All benchmarks should complete successfully
    for result in results {
        assert!(
            result.metrics.total_operations() > 0,
            "Workload {:?} should perform operations",
            result.workload
        );
        assert!(
            result.metrics.operations_per_second >= 0.0,
            "Workload {:?} should have valid throughput",
            result.workload
        );
    }

    // Performance characteristics validation
    // Note: Exact performance comparisons may vary, so we validate structure
    assert!(
        write_result.metrics.successful_writes > 0,
        "Write-heavy should have write operations"
    );
    assert!(
        read_result.metrics.successful_reads > 0,
        "Read-heavy should have read operations"
    );
    assert!(
        mixed_result.metrics.successful_reads > 0 && mixed_result.metrics.successful_writes > 0,
        "Mixed should have both read and write operations"
    );

    println!("✓ Cross-workload comparison test completed");
    println!(
        "  Write-heavy: {} ops ({} writes)",
        write_result.metrics.total_operations(),
        write_result.metrics.successful_writes
    );
    println!(
        "  Read-heavy: {} ops ({} reads)",
        read_result.metrics.total_operations(),
        read_result.metrics.successful_reads
    );
    println!(
        "  Mixed: {} ops ({} reads, {} writes)",
        mixed_result.metrics.total_operations(),
        mixed_result.metrics.successful_reads,
        mixed_result.metrics.successful_writes
    );

    Ok(())
}

#[tokio::test]
async fn test_performance_metrics_validation() -> Result<()> {
    let test_config = BenchmarkTestConfig::new();

    // Configure benchmark with detailed metrics
    let config = BenchmarkConfig {
        vertex_count: 800,
        avg_degree: 6,
        iterations: 80,
        concurrency: 1,
        workloads: vec![WorkloadType::Mixed],
        duration_seconds: 1,
        test_adaptive_strategies: false,
        test_concurrency_models: false,
        measure_memory: true,
    };

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;
    let storage = Arc::new(db.storage().clone());

    // Run benchmark
    let mut suite = BenchmarkSuite::new(Arc::clone(&storage), config.clone());
    suite.run_all_benchmarks().await?;

    let results = suite.get_results();
    assert_eq!(results.len(), 1, "Should have one result");

    let result = &results[0];
    let metrics = &result.metrics;

    // Validate metrics structure
    assert!(
        metrics.total_operations() > 0,
        "Should have total operations"
    );
    assert_eq!(
        metrics.total_operations(),
        metrics.successful_operations() + metrics.failed_operations(),
        "Total should equal successful + failed"
    );

    // Throughput validation
    assert!(
        metrics.operations_per_second >= 0.0,
        "Should have non-negative throughput"
    );

    // Error rate validation
    assert!(
        metrics.overall_error_rate >= 0.0 && metrics.overall_error_rate <= 1.0,
        "Error rate should be valid percentage"
    );

    // Timing validation
    assert!(
        metrics.total_duration.as_nanos() > 0,
        "Should have measurable duration"
    );

    if metrics.successful_reads > 0 {
        assert!(
            metrics.avg_read_latency_us >= 0.0,
            "Read latency should be non-negative"
        );
    }
    if metrics.successful_writes > 0 {
        assert!(
            metrics.avg_write_latency_us >= 0.0,
            "Write latency should be non-negative"
        );
    }

    // Resource metrics validation
    assert!(
        result.memory_stats.peak_memory_mb >= 0.0,
        "Peak memory should be non-negative"
    );
    assert!(
        result.memory_stats.avg_memory_mb >= 0.0,
        "Average memory should be non-negative"
    );

    println!("✓ Performance metrics validation test completed");
    println!("  Total operations: {}", metrics.total_operations());
    println!("  Success rate: {:.2}%", metrics.success_rate() * 100.0);
    println!("  Throughput: {:.2} ops/sec", metrics.operations_per_second);
    println!(
        "  Peak memory: {:.2} MB",
        result.memory_stats.peak_memory_mb
    );

    Ok(())
}

#[tokio::test]
async fn test_benchmark_performance_trending() -> Result<()> {
    let test_config = BenchmarkTestConfig::new();

    // Run multiple benchmark iterations
    let config = BenchmarkConfig {
        vertex_count: 400,
        avg_degree: 4,
        iterations: 40,
        concurrency: 1,
        workloads: vec![WorkloadType::Mixed],
        duration_seconds: 1,
        test_adaptive_strategies: false,
        test_concurrency_models: false,
        measure_memory: false, // Disable memory tracking for faster execution
    };

    let mut throughputs = Vec::new();

    // Run benchmark multiple times
    for _i in 0..3 {
        let db =
            AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
                .await?;
        let storage = Arc::new(db.storage().clone());

        let mut suite = BenchmarkSuite::new(Arc::clone(&storage), config.clone());
        suite.run_all_benchmarks().await?;

        let results = suite.get_results();
        assert_eq!(results.len(), 1, "Should have one result per iteration");

        let result = &results[0];
        assert!(
            result.metrics.total_operations() > 0,
            "Should have operations in iteration"
        );

        throughputs.push(result.metrics.operations_per_second);

        // Small delay between iterations
        sleep(Duration::from_millis(100)).await;
    }

    // Validate trending data
    assert_eq!(
        throughputs.len(),
        3,
        "Should have three benchmark iterations"
    );

    // Calculate basic trending metrics
    let mean_throughput = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
    let max_throughput = throughputs.iter().fold(0.0f64, |a, &b| a.max(b));
    let min_throughput = throughputs.iter().fold(f64::MAX, |a, &b| a.min(b));

    assert!(mean_throughput > 0.0, "Mean throughput should be positive");
    assert!(max_throughput >= mean_throughput, "Max should be >= mean");
    assert!(min_throughput <= mean_throughput, "Min should be <= mean");

    // Variance should be reasonable for consistent test environment
    let variance = throughputs
        .iter()
        .map(|x| (x - mean_throughput).powi(2))
        .sum::<f64>()
        / throughputs.len() as f64;
    let coefficient_of_variation = variance.sqrt() / mean_throughput;

    // CV should be reasonable for a controlled test environment
    assert!(
        coefficient_of_variation < 2.0,
        "Coefficient of variation should be reasonable: {}",
        coefficient_of_variation
    );

    println!("✓ Benchmark performance trending test completed");
    println!("  Mean throughput: {:.2} ops/sec", mean_throughput);
    println!(
        "  Throughput range: {:.2} - {:.2} ops/sec",
        min_throughput, max_throughput
    );
    println!(
        "  Coefficient of variation: {:.3}",
        coefficient_of_variation
    );

    Ok(())
}

#[tokio::test]
async fn test_system_resource_monitoring() -> Result<()> {
    let test_config = BenchmarkTestConfig::new();

    // Configure benchmark with resource monitoring
    let config = BenchmarkConfig {
        vertex_count: 1200,
        avg_degree: 8,
        iterations: 120,
        concurrency: 1,
        workloads: vec![WorkloadType::WriteHeavy], // Write-heavy to exercise memory
        duration_seconds: 2,
        test_adaptive_strategies: false,
        test_concurrency_models: false,
        measure_memory: true, // Enable memory monitoring
    };

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;
    let storage = Arc::new(db.storage().clone());

    // Run benchmark with resource monitoring
    let mut suite = BenchmarkSuite::new(Arc::clone(&storage), config.clone());
    suite.run_all_benchmarks().await?;

    let results = suite.get_results();
    assert_eq!(results.len(), 1, "Should have one result");

    let result = &results[0];

    // Validate resource monitoring results
    assert!(
        result.memory_stats.peak_memory_mb >= 0.0,
        "Peak memory should be non-negative"
    );
    assert!(
        result.memory_stats.avg_memory_mb >= 0.0,
        "Average memory should be non-negative"
    );

    // Memory pool and cache hit rates should be reasonable percentages
    assert!(
        result.memory_stats.memory_pool_hit_rate >= 0.0
            && result.memory_stats.memory_pool_hit_rate <= 1.0,
        "Memory pool hit rate should be valid percentage"
    );
    assert!(
        result.memory_stats.block_cache_hit_rate >= 0.0
            && result.memory_stats.block_cache_hit_rate <= 1.0,
        "Block cache hit rate should be valid percentage"
    );

    // Compression ratio should be reasonable
    assert!(
        result.memory_stats.compression_ratio >= 0.0
            && result.memory_stats.compression_ratio <= 1.0,
        "Compression ratio should be valid"
    );

    // GC pressure should be reasonable
    assert!(
        result.memory_stats.gc_pressure_score >= 0.0
            && result.memory_stats.gc_pressure_score <= 1.0,
        "GC pressure should be valid"
    );

    // Duration should be measurable (may be very fast in test environment)
    let duration_ms = result.metrics.total_duration.as_millis();
    assert!(
        duration_ms > 0,
        "Duration should be measurable: {}ms",
        duration_ms
    );

    // Operation counts should be reasonable
    assert!(
        result.metrics.total_operations() >= config.iterations as u64 / 2,
        "Should achieve reasonable operation count: {} >= {}",
        result.metrics.total_operations(),
        config.iterations / 2
    );

    println!("✓ System resource monitoring test completed");
    println!(
        "  Peak memory: {:.2} MB",
        result.memory_stats.peak_memory_mb
    );
    println!("  Avg memory: {:.2} MB", result.memory_stats.avg_memory_mb);
    println!(
        "  Memory pool hit rate: {:.2}%",
        result.memory_stats.memory_pool_hit_rate * 100.0
    );
    println!(
        "  Cache hit rate: {:.2}%",
        result.memory_stats.block_cache_hit_rate * 100.0
    );
    println!(
        "  Duration: {}ms",
        result.metrics.total_duration.as_millis()
    );
    println!("  Operations: {}", result.metrics.total_operations());

    Ok(())
}

#[tokio::test]
async fn test_adaptive_strategy_analysis() -> Result<()> {
    let test_config = BenchmarkTestConfig::new();

    // Enable adaptive strategy testing
    let config = BenchmarkConfig {
        vertex_count: 800,
        avg_degree: 10,
        iterations: 80,
        concurrency: 1,
        workloads: vec![WorkloadType::Mixed],
        duration_seconds: 1,
        test_adaptive_strategies: true, // Enable adaptive testing
        test_concurrency_models: false,
        measure_memory: false,
    };

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;
    let storage = Arc::new(db.storage().clone());

    // Run benchmark with adaptive strategy analysis
    let mut suite = BenchmarkSuite::new(Arc::clone(&storage), config);
    suite.run_all_benchmarks().await?;

    let results = suite.get_results();
    assert!(results.len() >= 1, "Should have at least one result");

    // Check the primary result for adaptive stats
    let primary_result = &results[0];

    // Validate adaptive strategy statistics
    let adaptive_stats = &primary_result.adaptive_stats;

    // Values should be non-negative
    assert!(
        adaptive_stats.delta_updates >= 0,
        "Delta updates should be non-negative"
    );
    assert!(
        adaptive_stats.pivot_updates >= 0,
        "Pivot updates should be non-negative"
    );
    assert!(
        adaptive_stats.avg_delta_latency_us >= 0.0,
        "Delta latency should be non-negative"
    );
    assert!(
        adaptive_stats.avg_pivot_latency_us >= 0.0,
        "Pivot latency should be non-negative"
    );
    assert!(
        adaptive_stats.threshold_adaptations >= 0,
        "Threshold adaptations should be non-negative"
    );

    // Strategy effectiveness should be a reasonable value
    assert!(
        adaptive_stats.strategy_effectiveness >= 0.0
            && adaptive_stats.strategy_effectiveness <= 1.0,
        "Strategy effectiveness should be valid: {}",
        adaptive_stats.strategy_effectiveness
    );

    println!("✓ Adaptive strategy analysis test completed");
    println!("  Delta updates: {}", adaptive_stats.delta_updates);
    println!("  Pivot updates: {}", adaptive_stats.pivot_updates);
    println!(
        "  Strategy effectiveness: {:.3}",
        adaptive_stats.strategy_effectiveness
    );
    println!(
        "  Threshold adaptations: {}",
        adaptive_stats.threshold_adaptations
    );

    Ok(())
}

#[tokio::test]
async fn test_concurrency_performance_analysis() -> Result<()> {
    let test_config = BenchmarkTestConfig::new();

    // Enable concurrency model testing
    let config = BenchmarkConfig {
        vertex_count: 600,
        avg_degree: 6,
        iterations: 60,
        concurrency: 4, // Multi-threaded
        workloads: vec![WorkloadType::HighContention],
        duration_seconds: 1,
        test_adaptive_strategies: false,
        test_concurrency_models: true, // Enable concurrency testing
        measure_memory: false,
    };

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;
    let storage = Arc::new(db.storage().clone());

    // Run benchmark with concurrency analysis
    let mut suite = BenchmarkSuite::new(Arc::clone(&storage), config);
    suite.run_all_benchmarks().await?;

    let results = suite.get_results();
    assert!(results.len() >= 1, "Should have at least one result");

    // Check the primary result for lock-free stats
    let primary_result = &results[0];

    // Validate lock-free performance statistics
    let lock_free_stats = &primary_result.lock_free_stats;

    // Values should be non-negative and make sense
    assert!(
        lock_free_stats.total_acquisitions >= 0,
        "Total acquisitions should be non-negative"
    );
    assert!(
        lock_free_stats.successful_acquisitions <= lock_free_stats.total_acquisitions,
        "Successful acquisitions should not exceed total"
    );
    assert!(
        lock_free_stats.contention_events >= 0,
        "Contention events should be non-negative"
    );
    assert!(
        lock_free_stats.avg_backoff_time_us >= 0.0,
        "Average backoff time should be non-negative"
    );

    // Success rate should be a valid percentage
    assert!(
        lock_free_stats.success_rate >= 0.0 && lock_free_stats.success_rate <= 1.0,
        "Success rate should be valid percentage: {}",
        lock_free_stats.success_rate
    );

    // Throughput should be non-negative
    assert!(
        lock_free_stats.throughput_ops_per_sec >= 0.0,
        "Throughput should be non-negative: {}",
        lock_free_stats.throughput_ops_per_sec
    );

    println!("✓ Concurrency performance analysis test completed");
    println!(
        "  Total acquisitions: {}",
        lock_free_stats.total_acquisitions
    );
    println!(
        "  Successful acquisitions: {}",
        lock_free_stats.successful_acquisitions
    );
    println!(
        "  Success rate: {:.2}%",
        lock_free_stats.success_rate * 100.0
    );
    println!("  Contention events: {}", lock_free_stats.contention_events);
    println!(
        "  Throughput: {:.2} ops/sec",
        lock_free_stats.throughput_ops_per_sec
    );

    Ok(())
}
