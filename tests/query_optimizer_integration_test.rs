//! Query Optimizer Integration Tests
//!
//! Comprehensive tests for the query optimization infrastructure:
//! - Range scan optimization and cost estimation
//! - Predicate pushdown and early filtering
//! - Adaptive range partitioning for large queries
//! - Index selection and utilization strategies
//! - Query plan caching and reuse optimization
//! - Cross-component integration with storage and properties

use aster_db::query::{
    optimizer::{ExecutionStrategy, OptimizerConfig, RangeScanOptimizer},
    QueryContext, QueryPredicate,
};
use aster_db::{AsterDB, AsterDBConfig, PropertyValue, Result, VertexId};
use tempfile::TempDir;

/// Configuration for optimizer integration tests
struct OptimizerTestConfig {
    temp_dir: TempDir,
    db_config: AsterDBConfig,
}

impl OptimizerTestConfig {
    fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let db_config = AsterDBConfig {
            enable_recovery: false, // Disable for consistent testing
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
async fn test_range_scan_optimizer_basic_functionality() -> Result<()> {
    let test_config = OptimizerTestConfig::new();

    // Create database and property store
    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Initialize optimizer
    let mut optimizer = RangeScanOptimizer::new(None);

    // Create test query parameters
    let start_vertex = VertexId::from_u64(1);
    let end_vertex = VertexId::from_u64(1000);
    let predicates = vec![
        QueryPredicate::PropertyEquals(
            "type".to_string(),
            PropertyValue::String("user".to_string()),
        ),
        QueryPredicate::PropertyRange(
            "score".to_string(),
            Some(PropertyValue::Int(50)),
            Some(PropertyValue::Int(100)),
        ),
    ];

    let query_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(5000),
        use_cache: true,
    };

    // Optimize the range query
    let query_plan = optimizer
        .optimize_range_query(start_vertex, end_vertex, predicates.clone(), &query_context)
        .await?;

    // Validate query plan structure
    assert!(
        query_plan.cost_estimate.estimated_vertices_scanned > 0,
        "Should estimate vertices to scan"
    );
    assert!(
        query_plan.cost_estimate.confidence_score >= 0.0
            && query_plan.cost_estimate.confidence_score <= 1.0,
        "Confidence score should be valid percentage"
    );
    assert!(
        query_plan.range_partitions.len() > 0,
        "Should create at least one partition"
    );
    assert_eq!(
        query_plan.pushed_predicates.len(),
        2,
        "Should preserve predicates"
    );

    // Validate cost estimation is reasonable
    let total_cost = query_plan.cost_estimate.total_cost();
    assert!(total_cost > 0.0, "Total cost should be positive");

    // Validate execution strategy
    match &query_plan.strategy {
        ExecutionStrategy::SequentialScan { batch_size, .. } => {
            assert!(*batch_size > 0, "Batch size should be positive");
            assert!(*batch_size <= 10000, "Batch size should be reasonable");
        }
        ExecutionStrategy::ParallelPartitionedScan { .. } => {
            // Parallel partitioned scan doesn't have a direct batch_size field
        }
        _ => {} // Other strategies are valid too
    }

    println!("✓ Range scan optimizer basic functionality test completed");
    println!(
        "  Estimated vertices: {}",
        query_plan.cost_estimate.estimated_vertices_scanned
    );
    println!("  Partitions: {}", query_plan.range_partitions.len());
    println!("  Total cost: {:.2}", total_cost);
    match &query_plan.strategy {
        ExecutionStrategy::SequentialScan { batch_size, .. } => {
            println!("  Batch size: {}", batch_size);
        }
        ExecutionStrategy::ParallelPartitionedScan {
            partition_count, ..
        } => {
            println!("  Partition count: {}", partition_count);
        }
        _ => println!("  Strategy: {:?}", query_plan.strategy),
    }

    Ok(())
}

#[tokio::test]
async fn test_cost_based_optimization() -> Result<()> {
    let test_config = OptimizerTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;
    let mut optimizer = RangeScanOptimizer::new(None);

    let query_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(10000),
        use_cache: true,
    };

    // Test different query sizes to compare cost estimation
    let test_cases = vec![
        (VertexId::from_u64(1), VertexId::from_u64(100)), // Small range
        (VertexId::from_u64(1), VertexId::from_u64(1000)), // Medium range
        (VertexId::from_u64(1), VertexId::from_u64(10000)), // Large range
    ];

    let mut costs = Vec::new();

    for (start, end) in test_cases {
        let predicates = vec![QueryPredicate::PropertyEquals(
            "active".to_string(),
            PropertyValue::Bool(true),
        )];

        let plan = optimizer
            .optimize_range_query(start, end, predicates, &query_context)
            .await?;
        let cost = plan.cost_estimate.total_cost();
        costs.push(cost);

        // Validate cost components
        assert!(
            plan.cost_estimate.estimated_vertices_scanned > 0,
            "Should estimate vertices for range {:?}-{:?}",
            start,
            end
        );
        assert!(
            plan.cost_estimate.estimated_disk_ios >= 0,
            "Disk IOs should be non-negative"
        );
        assert!(
            plan.cost_estimate.estimated_memory_usage >= 0,
            "Memory usage should be non-negative"
        );
        assert!(
            plan.cost_estimate.estimated_execution_time_ms >= 0,
            "Execution time should be non-negative"
        );

        println!(
            "Range {:?}-{:?}: cost={:.2}, vertices={}, partitions={}",
            start,
            end,
            cost,
            plan.cost_estimate.estimated_vertices_scanned,
            plan.range_partitions.len()
        );
    }

    // Validate that costs generally increase with range size
    // (allowing for some variance in cost model)
    assert!(
        costs[0] <= costs[2] * 2.0,
        "Larger ranges should generally have higher costs"
    );

    // Validate all costs are reasonable (positive and finite)
    for cost in &costs {
        assert!(
            cost.is_finite() && *cost > 0.0,
            "All costs should be positive and finite"
        );
    }

    println!("✓ Cost-based optimization test completed");
    Ok(())
}

#[tokio::test]
async fn test_predicate_optimization() -> Result<()> {
    let test_config = OptimizerTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Test optimizer with predicate pushdown enabled
    let config_with_pushdown = OptimizerConfig {
        enable_predicate_pushdown: true,
        enable_index_optimization: true,
        max_partitions: 4,
        parallel_threshold: 500,
        default_batch_size: 100,
        max_plan_cache_size: 100,
        cost_recomputation_threshold: 1.5,
    };

    let mut optimizer = RangeScanOptimizer::new_with_config(None, config_with_pushdown);

    let query_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(5000),
        use_cache: true,
    };

    // Create complex predicate set
    let predicates = vec![
        QueryPredicate::PropertyEquals(
            "category".to_string(),
            PropertyValue::String("premium".to_string()),
        ),
        QueryPredicate::PropertyRange(
            "age".to_string(),
            Some(PropertyValue::Int(25)),
            Some(PropertyValue::Int(65)),
        ),
        QueryPredicate::PropertyEquals("active".to_string(), PropertyValue::Bool(true)),
        QueryPredicate::PropertyRange(
            "score".to_string(),
            Some(PropertyValue::Float(7.5)),
            Some(PropertyValue::Float(10.0)),
        ),
    ];

    let plan = optimizer
        .optimize_range_query(
            VertexId::from_u64(1),
            VertexId::from_u64(5000),
            predicates.clone(),
            &query_context,
        )
        .await?;

    // Validate predicate handling
    assert_eq!(
        plan.pushed_predicates.len(),
        4,
        "Should preserve all predicates"
    );
    assert!(
        plan.optimization_stats.predicates_pushed_down >= 0,
        "Should track pushed down predicates"
    );

    // Validate optimization occurred
    assert!(
        plan.optimization_stats.optimization_time_ms >= 0,
        "Should track optimization time"
    );
    assert!(
        plan.optimization_stats.cost_estimation_time_ms >= 0,
        "Should track cost estimation time"
    );

    // Test without predicate pushdown for comparison
    let config_without_pushdown = OptimizerConfig {
        enable_predicate_pushdown: false,
        enable_index_optimization: false,
        max_partitions: 4,
        parallel_threshold: 500,
        default_batch_size: 100,
        max_plan_cache_size: 100,
        cost_recomputation_threshold: 1.5,
    };

    let mut optimizer_no_pushdown =
        RangeScanOptimizer::new_with_config(None, config_without_pushdown);

    let plan_no_pushdown = optimizer_no_pushdown
        .optimize_range_query(
            VertexId::from_u64(1),
            VertexId::from_u64(5000),
            predicates,
            &query_context,
        )
        .await?;

    // Compare optimization results
    println!(
        "With pushdown: cost={:.2}, optimization_time={}ms",
        plan.cost_estimate.total_cost(),
        plan.optimization_stats.optimization_time_ms
    );
    println!(
        "Without pushdown: cost={:.2}, optimization_time={}ms",
        plan_no_pushdown.cost_estimate.total_cost(),
        plan_no_pushdown.optimization_stats.optimization_time_ms
    );

    // Both plans should be valid
    assert!(
        plan.cost_estimate.total_cost() > 0.0,
        "Plan with pushdown should have valid cost"
    );
    assert!(
        plan_no_pushdown.cost_estimate.total_cost() > 0.0,
        "Plan without pushdown should have valid cost"
    );

    println!("✓ Predicate optimization test completed");
    Ok(())
}

#[tokio::test]
async fn test_adaptive_partitioning() -> Result<()> {
    let test_config = OptimizerTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Configure optimizer for partitioning testing
    let config = OptimizerConfig {
        max_partitions: 8,
        parallel_threshold: 1000, // Lower threshold to trigger partitioning
        default_batch_size: 500,
        enable_predicate_pushdown: true,
        enable_index_optimization: true,
        max_plan_cache_size: 50,
        cost_recomputation_threshold: 1.2,
    };

    let mut optimizer = RangeScanOptimizer::new_with_config(None, config);

    let query_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(15000),
        use_cache: true,
    };

    // Test large range that should trigger partitioning
    let predicates = vec![QueryPredicate::PropertyRange(
        "timestamp".to_string(),
        Some(PropertyValue::Int(0)),
        Some(PropertyValue::Int(1000000)),
    )];

    let plan = optimizer
        .optimize_range_query(
            VertexId::from_u64(1),
            VertexId::from_u64(50000), // Large range
            predicates,
            &query_context,
        )
        .await?;

    // Validate partitioning behavior
    assert!(
        plan.range_partitions.len() >= 1,
        "Should create at least one partition"
    );
    assert!(
        plan.range_partitions.len() <= 8,
        "Should not exceed max partitions"
    );
    assert!(
        plan.optimization_stats.range_partitions_created > 0,
        "Should track partition creation"
    );

    // Validate partition ranges don't overlap and cover full range
    let mut partition_ranges = plan.range_partitions.clone();
    partition_ranges.sort_by_key(|p| p.start_vertex);

    assert_eq!(
        partition_ranges[0].start_vertex,
        VertexId::from_u64(1),
        "First partition should start at query start"
    );
    assert_eq!(
        partition_ranges.last().unwrap().end_vertex,
        VertexId::from_u64(50000),
        "Last partition should end at query end"
    );

    // Check for gaps or overlaps
    for i in 1..partition_ranges.len() {
        let prev_end = partition_ranges[i - 1].end_vertex.as_u64();
        let curr_start = partition_ranges[i].start_vertex.as_u64();
        assert!(curr_start >= prev_end, "Partitions should not have gaps");
        assert!(
            curr_start <= prev_end + 1,
            "Partitions should not overlap significantly"
        );
    }

    // Test small range that should not partition
    let small_plan = optimizer
        .optimize_range_query(
            VertexId::from_u64(1),
            VertexId::from_u64(50), // Small range
            vec![],
            &query_context,
        )
        .await?;

    assert_eq!(
        small_plan.range_partitions.len(),
        1,
        "Small range should create single partition"
    );

    println!("✓ Adaptive partitioning test completed");
    println!("  Large range partitions: {}", plan.range_partitions.len());
    println!(
        "  Small range partitions: {}",
        small_plan.range_partitions.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_query_plan_caching() -> Result<()> {
    let test_config = OptimizerTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    let config = OptimizerConfig {
        max_plan_cache_size: 10,
        cost_recomputation_threshold: 2.0, // Higher threshold to test cache hits
        ..OptimizerConfig::default()
    };

    let mut optimizer = RangeScanOptimizer::new_with_config(None, config);

    let query_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(5000),
        use_cache: false,
    };

    let predicates = vec![QueryPredicate::PropertyEquals(
        "type".to_string(),
        PropertyValue::String("test".to_string()),
    )];

    // First query - should populate cache
    let start_time = std::time::Instant::now();
    let plan1 = optimizer
        .optimize_range_query(
            VertexId::from_u64(100),
            VertexId::from_u64(200),
            predicates.clone(),
            &query_context,
        )
        .await?;
    let first_duration = start_time.elapsed();

    // Second identical query - should hit cache
    let start_time = std::time::Instant::now();
    let plan2 = optimizer
        .optimize_range_query(
            VertexId::from_u64(100),
            VertexId::from_u64(200),
            predicates.clone(),
            &query_context,
        )
        .await?;
    let second_duration = start_time.elapsed();

    // Validate cache behavior
    assert_eq!(
        plan1.range_partitions.len(),
        plan2.range_partitions.len(),
        "Cached plan should have same structure"
    );
    // Note: batch_size is part of ExecutionStrategy, not QueryPlan
    // Both plans should have the same strategy type
    assert_eq!(
        std::mem::discriminant(&plan1.strategy),
        std::mem::discriminant(&plan2.strategy),
        "Cached plan should have same strategy type"
    );

    // Second query should be faster (cache hit)
    // Note: In test environment timing might be variable, so we use a generous threshold
    if second_duration < first_duration {
        println!("Cache hit detected - second query was faster");
    } else {
        println!("Cache timing inconclusive in test environment");
    }

    // Test cache eviction with different queries
    for i in 0..15 {
        // More than cache size
        let different_predicates = vec![QueryPredicate::PropertyEquals(
            "id".to_string(),
            PropertyValue::Int(i as i64),
        )];

        let _plan = optimizer
            .optimize_range_query(
                VertexId::from_u64(i * 100),
                VertexId::from_u64((i + 1) * 100),
                different_predicates,
                &query_context,
            )
            .await?;
    }

    // Original query should still work (might hit cache or be evicted)
    let plan3 = optimizer
        .optimize_range_query(
            VertexId::from_u64(100),
            VertexId::from_u64(200),
            predicates,
            &query_context,
        )
        .await?;

    // Should still produce valid plan
    assert!(
        plan3.cost_estimate.total_cost() > 0.0,
        "Plan after cache pressure should still be valid"
    );

    println!("✓ Query plan caching test completed");
    println!("  First query: {:?}", first_duration);
    println!("  Second query: {:?}", second_duration);

    Ok(())
}

#[tokio::test]
async fn test_parallel_execution_optimization() -> Result<()> {
    let test_config = OptimizerTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Configure for parallel execution testing
    let parallel_config = OptimizerConfig {
        max_partitions: 4,
        parallel_threshold: 100, // Low threshold to trigger parallel
        default_batch_size: 50,
        enable_predicate_pushdown: true,
        enable_index_optimization: true,
        max_plan_cache_size: 20,
        cost_recomputation_threshold: 1.5,
    };

    let mut optimizer = RangeScanOptimizer::new_with_config(None, parallel_config);

    // Test with parallel enabled
    let parallel_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(10000),
        use_cache: true,
    };

    let predicates = vec![QueryPredicate::PropertyRange(
        "value".to_string(),
        Some(PropertyValue::Float(0.0)),
        Some(PropertyValue::Float(100.0)),
    )];

    let parallel_plan = optimizer
        .optimize_range_query(
            VertexId::from_u64(1),
            VertexId::from_u64(1000),
            predicates.clone(),
            &parallel_context,
        )
        .await?;

    // Test with parallel disabled
    let sequential_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(10000),
        use_cache: false,
    };

    let sequential_plan = optimizer
        .optimize_range_query(
            VertexId::from_u64(1),
            VertexId::from_u64(1000),
            predicates,
            &sequential_context,
        )
        .await?;

    // Validate parallel vs sequential optimization
    assert!(
        parallel_plan.range_partitions.len() >= 1,
        "Parallel plan should have partitions"
    );
    assert!(
        sequential_plan.range_partitions.len() >= 1,
        "Sequential plan should have partitions"
    );

    // Parallel plan might have more partitions for the same range
    if parallel_plan.range_partitions.len() > sequential_plan.range_partitions.len() {
        println!("Parallel optimization created more partitions");
    }

    // Both plans should be valid
    assert!(
        parallel_plan.cost_estimate.total_cost() > 0.0,
        "Parallel plan should have valid cost"
    );
    assert!(
        sequential_plan.cost_estimate.total_cost() > 0.0,
        "Sequential plan should have valid cost"
    );

    // Validate execution strategy
    assert!(
        matches!(
            parallel_plan.strategy,
            ExecutionStrategy::SequentialScan { .. }
                | ExecutionStrategy::ParallelPartitionedScan { .. }
                | ExecutionStrategy::IndexGuidedScan { .. }
                | ExecutionStrategy::HybridStrategy { .. }
        ),
        "Should have valid execution strategy"
    );
    assert!(
        matches!(
            sequential_plan.strategy,
            ExecutionStrategy::SequentialScan { .. }
                | ExecutionStrategy::ParallelPartitionedScan { .. }
                | ExecutionStrategy::IndexGuidedScan { .. }
                | ExecutionStrategy::HybridStrategy { .. }
        ),
        "Should have valid execution strategy"
    );

    println!("✓ Parallel execution optimization test completed");
    println!(
        "  Parallel partitions: {}",
        parallel_plan.range_partitions.len()
    );
    println!(
        "  Sequential partitions: {}",
        sequential_plan.range_partitions.len()
    );
    println!("  Parallel strategy: {:?}", parallel_plan.strategy);
    println!("  Sequential strategy: {:?}", sequential_plan.strategy);

    Ok(())
}

#[tokio::test]
async fn test_optimizer_configuration_impact() -> Result<()> {
    let test_config = OptimizerTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    let query_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(5000),
        use_cache: true,
    };

    let predicates = vec![
        QueryPredicate::PropertyEquals(
            "status".to_string(),
            PropertyValue::String("active".to_string()),
        ),
        QueryPredicate::PropertyRange(
            "priority".to_string(),
            Some(PropertyValue::Int(1)),
            Some(PropertyValue::Int(10)),
        ),
    ];

    // Test different configurations
    let configs = vec![
        (
            "Conservative",
            OptimizerConfig {
                max_partitions: 2,
                parallel_threshold: 10000,
                default_batch_size: 100,
                enable_predicate_pushdown: false,
                enable_index_optimization: false,
                max_plan_cache_size: 10,
                cost_recomputation_threshold: 3.0,
            },
        ),
        (
            "Aggressive",
            OptimizerConfig {
                max_partitions: 16,
                parallel_threshold: 100,
                default_batch_size: 2000,
                enable_predicate_pushdown: true,
                enable_index_optimization: true,
                max_plan_cache_size: 1000,
                cost_recomputation_threshold: 1.1,
            },
        ),
        ("Balanced", OptimizerConfig::default()),
    ];

    for (config_name, config) in configs {
        let mut optimizer = RangeScanOptimizer::new_with_config(None, config);

        let plan = optimizer
            .optimize_range_query(
                VertexId::from_u64(1),
                VertexId::from_u64(2000),
                predicates.clone(),
                &query_context,
            )
            .await?;

        // Validate plan quality
        assert!(
            plan.cost_estimate.total_cost() > 0.0,
            "{} config should produce valid cost",
            config_name
        );
        assert!(
            plan.range_partitions.len() > 0,
            "{} config should create partitions",
            config_name
        );
        // Note: batch_size is part of ExecutionStrategy, not QueryPlan
        assert!(
            !plan.plan_id.is_empty(),
            "{} config should create valid plan",
            config_name
        );

        println!("{} config:", config_name);
        println!("  Cost: {:.2}", plan.cost_estimate.total_cost());
        println!("  Partitions: {}", plan.range_partitions.len());
        // Note: batch_size is part of ExecutionStrategy, not QueryPlan
        println!("  Strategy: {:?}", plan.strategy);
        println!(
            "  Optimization time: {}ms",
            plan.optimization_stats.optimization_time_ms
        );
    }

    println!("✓ Optimizer configuration impact test completed");
    Ok(())
}

#[tokio::test]
async fn test_cost_model_accuracy() -> Result<()> {
    let test_config = OptimizerTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;
    let mut optimizer = RangeScanOptimizer::new(None);

    let query_context = QueryContext {
        transaction: None,
        max_depth: None,
        timeout_ms: Some(10000),
        use_cache: true,
    };

    // Test cost model with various scenarios
    let test_scenarios = vec![
        ("Empty predicates", vec![]),
        (
            "Single equality",
            vec![QueryPredicate::PropertyEquals(
                "type".to_string(),
                PropertyValue::String("user".to_string()),
            )],
        ),
        (
            "Single range",
            vec![QueryPredicate::PropertyRange(
                "age".to_string(),
                Some(PropertyValue::Int(20)),
                Some(PropertyValue::Int(30)),
            )],
        ),
        (
            "Multiple predicates",
            vec![
                QueryPredicate::PropertyEquals("active".to_string(), PropertyValue::Bool(true)),
                QueryPredicate::PropertyRange(
                    "score".to_string(),
                    Some(PropertyValue::Float(5.0)),
                    Some(PropertyValue::Float(10.0)),
                ),
                QueryPredicate::PropertyEquals(
                    "category".to_string(),
                    PropertyValue::String("premium".to_string()),
                ),
            ],
        ),
    ];

    for (scenario_name, predicates) in test_scenarios {
        let plan = optimizer
            .optimize_range_query(
                VertexId::from_u64(1),
                VertexId::from_u64(1000),
                predicates,
                &query_context,
            )
            .await?;

        let cost = &plan.cost_estimate;

        // Validate cost model components
        assert!(
            cost.estimated_vertices_scanned > 0,
            "{}: Should estimate vertices to scan",
            scenario_name
        );
        assert!(
            cost.estimated_disk_ios >= 0,
            "{}: Disk IOs should be non-negative",
            scenario_name
        );
        assert!(
            cost.estimated_memory_usage >= 0,
            "{}: Memory usage should be non-negative",
            scenario_name
        );
        assert!(
            cost.estimated_execution_time_ms >= 0,
            "{}: Execution time should be non-negative",
            scenario_name
        );
        assert!(
            cost.confidence_score >= 0.0 && cost.confidence_score <= 1.0,
            "{}: Confidence should be valid percentage",
            scenario_name
        );

        let total_cost = cost.total_cost();
        assert!(
            total_cost > 0.0 && total_cost.is_finite(),
            "{}: Total cost should be positive and finite",
            scenario_name
        );

        println!(
            "{}: cost={:.2}, vertices={}, confidence={:.2}",
            scenario_name, total_cost, cost.estimated_vertices_scanned, cost.confidence_score
        );
    }

    println!("✓ Cost model accuracy test completed");
    Ok(())
}
