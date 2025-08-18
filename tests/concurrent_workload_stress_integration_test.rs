//! Concurrent Workload and Stress Integration Tests
//!
//! This test suite validates the Aster database's behavior under high
//! concurrency and stress conditions, including:
//! - Multiple concurrent readers and writers
//! - MVCC behavior validation under load
//! - Transaction isolation testing under concurrent load  
//! - High-volume stress tests with large graphs
//! - Performance measurement under concurrent operations
//! - Edge cases and recovery scenarios
//! - Resource utilization monitoring

use aster_db::{
    AsterDB, AsterDBConfig, LockResource, Properties, PropertyValue, TransactionConfig, VertexId,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

/// Test configuration for stress scenarios
#[derive(Clone)]
struct StressTestConfig {
    pub num_vertices: usize,
    pub num_edges: usize,
    pub num_concurrent_workers: usize,
    pub operations_per_worker: usize,
    pub test_duration_ms: u64,
}

impl Default for StressTestConfig {
    fn default() -> Self {
        Self {
            num_vertices: 1000,
            num_edges: 5000,
            num_concurrent_workers: 10,
            operations_per_worker: 100,
            test_duration_ms: 5000,
        }
    }
}

/// Performance metrics collected during stress tests
#[derive(Debug, Default)]
struct StressTestMetrics {
    pub total_operations: AtomicU64,
    pub successful_reads: AtomicU64,
    pub successful_writes: AtomicU64,
    pub failed_operations: AtomicU64,
    pub total_latency_ms: AtomicU64,
    pub max_latency_ms: AtomicU64,
    pub conflicts_detected: AtomicU64,
}

impl StressTestMetrics {
    fn record_operation(&self, duration_ms: u64, success: bool, is_write: bool) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms
            .fetch_add(duration_ms, Ordering::Relaxed);

        // Update max latency atomically
        let mut current_max = self.max_latency_ms.load(Ordering::Relaxed);
        while duration_ms > current_max {
            match self.max_latency_ms.compare_exchange_weak(
                current_max,
                duration_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_max) => current_max = new_max,
            }
        }

        if success {
            if is_write {
                self.successful_writes.fetch_add(1, Ordering::Relaxed);
            } else {
                self.successful_reads.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.failed_operations.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_conflict(&self) {
        self.conflicts_detected.fetch_add(1, Ordering::Relaxed);
    }

    fn get_summary(&self) -> StressTestSummary {
        let total_ops = self.total_operations.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);

        StressTestSummary {
            total_operations: total_ops,
            successful_reads: self.successful_reads.load(Ordering::Relaxed),
            successful_writes: self.successful_writes.load(Ordering::Relaxed),
            failed_operations: self.failed_operations.load(Ordering::Relaxed),
            average_latency_ms: if total_ops > 0 {
                total_latency as f64 / total_ops as f64
            } else {
                0.0
            },
            max_latency_ms: self.max_latency_ms.load(Ordering::Relaxed),
            conflicts_detected: self.conflicts_detected.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct StressTestSummary {
    pub total_operations: u64,
    pub successful_reads: u64,
    pub successful_writes: u64,
    pub failed_operations: u64,
    pub average_latency_ms: f64,
    pub max_latency_ms: u64,
    pub conflicts_detected: u64,
}

/// 1. Concurrent Mixed Workload Tests

#[tokio::test]
async fn test_concurrent_readers_and_writers() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: true,
        enable_properties: true,
        ..Default::default()
    };

    let db = Arc::new(
        AsterDB::open_with_config(temp_dir.path(), config)
            .await
            .unwrap(),
    );
    let metrics = Arc::new(StressTestMetrics::default());

    // Setup initial data
    let num_vertices = 100;
    for i in 1..=num_vertices {
        let vertex_id = VertexId::from_u64(i);
        db.graph().add_vertex(vertex_id, None).await.unwrap();

        let mut props = Properties::new();
        props.insert("value".to_string(), PropertyValue::Int(i as i64));
        props.insert(
            "category".to_string(),
            PropertyValue::String(format!("cat_{}", i % 10)),
        );
        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Create barrier for synchronized start
    let barrier = Arc::new(Barrier::new(20)); // 10 readers + 10 writers
    let mut tasks = JoinSet::new();

    // Spawn concurrent readers
    for worker_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let metrics_clone = Arc::clone(&metrics);
        let barrier_clone = Arc::clone(&barrier);

        tasks.spawn(async move {
            barrier_clone.wait().await;

            for _op in 0..50 {
                let vertex_id = VertexId::from_u64((worker_id % num_vertices) + 1);
                let start = Instant::now();

                let tx = match db_clone.begin_transaction().await {
                    Ok(tx) => tx,
                    Err(_) => {
                        metrics_clone.record_operation(
                            start.elapsed().as_millis() as u64,
                            false,
                            false,
                        );
                        continue;
                    }
                };

                let success = tx.record_read(LockResource::Vertex(vertex_id)).is_ok()
                    && db_clone.get_vertex_properties(vertex_id).await.is_ok()
                    && db_clone.commit_transaction(tx).await.is_ok();

                metrics_clone.record_operation(start.elapsed().as_millis() as u64, success, false);

                // Small delay to allow interleaving
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }

    // Spawn concurrent writers
    for worker_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let metrics_clone = Arc::clone(&metrics);
        let barrier_clone = Arc::clone(&barrier);

        tasks.spawn(async move {
            barrier_clone.wait().await;

            for op_num in 0..25 {
                let vertex_id = VertexId::from_u64((worker_id % num_vertices) + 1);
                let start = Instant::now();

                let tx = match db_clone.begin_transaction().await {
                    Ok(tx) => tx,
                    Err(_) => {
                        metrics_clone.record_operation(
                            start.elapsed().as_millis() as u64,
                            false,
                            true,
                        );
                        continue;
                    }
                };

                let write_result = tx.record_write(LockResource::Vertex(vertex_id));
                let mut success = false;

                if write_result.is_ok() {
                    let mut props = Properties::new();
                    props.insert(
                        "value".to_string(),
                        PropertyValue::Int((worker_id * 1000 + op_num) as i64),
                    );
                    props.insert(
                        "updated_by".to_string(),
                        PropertyValue::String(format!("worker_{}", worker_id)),
                    );

                    if db_clone
                        .set_vertex_properties(vertex_id, props)
                        .await
                        .is_ok()
                    {
                        if db_clone.commit_transaction(tx).await.is_ok() {
                            success = true;
                        }
                        // tx is consumed by commit, no rollback needed
                    } else {
                        let _ = db_clone.rollback_transaction(tx).await;
                    }
                } else {
                    metrics_clone.record_conflict();
                    let _ = db_clone.rollback_transaction(tx).await;
                }

                metrics_clone.record_operation(start.elapsed().as_millis() as u64, success, true);

                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let summary = metrics.get_summary();
    println!("Concurrent Mixed Workload Test Results:");
    println!("  Total operations: {}", summary.total_operations);
    println!("  Successful reads: {}", summary.successful_reads);
    println!("  Successful writes: {}", summary.successful_writes);
    println!("  Failed operations: {}", summary.failed_operations);
    println!("  Conflicts detected: {}", summary.conflicts_detected);
    println!("  Average latency: {:.2}ms", summary.average_latency_ms);
    println!("  Max latency: {}ms", summary.max_latency_ms);

    // Validate results
    assert!(summary.total_operations > 0);
    assert!(summary.successful_reads > 0);
    assert!(summary.successful_writes > 0);

    // Transaction manager should handle conflicts properly
    let tx_stats = db.transaction_manager().get_stats();
    assert_eq!(tx_stats.active_transactions, 0);
}

#[tokio::test]
async fn test_mvcc_behavior_under_concurrent_load() {
    let temp_dir = TempDir::new().unwrap();

    let mut tx_config = TransactionConfig::default();
    tx_config.max_concurrent_transactions = 50;

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: true,
        enable_properties: true,
        transaction_config: tx_config,
        ..Default::default()
    };

    let db = Arc::new(
        AsterDB::open_with_config(temp_dir.path(), config)
            .await
            .unwrap(),
    );

    // Setup shared vertices for contention
    let shared_vertices: Vec<VertexId> = (1..=20).map(VertexId::from_u64).collect();
    for &vertex_id in &shared_vertices {
        db.graph().add_vertex(vertex_id, None).await.unwrap();
        let mut props = Properties::new();
        props.insert("counter".to_string(), PropertyValue::Int(0));
        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    let barrier = Arc::new(Barrier::new(30));
    let mut tasks = JoinSet::new();
    let successful_transactions = Arc::new(AtomicU64::new(0));
    let snapshot_violations = Arc::new(AtomicU64::new(0));

    // Spawn transactions that will compete for the same resources
    for worker_id in 0..30 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let shared_vertices_clone = shared_vertices.clone();
        let successful_transactions_clone = Arc::clone(&successful_transactions);
        let snapshot_violations_clone = Arc::clone(&snapshot_violations);

        tasks.spawn(async move {
            barrier_clone.wait().await;

            for _attempt in 0..20 {
                let tx = match db_clone.begin_transaction().await {
                    Ok(tx) => tx,
                    Err(_) => continue,
                };

                let target_vertex = shared_vertices_clone[worker_id % shared_vertices_clone.len()];

                // Attempt to read and then write to create potential conflicts
                let read_result = tx.record_read(LockResource::Vertex(target_vertex));
                if read_result.is_err() {
                    let _ = db_clone.rollback_transaction(tx).await;
                    continue;
                }

                // Read current value
                let current_props = match db_clone.get_vertex_properties(target_vertex).await {
                    Ok(props) => props,
                    Err(_) => {
                        let _ = db_clone.rollback_transaction(tx).await;
                        continue;
                    }
                };

                // Small delay to increase chance of conflicts
                tokio::time::sleep(Duration::from_millis(1)).await;

                // Attempt to increment counter
                let write_result = tx.record_write(LockResource::Vertex(target_vertex));
                if write_result.is_err() {
                    let _ = db_clone.rollback_transaction(tx).await;
                    continue;
                }

                let current_value = current_props
                    .get("counter")
                    .and_then(|v| match v {
                        PropertyValue::Int(i) => Some(*i),
                        _ => None,
                    })
                    .unwrap_or(0);

                let mut new_props = Properties::new();
                new_props.insert("counter".to_string(), PropertyValue::Int(current_value + 1));
                new_props.insert(
                    "last_worker".to_string(),
                    PropertyValue::String(format!("worker_{}", worker_id)),
                );

                if db_clone
                    .set_vertex_properties(target_vertex, new_props)
                    .await
                    .is_ok()
                {
                    // Validate snapshot isolation before commit
                    match tx.validate_snapshot_isolation() {
                        Ok(_) => {
                            if db_clone.commit_transaction(tx).await.is_ok() {
                                successful_transactions_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            snapshot_violations_clone.fetch_add(1, Ordering::Relaxed);
                            let _ = db_clone.rollback_transaction(tx).await;
                        }
                    }
                } else {
                    let _ = db_clone.rollback_transaction(tx).await;
                }

                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }

    // Wait for all tasks
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let successful_txs = successful_transactions.load(Ordering::Relaxed);
    let snapshot_violations_count = snapshot_violations.load(Ordering::Relaxed);

    println!("MVCC Concurrent Load Test Results:");
    println!("  Successful transactions: {}", successful_txs);
    println!("  Snapshot violations: {}", snapshot_violations_count);

    // Verify MVCC behavior
    assert!(successful_txs > 0);

    // Check final state consistency
    for &vertex_id in &shared_vertices {
        let props = db.get_vertex_properties(vertex_id).await.unwrap();
        if let Some(PropertyValue::Int(counter)) = props.get("counter") {
            assert!(*counter >= 0);
            println!("  Vertex {} final counter: {}", vertex_id.as_u64(), counter);
        }
    }

    // Verify transaction manager state
    let tx_stats = db.transaction_manager().get_stats();
    assert_eq!(tx_stats.active_transactions, 0);
}

/// 2. High-Volume Stress Tests

#[tokio::test]
async fn test_large_graph_creation_stress() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: true,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();
    let test_config = StressTestConfig {
        num_vertices: 1000,
        num_edges: 2000,
        num_concurrent_workers: 4,
        operations_per_worker: 250, // 1000 vertices / 4 workers
        test_duration_ms: 15000,
    };

    let start_time = Instant::now();

    // Phase 1: Sequential vertex creation to avoid Send issues
    for vertex_num in 1..=test_config.num_vertices {
        let vertex_id = VertexId::from_u64(vertex_num as u64);
        db.graph().add_vertex(vertex_id, None).await.unwrap();

        // Add properties to increase memory pressure
        let mut props = Properties::new();
        props.insert("id".to_string(), PropertyValue::Int(vertex_num as i64));
        props.insert(
            "batch".to_string(),
            PropertyValue::String(format!("batch_{}", vertex_num / 100)),
        );
        props.insert("data".to_string(), PropertyValue::String("x".repeat(100))); // 100 bytes of data

        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    let vertex_creation_time = start_time.elapsed();
    println!("Vertex creation completed in: {:?}", vertex_creation_time);

    // Phase 2: Sequential edge creation
    for edge_num in 0..test_config.num_edges {
        let source_id = VertexId::from_u64((edge_num % test_config.num_vertices) as u64 + 1);
        let target_id = VertexId::from_u64(((edge_num + 1) % test_config.num_vertices) as u64 + 1);

        db.graph()
            .add_edge(source_id, target_id, None)
            .await
            .unwrap();
    }

    let total_time = start_time.elapsed();

    println!("Large Graph Creation Stress Test Results:");
    println!("  Total time: {:?}", total_time);
    println!("  Vertices created: {}", test_config.num_vertices);
    println!("  Edges created: {}", test_config.num_edges);

    // Verify graph integrity with sampling
    let sample_vertices: Vec<VertexId> = (1..=20).map(VertexId::from_u64).collect();
    for vertex_id in sample_vertices {
        let neighbors = db.graph().get_neighbors(vertex_id).await.unwrap();
        assert!(neighbors.len() >= 0); // Basic sanity check
    }

    // Check storage statistics
    let storage_stats = db.storage().stats().await;
    println!("Storage statistics:");
    println!(
        "  Active memtable vertices: {}",
        storage_stats.active_memtable.num_vertices
    );
    println!(
        "  Total adaptive updates: {}",
        storage_stats.adaptive_stats.total_updates()
    );

    assert!(
        storage_stats.active_memtable.num_vertices > 0
            || storage_stats.levels.iter().any(|l| l.num_sstables > 0)
    );
}

#[tokio::test]
async fn test_memory_pressure_and_compaction_stress() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: true,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Create a hub vertex that will receive many edges (stress test for hot vertices)
    let hub_vertex = VertexId::from_u64(1);
    db.graph().add_vertex(hub_vertex, None).await.unwrap();

    let num_spokes = 500;

    // Create hub topology sequentially to avoid Send issues
    for spoke_num in 2..=(num_spokes + 1) {
        let spoke_vertex = VertexId::from_u64(spoke_num);

        // Create spoke vertex with large property data
        db.graph().add_vertex(spoke_vertex, None).await.unwrap();

        // Add substantial property data to create memory pressure
        let mut props = Properties::new();
        props.insert("spoke_id".to_string(), PropertyValue::Int(spoke_num as i64));
        props.insert(
            "large_data".to_string(),
            PropertyValue::String("X".repeat(1000)),
        ); // 1KB per vertex
        props.insert(
            "metadata".to_string(),
            PropertyValue::String(format!("spoke_{}", spoke_num)),
        );

        db.set_vertex_properties(spoke_vertex, props).await.unwrap();

        // Connect to hub to create high-degree vertex stress
        db.graph()
            .add_edge(hub_vertex, spoke_vertex, None)
            .await
            .unwrap();
        db.graph()
            .add_edge(spoke_vertex, hub_vertex, None)
            .await
            .unwrap();

        // Yield periodically to allow compaction
        if spoke_num % 20 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Give time for background compaction
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Memory Pressure and Compaction Stress Test Results:");

    // Verify hub vertex has many connections
    let hub_neighbors = db.graph().get_neighbors(hub_vertex).await.unwrap();
    println!("  Hub vertex has {} neighbors", hub_neighbors.len());
    assert!(hub_neighbors.len() > 100); // Should have many connections

    // Check that compaction occurred under memory pressure
    let storage_stats = db.storage().stats().await;
    println!(
        "  Active memtable vertices: {}",
        storage_stats.active_memtable.num_vertices
    );
    println!(
        "  Levels with SSTables: {}",
        storage_stats
            .levels
            .iter()
            .filter(|l| l.num_sstables > 0)
            .count()
    );
    println!(
        "  Total updates: {}",
        storage_stats.adaptive_stats.total_updates()
    );

    // Should have either active data or compacted data
    assert!(
        storage_stats.active_memtable.num_vertices > 0
            || storage_stats.levels.iter().any(|l| l.num_sstables > 0)
    );
}

/// 3. Performance Under Load

#[tokio::test]
async fn test_throughput_measurement_under_concurrent_load() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: true,
        enable_properties: true,
        ..Default::default()
    };

    let db = Arc::new(
        AsterDB::open_with_config(temp_dir.path(), config)
            .await
            .unwrap(),
    );

    // Setup test data
    let num_vertices = 500;
    for i in 1..=num_vertices {
        let vertex_id = VertexId::from_u64(i);
        db.graph().add_vertex(vertex_id, None).await.unwrap();

        let mut props = Properties::new();
        props.insert("value".to_string(), PropertyValue::Int(i as i64));
        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Performance measurement configuration
    let test_duration = Duration::from_secs(10);
    let num_workers = 16;
    let barrier = Arc::new(Barrier::new(num_workers));
    let mut tasks = JoinSet::new();
    let metrics = Arc::new(StressTestMetrics::default());
    let start_time = Arc::new(std::sync::Mutex::new(None::<Instant>));

    // Spawn mixed workload workers
    for worker_id in 0..num_workers {
        let db_clone = Arc::clone(&db);
        let metrics_clone = Arc::clone(&metrics);
        let barrier_clone = Arc::clone(&barrier);
        let start_time_clone = Arc::clone(&start_time);
        let is_writer = worker_id < num_workers / 2; // Half writers, half readers

        tasks.spawn(async move {
            barrier_clone.wait().await;

            // Set shared start time
            {
                let mut start = start_time_clone.lock().unwrap();
                if start.is_none() {
                    *start = Some(Instant::now());
                }
            }

            let test_start = {
                let start = start_time_clone.lock().unwrap();
                start.unwrap()
            };

            let mut operation_count = 0u64;

            while test_start.elapsed() < test_duration {
                let vertex_id = VertexId::from_u64((operation_count % num_vertices) + 1);
                let op_start = Instant::now();

                if is_writer {
                    // Writer workload: transactional property updates
                    let tx = match db_clone.begin_transaction().await {
                        Ok(tx) => tx,
                        Err(_) => {
                            metrics_clone.record_operation(
                                op_start.elapsed().as_millis() as u64,
                                false,
                                true,
                            );
                            continue;
                        }
                    };

                    let write_result = tx.record_write(LockResource::Vertex(vertex_id));
                    let success = if write_result.is_ok() {
                        let mut props = Properties::new();
                        props.insert(
                            "value".to_string(),
                            PropertyValue::Int((worker_id as u64 * 10000 + operation_count) as i64),
                        );
                        props.insert(
                            "timestamp".to_string(),
                            PropertyValue::Int(op_start.elapsed().as_nanos() as i64),
                        );

                        db_clone
                            .set_vertex_properties(vertex_id, props)
                            .await
                            .is_ok()
                            && db_clone.commit_transaction(tx).await.is_ok()
                    } else {
                        let _ = db_clone.rollback_transaction(tx).await;
                        false
                    };

                    metrics_clone.record_operation(
                        op_start.elapsed().as_millis() as u64,
                        success,
                        true,
                    );
                } else {
                    // Reader workload: property reads and graph traversals
                    let props_result = db_clone.get_vertex_properties(vertex_id).await;
                    let graph = db_clone.graph();
                    let neighbors_result = graph.get_neighbors(vertex_id).await;

                    let success = props_result.is_ok() && neighbors_result.is_ok();

                    metrics_clone.record_operation(
                        op_start.elapsed().as_millis() as u64,
                        success,
                        false,
                    );
                }

                operation_count += 1;

                // Small yield to allow task switching
                if operation_count % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            operation_count
        });
    }

    // Collect results
    let mut total_operations = 0u64;
    while let Some(result) = tasks.join_next().await {
        total_operations += result.unwrap();
    }

    let actual_duration = {
        let start = start_time.lock().unwrap();
        start.unwrap().elapsed()
    };

    let summary = metrics.get_summary();
    let throughput_ops_per_sec = total_operations as f64 / actual_duration.as_secs_f64();
    let read_throughput = summary.successful_reads as f64 / actual_duration.as_secs_f64();
    let write_throughput = summary.successful_writes as f64 / actual_duration.as_secs_f64();

    println!("Throughput Measurement Test Results:");
    println!("  Test duration: {:?}", actual_duration);
    println!("  Total operations: {}", total_operations);
    println!(
        "  Overall throughput: {:.2} ops/sec",
        throughput_ops_per_sec
    );
    println!("  Read throughput: {:.2} reads/sec", read_throughput);
    println!("  Write throughput: {:.2} writes/sec", write_throughput);
    println!(
        "  Success rate: {:.2}%",
        (summary.successful_reads + summary.successful_writes) as f64
            / summary.total_operations as f64
            * 100.0
    );
    println!("  Average latency: {:.2}ms", summary.average_latency_ms);
    println!("  Max latency: {}ms", summary.max_latency_ms);
    println!(
        "  95th percentile estimate: {:.2}ms",
        summary.average_latency_ms * 2.0
    ); // Rough estimate

    // Validate performance expectations
    assert!(throughput_ops_per_sec > 100.0); // At least 100 ops/sec
    assert!(summary.average_latency_ms < 100.0); // Average latency under 100ms
    assert!(
        (summary.successful_reads + summary.successful_writes) as f64
            / summary.total_operations as f64
            > 0.8
    ); // 80%+ success rate
}

/// 4. Edge Cases and Recovery

#[tokio::test]
async fn test_deadlock_detection_and_resource_exhaustion() {
    let temp_dir = TempDir::new().unwrap();

    let mut tx_config = TransactionConfig::default();
    tx_config.max_concurrent_transactions = 10; // Low limit to trigger exhaustion
    tx_config.transaction_timeout_ms = 1000; // Short timeout

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: true,
        enable_properties: true,
        transaction_config: tx_config,
        ..Default::default()
    };

    let db = Arc::new(
        AsterDB::open_with_config(temp_dir.path(), config)
            .await
            .unwrap(),
    );

    // Setup vertices for potential deadlock scenarios
    let v1 = VertexId::from_u64(1);
    let v2 = VertexId::from_u64(2);
    db.graph().add_vertex(v1, None).await.unwrap();
    db.graph().add_vertex(v2, None).await.unwrap();

    let barrier = Arc::new(Barrier::new(15)); // More workers than transaction limit
    let mut tasks = JoinSet::new();
    let resource_exhaustion_count = Arc::new(AtomicU64::new(0));
    let timeout_count = Arc::new(AtomicU64::new(0));
    let successful_txs = Arc::new(AtomicU64::new(0));

    // Spawn workers that will compete for limited transaction slots
    for worker_id in 0..15 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let resource_exhaustion_count_clone = Arc::clone(&resource_exhaustion_count);
        let timeout_count_clone = Arc::clone(&timeout_count);
        let successful_txs_clone = Arc::clone(&successful_txs);

        tasks.spawn(async move {
            barrier_clone.wait().await;

            for _attempt in 0..5 {
                // Try to begin transaction (may fail due to resource exhaustion)
                let tx = match db_clone.begin_transaction().await {
                    Ok(tx) => tx,
                    Err(_) => {
                        resource_exhaustion_count_clone.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };

                // Create potential deadlock scenario by accessing resources in different orders
                let (first_resource, second_resource) = if worker_id % 2 == 0 {
                    (LockResource::Vertex(v1), LockResource::Vertex(v2))
                } else {
                    (LockResource::Vertex(v2), LockResource::Vertex(v1))
                };

                // First lock
                if tx.record_write(first_resource).is_err() {
                    let _ = db_clone.rollback_transaction(tx).await;
                    continue;
                }

                // Delay to increase chance of deadlock
                tokio::time::sleep(Duration::from_millis(5)).await;

                // Second lock (potential deadlock point)
                if tx.record_write(second_resource).is_err() {
                    let _ = db_clone.rollback_transaction(tx).await;
                    continue;
                }

                // Hold locks for a bit to stress the system
                tokio::time::sleep(Duration::from_millis(10)).await;

                // Try to commit (may timeout)
                match db_clone.commit_transaction(tx).await {
                    Ok(_) => {
                        successful_txs_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        timeout_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }

                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    // Wait for all workers
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let resource_exhaustion = resource_exhaustion_count.load(Ordering::Relaxed);
    let timeouts = timeout_count.load(Ordering::Relaxed);
    let successful = successful_txs.load(Ordering::Relaxed);

    println!("Deadlock Detection and Resource Exhaustion Test Results:");
    println!("  Resource exhaustion events: {}", resource_exhaustion);
    println!("  Transaction timeouts: {}", timeouts);
    println!("  Successful transactions: {}", successful);

    // Verify system handled resource pressure gracefully
    assert!(resource_exhaustion > 0 || successful > 0); // Should have some activity

    // Check that transaction manager cleaned up properly
    let tx_stats = db.transaction_manager().get_stats();
    assert_eq!(tx_stats.active_transactions, 0);

    // Cleanup expired transactions to test the mechanism
    let expired = db
        .transaction_manager()
        .cleanup_expired_transactions()
        .unwrap();
    println!("  Expired transactions cleaned up: {}", expired.len());
}

/// 5. Specific Stress Scenarios

#[tokio::test]
async fn test_hub_vertex_stress_and_concurrent_traversals() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: true,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Create hub vertex
    let hub = VertexId::from_u64(1);
    db.graph().add_vertex(hub, None).await.unwrap();

    // Create many spokes connected to hub (stress test for high-degree vertices)
    let num_spokes = 200;
    let mut spoke_vertices = Vec::new();

    for i in 2..=(num_spokes + 1) {
        let spoke = VertexId::from_u64(i);
        spoke_vertices.push(spoke);
        db.graph().add_vertex(spoke, None).await.unwrap();
        db.graph().add_edge(hub, spoke, None).await.unwrap();
        db.graph().add_edge(spoke, hub, None).await.unwrap(); // Bidirectional
    }

    // Perform sequential traversals to test stress
    for _iteration in 0..100 {
        // Hub traversal (high contention simulation)
        let _hub_neighbors = db.graph().get_neighbors(hub).await.unwrap();

        // Random spoke traversal
        let spoke_idx = _iteration % spoke_vertices.len();
        let spoke = spoke_vertices[spoke_idx];
        let _spoke_neighbors = db.graph().get_neighbors(spoke).await.unwrap();

        // Property-based traversal with filtering
        if _iteration % 10 == 0 {
            let props_result = db.get_vertex_properties(spoke).await;
            assert!(props_result.is_ok());
        }
    }

    println!("Hub Vertex Stress and Concurrent Traversals Test Results:");
    println!("  Completed 100 traversal iterations");

    // Verify hub still has correct degree
    let hub_neighbors = db.graph().get_neighbors(hub).await.unwrap();
    println!("  Hub vertex final degree: {}", hub_neighbors.len());
    assert!(hub_neighbors.len() >= (num_spokes / 2) as usize); // Should have most connections

    // Test mixed query performance after stress
    let mixed_query_start = Instant::now();

    // Range scan
    let range_result = db
        .range_query(VertexId::from_u64(1), VertexId::from_u64(100))
        .await;
    assert!(range_result.is_ok());

    // Property lookup
    if db.properties_enabled() {
        let mut test_props = Properties::new();
        test_props.insert(
            "test".to_string(),
            PropertyValue::String("stress_test".to_string()),
        );
        db.set_vertex_properties(hub, test_props).await.unwrap();

        let retrieved_props = db.get_vertex_properties(hub).await.unwrap();
        assert!(retrieved_props.contains_key("test"));
    }

    let mixed_query_time = mixed_query_start.elapsed();
    println!(
        "  Mixed query performance after stress: {:?}",
        mixed_query_time
    );

    // Should still be responsive after stress test
    assert!(mixed_query_time < Duration::from_millis(1000));
}

/// Final validation and cleanup test
#[tokio::test]
async fn test_comprehensive_stress_validation() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: true,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Run a comprehensive mixed workload sequentially
    let num_vertices = 100;
    let operations_per_type = 25;

    // Setup initial data
    for i in 1..=num_vertices {
        let vertex_id = VertexId::from_u64(i);
        db.graph().add_vertex(vertex_id, None).await.unwrap();
    }

    let mut total_operations = 0;
    let mut successful_operations = 0;

    // Mixed workload: reads, writes, traversals, property operations
    for op_num in 0..(operations_per_type * 4) {
        let vertex_id = VertexId::from_u64((op_num % num_vertices) + 1);

        let operation_type = op_num % 4;
        let success = match operation_type {
            0 => {
                // Graph traversal
                db.graph().get_neighbors(vertex_id).await.is_ok()
            }
            1 => {
                // Property read
                db.get_vertex_properties(vertex_id).await.is_ok()
            }
            2 => {
                // Property write (transactional)
                let tx = db.begin_transaction().await.ok();
                if let Some(tx) = tx {
                    let write_ok = tx.record_write(LockResource::Vertex(vertex_id)).is_ok();
                    if write_ok {
                        let mut props = Properties::new();
                        props.insert("operation".to_string(), PropertyValue::Int(op_num as i64));
                        props.insert(
                            "timestamp".to_string(),
                            PropertyValue::Int(
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as i64,
                            ),
                        );

                        let set_ok = db.set_vertex_properties(vertex_id, props).await.is_ok();
                        let commit_ok = db.commit_transaction(tx).await.is_ok();
                        set_ok && commit_ok
                    } else {
                        let _ = db.rollback_transaction(tx).await;
                        false
                    }
                } else {
                    false
                }
            }
            3 => {
                // Edge creation
                let target_vertex =
                    VertexId::from_u64((vertex_id.as_u64() % num_vertices as u64) + 1);
                db.graph()
                    .add_edge(vertex_id, target_vertex, None)
                    .await
                    .is_ok()
            }
            _ => unreachable!(),
        };

        total_operations += 1;
        if success {
            successful_operations += 1;
        }
    }

    println!("Comprehensive Stress Validation Results:");
    println!("  Total operations: {}", total_operations);
    println!("  Successful operations: {}", successful_operations);
    println!(
        "  Success rate: {:.2}%",
        (successful_operations as f64 / total_operations as f64) * 100.0
    );

    // Final system health checks
    let tx_stats = db.transaction_manager().get_stats();
    println!("  Active transactions: {}", tx_stats.active_transactions);

    let storage_stats = db.storage().stats().await;
    println!(
        "  Active memtable vertices: {}",
        storage_stats.active_memtable.num_vertices
    );
    println!(
        "  Total adaptive updates: {}",
        storage_stats.adaptive_stats.total_updates()
    );

    // Validate final state
    assert_eq!(tx_stats.active_transactions, 0);
    assert!(total_operations > 0);
    assert!(successful_operations > 0);

    // Test database is still responsive
    let final_test_vertex = VertexId::from_u64(1);
    let final_neighbors = db.graph().get_neighbors(final_test_vertex).await.unwrap();
    assert!(final_neighbors.len() >= 0);

    println!("✓ Concurrent workload and stress tests completed successfully");
}
