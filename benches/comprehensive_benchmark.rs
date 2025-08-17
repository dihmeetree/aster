use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

use aster_db::benchmarks::{BenchmarkConfig, BenchmarkSuite, WorkloadType};
use aster_db::graph::Graph;
use aster_db::storage::poly_lsm::PolyLSM;
use aster_db::types::PolyLSMConfig;
use aster_db::types::VertexId;

/// Comprehensive benchmark using Criterion for statistical analysis
pub fn aster_performance_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Create test database with paper-compliant configuration
    let config = PolyLSMConfig::paper_specification();
    let storage = rt.block_on(async { PolyLSM::new(config).await.unwrap() });
    let storage = Arc::new(storage);

    // Small-scale benchmarks for Criterion
    let benchmark_config = BenchmarkConfig {
        vertex_count: 10_000, // Smaller for Criterion
        avg_degree: 10,
        iterations: 1000,
        concurrency: 8,
        workloads: WorkloadType::performance_workloads(),
        duration_seconds: 10, // Shorter for Criterion
        test_adaptive_strategies: true,
        test_concurrency_models: true,
        measure_memory: true,
    };

    // Individual operation benchmarks
    benchmark_vertex_operations(c, storage.clone(), &rt);
    benchmark_edge_operations(c, storage.clone(), &rt);
    benchmark_neighbor_queries(c, storage.clone(), &rt);
    benchmark_adaptive_strategies(c, storage.clone(), &rt);
    benchmark_concurrent_operations(c, storage.clone(), &rt);

    // Workload benchmarks
    benchmark_workloads(c, storage, &rt, benchmark_config);
}

/// Benchmark individual vertex operations
fn benchmark_vertex_operations(c: &mut Criterion, storage: Arc<PolyLSM>, rt: &Runtime) {
    let graph = Graph::new(&storage);

    let mut group = c.benchmark_group("vertex_operations");
    group.throughput(Throughput::Elements(1));

    group.bench_function("add_vertex", |b| {
        let mut vertex_id = 0u64;
        b.iter(|| {
            vertex_id += 1;
            rt.block_on(async { black_box(graph.add_vertex(VertexId::new(vertex_id), None).await) })
        });
    });

    // Setup vertices for existence checks
    rt.block_on(async {
        for i in 0..1000 {
            let _ = graph.add_vertex(VertexId::new(i), None).await;
        }
    });

    group.bench_function("vertex_exists_existing", |b| {
        b.iter(|| {
            let vertex_id = black_box(VertexId::new(500));
            rt.block_on(async { black_box(graph.get_neighbors(vertex_id).await.is_ok()) })
        });
    });

    group.bench_function("vertex_exists_missing", |b| {
        b.iter(|| {
            let vertex_id = black_box(VertexId::new(9999));
            rt.block_on(async { black_box(graph.get_neighbors(vertex_id).await.is_ok()) })
        });
    });

    group.finish();
}

/// Benchmark edge operations
fn benchmark_edge_operations(c: &mut Criterion, storage: Arc<PolyLSM>, rt: &Runtime) {
    let graph = Graph::new(&storage);

    // Setup test vertices
    rt.block_on(async {
        for i in 0..1000 {
            let _ = graph.add_vertex(VertexId::new(i), None).await;
        }
    });

    let mut group = c.benchmark_group("edge_operations");
    group.throughput(Throughput::Elements(1));

    group.bench_function("add_edge", |b| {
        let mut edge_id = 0u64;
        b.iter(|| {
            let source = VertexId::new(edge_id % 1000);
            let target = VertexId::new((edge_id + 1) % 1000);
            edge_id += 1;
            rt.block_on(async { black_box(graph.add_edge(source, target, None).await) })
        });
    });

    // Setup edges for existence checks
    rt.block_on(async {
        for i in 0..500 {
            let source = VertexId::new(i);
            let target = VertexId::new(i + 500);
            let _ = graph.add_edge(source, target, None).await;
        }
    });

    group.bench_function("edge_exists_existing", |b| {
        b.iter(|| {
            let source = black_box(VertexId::new(250));
            let target = black_box(VertexId::new(750));
            rt.block_on(async { black_box(graph.has_edge(source, target).await) })
        });
    });

    group.bench_function("edge_exists_missing", |b| {
        b.iter(|| {
            let source = black_box(VertexId::new(100));
            let target = black_box(VertexId::new(200));
            rt.block_on(async { black_box(graph.has_edge(source, target).await) })
        });
    });

    group.finish();
}

/// Benchmark neighbor queries with different degrees
fn benchmark_neighbor_queries(c: &mut Criterion, storage: Arc<PolyLSM>, rt: &Runtime) {
    let graph = Graph::new(&storage);

    // Setup vertices with different degrees
    rt.block_on(async {
        // Low-degree vertex (degree 5)
        let low_degree_vertex = VertexId::new(10000);
        graph.add_vertex(low_degree_vertex, None).await.unwrap();
        for i in 0..5 {
            let target = VertexId::new(10001 + i);
            graph.add_vertex(target, None).await.unwrap();
            graph
                .add_edge(low_degree_vertex, target, None)
                .await
                .unwrap();
        }

        // Medium-degree vertex (degree 50)
        let medium_degree_vertex = VertexId::new(20000);
        graph.add_vertex(medium_degree_vertex, None).await.unwrap();
        for i in 0..50 {
            let target = VertexId::new(20001 + i);
            graph.add_vertex(target, None).await.unwrap();
            graph
                .add_edge(medium_degree_vertex, target, None)
                .await
                .unwrap();
        }

        // High-degree vertex (degree 200)
        let high_degree_vertex = VertexId::new(30000);
        graph.add_vertex(high_degree_vertex, None).await.unwrap();
        for i in 0..200 {
            let target = VertexId::new(30001 + i);
            graph.add_vertex(target, None).await.unwrap();
            graph
                .add_edge(high_degree_vertex, target, None)
                .await
                .unwrap();
        }
    });

    let mut group = c.benchmark_group("neighbor_queries");

    group.bench_function("get_neighbors_degree_5", |b| {
        b.iter(|| {
            let vertex = black_box(VertexId::new(10000));
            rt.block_on(async { black_box(graph.get_neighbors(vertex).await) })
        });
    });

    group.bench_function("get_neighbors_degree_50", |b| {
        b.iter(|| {
            let vertex = black_box(VertexId::new(20000));
            rt.block_on(async { black_box(graph.get_neighbors(vertex).await) })
        });
    });

    group.bench_function("get_neighbors_degree_200", |b| {
        b.iter(|| {
            let vertex = black_box(VertexId::new(30000));
            rt.block_on(async { black_box(graph.get_neighbors(vertex).await) })
        });
    });

    group.finish();
}

/// Benchmark adaptive strategy performance
fn benchmark_adaptive_strategies(c: &mut Criterion, storage: Arc<PolyLSM>, rt: &Runtime) {
    let graph = Graph::new(&storage);

    // Setup vertices for adaptive strategy testing
    rt.block_on(async {
        for i in 40000..40100 {
            graph.add_vertex(VertexId::new(i), None).await.unwrap();
        }
    });

    let mut group = c.benchmark_group("adaptive_strategies");

    // Test delta vs pivot performance with different degree patterns
    group.bench_function("mixed_degree_updates", |b| {
        let mut operation_count = 0u64;
        b.iter(|| {
            // Mix of low and high degree operations
            let is_high_degree = operation_count % 10 < 3; // 30% high degree
            let source = if is_high_degree {
                VertexId::new(40050) // Use same vertex for high degree
            } else {
                VertexId::new(40000 + (operation_count % 50)) // Spread for low degree
            };
            let target = VertexId::new(40000 + ((operation_count + 17) % 100));
            operation_count += 1;

            rt.block_on(async { black_box(graph.add_edge(source, target, None).await) })
        });
    });

    group.finish();
}

/// Benchmark concurrent operations
fn benchmark_concurrent_operations(c: &mut Criterion, storage: Arc<PolyLSM>, rt: &Runtime) {
    let graph = Graph::new(&storage);

    // Setup vertices for concurrency testing
    rt.block_on(async {
        for i in 50000..50100 {
            graph.add_vertex(VertexId::new(i), None).await.unwrap();
        }
    });

    let mut group = c.benchmark_group("concurrent_operations");
    group.sample_size(10); // Fewer samples for concurrent tests

    // Test with different concurrency levels (simplified to avoid Send issues)
    for concurrency in [1, 4, 8, 16] {
        group.bench_function(
            &format!("sequential_edge_additions_{}_ops", concurrency * 10),
            |b| {
                b.iter(|| {
                    rt.block_on(async {
                        let graph_clone = Graph::new(&storage);
                        // Sequential operations instead of concurrent to avoid Send issues
                        for op_id in 0..(concurrency * 10) {
                            let source = VertexId::new(50000 + (op_id % 100) as u64);
                            let target = VertexId::new(50000 + ((op_id + 13) % 100) as u64);
                            let _ = graph_clone.add_edge(source, target, None).await;
                        }
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark different workload patterns
fn benchmark_workloads(
    c: &mut Criterion,
    storage: Arc<PolyLSM>,
    rt: &Runtime,
    mut config: BenchmarkConfig,
) {
    let mut group = c.benchmark_group("workloads");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for &workload in &[
        WorkloadType::WriteHeavy,
        WorkloadType::ReadHeavy,
        WorkloadType::Mixed,
    ] {
        group.bench_function(&format!("workload_{}", workload), |b| {
            b.iter(|| {
                rt.block_on(async {
                    // Configure for single workload
                    config.workloads = vec![workload];
                    config.iterations = 100; // Smaller for Criterion

                    let mut suite = BenchmarkSuite::new(storage.clone(), config.clone());
                    let result = suite.run_all_benchmarks().await;
                    black_box(result)
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, aster_performance_benchmarks);
criterion_main!(benches);
