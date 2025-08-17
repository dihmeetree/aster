# Aster Database Benchmarking Guide

This guide explains how to run performance benchmarks for the Aster graph database. There are two main benchmarking tools available:

1. **Criterion Benchmarks** - Statistical micro-benchmarks using the Criterion framework
2. **Example Benchmark Runner** - Comprehensive application-level benchmarks

## Criterion Benchmarks (Statistical Analysis)

The Criterion benchmarks provide detailed statistical analysis of individual operations with confidence intervals and performance regression detection.

### Running Criterion Benchmarks

```bash
# Run all Criterion benchmarks
cargo bench

# Run specific benchmark group
cargo bench vertex_operations
cargo bench edge_operations
cargo bench neighbor_queries
cargo bench workloads

# Generate HTML reports (saved to target/criterion/)
cargo bench -- --output-format html

# Run with specific parameters
cargo bench -- --measurement-time 30 --sample-size 1000
```

### Available Benchmark Groups

- **vertex_operations**: Add vertex, vertex existence checks
- **edge_operations**: Add edge, edge existence checks
- **neighbor_queries**: Get neighbors with different degree distributions (5, 50, 200 neighbors)
- **adaptive_strategies**: Mixed degree update patterns to test adaptive algorithms
- **concurrent_operations**: Sequential operations simulating concurrency
- **workloads**: Write-heavy, Read-heavy, and Mixed workload patterns

### Criterion Output

Criterion provides:

- Throughput measurements (operations/second)
- Statistical confidence intervals
- Performance regression detection
- HTML reports with charts and graphs
- Comparison with previous runs

## Example Benchmark Runner (Application-Level)

The example benchmark runner provides comprehensive workload analysis with detailed reporting in multiple formats.

### Basic Usage

```bash
# Quick test with reduced parameters
cargo run --example benchmark_runner -- --quick --vertices 1000 --workloads mixed

# Standard benchmark
cargo run --example benchmark_runner -- --vertices 50000 --iterations 5000

# High-performance configuration test
cargo run --example benchmark_runner -- --config high-performance --vertices 100000 --concurrency 32
```

### Command Line Options

| Option               | Short | Description                                 | Default                                      |
| -------------------- | ----- | ------------------------------------------- | -------------------------------------------- |
| `--vertices`         | `-v`  | Number of vertices to create                | 100,000                                      |
| `--degree`           | `-d`  | Average degree per vertex                   | 10                                           |
| `--iterations`       | `-i`  | Number of iterations per benchmark          | 10,000                                       |
| `--concurrency`      | `-c`  | Concurrency level for parallel benchmarks   | 16                                           |
| `--duration`         |       | Duration for sustained load tests (seconds) | 60                                           |
| `--workloads`        | `-w`  | Comma-separated workload types              | write-heavy,read-heavy,mixed,high-contention |
| `--config`           |       | Database configuration preset               | paper                                        |
| `--format`           | `-f`  | Output format                               | console                                      |
| `--output`           | `-o`  | Output file path                            | stdout                                       |
| `--quick`            | `-q`  | Run quick benchmark with reduced iterations | false                                        |
| `--adaptive-test`    |       | Test adaptive strategy performance          | false                                        |
| `--concurrency-test` |       | Test lock-free vs mutex performance         | false                                        |
| `--memory-analysis`  |       | Include detailed memory usage analysis      | false                                        |

### Database Configuration Presets

- **`paper`**: Exact paper specification (T=10, L=4, B=4KB) - Default
- **`high-performance`**: Optimized for maximum throughput
- **`low-memory`**: Optimized for memory-constrained environments

### Available Workloads

- **`write-heavy`**: 80% writes, 20% reads
- **`read-heavy`**: 20% writes, 80% reads
- **`mixed`**: 50% writes, 50% reads
- **`high-contention`**: Focus on small vertex set for contention testing
- **`traversal`**: Graph traversal operations (5-hop random walks)
- **`bulk-load`**: Large batch insert operations

### Output Formats

- **`console`**: Formatted terminal output with tables and charts (default)
- **`json`**: Machine-readable JSON format
- **`csv`**: CSV format for spreadsheet analysis
- **`html`**: HTML report with interactive charts
- **`prometheus`**: Prometheus metrics format

## Example Benchmark Commands

### Quick Performance Check

```bash
cargo run --example benchmark_runner -- \
  --quick \
  --vertices 1000 \
  --workloads mixed \
  --format console
```

### Comprehensive Analysis

```bash
cargo run --example benchmark_runner -- \
  --vertices 25000 \
  --iterations 2000 \
  --workloads write-heavy,read-heavy,mixed,high-contention \
  --adaptive-test \
  --memory-analysis \
  --format json \
  --output comprehensive_results.json
```

### Memory-Constrained Environment

```bash
cargo run --example benchmark_runner -- \
  --config low-memory \
  --vertices 5000 \
  --workloads mixed \
  --memory-analysis \
  --format console
```

### High-Performance Testing

```bash
cargo run --example benchmark_runner -- \
  --config high-performance \
  --vertices 100000 \
  --concurrency 32 \
  --workloads mixed,high-contention \
  --duration 120 \
  --format html \
  --output high_perf_report.html
```

### Adaptive Strategy Analysis

```bash
cargo run --example benchmark_runner -- \
  --vertices 50000 \
  --workloads mixed \
  --adaptive-test \
  --format json \
  --output adaptive_analysis.json
```

### Lock-Free Concurrency Testing

```bash
cargo run --example benchmark_runner -- \
  --vertices 20000 \
  --concurrency 16 \
  --workloads high-contention \
  --concurrency-test \
  --format console
```

## Understanding Benchmark Output

### Console Output Includes:

- **Performance Summary**: Operations/sec, latency, success rates
- **Detailed Analysis**: Per-workload breakdowns with operation counts
- **Adaptive Strategy Analysis**: Delta vs Pivot update effectiveness
- **Lock-Free Analysis**: Contention rates and success rates
- **Memory Analysis**: Cache hit rates, compression ratios
- **Performance Recommendations**: Optimization suggestions

### Key Metrics to Monitor:

- **Throughput**: Operations per second
- **Latency**: Average, P95, P99 latencies
- **Success Rate**: Percentage of successful operations
- **Cache Hit Rate**: Block cache and memory pool efficiency
- **Adaptive Effectiveness**: Strategy selection accuracy
- **Contention Rate**: Lock-free operation success

## Performance Baselines

Based on paper-compliant configuration:

| Operation      | Expected Throughput | Notes                      |
| -------------- | ------------------- | -------------------------- |
| Vertex Add     | 500K+ ops/sec       | Simple vertex creation     |
| Edge Add       | 300K+ ops/sec       | With neighbor list updates |
| Neighbor Query | 1M+ ops/sec         | Cached access patterns     |
| Mixed Workload | 400K+ ops/sec       | 50/50 read/write mix       |

## Troubleshooting

### Low Performance Issues:

1. Check system resources (CPU, memory, disk I/O)
2. Verify paper-compliant configuration
3. Adjust concurrency level for your system
4. Monitor cache hit rates
5. Check for memory pressure

### Memory Issues:

1. Use `--config low-memory` for constrained environments
2. Reduce vertex count and iterations
3. Enable memory analysis to identify bottlenecks
4. Monitor compression ratios

### Benchmark Failures:

1. Ensure sufficient disk space for temporary files
2. Check system limits (file descriptors, memory)
3. Use `--quick` flag for initial testing
4. Review error messages in benchmark output

## Continuous Integration

For CI/CD pipelines, use quick benchmarks:

```bash
# CI-friendly benchmark
cargo run --example benchmark_runner -- \
  --quick \
  --vertices 1000 \
  --iterations 100 \
  --workloads mixed \
  --format json \
  --output ci_benchmark.json
```

This ensures fast execution while still validating performance characteristics.
