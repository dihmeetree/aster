use clap::{Arg, Command};
use std::fs::File;
use std::io::{self};
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber;

use aster_db::benchmarks::{
    BenchmarkConfig, BenchmarkReporter, BenchmarkSuite, OutputFormat, WorkloadType,
};
use aster_db::storage::poly_lsm::PolyLSM;
use aster_db::types::PolyLSMConfig;

/// Command-line benchmark runner for Aster database
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Parse command-line arguments
    let matches = Command::new("Aster Benchmark Runner")
        .version("1.0")
        .author("Aster Database Team")
        .about("Comprehensive benchmarking tool for Aster graph database")
        .arg(Arg::new("output")
            .short('o')
            .long("output")
            .value_name("FILE")
            .help("Output file for benchmark results (stdout if not specified)")
            .required(false))
        .arg(Arg::new("format")
            .short('f')
            .long("format")
            .value_name("FORMAT")
            .help("Output format: console, json, csv, html, prometheus")
            .default_value("console"))
        .arg(Arg::new("vertices")
            .short('v')
            .long("vertices")
            .value_name("COUNT")
            .help("Number of vertices to create")
            .default_value("100000"))
        .arg(Arg::new("degree")
            .short('d')
            .long("degree")
            .value_name("DEGREE")
            .help("Average degree per vertex")
            .default_value("10"))
        .arg(Arg::new("iterations")
            .short('i')
            .long("iterations")
            .value_name("COUNT")
            .help("Number of iterations per benchmark")
            .default_value("10000"))
        .arg(Arg::new("concurrency")
            .short('c')
            .long("concurrency")
            .value_name("THREADS")
            .help("Concurrency level for parallel benchmarks")
            .default_value("16"))
        .arg(Arg::new("duration")
            .long("duration")
            .value_name("SECONDS")
            .help("Duration for sustained load tests")
            .default_value("60"))
        .arg(Arg::new("workloads")
            .short('w')
            .long("workloads")
            .value_name("WORKLOADS")
            .help("Workloads to run (comma-separated): write-heavy,read-heavy,mixed,high-contention,traversal,bulk-load")
            .default_value("write-heavy,read-heavy,mixed,high-contention"))
        .arg(Arg::new("config")
            .long("config")
            .value_name("CONFIG")
            .help("Database configuration: paper, high-performance, low-memory")
            .default_value("paper"))
        .arg(Arg::new("adaptive-test")
            .long("adaptive-test")
            .help("Test adaptive strategy performance")
            .action(clap::ArgAction::SetTrue))
        .arg(Arg::new("concurrency-test")
            .long("concurrency-test")
            .help("Test lock-free vs mutex performance")
            .action(clap::ArgAction::SetTrue))
        .arg(Arg::new("memory-analysis")
            .long("memory-analysis")
            .help("Include detailed memory usage analysis")
            .action(clap::ArgAction::SetTrue))
        .arg(Arg::new("quick")
            .short('q')
            .long("quick")
            .help("Run quick benchmark with reduced iterations")
            .action(clap::ArgAction::SetTrue))
        .get_matches();

    // Parse output format
    let output_format = match matches.get_one::<String>("format").unwrap().as_str() {
        "console" => OutputFormat::Console,
        "json" => OutputFormat::Json,
        "csv" => OutputFormat::Csv,
        "html" => OutputFormat::Html,
        "prometheus" => OutputFormat::Prometheus,
        _ => {
            eprintln!("Invalid output format. Using console.");
            OutputFormat::Console
        }
    };

    // Parse workload types
    let workload_str = matches.get_one::<String>("workloads").unwrap();
    let workloads = parse_workloads(workload_str)?;

    // Create database configuration
    let db_config = match matches.get_one::<String>("config").unwrap().as_str() {
        "paper" => PolyLSMConfig::paper_specification(),
        "high-performance" => PolyLSMConfig::high_performance(),
        "low-memory" => PolyLSMConfig::low_memory(),
        _ => {
            eprintln!("Invalid config. Using paper specification.");
            PolyLSMConfig::paper_specification()
        }
    };

    // Parse benchmark parameters
    let vertex_count: usize = matches.get_one::<String>("vertices").unwrap().parse()?;
    let avg_degree: u32 = matches.get_one::<String>("degree").unwrap().parse()?;
    let mut iterations: usize = matches.get_one::<String>("iterations").unwrap().parse()?;
    let concurrency: usize = matches.get_one::<String>("concurrency").unwrap().parse()?;
    let duration: u64 = matches.get_one::<String>("duration").unwrap().parse()?;

    // Quick mode adjustments
    if matches.get_flag("quick") {
        iterations = iterations.min(1000);
        info!("Quick mode: reducing iterations to {}", iterations);
    }

    // Create benchmark configuration
    let benchmark_config = BenchmarkConfig {
        vertex_count,
        avg_degree,
        iterations,
        concurrency,
        workloads,
        duration_seconds: duration,
        test_adaptive_strategies: matches.get_flag("adaptive-test"),
        test_concurrency_models: matches.get_flag("concurrency-test"),
        measure_memory: matches.get_flag("memory-analysis"),
    };

    info!("Starting Aster benchmark with configuration:");
    info!("  Vertices: {}", vertex_count);
    info!("  Average degree: {}", avg_degree);
    info!("  Iterations: {}", iterations);
    info!("  Concurrency: {}", concurrency);
    info!("  Workloads: {:?}", benchmark_config.workloads);

    // Initialize database
    let storage = Arc::new(PolyLSM::new(db_config).await?);

    // Create and run benchmark suite
    let mut benchmark_suite = BenchmarkSuite::new(storage, benchmark_config);
    benchmark_suite.run_all_benchmarks().await?;

    // Generate report
    let reporter = BenchmarkReporter::new(output_format)
        .with_adaptive_analysis(matches.get_flag("adaptive-test"))
        .with_lock_free_analysis(matches.get_flag("concurrency-test"))
        .with_memory_analysis(matches.get_flag("memory-analysis"));

    // Output results
    let results = benchmark_suite.get_results();
    if let Some(output_file) = matches.get_one::<String>("output") {
        let file = File::create(output_file)?;
        reporter.generate_report(results, file)?;
        info!("Benchmark results written to: {}", output_file);
    } else {
        let stdout = io::stdout();
        reporter.generate_report(results, stdout.lock())?;
    }

    info!("Benchmark completed successfully!");
    Ok(())
}

/// Parse workload types from comma-separated string
fn parse_workloads(workload_str: &str) -> Result<Vec<WorkloadType>, Box<dyn std::error::Error>> {
    let mut workloads = Vec::new();

    for workload_name in workload_str.split(',') {
        let workload = match workload_name.trim() {
            "write-heavy" => WorkloadType::WriteHeavy,
            "read-heavy" => WorkloadType::ReadHeavy,
            "mixed" => WorkloadType::Mixed,
            "high-contention" => WorkloadType::HighContention,
            "traversal" => WorkloadType::Traversal,
            "bulk-load" => WorkloadType::BulkLoad,
            _ => return Err(format!("Unknown workload type: {}", workload_name).into()),
        };
        workloads.push(workload);
    }

    if workloads.is_empty() {
        workloads = WorkloadType::performance_workloads();
    }

    Ok(workloads)
}

/// Example usage scenarios
#[cfg(test)]
mod examples {
    use super::*;

    /// Example: Quick performance check
    #[tokio::test]
    async fn example_quick_benchmark() -> Result<(), Box<dyn std::error::Error>> {
        let config = BenchmarkConfig {
            vertex_count: 1000,
            avg_degree: 5,
            iterations: 100,
            concurrency: 4,
            workloads: vec![WorkloadType::Mixed],
            duration_seconds: 5,
            test_adaptive_strategies: false,
            test_concurrency_models: false,
            measure_memory: false,
        };

        let storage = Arc::new(PolyLSM::new(PolyLSMConfig::paper_specification()).await?);
        let mut suite = BenchmarkSuite::new(storage, config);
        suite.run_all_benchmarks().await?;

        let reporter = BenchmarkReporter::new(OutputFormat::Console);
        let stdout = io::stdout();
        reporter.generate_report(suite.get_results(), stdout.lock())?;

        Ok(())
    }

    /// Example: Comprehensive performance analysis
    #[tokio::test]
    async fn example_comprehensive_benchmark() -> Result<(), Box<dyn std::error::Error>> {
        let config = BenchmarkConfig {
            vertex_count: 10000,
            avg_degree: 15,
            iterations: 5000,
            concurrency: 8,
            workloads: WorkloadType::all(),
            duration_seconds: 30,
            test_adaptive_strategies: true,
            test_concurrency_models: true,
            measure_memory: true,
        };

        let storage = Arc::new(PolyLSM::new(PolyLSMConfig::high_performance()).await?);
        let mut suite = BenchmarkSuite::new(storage, config);
        suite.run_all_benchmarks().await?;

        // Generate JSON report
        let reporter = BenchmarkReporter::new(OutputFormat::Json)
            .with_detailed_stats(true)
            .with_adaptive_analysis(true)
            .with_lock_free_analysis(true)
            .with_memory_analysis(true);

        let mut output = Vec::new();
        reporter.generate_report(suite.get_results(), &mut output)?;

        // Verify JSON is valid
        let _parsed: serde_json::Value = serde_json::from_slice(&output)?;

        Ok(())
    }

    /// Example: Memory-constrained environment benchmark
    #[tokio::test]
    async fn example_low_memory_benchmark() -> Result<(), Box<dyn std::error::Error>> {
        let config = BenchmarkConfig {
            vertex_count: 5000,
            avg_degree: 8,
            iterations: 2000,
            concurrency: 4,
            workloads: vec![WorkloadType::WriteHeavy, WorkloadType::ReadHeavy],
            duration_seconds: 20,
            test_adaptive_strategies: true,
            test_concurrency_models: false,
            measure_memory: true,
        };

        let storage = Arc::new(PolyLSM::new(PolyLSMConfig::low_memory()).await?);
        let mut suite = BenchmarkSuite::new(storage, config);
        suite.run_all_benchmarks().await?;

        let reporter = BenchmarkReporter::new(OutputFormat::Console).with_memory_analysis(true);

        let stdout = io::stdout();
        reporter.generate_report(suite.get_results(), stdout.lock())?;

        Ok(())
    }
}
