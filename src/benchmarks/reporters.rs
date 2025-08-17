use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

use crate::benchmarks::{BenchmarkResults, BenchmarkSummary};

/// Formats for benchmark result output
#[derive(Debug, Clone)]
pub enum OutputFormat {
    Console,
    Json,
    Csv,
    Html,
    Prometheus,
}

/// Comprehensive benchmark reporter
pub struct BenchmarkReporter {
    format: OutputFormat,
    include_detailed_stats: bool,
    include_adaptive_analysis: bool,
    include_lock_free_analysis: bool,
    include_memory_analysis: bool,
}

impl BenchmarkReporter {
    pub fn new(format: OutputFormat) -> Self {
        Self {
            format,
            include_detailed_stats: true,
            include_adaptive_analysis: true,
            include_lock_free_analysis: true,
            include_memory_analysis: true,
        }
    }

    pub fn with_detailed_stats(mut self, include: bool) -> Self {
        self.include_detailed_stats = include;
        self
    }

    pub fn with_adaptive_analysis(mut self, include: bool) -> Self {
        self.include_adaptive_analysis = include;
        self
    }

    pub fn with_lock_free_analysis(mut self, include: bool) -> Self {
        self.include_lock_free_analysis = include;
        self
    }

    pub fn with_memory_analysis(mut self, include: bool) -> Self {
        self.include_memory_analysis = include;
        self
    }

    /// Generate and output benchmark report
    pub fn generate_report<W: Write>(
        &self,
        results: &[BenchmarkResults],
        mut writer: W,
    ) -> io::Result<()> {
        match self.format {
            OutputFormat::Console => self.write_console_report(results, &mut writer),
            OutputFormat::Json => self.write_json_report(results, &mut writer),
            OutputFormat::Csv => self.write_csv_report(results, &mut writer),
            OutputFormat::Html => self.write_html_report(results, &mut writer),
            OutputFormat::Prometheus => self.write_prometheus_report(results, &mut writer),
        }
    }

    /// Write console-formatted report
    fn write_console_report<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        writeln!(
            writer,
            "╔══════════════════════════════════════════════════════════════════════════════╗"
        )?;
        writeln!(
            writer,
            "║                            ASTER DATABASE BENCHMARK REPORT                  ║"
        )?;
        writeln!(
            writer,
            "╠══════════════════════════════════════════════════════════════════════════════╣"
        )?;
        writeln!(
            writer,
            "║ Timestamp: {:<63} ║",
            Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        )?;
        writeln!(
            writer,
            "║ Test Results: {:<59} ║",
            format!("{} workloads", results.len())
        )?;
        writeln!(
            writer,
            "╚══════════════════════════════════════════════════════════════════════════════╝"
        )?;
        writeln!(writer)?;

        // Summary table
        writeln!(writer, "📊 PERFORMANCE SUMMARY")?;
        writeln!(
            writer,
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )?;
        writeln!(
            writer,
            "{:<15} {:<12} {:<12} {:<12} {:<12} {:<12}",
            "Workload", "Ops/sec", "Avg Lat(ms)", "P95 Lat(ms)", "Success %", "Error %"
        )?;
        writeln!(
            writer,
            "────────────────────────────────────────────────────────────────────────────"
        )?;

        for result in results {
            let summary =
                BenchmarkSummary::from_metrics(result.workload.to_string(), &result.metrics);
            writeln!(
                writer,
                "{:<15} {:<12.0} {:<12.2} {:<12.2} {:<12.1} {:<12.3}",
                result.workload,
                summary.throughput_ops_per_sec,
                summary.avg_latency_ms,
                summary.p95_latency_ms,
                summary.success_rate * 100.0,
                summary.error_rate * 100.0
            )?;
        }
        writeln!(writer)?;

        // Detailed stats for each workload
        if self.include_detailed_stats {
            for result in results {
                self.write_detailed_workload_stats(result, writer)?;
            }
        }

        // Adaptive strategy analysis
        if self.include_adaptive_analysis {
            self.write_adaptive_analysis(results, writer)?;
        }

        // Lock-free performance analysis
        if self.include_lock_free_analysis {
            self.write_lock_free_analysis(results, writer)?;
        }

        // Memory usage analysis
        if self.include_memory_analysis {
            self.write_memory_analysis(results, writer)?;
        }

        // Performance recommendations
        self.write_performance_recommendations(results, writer)?;

        Ok(())
    }

    /// Write detailed stats for a single workload
    fn write_detailed_workload_stats<W: Write>(
        &self,
        result: &BenchmarkResults,
        writer: &mut W,
    ) -> io::Result<()> {
        writeln!(writer, "🔍 DETAILED ANALYSIS: {}", result.workload)?;
        writeln!(
            writer,
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )?;

        let metrics = &result.metrics;

        writeln!(writer, "  Operation Counts:")?;
        writeln!(
            writer,
            "    Successful Reads:  {:>12}",
            metrics.successful_reads
        )?;
        writeln!(
            writer,
            "    Failed Reads:      {:>12}",
            metrics.failed_reads
        )?;
        writeln!(
            writer,
            "    Successful Writes: {:>12}",
            metrics.successful_writes
        )?;
        writeln!(
            writer,
            "    Failed Writes:     {:>12}",
            metrics.failed_writes
        )?;
        writeln!(
            writer,
            "    Total Duration:    {:>12.2}s",
            metrics.total_duration.as_secs_f64()
        )?;

        writeln!(writer, "  Latency Distribution:")?;
        writeln!(
            writer,
            "    Average Read:      {:>12.2}µs",
            metrics.avg_read_latency_us
        )?;
        writeln!(
            writer,
            "    Average Write:     {:>12.2}µs",
            metrics.avg_write_latency_us
        )?;
        writeln!(
            writer,
            "    P50 Latency:       {:>12.2}µs",
            metrics.p50_latency_us
        )?;
        writeln!(
            writer,
            "    P95 Latency:       {:>12.2}µs",
            metrics.p95_latency_us
        )?;
        writeln!(
            writer,
            "    P99 Latency:       {:>12.2}µs",
            metrics.p99_latency_us
        )?;

        writeln!(writer, "  Throughput:")?;
        writeln!(
            writer,
            "    Operations/sec:    {:>12.0}",
            metrics.operations_per_second
        )?;
        writeln!(
            writer,
            "    Reads/sec:         {:>12.0}",
            metrics.reads_per_second
        )?;
        writeln!(
            writer,
            "    Writes/sec:        {:>12.0}",
            metrics.writes_per_second
        )?;
        writeln!(
            writer,
            "    Data Rate:         {:>12.2} MB/s",
            metrics.disk_io_rate / (1024.0 * 1024.0)
        )?;

        writeln!(writer)?;
        Ok(())
    }

    /// Write adaptive strategy analysis
    fn write_adaptive_analysis<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        writeln!(writer, "🧠 ADAPTIVE STRATEGY ANALYSIS")?;
        writeln!(
            writer,
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )?;

        writeln!(
            writer,
            "{:<15} {:<12} {:<12} {:<12} {:<12}",
            "Workload", "Delta Ops", "Pivot Ops", "Effectiveness", "Adaptations"
        )?;
        writeln!(
            writer,
            "────────────────────────────────────────────────────────────────────────────"
        )?;

        for result in results {
            let adaptive = &result.adaptive_stats;
            writeln!(
                writer,
                "{:<15} {:<12} {:<12} {:<12.3} {:<12}",
                result.workload,
                adaptive.delta_updates,
                adaptive.pivot_updates,
                adaptive.strategy_effectiveness,
                adaptive.threshold_adaptations
            )?;
        }

        writeln!(writer)?;
        writeln!(writer, "  Strategy Insights:")?;

        let total_delta: u64 = results.iter().map(|r| r.adaptive_stats.delta_updates).sum();
        let total_pivot: u64 = results.iter().map(|r| r.adaptive_stats.pivot_updates).sum();
        let avg_effectiveness: f64 = results
            .iter()
            .map(|r| r.adaptive_stats.strategy_effectiveness)
            .sum::<f64>()
            / results.len() as f64;

        if total_delta + total_pivot > 0 {
            let delta_ratio = total_delta as f64 / (total_delta + total_pivot) as f64;
            writeln!(
                writer,
                "    Delta Strategy Usage:  {:.1}%",
                delta_ratio * 100.0
            )?;
            writeln!(
                writer,
                "    Pivot Strategy Usage:  {:.1}%",
                (1.0 - delta_ratio) * 100.0
            )?;
        }
        writeln!(
            writer,
            "    Average Effectiveness: {:.3}",
            avg_effectiveness
        )?;

        writeln!(writer)?;
        Ok(())
    }

    /// Write lock-free performance analysis
    fn write_lock_free_analysis<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        writeln!(writer, "🔒 LOCK-FREE CONCURRENCY ANALYSIS")?;
        writeln!(
            writer,
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )?;

        writeln!(
            writer,
            "{:<15} {:<12} {:<12} {:<12} {:<12}",
            "Workload", "Success %", "Contentions", "Avg Backoff", "Throughput"
        )?;
        writeln!(
            writer,
            "────────────────────────────────────────────────────────────────────────────"
        )?;

        for result in results {
            let lock_free = &result.lock_free_stats;
            writeln!(
                writer,
                "{:<15} {:<12.1} {:<12} {:<12.2} {:<12.0}",
                result.workload,
                lock_free.success_rate * 100.0,
                lock_free.contention_events,
                lock_free.avg_backoff_time_us,
                lock_free.throughput_ops_per_sec
            )?;
        }

        writeln!(writer)?;
        writeln!(writer, "  Concurrency Insights:")?;

        let avg_success_rate: f64 = results
            .iter()
            .map(|r| r.lock_free_stats.success_rate)
            .sum::<f64>()
            / results.len() as f64;
        let total_contentions: usize = results
            .iter()
            .map(|r| r.lock_free_stats.contention_events)
            .sum();

        writeln!(
            writer,
            "    Average Success Rate:   {:.1}%",
            avg_success_rate * 100.0
        )?;
        writeln!(writer, "    Total Contention Events: {}", total_contentions)?;

        if avg_success_rate > 0.95 {
            writeln!(writer, "    ✅ Excellent lock-free performance")?;
        } else if avg_success_rate > 0.90 {
            writeln!(
                writer,
                "    ⚠️  Good lock-free performance with some contention"
            )?;
        } else {
            writeln!(writer, "    ❌ High contention detected - consider tuning")?;
        }

        writeln!(writer)?;
        Ok(())
    }

    /// Write memory usage analysis
    fn write_memory_analysis<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        writeln!(writer, "💾 MEMORY USAGE ANALYSIS")?;
        writeln!(
            writer,
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )?;

        writeln!(
            writer,
            "{:<15} {:<12} {:<12} {:<12} {:<12}",
            "Workload", "Peak (MB)", "Pool Hit %", "Cache Hit %", "Compress %"
        )?;
        writeln!(
            writer,
            "────────────────────────────────────────────────────────────────────────────"
        )?;

        for result in results {
            let memory = &result.memory_stats;
            writeln!(
                writer,
                "{:<15} {:<12.1} {:<12.1} {:<12.1} {:<12.1}",
                result.workload,
                memory.peak_memory_mb,
                memory.memory_pool_hit_rate * 100.0,
                memory.block_cache_hit_rate * 100.0,
                memory.compression_ratio * 100.0
            )?;
        }

        writeln!(writer)?;
        writeln!(writer, "  Memory Insights:")?;

        let avg_pool_hit_rate: f64 = results
            .iter()
            .map(|r| r.memory_stats.memory_pool_hit_rate)
            .sum::<f64>()
            / results.len() as f64;
        let avg_cache_hit_rate: f64 = results
            .iter()
            .map(|r| r.memory_stats.block_cache_hit_rate)
            .sum::<f64>()
            / results.len() as f64;
        let avg_compression_ratio: f64 = results
            .iter()
            .map(|r| r.memory_stats.compression_ratio)
            .sum::<f64>()
            / results.len() as f64;

        writeln!(
            writer,
            "    Average Pool Hit Rate:    {:.1}%",
            avg_pool_hit_rate * 100.0
        )?;
        writeln!(
            writer,
            "    Average Cache Hit Rate:   {:.1}%",
            avg_cache_hit_rate * 100.0
        )?;
        writeln!(
            writer,
            "    Average Compression:      {:.1}%",
            avg_compression_ratio * 100.0
        )?;

        writeln!(writer)?;
        Ok(())
    }

    /// Write performance recommendations
    fn write_performance_recommendations<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        writeln!(writer, "💡 PERFORMANCE RECOMMENDATIONS")?;
        writeln!(
            writer,
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )?;

        // Analyze results and provide recommendations
        let mut recommendations = Vec::new();

        // Check for high error rates
        for result in results {
            if result.metrics.overall_error_rate > 0.05 {
                recommendations.push(format!(
                    "⚠️  High error rate ({:.1}%) in {} workload - check system resources",
                    result.metrics.overall_error_rate * 100.0,
                    result.workload
                ));
            }
        }

        // Check for low cache hit rates
        let avg_cache_hit_rate: f64 = results
            .iter()
            .map(|r| r.memory_stats.block_cache_hit_rate)
            .sum::<f64>()
            / results.len() as f64;

        if avg_cache_hit_rate < 0.8 {
            recommendations.push(format!(
                "📈 Consider increasing block cache size (current hit rate: {:.1}%)",
                avg_cache_hit_rate * 100.0
            ));
        }

        // Check for high contention
        let avg_success_rate: f64 = results
            .iter()
            .map(|r| r.lock_free_stats.success_rate)
            .sum::<f64>()
            / results.len() as f64;

        if avg_success_rate < 0.9 {
            recommendations.push(format!(
                "🔧 High contention detected (success rate: {:.1}%) - consider reducing concurrency or optimizing access patterns",
                avg_success_rate * 100.0
            ));
        }

        // Check adaptive strategy effectiveness
        let avg_effectiveness: f64 = results
            .iter()
            .map(|r| r.adaptive_stats.strategy_effectiveness)
            .sum::<f64>()
            / results.len() as f64;

        if avg_effectiveness < 0.7 {
            recommendations.push(format!(
                "🎯 Adaptive strategy effectiveness could be improved ({:.1}%) - review degree estimation accuracy",
                avg_effectiveness * 100.0
            ));
        }

        // Output recommendations
        if recommendations.is_empty() {
            writeln!(
                writer,
                "✅ No major performance issues detected. System is performing well!"
            )?;
        } else {
            for recommendation in recommendations {
                writeln!(writer, "  {}", recommendation)?;
            }
        }

        writeln!(writer)?;
        writeln!(writer, "  General Optimization Tips:")?;
        writeln!(
            writer,
            "    • Monitor system resource usage during peak loads"
        )?;
        writeln!(writer, "    • Adjust MemTable size based on write patterns")?;
        writeln!(
            writer,
            "    • Tune compression settings for your data characteristics"
        )?;
        writeln!(
            writer,
            "    • Consider workload-specific configuration profiles"
        )?;

        writeln!(writer)?;
        Ok(())
    }

    /// Write JSON report
    fn write_json_report<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        let report = JsonBenchmarkReport {
            timestamp: Utc::now(),
            results: results.iter().map(JsonBenchmarkResult::from).collect(),
        };

        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        writeln!(writer, "{}", json)?;
        Ok(())
    }

    /// Write CSV report
    fn write_csv_report<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        // Write CSV header
        writeln!(writer, "workload,total_ops,success_rate,throughput_ops_per_sec,avg_latency_ms,p95_latency_ms,p99_latency_ms,error_rate,data_throughput_mb_per_sec,delta_updates,pivot_updates,lock_free_success_rate,memory_pool_hit_rate,cache_hit_rate")?;

        // Write data rows
        for result in results {
            let summary =
                BenchmarkSummary::from_metrics(result.workload.to_string(), &result.metrics);
            writeln!(
                writer,
                "{},{},{:.4},{:.2},{:.4},{:.4},{:.4},{:.4},{:.4},{},{},{:.4},{:.4},{:.4}",
                result.workload,
                summary.total_operations,
                summary.success_rate,
                summary.throughput_ops_per_sec,
                summary.avg_latency_ms,
                summary.p95_latency_ms,
                summary.p99_latency_ms,
                summary.error_rate,
                summary.data_throughput_mb_per_sec,
                result.adaptive_stats.delta_updates,
                result.adaptive_stats.pivot_updates,
                result.lock_free_stats.success_rate,
                result.memory_stats.memory_pool_hit_rate,
                result.memory_stats.block_cache_hit_rate
            )?;
        }

        Ok(())
    }

    /// Write HTML report
    fn write_html_report<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        writeln!(writer, "<!DOCTYPE html>")?;
        writeln!(
            writer,
            "<html><head><title>Aster Database Benchmark Report</title>"
        )?;
        writeln!(writer, "<style>")?;
        writeln!(
            writer,
            "body {{ font-family: Arial, sans-serif; margin: 20px; }}"
        )?;
        writeln!(
            writer,
            "table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}"
        )?;
        writeln!(
            writer,
            "th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}"
        )?;
        writeln!(writer, "th {{ background-color: #f2f2f2; }}")?;
        writeln!(writer, ".metric {{ font-weight: bold; color: #333; }}")?;
        writeln!(writer, ".good {{ color: green; }}")?;
        writeln!(writer, ".warning {{ color: orange; }}")?;
        writeln!(writer, ".error {{ color: red; }}")?;
        writeln!(writer, "</style></head><body>")?;

        writeln!(writer, "<h1>Aster Database Benchmark Report</h1>")?;
        writeln!(
            writer,
            "<p>Generated: {}</p>",
            Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        )?;

        writeln!(writer, "<h2>Performance Summary</h2>")?;
        writeln!(writer, "<table>")?;
        writeln!(writer, "<tr><th>Workload</th><th>Ops/sec</th><th>Avg Latency (ms)</th><th>P95 Latency (ms)</th><th>Success Rate</th><th>Error Rate</th></tr>")?;

        for result in results {
            let summary =
                BenchmarkSummary::from_metrics(result.workload.to_string(), &result.metrics);
            let success_class = if summary.success_rate > 0.95 {
                "good"
            } else if summary.success_rate > 0.9 {
                "warning"
            } else {
                "error"
            };

            writeln!(writer, "<tr>")?;
            writeln!(writer, "<td>{}</td>", result.workload)?;
            writeln!(writer, "<td>{:.0}</td>", summary.throughput_ops_per_sec)?;
            writeln!(writer, "<td>{:.2}</td>", summary.avg_latency_ms)?;
            writeln!(writer, "<td>{:.2}</td>", summary.p95_latency_ms)?;
            writeln!(
                writer,
                "<td class=\"{}\">{:.1}%</td>",
                success_class,
                summary.success_rate * 100.0
            )?;
            writeln!(writer, "<td>{:.3}%</td>", summary.error_rate * 100.0)?;
            writeln!(writer, "</tr>")?;
        }

        writeln!(writer, "</table>")?;
        writeln!(writer, "</body></html>")?;

        Ok(())
    }

    /// Write Prometheus metrics format
    fn write_prometheus_report<W: Write>(
        &self,
        results: &[BenchmarkResults],
        writer: &mut W,
    ) -> io::Result<()> {
        writeln!(writer, "# Aster Database Benchmark Metrics")?;
        writeln!(
            writer,
            "# Generated: {}",
            Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        )?;
        writeln!(writer)?;

        for result in results {
            let workload_label = format!("workload=\"{}\"", result.workload);
            let metrics = &result.metrics;

            writeln!(writer, "# TYPE aster_benchmark_throughput gauge")?;
            writeln!(
                writer,
                "aster_benchmark_throughput{{{}}} {}",
                workload_label, metrics.operations_per_second
            )?;

            writeln!(writer, "# TYPE aster_benchmark_latency_avg gauge")?;
            writeln!(
                writer,
                "aster_benchmark_latency_avg{{{}}} {}",
                workload_label,
                (metrics.avg_read_latency_us + metrics.avg_write_latency_us) / 2.0
            )?;

            writeln!(writer, "# TYPE aster_benchmark_success_rate gauge")?;
            writeln!(
                writer,
                "aster_benchmark_success_rate{{{}}} {}",
                workload_label,
                metrics.success_rate()
            )?;

            writeln!(writer, "# TYPE aster_adaptive_delta_updates counter")?;
            writeln!(
                writer,
                "aster_adaptive_delta_updates{{{}}} {}",
                workload_label, result.adaptive_stats.delta_updates
            )?;

            writeln!(writer, "# TYPE aster_adaptive_pivot_updates counter")?;
            writeln!(
                writer,
                "aster_adaptive_pivot_updates{{{}}} {}",
                workload_label, result.adaptive_stats.pivot_updates
            )?;

            writeln!(writer, "# TYPE aster_lock_free_success_rate gauge")?;
            writeln!(
                writer,
                "aster_lock_free_success_rate{{{}}} {}",
                workload_label, result.lock_free_stats.success_rate
            )?;

            writeln!(writer)?;
        }

        Ok(())
    }
}

/// JSON serializable structures for JSON output
#[derive(Serialize, Deserialize)]
struct JsonBenchmarkReport {
    timestamp: DateTime<Utc>,
    results: Vec<JsonBenchmarkResult>,
}

#[derive(Serialize, Deserialize)]
struct JsonBenchmarkResult {
    workload: String,
    metrics: JsonBenchmarkMetrics,
    adaptive_stats: JsonAdaptiveStats,
    lock_free_stats: JsonLockFreeStats,
    memory_stats: JsonMemoryStats,
}

#[derive(Serialize, Deserialize)]
struct JsonBenchmarkMetrics {
    total_operations: u64,
    success_rate: f64,
    throughput_ops_per_sec: f64,
    avg_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
    error_rate: f64,
}

#[derive(Serialize, Deserialize)]
struct JsonAdaptiveStats {
    delta_updates: u64,
    pivot_updates: u64,
    effectiveness: f64,
}

#[derive(Serialize, Deserialize)]
struct JsonLockFreeStats {
    success_rate: f64,
    contention_events: usize,
    throughput_ops_per_sec: f64,
}

#[derive(Serialize, Deserialize)]
struct JsonMemoryStats {
    peak_memory_mb: f64,
    pool_hit_rate: f64,
    cache_hit_rate: f64,
    compression_ratio: f64,
}

impl From<&BenchmarkResults> for JsonBenchmarkResult {
    fn from(result: &BenchmarkResults) -> Self {
        Self {
            workload: result.workload.to_string(),
            metrics: JsonBenchmarkMetrics {
                total_operations: result.metrics.total_operations(),
                success_rate: result.metrics.success_rate(),
                throughput_ops_per_sec: result.metrics.operations_per_second,
                avg_latency_ms: (result.metrics.avg_read_latency_us
                    + result.metrics.avg_write_latency_us)
                    / 2000.0,
                p95_latency_ms: result.metrics.p95_latency_us / 1000.0,
                p99_latency_ms: result.metrics.p99_latency_us / 1000.0,
                error_rate: result.metrics.overall_error_rate,
            },
            adaptive_stats: JsonAdaptiveStats {
                delta_updates: result.adaptive_stats.delta_updates,
                pivot_updates: result.adaptive_stats.pivot_updates,
                effectiveness: result.adaptive_stats.strategy_effectiveness,
            },
            lock_free_stats: JsonLockFreeStats {
                success_rate: result.lock_free_stats.success_rate,
                contention_events: result.lock_free_stats.contention_events,
                throughput_ops_per_sec: result.lock_free_stats.throughput_ops_per_sec,
            },
            memory_stats: JsonMemoryStats {
                peak_memory_mb: result.memory_stats.peak_memory_mb,
                pool_hit_rate: result.memory_stats.memory_pool_hit_rate,
                cache_hit_rate: result.memory_stats.block_cache_hit_rate,
                compression_ratio: result.memory_stats.compression_ratio,
            },
        }
    }
}
