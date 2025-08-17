use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

use crate::benchmarks::{BenchmarkResults, BenchmarkSummary};

/// Format number with commas for better readability
fn format_number(num: u64) -> String {
    if num < 1000 {
        return num.to_string();
    }

    let mut formatted = String::new();
    let s = num.to_string();
    let chars: Vec<char> = s.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(c);
    }

    formatted
}

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
        writeln!(writer, "<html lang=\"en\">")?;
        writeln!(writer, "<head>")?;
        writeln!(writer, "    <meta charset=\"UTF-8\">")?;
        writeln!(
            writer,
            "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">"
        )?;
        writeln!(writer, "    <title>Aster Database Benchmark Report</title>")?;
        writeln!(
            writer,
            "    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>"
        )?;
        writeln!(writer, "    <style>")?;

        // Enhanced CSS styling
        writeln!(writer, "        :root {{")?;
        writeln!(writer, "            --primary-color: #2563eb;")?;
        writeln!(writer, "            --secondary-color: #64748b;")?;
        writeln!(writer, "            --success-color: #10b981;")?;
        writeln!(writer, "            --warning-color: #f59e0b;")?;
        writeln!(writer, "            --error-color: #ef4444;")?;
        writeln!(writer, "            --bg-color: #f8fafc;")?;
        writeln!(writer, "            --card-bg: #ffffff;")?;
        writeln!(writer, "            --border-color: #e2e8f0;")?;
        writeln!(writer, "            --text-color: #1e293b;")?;
        writeln!(writer, "            --text-secondary: #64748b;")?;
        writeln!(writer, "        }}")?;

        writeln!(
            writer,
            "        * {{ box-sizing: border-box; margin: 0; padding: 0; }}"
        )?;
        writeln!(writer, "        body {{")?;
        writeln!(writer, "            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;")?;
        writeln!(writer, "            line-height: 1.6;")?;
        writeln!(writer, "            color: var(--text-color);")?;
        writeln!(writer, "            background-color: var(--bg-color);")?;
        writeln!(writer, "            padding: 20px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .container {{")?;
        writeln!(writer, "            max-width: 1200px;")?;
        writeln!(writer, "            margin: 0 auto;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .header {{")?;
        writeln!(
            writer,
            "            background: linear-gradient(135deg, var(--primary-color), #1d4ed8);"
        )?;
        writeln!(writer, "            color: white;")?;
        writeln!(writer, "            padding: 40px;")?;
        writeln!(writer, "            border-radius: 16px;")?;
        writeln!(writer, "            margin-bottom: 30px;")?;
        writeln!(
            writer,
            "            box-shadow: 0 10px 25px rgba(37, 99, 235, 0.2);"
        )?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .header h1 {{")?;
        writeln!(writer, "            font-size: 2.5rem;")?;
        writeln!(writer, "            font-weight: 700;")?;
        writeln!(writer, "            margin-bottom: 10px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .header p {{")?;
        writeln!(writer, "            font-size: 1.1rem;")?;
        writeln!(writer, "            opacity: 0.9;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .metrics-grid {{")?;
        writeln!(writer, "            display: grid;")?;
        writeln!(
            writer,
            "            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));"
        )?;
        writeln!(writer, "            gap: 20px;")?;
        writeln!(writer, "            margin-bottom: 30px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .card {{")?;
        writeln!(writer, "            background: var(--card-bg);")?;
        writeln!(writer, "            border-radius: 12px;")?;
        writeln!(writer, "            padding: 24px;")?;
        writeln!(
            writer,
            "            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);"
        )?;
        writeln!(writer, "            border: 1px solid var(--border-color);")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .card h3 {{")?;
        writeln!(writer, "            font-size: 1.25rem;")?;
        writeln!(writer, "            font-weight: 600;")?;
        writeln!(writer, "            margin-bottom: 16px;")?;
        writeln!(writer, "            color: var(--text-color);")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .metric-value {{")?;
        writeln!(writer, "            font-size: 2rem;")?;
        writeln!(writer, "            font-weight: 700;")?;
        writeln!(writer, "            margin-bottom: 8px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .metric-label {{")?;
        writeln!(writer, "            font-size: 0.875rem;")?;
        writeln!(writer, "            color: var(--text-secondary);")?;
        writeln!(writer, "            text-transform: uppercase;")?;
        writeln!(writer, "            font-weight: 500;")?;
        writeln!(writer, "            letter-spacing: 0.5px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .performance-table {{")?;
        writeln!(writer, "            width: 100%;")?;
        writeln!(writer, "            border-collapse: collapse;")?;
        writeln!(writer, "            background: var(--card-bg);")?;
        writeln!(writer, "            border-radius: 12px;")?;
        writeln!(writer, "            overflow: hidden;")?;
        writeln!(
            writer,
            "            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);"
        )?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .performance-table th {{")?;
        writeln!(writer, "            background: var(--primary-color);")?;
        writeln!(writer, "            color: white;")?;
        writeln!(writer, "            padding: 16px;")?;
        writeln!(writer, "            text-align: left;")?;
        writeln!(writer, "            font-weight: 600;")?;
        writeln!(writer, "            font-size: 0.875rem;")?;
        writeln!(writer, "            text-transform: uppercase;")?;
        writeln!(writer, "            letter-spacing: 0.5px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .performance-table td {{")?;
        writeln!(writer, "            padding: 16px;")?;
        writeln!(
            writer,
            "            border-bottom: 1px solid var(--border-color);"
        )?;
        writeln!(writer, "            font-size: 0.95rem;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .performance-table tr:hover {{")?;
        writeln!(writer, "            background-color: #f8fafc;")?;
        writeln!(writer, "        }}")?;

        writeln!(
            writer,
            "        .status-good {{ color: var(--success-color); font-weight: 600; }}"
        )?;
        writeln!(
            writer,
            "        .status-warning {{ color: var(--warning-color); font-weight: 600; }}"
        )?;
        writeln!(
            writer,
            "        .status-error {{ color: var(--error-color); font-weight: 600; }}"
        )?;

        writeln!(writer, "        .chart-container {{")?;
        writeln!(writer, "            position: relative;")?;
        writeln!(writer, "            height: 400px;")?;
        writeln!(writer, "            margin: 20px 0;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .section {{")?;
        writeln!(writer, "            margin-bottom: 40px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .section h2 {{")?;
        writeln!(writer, "            font-size: 1.75rem;")?;
        writeln!(writer, "            font-weight: 600;")?;
        writeln!(writer, "            margin-bottom: 20px;")?;
        writeln!(writer, "            color: var(--text-color);")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .badge {{")?;
        writeln!(writer, "            display: inline-block;")?;
        writeln!(writer, "            padding: 4px 12px;")?;
        writeln!(writer, "            border-radius: 20px;")?;
        writeln!(writer, "            font-size: 0.75rem;")?;
        writeln!(writer, "            font-weight: 600;")?;
        writeln!(writer, "            text-transform: uppercase;")?;
        writeln!(writer, "            letter-spacing: 0.5px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .badge-success {{")?;
        writeln!(
            writer,
            "            background-color: rgba(16, 185, 129, 0.1);"
        )?;
        writeln!(writer, "            color: var(--success-color);")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .badge-warning {{")?;
        writeln!(
            writer,
            "            background-color: rgba(245, 158, 11, 0.1);"
        )?;
        writeln!(writer, "            color: var(--warning-color);")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .disabled-pill {{")?;
        writeln!(writer, "            display: inline-block;")?;
        writeln!(writer, "            padding: 8px 16px;")?;
        writeln!(
            writer,
            "            background-color: rgba(239, 68, 68, 0.1);"
        )?;
        writeln!(writer, "            color: var(--error-color);")?;
        writeln!(
            writer,
            "            border: 1px solid rgba(239, 68, 68, 0.3);"
        )?;
        writeln!(writer, "            border-radius: 20px;")?;
        writeln!(writer, "            font-size: 0.9rem;")?;
        writeln!(writer, "            font-weight: 600;")?;
        writeln!(writer, "            text-transform: uppercase;")?;
        writeln!(writer, "            letter-spacing: 0.5px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .disabled-section {{")?;
        writeln!(writer, "            display: flex;")?;
        writeln!(writer, "            align-items: center;")?;
        writeln!(writer, "            justify-content: center;")?;
        writeln!(writer, "            min-height: 200px;")?;
        writeln!(
            writer,
            "            background-color: rgba(254, 242, 242, 0.8);"
        )?;
        writeln!(writer, "            border-radius: 8px;")?;
        writeln!(
            writer,
            "            border: 2px dashed rgba(239, 68, 68, 0.3);"
        )?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .disabled-message {{")?;
        writeln!(writer, "            text-align: center;")?;
        writeln!(writer, "            color: var(--text-secondary);")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .disabled-message p {{")?;
        writeln!(writer, "            margin-top: 12px;")?;
        writeln!(writer, "            font-size: 0.9rem;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .disabled-message code {{")?;
        writeln!(
            writer,
            "            background-color: rgba(100, 116, 139, 0.1);"
        )?;
        writeln!(writer, "            padding: 2px 6px;")?;
        writeln!(writer, "            border-radius: 4px;")?;
        writeln!(
            writer,
            "            font-family: 'Monaco', 'Consolas', monospace;"
        )?;
        writeln!(writer, "            font-size: 0.85rem;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        .recommendations {{")?;
        writeln!(
            writer,
            "            background: linear-gradient(135deg, #f0fdf4, #ecfdf5);"
        )?;
        writeln!(
            writer,
            "            border-left: 4px solid var(--success-color);"
        )?;
        writeln!(writer, "            padding: 20px;")?;
        writeln!(writer, "            border-radius: 8px;")?;
        writeln!(writer, "            margin-top: 30px;")?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "        @media (max-width: 768px) {{")?;
        writeln!(writer, "            .container {{ padding: 10px; }}")?;
        writeln!(writer, "            .header {{ padding: 20px; }}")?;
        writeln!(writer, "            .header h1 {{ font-size: 2rem; }}")?;
        writeln!(
            writer,
            "            .metrics-grid {{ grid-template-columns: 1fr; }}"
        )?;
        writeln!(
            writer,
            "            .performance-table {{ font-size: 0.8rem; }}"
        )?;
        writeln!(
            writer,
            "            .performance-table th, .performance-table td {{ padding: 8px; }}"
        )?;
        writeln!(writer, "        }}")?;

        writeln!(writer, "    </style>")?;
        writeln!(writer, "</head>")?;
        writeln!(writer, "<body>")?;
        writeln!(writer, "    <div class=\"container\">")?;

        // Header section
        writeln!(writer, "        <div class=\"header\">")?;
        writeln!(
            writer,
            "            <h1>🚀 Aster Database Benchmark Report</h1>"
        )?;
        writeln!(
            writer,
            "            <p>Generated on {} | {} workload{} analyzed</p>",
            Utc::now().format("%B %d, %Y at %H:%M:%S UTC"),
            results.len(),
            if results.len() == 1 { "" } else { "s" }
        )?;
        writeln!(writer, "        </div>")?;

        // Key metrics overview
        let total_ops: u64 = results.iter().map(|r| r.metrics.total_operations()).sum();
        let avg_throughput: f64 = results
            .iter()
            .map(|r| r.metrics.operations_per_second)
            .sum::<f64>()
            / results.len() as f64;
        let avg_success_rate: f64 = results
            .iter()
            .map(|r| r.metrics.success_rate())
            .sum::<f64>()
            / results.len() as f64;
        let peak_memory: f64 = results
            .iter()
            .map(|r| r.memory_stats.peak_memory_mb)
            .fold(0.0, f64::max);

        writeln!(writer, "        <div class=\"metrics-grid\">")?;
        writeln!(writer, "            <div class=\"card\">")?;
        writeln!(writer, "                <h3>📊 Total Operations</h3>")?;
        writeln!(
            writer,
            "                <div class=\"metric-value\">{}</div>",
            format_number(total_ops)
        )?;
        writeln!(
            writer,
            "                <div class=\"metric-label\">Operations Executed</div>"
        )?;
        writeln!(writer, "            </div>")?;

        writeln!(writer, "            <div class=\"card\">")?;
        writeln!(writer, "                <h3>⚡ Average Throughput</h3>")?;
        writeln!(
            writer,
            "                <div class=\"metric-value\">{}</div>",
            format_number(avg_throughput as u64)
        )?;
        writeln!(
            writer,
            "                <div class=\"metric-label\">Operations per Second</div>"
        )?;
        writeln!(writer, "            </div>")?;

        writeln!(writer, "            <div class=\"card\">")?;
        writeln!(writer, "                <h3>✅ Success Rate</h3>")?;
        writeln!(
            writer,
            "                <div class=\"metric-value status-good\">{:.1}%</div>",
            avg_success_rate * 100.0
        )?;
        writeln!(
            writer,
            "                <div class=\"metric-label\">Successful Operations</div>"
        )?;
        writeln!(writer, "            </div>")?;

        writeln!(writer, "            <div class=\"card\">")?;
        writeln!(writer, "                <h3>💾 Peak Memory</h3>")?;
        if peak_memory > 0.0 {
            writeln!(
                writer,
                "                <div class=\"metric-value\">{:.1} MB</div>",
                peak_memory
            )?;
            writeln!(
                writer,
                "                <div class=\"metric-label\">Memory Usage</div>"
            )?;
        } else {
            writeln!(writer, "                <div class=\"metric-value\"><span class=\"disabled-pill\">Disabled</span></div>")?;
            writeln!(
                writer,
                "                <div class=\"metric-label\">Use --memory-analysis to enable</div>"
            )?;
        }
        writeln!(writer, "            </div>")?;
        writeln!(writer, "        </div>")?;

        // Performance summary table
        writeln!(writer, "        <div class=\"section\">")?;
        writeln!(writer, "            <h2>📈 Performance Summary</h2>")?;
        writeln!(writer, "            <table class=\"performance-table\">")?;
        writeln!(writer, "                <thead>")?;
        writeln!(writer, "                    <tr>")?;
        writeln!(writer, "                        <th>Workload</th>")?;
        writeln!(writer, "                        <th>Throughput</th>")?;
        writeln!(writer, "                        <th>Avg Latency</th>")?;
        writeln!(writer, "                        <th>P95 Latency</th>")?;
        writeln!(writer, "                        <th>Success Rate</th>")?;
        writeln!(writer, "                        <th>Status</th>")?;
        writeln!(writer, "                    </tr>")?;
        writeln!(writer, "                </thead>")?;
        writeln!(writer, "                <tbody>")?;

        for result in results {
            let summary =
                BenchmarkSummary::from_metrics(result.workload.to_string(), &result.metrics);
            let (status_class, status_text, badge_class) = if summary.success_rate > 0.95 {
                ("status-good", "Excellent", "badge-success")
            } else if summary.success_rate > 0.9 {
                ("status-warning", "Good", "badge-warning")
            } else {
                ("status-error", "Poor", "badge-error")
            };

            writeln!(writer, "                    <tr>")?;
            writeln!(
                writer,
                "                        <td><strong>{}</strong></td>",
                result.workload
            )?;
            writeln!(
                writer,
                "                        <td>{} ops/sec</td>",
                format_number(summary.throughput_ops_per_sec as u64)
            )?;
            writeln!(
                writer,
                "                        <td>{:.2} ms</td>",
                summary.avg_latency_ms
            )?;
            writeln!(
                writer,
                "                        <td>{:.2} ms</td>",
                summary.p95_latency_ms
            )?;
            writeln!(
                writer,
                "                        <td class=\"{}\">{:.1}%</td>",
                status_class,
                summary.success_rate * 100.0
            )?;
            writeln!(
                writer,
                "                        <td><span class=\"badge {}\">{}</span></td>",
                badge_class, status_text
            )?;
            writeln!(writer, "                    </tr>")?;
        }

        writeln!(writer, "                </tbody>")?;
        writeln!(writer, "            </table>")?;
        writeln!(writer, "        </div>")?;

        // Throughput chart
        writeln!(writer, "        <div class=\"section\">")?;
        writeln!(writer, "            <div class=\"card\">")?;
        writeln!(writer, "                <h3>📊 Throughput Comparison</h3>")?;
        writeln!(writer, "                <div class=\"chart-container\">")?;
        writeln!(
            writer,
            "                    <canvas id=\"throughputChart\"></canvas>"
        )?;
        writeln!(writer, "                </div>")?;
        writeln!(writer, "            </div>")?;
        writeln!(writer, "        </div>")?;

        // Latency chart
        writeln!(writer, "        <div class=\"section\">")?;
        writeln!(writer, "            <div class=\"card\">")?;
        writeln!(writer, "                <h3>⏱️ Latency Analysis</h3>")?;
        writeln!(writer, "                <div class=\"chart-container\">")?;
        writeln!(
            writer,
            "                    <canvas id=\"latencyChart\"></canvas>"
        )?;
        writeln!(writer, "                </div>")?;
        writeln!(writer, "            </div>")?;
        writeln!(writer, "        </div>")?;

        // System resource usage if memory analysis is enabled
        writeln!(writer, "        <div class=\"section\">")?;
        writeln!(writer, "            <div class=\"card\">")?;
        writeln!(
            writer,
            "                <h3>💾 System Resource Utilization</h3>"
        )?;
        if self.include_memory_analysis && peak_memory > 0.0 {
            writeln!(writer, "                <div class=\"chart-container\">")?;
            writeln!(
                writer,
                "                    <canvas id=\"memoryChart\"></canvas>"
            )?;
            writeln!(writer, "                </div>")?;
        } else {
            writeln!(writer, "                <div class=\"disabled-section\">")?;
            writeln!(
                writer,
                "                    <div class=\"disabled-message\">"
            )?;
            writeln!(writer, "                        <span class=\"disabled-pill\">Memory Analysis Disabled</span>")?;
            writeln!(writer, "                        <p>Use <code>--memory-analysis</code> flag to enable detailed memory tracking and charts.</p>")?;
            writeln!(writer, "                    </div>")?;
            writeln!(writer, "                </div>")?;
        }
        writeln!(writer, "            </div>")?;
        writeln!(writer, "        </div>")?;

        // Recommendations section
        writeln!(writer, "        <div class=\"recommendations\">")?;
        writeln!(
            writer,
            "            <h3>💡 Performance Recommendations</h3>"
        )?;

        // Analyze and provide recommendations
        let avg_cache_hit_rate: f64 = results
            .iter()
            .map(|r| r.memory_stats.block_cache_hit_rate)
            .sum::<f64>()
            / results.len() as f64;
        let avg_lock_free_success: f64 = results
            .iter()
            .map(|r| r.lock_free_stats.success_rate)
            .sum::<f64>()
            / results.len() as f64;

        if avg_success_rate > 0.95 && avg_cache_hit_rate > 0.85 && avg_lock_free_success > 0.95 {
            writeln!(writer, "            <p>✅ <strong>Excellent Performance!</strong> Your Aster database is performing optimally across all metrics.</p>")?;
        } else {
            writeln!(writer, "            <ul>")?;
            if avg_cache_hit_rate < 0.8 {
                writeln!(writer, "                <li>📈 Consider increasing block cache size (current hit rate: {:.1}%)</li>", avg_cache_hit_rate * 100.0)?;
            }
            if avg_lock_free_success < 0.9 {
                writeln!(writer, "                <li>🔧 High contention detected - consider reducing concurrency or optimizing access patterns</li>")?;
            }
            writeln!(
                writer,
                "                <li>📊 Monitor system resource usage during peak loads</li>"
            )?;
            writeln!(
                writer,
                "                <li>⚙️ Adjust MemTable size based on write patterns</li>"
            )?;
            writeln!(writer, "                <li>🗜️ Tune compression settings for your data characteristics</li>")?;
            writeln!(writer, "            </ul>")?;
        }

        writeln!(writer, "        </div>")?;
        writeln!(writer, "    </div>")?;

        // JavaScript for charts
        writeln!(writer, "    <script>")?;
        writeln!(writer, "        // Throughput Chart")?;
        writeln!(writer, "        const throughputCtx = document.getElementById('throughputChart').getContext('2d');")?;
        writeln!(writer, "        new Chart(throughputCtx, {{")?;
        writeln!(writer, "            type: 'bar',")?;
        writeln!(writer, "            data: {{")?;
        write!(writer, "                labels: [")?;
        for (i, result) in results.iter().enumerate() {
            if i > 0 {
                write!(writer, ", ")?;
            }
            write!(writer, "'{}'", result.workload)?;
        }
        writeln!(writer, "],")?;
        writeln!(writer, "                datasets: [{{")?;
        writeln!(writer, "                    label: 'Throughput (ops/sec)',")?;
        write!(writer, "                    data: [")?;
        for (i, result) in results.iter().enumerate() {
            if i > 0 {
                write!(writer, ", ")?;
            }
            write!(writer, "{:.0}", result.metrics.operations_per_second)?;
        }
        writeln!(writer, "],")?;
        writeln!(
            writer,
            "                    backgroundColor: 'rgba(37, 99, 235, 0.8)',"
        )?;
        writeln!(
            writer,
            "                    borderColor: 'rgba(37, 99, 235, 1)',"
        )?;
        writeln!(writer, "                    borderWidth: 1")?;
        writeln!(writer, "                }}]")?;
        writeln!(writer, "            }},")?;
        writeln!(writer, "            options: {{")?;
        writeln!(writer, "                responsive: true,")?;
        writeln!(writer, "                maintainAspectRatio: false,")?;
        writeln!(writer, "                scales: {{")?;
        writeln!(writer, "                    y: {{ beginAtZero: true }}")?;
        writeln!(writer, "                }}")?;
        writeln!(writer, "            }}")?;
        writeln!(writer, "        }});")?;

        writeln!(writer, "        // Latency Chart")?;
        writeln!(
            writer,
            "        const latencyCtx = document.getElementById('latencyChart').getContext('2d');"
        )?;
        writeln!(writer, "        new Chart(latencyCtx, {{")?;
        writeln!(writer, "            type: 'line',")?;
        writeln!(writer, "            data: {{")?;
        write!(writer, "                labels: [")?;
        for (i, result) in results.iter().enumerate() {
            if i > 0 {
                write!(writer, ", ")?;
            }
            write!(writer, "'{}'", result.workload)?;
        }
        writeln!(writer, "],")?;
        writeln!(writer, "                datasets: [{{")?;
        writeln!(writer, "                    label: 'Average Latency (µs)',")?;
        write!(writer, "                    data: [")?;
        for (i, result) in results.iter().enumerate() {
            if i > 0 {
                write!(writer, ", ")?;
            }
            let avg_latency =
                (result.metrics.avg_read_latency_us + result.metrics.avg_write_latency_us) / 2.0;
            write!(writer, "{:.2}", avg_latency)?;
        }
        writeln!(writer, "],")?;
        writeln!(
            writer,
            "                    borderColor: 'rgba(16, 185, 129, 1)',"
        )?;
        writeln!(
            writer,
            "                    backgroundColor: 'rgba(16, 185, 129, 0.1)',"
        )?;
        writeln!(writer, "                    tension: 0.4")?;
        writeln!(writer, "                }}, {{")?;
        writeln!(writer, "                    label: 'P95 Latency (µs)',")?;
        write!(writer, "                    data: [")?;
        for (i, result) in results.iter().enumerate() {
            if i > 0 {
                write!(writer, ", ")?;
            }
            write!(writer, "{:.2}", result.metrics.p95_latency_us)?;
        }
        writeln!(writer, "],")?;
        writeln!(
            writer,
            "                    borderColor: 'rgba(245, 158, 11, 1)',"
        )?;
        writeln!(
            writer,
            "                    backgroundColor: 'rgba(245, 158, 11, 0.1)',"
        )?;
        writeln!(writer, "                    tension: 0.4")?;
        writeln!(writer, "                }}]")?;
        writeln!(writer, "            }},")?;
        writeln!(writer, "            options: {{")?;
        writeln!(writer, "                responsive: true,")?;
        writeln!(writer, "                maintainAspectRatio: false,")?;
        writeln!(writer, "                scales: {{")?;
        writeln!(writer, "                    y: {{ beginAtZero: true }}")?;
        writeln!(writer, "                }}")?;
        writeln!(writer, "            }}")?;
        writeln!(writer, "        }});")?;

        if self.include_memory_analysis && peak_memory > 0.0 {
            writeln!(writer, "        // Memory Chart")?;
            writeln!(writer, "        const memoryCtx = document.getElementById('memoryChart').getContext('2d');")?;
            writeln!(writer, "        new Chart(memoryCtx, {{")?;
            writeln!(writer, "            type: 'doughnut',")?;
            writeln!(writer, "            data: {{")?;
            writeln!(writer, "                labels: ['Cache Hits', 'Cache Misses', 'Pool Hits', 'Pool Misses'],")?;
            writeln!(writer, "                datasets: [{{")?;
            let avg_cache_hit = avg_cache_hit_rate * 100.0;
            let avg_pool_hit: f64 = results
                .iter()
                .map(|r| r.memory_stats.memory_pool_hit_rate)
                .sum::<f64>()
                / results.len() as f64
                * 100.0;
            write!(
                writer,
                "                    data: [{:.1}, {:.1}, {:.1}, {:.1}],",
                avg_cache_hit,
                100.0 - avg_cache_hit,
                avg_pool_hit,
                100.0 - avg_pool_hit
            )?;
            writeln!(writer, "                    backgroundColor: [")?;
            writeln!(writer, "                        'rgba(16, 185, 129, 0.8)',")?;
            writeln!(writer, "                        'rgba(239, 68, 68, 0.8)',")?;
            writeln!(writer, "                        'rgba(37, 99, 235, 0.8)',")?;
            writeln!(writer, "                        'rgba(245, 158, 11, 0.8)'")?;
            writeln!(writer, "                    ]")?;
            writeln!(writer, "                }}]")?;
            writeln!(writer, "            }},")?;
            writeln!(writer, "            options: {{")?;
            writeln!(writer, "                responsive: true,")?;
            writeln!(writer, "                maintainAspectRatio: false")?;
            writeln!(writer, "            }}")?;
            writeln!(writer, "        }});")?;
        }

        writeln!(writer, "    </script>")?;
        writeln!(writer, "</body>")?;
        writeln!(writer, "</html>")?;

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
