use std::time::Duration;

/// Core benchmark metrics collected during testing
#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    // Operation counts
    pub successful_reads: u64,
    pub failed_reads: u64,
    pub successful_writes: u64,
    pub failed_writes: u64,

    // Timing metrics
    pub total_duration: Duration,
    pub total_read_time: Duration,
    pub total_write_time: Duration,

    // Throughput metrics
    pub operations_per_second: f64,
    pub reads_per_second: f64,
    pub writes_per_second: f64,

    // Latency metrics
    pub avg_read_latency_us: f64,
    pub avg_write_latency_us: f64,
    pub p50_latency_us: f64,
    pub p95_latency_us: f64,
    pub p99_latency_us: f64,

    // Data transfer metrics
    pub total_data_read: usize,    // in bytes
    pub total_data_written: usize, // in bytes
    pub avg_read_size: f64,        // in bytes
    pub avg_write_size: f64,       // in bytes

    // Error rates
    pub read_error_rate: f64,
    pub write_error_rate: f64,
    pub overall_error_rate: f64,

    // Resource utilization
    pub cpu_utilization: f64,    // percentage
    pub memory_utilization: f64, // percentage
    pub disk_io_rate: f64,       // bytes per second
    pub network_io_rate: f64,    // bytes per second

    // Latency distribution (for percentile calculations)
    pub latency_samples: Vec<u64>, // in microseconds
}

impl BenchmarkMetrics {
    pub fn new() -> Self {
        Self {
            successful_reads: 0,
            failed_reads: 0,
            successful_writes: 0,
            failed_writes: 0,
            total_duration: Duration::new(0, 0),
            total_read_time: Duration::new(0, 0),
            total_write_time: Duration::new(0, 0),
            operations_per_second: 0.0,
            reads_per_second: 0.0,
            writes_per_second: 0.0,
            avg_read_latency_us: 0.0,
            avg_write_latency_us: 0.0,
            p50_latency_us: 0.0,
            p95_latency_us: 0.0,
            p99_latency_us: 0.0,
            total_data_read: 0,
            total_data_written: 0,
            avg_read_size: 0.0,
            avg_write_size: 0.0,
            read_error_rate: 0.0,
            write_error_rate: 0.0,
            overall_error_rate: 0.0,
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            disk_io_rate: 0.0,
            network_io_rate: 0.0,
            latency_samples: Vec::new(),
        }
    }

    /// Calculate derived metrics from raw counts and timings
    pub fn calculate_derived_metrics(&mut self) {
        let total_seconds = self.total_duration.as_secs_f64();

        if total_seconds > 0.0 {
            // Throughput calculations
            let total_operations = self.successful_reads
                + self.failed_reads
                + self.successful_writes
                + self.failed_writes;
            self.operations_per_second = total_operations as f64 / total_seconds;
            self.reads_per_second =
                (self.successful_reads + self.failed_reads) as f64 / total_seconds;
            self.writes_per_second =
                (self.successful_writes + self.failed_writes) as f64 / total_seconds;

            // I/O rate calculations
            self.disk_io_rate =
                (self.total_data_read + self.total_data_written) as f64 / total_seconds;
        }

        // Latency calculations
        if self.successful_reads > 0 {
            self.avg_read_latency_us =
                self.total_read_time.as_micros() as f64 / self.successful_reads as f64;
        }

        if self.successful_writes > 0 {
            self.avg_write_latency_us =
                self.total_write_time.as_micros() as f64 / self.successful_writes as f64;
        }

        // Data size calculations
        if self.successful_reads > 0 {
            self.avg_read_size = self.total_data_read as f64 / self.successful_reads as f64;
        }

        if self.successful_writes > 0 {
            self.avg_write_size = self.total_data_written as f64 / self.successful_writes as f64;
        }

        // Error rate calculations
        let total_reads = self.successful_reads + self.failed_reads;
        if total_reads > 0 {
            self.read_error_rate = self.failed_reads as f64 / total_reads as f64;
        }

        let total_writes = self.successful_writes + self.failed_writes;
        if total_writes > 0 {
            self.write_error_rate = self.failed_writes as f64 / total_writes as f64;
        }

        let total_operations = total_reads + total_writes;
        if total_operations > 0 {
            self.overall_error_rate =
                (self.failed_reads + self.failed_writes) as f64 / total_operations as f64;
        }

        // Calculate percentiles from latency samples
        self.calculate_latency_percentiles();
    }

    /// Calculate latency percentiles from samples
    fn calculate_latency_percentiles(&mut self) {
        if self.latency_samples.is_empty() {
            return;
        }

        let mut sorted_samples = self.latency_samples.clone();
        sorted_samples.sort_unstable();

        let len = sorted_samples.len();

        // Calculate percentiles
        self.p50_latency_us = Self::percentile(&sorted_samples, 50.0);
        self.p95_latency_us = Self::percentile(&sorted_samples, 95.0);
        self.p99_latency_us = Self::percentile(&sorted_samples, 99.0);
    }

    /// Calculate a specific percentile from sorted samples
    fn percentile(sorted_samples: &[u64], percentile: f64) -> f64 {
        if sorted_samples.is_empty() {
            return 0.0;
        }

        let index = (percentile / 100.0) * (sorted_samples.len() - 1) as f64;
        let lower_index = index.floor() as usize;
        let upper_index = index.ceil() as usize;

        if lower_index == upper_index {
            sorted_samples[lower_index] as f64
        } else {
            let lower_value = sorted_samples[lower_index] as f64;
            let upper_value = sorted_samples[upper_index] as f64;
            let weight = index - lower_index as f64;
            lower_value + weight * (upper_value - lower_value)
        }
    }

    /// Add a latency sample for percentile calculation
    pub fn add_latency_sample(&mut self, latency_us: u64) {
        self.latency_samples.push(latency_us);
    }

    /// Merge metrics from another benchmark run
    pub fn merge(&mut self, other: BenchmarkMetrics) {
        self.successful_reads += other.successful_reads;
        self.failed_reads += other.failed_reads;
        self.successful_writes += other.successful_writes;
        self.failed_writes += other.failed_writes;

        self.total_read_time += other.total_read_time;
        self.total_write_time += other.total_write_time;

        self.total_data_read += other.total_data_read;
        self.total_data_written += other.total_data_written;

        self.latency_samples.extend(other.latency_samples);

        // Duration and derived metrics should be recalculated after merging
    }

    /// Get total number of operations
    pub fn total_operations(&self) -> u64 {
        self.successful_reads + self.failed_reads + self.successful_writes + self.failed_writes
    }

    /// Get total successful operations
    pub fn successful_operations(&self) -> u64 {
        self.successful_reads + self.successful_writes
    }

    /// Get total failed operations
    pub fn failed_operations(&self) -> u64 {
        self.failed_reads + self.failed_writes
    }

    /// Get overall success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.total_operations();
        if total > 0 {
            self.successful_operations() as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Get read/write ratio
    pub fn read_write_ratio(&self) -> f64 {
        let total_reads = self.successful_reads + self.failed_reads;
        let total_writes = self.successful_writes + self.failed_writes;

        if total_writes > 0 {
            total_reads as f64 / total_writes as f64
        } else if total_reads > 0 {
            f64::INFINITY
        } else {
            0.0
        }
    }

    /// Get efficiency score (successful ops per second)
    pub fn efficiency_score(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.successful_operations() as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

impl Default for BenchmarkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary statistics for a benchmark run
#[derive(Debug, Clone)]
pub struct BenchmarkSummary {
    pub workload_name: String,
    pub total_operations: u64,
    pub success_rate: f64,
    pub throughput_ops_per_sec: f64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub error_rate: f64,
    pub data_throughput_mb_per_sec: f64,
}

impl BenchmarkSummary {
    pub fn from_metrics(workload_name: String, metrics: &BenchmarkMetrics) -> Self {
        Self {
            workload_name,
            total_operations: metrics.total_operations(),
            success_rate: metrics.success_rate(),
            throughput_ops_per_sec: metrics.operations_per_second,
            avg_latency_ms: (metrics.avg_read_latency_us + metrics.avg_write_latency_us) / 2000.0, // Convert to ms
            p95_latency_ms: metrics.p95_latency_us / 1000.0,
            p99_latency_ms: metrics.p99_latency_us / 1000.0,
            error_rate: metrics.overall_error_rate,
            data_throughput_mb_per_sec: metrics.disk_io_rate / (1024.0 * 1024.0),
        }
    }
}
