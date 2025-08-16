//! Comprehensive monitoring and metrics system for Aster database
//!
//! This module provides centralized metrics collection, aggregation, and reporting
//! for all components of the Aster graph database. It includes:
//! - Real-time performance metrics
//! - Resource utilization tracking
//! - Health monitoring and alerting
//! - Historical data collection
//! - Prometheus-compatible metric export

use crate::query::QueryStats;
use crate::recovery::RecoveryStats;
use crate::storage::{
    adaptive_updates::AdaptiveStats,
    block_cache::CacheStats,
    compaction::CompactionStats,
    memtable::MemTableStats,
    poly_lsm::{LevelStats, PolyLSMStats},
    property_store::PropertyStoreStats,
    storage_manager::StorageStats,
};
use crate::transaction::TransactionManagerStats;
use crate::{Result, Timestamp};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Comprehensive database metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetrics {
    /// System-level metrics
    pub system: SystemMetrics,
    /// Storage layer metrics
    pub storage: StorageMetrics,
    /// Transaction system metrics
    pub transactions: TransactionMetrics,
    /// Query engine metrics
    pub queries: QueryMetrics,
    /// Recovery system metrics
    pub recovery: RecoveryMetrics,
    /// Performance metrics
    pub performance: PerformanceMetrics,
    /// Resource utilization metrics
    pub resources: ResourceMetrics,
    /// Health status metrics
    pub health: HealthMetrics,
    /// Timestamp when metrics were collected
    pub timestamp: u64,
}

/// System-level performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    /// Database uptime in seconds
    pub uptime_seconds: u64,
    /// Current timestamp
    pub current_time: u64,
    /// Total number of database connections
    pub active_connections: u32,
    /// Peak connections since startup
    pub peak_connections: u32,
    /// Total requests processed
    pub total_requests: u64,
    /// Requests per second (moving average)
    pub requests_per_second: f64,
    /// Average response time in milliseconds
    pub avg_response_time_ms: f64,
    /// Memory usage statistics
    pub memory_usage_bytes: u64,
    /// Estimated memory overhead percentage
    pub memory_overhead_percent: f64,
}

/// Aggregated storage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetrics {
    /// Total vertices stored
    pub total_vertices: u64,
    /// Total edges stored
    pub total_edges: u64,
    /// Total data size in bytes
    pub total_data_size_bytes: u64,
    /// Compression ratio across all data
    pub compression_ratio: f64,
    /// LSM-tree statistics
    pub lsm_stats: PolyLSMStats,
    /// Block cache statistics
    pub cache_stats: CacheStats,
    /// Storage manager statistics
    pub storage_stats: StorageStats,
    /// Compaction statistics
    pub compaction_stats: CompactionStats,
    /// Property store statistics
    pub property_stats: PropertyStoreStats,
    /// Adaptive update statistics
    pub adaptive_stats: AdaptiveStats,
    /// Write amplification factor
    pub write_amplification: f64,
    /// Read amplification factor
    pub read_amplification: f64,
}

/// Transaction system metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetrics {
    /// Transaction manager statistics
    pub manager_stats: TransactionManagerStats,
    /// Transaction throughput (commits/sec)
    pub commit_rate: f64,
    /// Transaction abort rate
    pub abort_rate: f64,
    /// Average transaction duration
    pub avg_transaction_duration_ms: f64,
    /// Lock contention rate
    pub lock_contention_rate: f64,
    /// Deadlock detection count
    pub deadlocks_detected: u64,
    /// Transaction queue depth
    pub transaction_queue_depth: u32,
}

/// Query engine metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetrics {
    /// Total queries executed
    pub total_queries: u64,
    /// Queries per second
    pub queries_per_second: f64,
    /// Average query execution time
    pub avg_execution_time_ms: f64,
    /// Query cache hit ratio
    pub cache_hit_ratio: f64,
    /// Average vertices visited per query
    pub avg_vertices_visited: f64,
    /// Average edges traversed per query
    pub avg_edges_traversed: f64,
    /// Slow query count (queries > threshold)
    pub slow_queries: u64,
    /// Query complexity distribution
    pub complexity_distribution: HashMap<String, u64>,
}

/// Recovery system metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryMetrics {
    /// Recovery system statistics
    pub recovery_stats: RecoveryStats,
    /// WAL file count
    pub wal_file_count: u32,
    /// WAL total size in bytes
    pub wal_total_size_bytes: u64,
    /// Checkpoint frequency
    pub checkpoint_frequency_minutes: f64,
    /// Recovery system health score (0-1)
    pub health_score: f64,
    /// Last successful backup time
    pub last_backup_time: Option<u64>,
}

/// Performance metrics and benchmarks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Operations per second
    pub ops_per_second: f64,
    /// Read latency percentiles (p50, p95, p99)
    pub read_latency_percentiles: LatencyPercentiles,
    /// Write latency percentiles
    pub write_latency_percentiles: LatencyPercentiles,
    /// Query latency percentiles
    pub query_latency_percentiles: LatencyPercentiles,
    /// Throughput trend (last 5 minutes)
    pub throughput_trend: Vec<f64>,
    /// Error rate percentage
    pub error_rate_percent: f64,
    /// Availability percentage (uptime)
    pub availability_percent: f64,
}

/// Latency percentile measurements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

/// Resource utilization metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetrics {
    /// CPU utilization percentage
    pub cpu_usage_percent: f64,
    /// Memory utilization
    pub memory_usage_bytes: u64,
    /// Memory utilization percentage
    pub memory_usage_percent: f64,
    /// Disk space used
    pub disk_usage_bytes: u64,
    /// Disk space utilization percentage
    pub disk_usage_percent: f64,
    /// Network I/O metrics
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
    /// File descriptor usage
    pub file_descriptors_used: u32,
    /// Thread count
    pub thread_count: u32,
}

/// Health status and monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMetrics {
    /// Overall health status
    pub status: HealthStatus,
    /// Component health scores
    pub component_health: HashMap<String, f64>,
    /// Active alerts
    pub active_alerts: Vec<Alert>,
    /// Health check results
    pub health_checks: HashMap<String, HealthCheckResult>,
    /// System warnings
    pub warnings: Vec<String>,
    /// Last health check timestamp
    pub last_health_check: u64,
}

/// Database health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

/// Alert information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub severity: AlertSeverity,
    pub message: String,
    pub component: String,
    pub timestamp: u64,
    pub acknowledged: bool,
}

/// Alert severity levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub passed: bool,
    pub message: String,
    pub duration_ms: u64,
    pub timestamp: u64,
}

/// Metrics collection configuration
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Collection interval in seconds
    pub collection_interval_seconds: u64,
    /// Retention period for historical metrics
    pub retention_period_days: u32,
    /// Enable detailed performance tracking
    pub enable_detailed_tracking: bool,
    /// Prometheus export configuration
    pub prometheus_config: Option<PrometheusConfig>,
    /// Alert configuration
    pub alert_config: AlertConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            collection_interval_seconds: 10,
            retention_period_days: 30,
            enable_detailed_tracking: true,
            prometheus_config: None,
            alert_config: AlertConfig::default(),
        }
    }
}

/// Prometheus export configuration
#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub port: u16,
    pub namespace: String,
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    pub enabled: bool,
    pub cpu_threshold_percent: f64,
    pub memory_threshold_percent: f64,
    pub disk_threshold_percent: f64,
    pub error_rate_threshold_percent: f64,
    pub response_time_threshold_ms: f64,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cpu_threshold_percent: 80.0,
            memory_threshold_percent: 85.0,
            disk_threshold_percent: 90.0,
            error_rate_threshold_percent: 5.0,
            response_time_threshold_ms: 1000.0,
        }
    }
}

/// Histogram for tracking latency distributions
#[derive(Debug)]
pub struct Histogram {
    buckets: Vec<AtomicU64>,
    bucket_bounds: Vec<f64>,
    count: AtomicU64,
    sum: AtomicU64,
}

impl Histogram {
    pub fn new() -> Self {
        let bucket_bounds = vec![
            0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0,
        ];
        let bucket_count = bucket_bounds.len() + 1; // +1 for infinity bucket

        Self {
            buckets: (0..bucket_count).map(|_| AtomicU64::new(0)).collect(),
            bucket_bounds,
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum
            .fetch_add((value * 1000.0) as u64, Ordering::Relaxed); // Store as microseconds

        // Find the appropriate bucket
        let bucket_index = self
            .bucket_bounds
            .iter()
            .position(|&bound| value <= bound)
            .unwrap_or(self.buckets.len() - 1);

        self.buckets[bucket_index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn percentile(&self, p: f64) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }

        let target = ((p / 100.0) * count as f64) as u64;
        let mut cumulative = 0;

        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                return if i < self.bucket_bounds.len() {
                    self.bucket_bounds[i]
                } else {
                    5000.0 // Max value for infinity bucket
                };
            }
        }

        0.0
    }

    pub fn average(&self) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        (self.sum.load(Ordering::Relaxed) as f64 / 1000.0) / count as f64
    }
}

/// Main metrics collector and aggregator
pub struct MetricsCollector {
    config: MetricsConfig,
    start_time: Instant,
    current_metrics: Arc<RwLock<DatabaseMetrics>>,
    historical_metrics: Arc<Mutex<Vec<DatabaseMetrics>>>,

    // Performance tracking
    read_histogram: Arc<Histogram>,
    write_histogram: Arc<Histogram>,
    query_histogram: Arc<Histogram>,

    // Request tracking
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    active_connections: AtomicU64,
    peak_connections: AtomicU64,

    // Alert management
    active_alerts: Arc<Mutex<Vec<Alert>>>,
    next_alert_id: AtomicU64,

    // Background task handle
    _collection_task: Option<tokio::task::JoinHandle<()>>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(config: MetricsConfig) -> Self {
        let initial_metrics = DatabaseMetrics {
            system: SystemMetrics {
                uptime_seconds: 0,
                current_time: current_timestamp(),
                active_connections: 0,
                peak_connections: 0,
                total_requests: 0,
                requests_per_second: 0.0,
                avg_response_time_ms: 0.0,
                memory_usage_bytes: 0,
                memory_overhead_percent: 0.0,
            },
            storage: StorageMetrics {
                total_vertices: 0,
                total_edges: 0,
                total_data_size_bytes: 0,
                compression_ratio: 1.0,
                lsm_stats: PolyLSMStats {
                    active_memtable: MemTableStats {
                        num_vertices: 0,
                        pivot_entries: 0,
                        delta_entries: 0,
                        tombstone_entries: 0,
                        size_bytes: 0,
                        created_at: crate::Timestamp::now(),
                    },
                    immutable_memtables: 0,
                    levels: Vec::new(),
                    total_vertices: 0,
                    adaptive_stats: AdaptiveStats::default(),
                },
                cache_stats: CacheStats::default(),
                storage_stats: StorageStats::default(),
                compaction_stats: CompactionStats::default(),
                property_stats: PropertyStoreStats {
                    vertex_properties: 0,
                    edge_properties: 0,
                    total_schemas: 0,
                    vertex_indexes: 0,
                    edge_indexes: 0,
                    memory_usage_bytes: 0,
                },
                adaptive_stats: AdaptiveStats::default(),
                write_amplification: 1.0,
                read_amplification: 1.0,
            },
            transactions: TransactionMetrics {
                manager_stats: TransactionManagerStats {
                    active_transactions: 0,
                    total_reads: 0,
                    total_writes: 0,
                    total_conflicts: 0,
                    lock_stats: HashMap::new(),
                },
                commit_rate: 0.0,
                abort_rate: 0.0,
                avg_transaction_duration_ms: 0.0,
                lock_contention_rate: 0.0,
                deadlocks_detected: 0,
                transaction_queue_depth: 0,
            },
            queries: QueryMetrics {
                total_queries: 0,
                queries_per_second: 0.0,
                avg_execution_time_ms: 0.0,
                cache_hit_ratio: 0.0,
                avg_vertices_visited: 0.0,
                avg_edges_traversed: 0.0,
                slow_queries: 0,
                complexity_distribution: HashMap::new(),
            },
            recovery: RecoveryMetrics {
                recovery_stats: RecoveryStats::default(),
                wal_file_count: 0,
                wal_total_size_bytes: 0,
                checkpoint_frequency_minutes: 0.0,
                health_score: 1.0,
                last_backup_time: None,
            },
            performance: PerformanceMetrics {
                ops_per_second: 0.0,
                read_latency_percentiles: LatencyPercentiles {
                    p50_ms: 0.0,
                    p95_ms: 0.0,
                    p99_ms: 0.0,
                    p999_ms: 0.0,
                },
                write_latency_percentiles: LatencyPercentiles {
                    p50_ms: 0.0,
                    p95_ms: 0.0,
                    p99_ms: 0.0,
                    p999_ms: 0.0,
                },
                query_latency_percentiles: LatencyPercentiles {
                    p50_ms: 0.0,
                    p95_ms: 0.0,
                    p99_ms: 0.0,
                    p999_ms: 0.0,
                },
                throughput_trend: Vec::new(),
                error_rate_percent: 0.0,
                availability_percent: 100.0,
            },
            resources: ResourceMetrics {
                cpu_usage_percent: 0.0,
                memory_usage_bytes: 0,
                memory_usage_percent: 0.0,
                disk_usage_bytes: 0,
                disk_usage_percent: 0.0,
                network_bytes_sent: 0,
                network_bytes_received: 0,
                file_descriptors_used: 0,
                thread_count: 0,
            },
            health: HealthMetrics {
                status: HealthStatus::Healthy,
                component_health: HashMap::new(),
                active_alerts: Vec::new(),
                health_checks: HashMap::new(),
                warnings: Vec::new(),
                last_health_check: current_timestamp(),
            },
            timestamp: current_timestamp(),
        };

        Self {
            config,
            start_time: Instant::now(),
            current_metrics: Arc::new(RwLock::new(initial_metrics)),
            historical_metrics: Arc::new(Mutex::new(Vec::new())),
            read_histogram: Arc::new(Histogram::new()),
            write_histogram: Arc::new(Histogram::new()),
            query_histogram: Arc::new(Histogram::new()),
            total_requests: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            peak_connections: AtomicU64::new(0),
            active_alerts: Arc::new(Mutex::new(Vec::new())),
            next_alert_id: AtomicU64::new(1),
            _collection_task: None,
        }
    }

    /// Start background metrics collection
    pub fn start_background_collection(&mut self) -> Result<()> {
        let config = self.config.clone();
        let current_metrics = Arc::clone(&self.current_metrics);
        let historical_metrics = Arc::clone(&self.historical_metrics);
        let active_alerts = Arc::clone(&self.active_alerts);
        let start_time = self.start_time;

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(config.collection_interval_seconds));

            loop {
                interval.tick().await;

                // Update system metrics
                let uptime = start_time.elapsed().as_secs();
                let timestamp = current_timestamp();

                // Update current metrics (simplified for now)
                {
                    let mut metrics = current_metrics.write();
                    metrics.system.uptime_seconds = uptime;
                    metrics.system.current_time = timestamp;
                    metrics.timestamp = timestamp;
                }

                // Check for alerts
                Self::check_alerts(&current_metrics, &active_alerts, &config.alert_config).await;

                // Store historical metrics
                {
                    let current = current_metrics.read().clone();
                    let mut historical = historical_metrics.lock();
                    historical.push(current);

                    // Cleanup old metrics beyond retention period
                    let retention_seconds = config.retention_period_days as u64 * 24 * 3600;
                    let cutoff_time = timestamp.saturating_sub(retention_seconds);
                    historical.retain(|m| m.timestamp > cutoff_time);
                }
            }
        });

        self._collection_task = Some(handle);
        Ok(())
    }

    /// Record a read operation
    pub fn record_read(&self, duration_ms: f64) {
        self.read_histogram.observe(duration_ms);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write operation
    pub fn record_write(&self, duration_ms: f64) {
        self.write_histogram.observe(duration_ms);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a query operation
    pub fn record_query(&self, duration_ms: f64) {
        self.query_histogram.observe(duration_ms);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Update connection count
    pub fn set_active_connections(&self, count: u64) {
        self.active_connections.store(count, Ordering::Relaxed);

        // Update peak
        let current_peak = self.peak_connections.load(Ordering::Relaxed);
        if count > current_peak {
            self.peak_connections.store(count, Ordering::Relaxed);
        }
    }

    /// Get current metrics
    pub fn get_current_metrics(&self) -> DatabaseMetrics {
        // Update performance metrics from histograms
        let mut metrics = self.current_metrics.read().clone();

        metrics.performance.read_latency_percentiles = LatencyPercentiles {
            p50_ms: self.read_histogram.percentile(50.0),
            p95_ms: self.read_histogram.percentile(95.0),
            p99_ms: self.read_histogram.percentile(99.0),
            p999_ms: self.read_histogram.percentile(99.9),
        };

        metrics.performance.write_latency_percentiles = LatencyPercentiles {
            p50_ms: self.write_histogram.percentile(50.0),
            p95_ms: self.write_histogram.percentile(95.0),
            p99_ms: self.write_histogram.percentile(99.0),
            p999_ms: self.write_histogram.percentile(99.9),
        };

        metrics.performance.query_latency_percentiles = LatencyPercentiles {
            p50_ms: self.query_histogram.percentile(50.0),
            p95_ms: self.query_histogram.percentile(95.0),
            p99_ms: self.query_histogram.percentile(99.0),
            p999_ms: self.query_histogram.percentile(99.9),
        };

        // Update system metrics
        metrics.system.active_connections = self.active_connections.load(Ordering::Relaxed) as u32;
        metrics.system.peak_connections = self.peak_connections.load(Ordering::Relaxed) as u32;
        metrics.system.total_requests = self.total_requests.load(Ordering::Relaxed);

        // Calculate error rate
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        let total_errors = self.total_errors.load(Ordering::Relaxed);
        metrics.performance.error_rate_percent = if total_requests > 0 {
            (total_errors as f64 / total_requests as f64) * 100.0
        } else {
            0.0
        };

        metrics
    }

    /// Get historical metrics
    pub fn get_historical_metrics(&self, duration_seconds: u64) -> Vec<DatabaseMetrics> {
        let historical = self.historical_metrics.lock();
        let cutoff_time = current_timestamp().saturating_sub(duration_seconds);

        historical
            .iter()
            .filter(|m| m.timestamp > cutoff_time)
            .cloned()
            .collect()
    }

    /// Update metrics from component statistics
    pub fn update_from_components(
        &self,
        storage_stats: Option<StorageStats>,
        poly_lsm_stats: Option<PolyLSMStats>,
        cache_stats: Option<CacheStats>,
        tx_stats: Option<TransactionManagerStats>,
        recovery_stats: Option<RecoveryStats>,
    ) {
        let mut metrics = self.current_metrics.write();

        if let Some(stats) = storage_stats {
            metrics.storage.storage_stats = stats;
        }

        if let Some(stats) = poly_lsm_stats {
            metrics.storage.lsm_stats = stats;
        }

        if let Some(stats) = cache_stats {
            metrics.storage.cache_stats = stats;
        }

        if let Some(stats) = tx_stats {
            metrics.transactions.manager_stats = stats;
        }

        if let Some(stats) = recovery_stats {
            metrics.recovery.recovery_stats = stats;
        }
    }

    /// Update property store statistics
    pub fn update_property_stats(&self, property_stats: PropertyStoreStats) {
        let mut metrics = self.current_metrics.write();
        metrics.storage.property_stats = property_stats;
    }

    /// Check for alert conditions
    async fn check_alerts(
        metrics: &Arc<RwLock<DatabaseMetrics>>,
        alerts: &Arc<Mutex<Vec<Alert>>>,
        config: &AlertConfig,
    ) {
        if !config.enabled {
            return;
        }

        let current_metrics = metrics.read();
        let mut active_alerts = alerts.lock();

        // Check CPU usage
        if current_metrics.resources.cpu_usage_percent > config.cpu_threshold_percent {
            let alert = Alert {
                id: format!("cpu_high_{}", current_timestamp()),
                severity: AlertSeverity::Warning,
                message: format!(
                    "High CPU usage: {:.1}%",
                    current_metrics.resources.cpu_usage_percent
                ),
                component: "system".to_string(),
                timestamp: current_timestamp(),
                acknowledged: false,
            };
            active_alerts.push(alert);
        }

        // Check memory usage
        if current_metrics.resources.memory_usage_percent > config.memory_threshold_percent {
            let alert = Alert {
                id: format!("memory_high_{}", current_timestamp()),
                severity: AlertSeverity::Warning,
                message: format!(
                    "High memory usage: {:.1}%",
                    current_metrics.resources.memory_usage_percent
                ),
                component: "system".to_string(),
                timestamp: current_timestamp(),
                acknowledged: false,
            };
            active_alerts.push(alert);
        }

        // Check error rate
        if current_metrics.performance.error_rate_percent > config.error_rate_threshold_percent {
            let alert = Alert {
                id: format!("error_rate_high_{}", current_timestamp()),
                severity: AlertSeverity::Critical,
                message: format!(
                    "High error rate: {:.1}%",
                    current_metrics.performance.error_rate_percent
                ),
                component: "performance".to_string(),
                timestamp: current_timestamp(),
                acknowledged: false,
            };
            active_alerts.push(alert);
        }

        // Cleanup old acknowledged alerts
        let one_hour_ago = current_timestamp().saturating_sub(3600);
        active_alerts.retain(|alert| !alert.acknowledged || alert.timestamp > one_hour_ago);
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus_metrics(&self) -> String {
        let metrics = self.get_current_metrics();
        let mut output = String::new();

        // System metrics
        output.push_str(&format!(
            "# HELP aster_uptime_seconds Database uptime in seconds\n"
        ));
        output.push_str(&format!("# TYPE aster_uptime_seconds counter\n"));
        output.push_str(&format!(
            "aster_uptime_seconds {}\n",
            metrics.system.uptime_seconds
        ));

        output.push_str(&format!(
            "# HELP aster_active_connections Current active connections\n"
        ));
        output.push_str(&format!("# TYPE aster_active_connections gauge\n"));
        output.push_str(&format!(
            "aster_active_connections {}\n",
            metrics.system.active_connections
        ));

        // Performance metrics
        output.push_str(&format!(
            "# HELP aster_requests_total Total requests processed\n"
        ));
        output.push_str(&format!("# TYPE aster_requests_total counter\n"));
        output.push_str(&format!(
            "aster_requests_total {}\n",
            metrics.system.total_requests
        ));

        output.push_str(&format!(
            "# HELP aster_error_rate_percent Error rate percentage\n"
        ));
        output.push_str(&format!("# TYPE aster_error_rate_percent gauge\n"));
        output.push_str(&format!(
            "aster_error_rate_percent {}\n",
            metrics.performance.error_rate_percent
        ));

        // Storage metrics
        output.push_str(&format!(
            "# HELP aster_vertices_total Total vertices stored\n"
        ));
        output.push_str(&format!("# TYPE aster_vertices_total gauge\n"));
        output.push_str(&format!(
            "aster_vertices_total {}\n",
            metrics.storage.total_vertices
        ));

        output.push_str(&format!("# HELP aster_cache_hit_ratio Cache hit ratio\n"));
        output.push_str(&format!("# TYPE aster_cache_hit_ratio gauge\n"));
        output.push_str(&format!(
            "aster_cache_hit_ratio {}\n",
            metrics.storage.cache_stats.hit_ratio
        ));

        // Add more metrics as needed...

        output
    }

    /// Get system health summary
    pub fn get_health_summary(&self) -> HealthMetrics {
        let metrics = self.get_current_metrics();
        let alerts = self.active_alerts.lock();

        // Determine overall health status
        let status = if alerts.iter().any(|a| {
            matches!(
                a.severity,
                AlertSeverity::Critical | AlertSeverity::Emergency
            )
        }) {
            HealthStatus::Critical
        } else if alerts
            .iter()
            .any(|a| matches!(a.severity, AlertSeverity::Warning))
        {
            HealthStatus::Warning
        } else {
            HealthStatus::Healthy
        };

        let mut component_health = HashMap::new();
        component_health.insert("storage".to_string(), 0.9); // Example scores
        component_health.insert("transactions".to_string(), 0.95);
        component_health.insert("queries".to_string(), 0.88);
        component_health.insert("recovery".to_string(), 0.92);

        HealthMetrics {
            status,
            component_health,
            active_alerts: alerts.clone(),
            health_checks: HashMap::new(),
            warnings: Vec::new(),
            last_health_check: current_timestamp(),
        }
    }
}

impl Default for AdaptiveStats {
    fn default() -> Self {
        Self {
            delta_updates: 0,
            pivot_updates: 0,
            cache_hits: 0,
            cache_misses: 0,
        }
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            total_size_bytes: 0,
            entry_count: 0,
            average_access_count: 0.0,
            hit_ratio: 0.0,
        }
    }
}

impl Default for StorageStats {
    fn default() -> Self {
        Self {
            reads: 0,
            writes: 0,
            cache_hits: 0,
            cache_misses: 0,
            sstables_opened: 0,
            sstables_closed: 0,
            bytes_read: 0,
            bytes_written: 0,
            cleanup_cycles: 0,
            last_cleanup: None,
        }
    }
}

impl Default for CompactionStats {
    fn default() -> Self {
        Self {
            entries_processed: 0,
            entries_merged: 0,
            pivot_entries_created: 0,
            delta_entries_eliminated: 0,
            bytes_read: 0,
            bytes_written: 0,
            compression_ratio: 1.0,
            duration_ms: 0,
            neighbor_compression_ratio: 1.0,
            graph_locality_score: 0.0,
        }
    }
}

impl Default for RecoveryStats {
    fn default() -> Self {
        Self {
            transactions_recovered: 0,
            transactions_rolled_back: 0,
            corruptions_detected: 0,
            corruptions_repaired: 0,
            checkpoints_created: 0,
            wal_entries_written: 0,
            wal_entries_replayed: 0,
            recovery_operations: 0,
            last_recovery_time: None,
            total_recovery_time_ms: 0,
        }
    }
}

/// Get current Unix timestamp
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_percentiles() {
        let histogram = Histogram::new();

        // Add sample data
        for i in 1..=100 {
            histogram.observe(i as f64);
        }

        assert!(histogram.percentile(50.0) >= 45.0 && histogram.percentile(50.0) <= 55.0);
        assert!(histogram.percentile(95.0) >= 90.0);
        assert!(histogram.average() >= 45.0 && histogram.average() <= 55.0);
    }

    #[tokio::test]
    async fn test_metrics_collector() {
        let config = MetricsConfig::default();
        let mut collector = MetricsCollector::new(config);

        // Record some operations
        collector.record_read(10.5);
        collector.record_write(25.3);
        collector.record_query(150.0);
        collector.set_active_connections(5);

        let metrics = collector.get_current_metrics();
        assert_eq!(metrics.system.active_connections, 5);
        assert!(metrics.performance.read_latency_percentiles.p50_ms > 0.0);
    }

    #[test]
    fn test_prometheus_export() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);

        let prometheus_output = collector.export_prometheus_metrics();
        assert!(prometheus_output.contains("aster_uptime_seconds"));
        assert!(prometheus_output.contains("aster_active_connections"));
    }
}
