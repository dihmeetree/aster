//! # Aster Graph Database
//!
//! A high-performance graph database built on the Poly-LSM storage engine.
//! Aster is designed for large-scale, evolving graphs with intensive updates and lookups.
//!
//! ## Features
//!
//! - **Poly-LSM Storage**: Hybrid vertex-based and edge-based storage with adaptive updates
//! - **High Performance**: Optimized for both read and write operations
//! - **Scalability**: Handles billion-scale graphs efficiently
//! - **ACID Transactions**: Full transaction support with MVCC
//! - **Flexible Querying**: Rich graph traversal and query capabilities

pub mod error;
pub mod graph;
pub mod metrics;
pub mod query;
pub mod recovery;
pub mod storage;
pub mod transaction;
pub mod types;
pub mod utils;

pub use error::{AsterError, Result};
pub use graph::{Edge, Graph, Vertex};
pub use metrics::{DatabaseMetrics, MetricsCollector, MetricsConfig};
pub use recovery::{RecoveryConfig, RecoveryManager, RecoveryStats};
pub use storage::{PolyLSM, PropertyStore, PropertyStoreConfig};
pub use transaction::{
    ConflictResolution, LockResource, LockType, Transaction, TransactionConfig, TransactionId,
    TransactionManager, TransactionStats,
};
pub use types::{EdgeId, PolyLSMConfig, Properties, PropertyValue, Timestamp, VertexId};

use std::sync::Arc;

/// Configuration for AsterDB
#[derive(Debug, Clone)]
pub struct AsterDBConfig {
    pub enable_recovery: bool,
    pub recovery_config: RecoveryConfig,
    pub transaction_config: TransactionConfig,
    pub enable_metrics: bool,
    pub metrics_config: MetricsConfig,
}

impl Default for AsterDBConfig {
    fn default() -> Self {
        Self {
            enable_recovery: true,
            recovery_config: RecoveryConfig::default(),
            transaction_config: TransactionConfig::default(),
            enable_metrics: true,
            metrics_config: MetricsConfig::default(),
        }
    }
}

/// Core graph database instance
pub struct AsterDB {
    storage: Arc<PolyLSM>,
    transaction_manager: Arc<TransactionManager>,
    recovery_manager: Option<Arc<RecoveryManager>>,
    metrics_collector: Option<Arc<MetricsCollector>>,
    config: AsterDBConfig,
}

impl AsterDB {
    /// Create a new AsterDB instance with default configuration
    pub async fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, AsterDBConfig::default()).await
    }

    /// Create a new AsterDB instance with the given configuration
    pub async fn open_with_config<P: AsRef<std::path::Path>>(
        path: P,
        config: AsterDBConfig,
    ) -> Result<Self> {
        let storage = Arc::new(PolyLSM::open(path).await?);
        let transaction_manager = Arc::new(TransactionManager::new_with_config(
            config.transaction_config.clone(),
        ));

        // Initialize recovery manager if enabled
        let recovery_manager = if config.enable_recovery {
            let recovery_mgr = Arc::new(RecoveryManager::new(config.recovery_config.clone())?);

            // Perform database recovery on startup
            recovery_mgr
                .recover_database(&storage, &transaction_manager)
                .await?;

            // Start background recovery tasks
            recovery_mgr
                .start_background_tasks(Arc::clone(&storage))
                .await?;

            Some(recovery_mgr)
        } else {
            None
        };

        // Initialize metrics collector if enabled
        let metrics_collector = if config.enable_metrics {
            let mut collector = MetricsCollector::new(config.metrics_config.clone());
            collector.start_background_collection()?;
            Some(Arc::new(collector))
        } else {
            None
        };

        Ok(Self {
            storage,
            transaction_manager,
            recovery_manager,
            metrics_collector,
            config,
        })
    }

    /// Begin a new transaction
    pub async fn begin_transaction(&self) -> Result<Transaction> {
        let mut tx = self.transaction_manager.begin()?;

        // Log transaction start for recovery if enabled
        if let Some(ref recovery) = self.recovery_manager {
            // Transaction start logging would be handled by the transaction itself
            // through hooks in commit/rollback operations
        }

        Ok(tx)
    }

    /// Commit a transaction with recovery logging
    pub async fn commit_transaction(&self, mut transaction: Transaction) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Get transaction changes before committing
        let tx_id = transaction.id();
        let changes = transaction.get_changes();

        // Commit the transaction
        let result = transaction.commit().await;

        // Record metrics
        let duration_ms = start_time.elapsed().as_millis() as f64;
        if let Some(ref metrics) = self.metrics_collector {
            if result.is_ok() {
                metrics.record_write(duration_ms);
            } else {
                metrics.record_error();
            }
        }

        result?;

        // Log commit for recovery if enabled
        if let Some(ref recovery) = self.recovery_manager {
            recovery.log_transaction_commit(tx_id, changes).await?;
        }

        Ok(())
    }

    /// Rollback a transaction with recovery logging
    pub async fn rollback_transaction(&self, mut transaction: Transaction) -> Result<()> {
        let start_time = std::time::Instant::now();
        let tx_id = transaction.id();

        // Rollback the transaction
        let result = transaction.rollback().await;

        // Record metrics
        let duration_ms = start_time.elapsed().as_millis() as f64;
        if let Some(ref metrics) = self.metrics_collector {
            if result.is_ok() {
                metrics.record_write(duration_ms);
            } else {
                metrics.record_error();
            }
        }

        result?;

        // Log rollback for recovery if enabled
        if let Some(ref recovery) = self.recovery_manager {
            recovery.log_transaction_rollback(tx_id).await?;
        }

        Ok(())
    }

    /// Get a graph view for read operations
    pub fn graph(&self) -> Graph {
        Graph::new(&self.storage)
    }

    /// Get recovery statistics
    pub fn get_recovery_stats(&self) -> Option<RecoveryStats> {
        self.recovery_manager.as_ref().map(|rm| rm.get_stats())
    }

    /// Get comprehensive database metrics
    pub async fn get_metrics(&self) -> Option<DatabaseMetrics> {
        if let Some(ref mc) = self.metrics_collector {
            // Update metrics with current component statistics
            let poly_lsm_stats = self.storage.stats().await;
            let tx_stats = self.transaction_manager.get_stats();
            let recovery_stats = self.recovery_manager.as_ref().map(|rm| rm.get_stats());

            // Update the metrics collector with latest stats
            mc.update_from_components(
                None, // Storage manager stats would need to be exposed
                Some(poly_lsm_stats),
                None, // Cache stats would need to be exposed
                Some(tx_stats),
                recovery_stats,
            );

            Some(mc.get_current_metrics())
        } else {
            None
        }
    }

    /// Get historical metrics for the specified duration
    pub fn get_historical_metrics(&self, duration_seconds: u64) -> Vec<DatabaseMetrics> {
        self.metrics_collector
            .as_ref()
            .map(|mc| mc.get_historical_metrics(duration_seconds))
            .unwrap_or_default()
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus_metrics(&self) -> Option<String> {
        self.metrics_collector
            .as_ref()
            .map(|mc| mc.export_prometheus_metrics())
    }

    /// Get system health summary
    pub fn get_health_summary(&self) -> Option<crate::metrics::HealthMetrics> {
        self.metrics_collector
            .as_ref()
            .map(|mc| mc.get_health_summary())
    }

    /// Record a read operation for metrics
    pub fn record_read_operation(&self, duration_ms: f64) {
        if let Some(ref metrics) = self.metrics_collector {
            metrics.record_read(duration_ms);
        }
    }

    /// Record a write operation for metrics
    pub fn record_write_operation(&self, duration_ms: f64) {
        if let Some(ref metrics) = self.metrics_collector {
            metrics.record_write(duration_ms);
        }
    }

    /// Record a query operation for metrics
    pub fn record_query_operation(&self, duration_ms: f64) {
        if let Some(ref metrics) = self.metrics_collector {
            metrics.record_query(duration_ms);
        }
    }

    /// Record an error for metrics
    pub fn record_error(&self) {
        if let Some(ref metrics) = self.metrics_collector {
            metrics.record_error();
        }
    }

    /// Update active connection count for metrics
    pub fn update_connection_count(&self, count: u64) {
        if let Some(ref metrics) = self.metrics_collector {
            metrics.set_active_connections(count);
        }
    }

    /// Manually trigger corruption detection and repair
    pub async fn check_and_repair_corruption(
        &self,
        vertices: Vec<VertexId>,
    ) -> Result<Vec<VertexId>> {
        if let Some(ref recovery) = self.recovery_manager {
            recovery
                .detect_and_repair_corruption(&self.storage, vertices)
                .await
        } else {
            Err(AsterError::configuration("Recovery not enabled"))
        }
    }

    /// Create a manual checkpoint
    pub async fn create_checkpoint(&self) -> Result<u64> {
        if let Some(ref recovery) = self.recovery_manager {
            let checkpoint_id = chrono::Utc::now().timestamp_millis() as u64;
            recovery.create_checkpoint(checkpoint_id).await?;
            Ok(checkpoint_id)
        } else {
            Err(AsterError::configuration("Recovery not enabled"))
        }
    }

    /// Perform manual cleanup of old recovery logs
    pub fn cleanup_recovery_logs(&self) -> Result<()> {
        if let Some(ref recovery) = self.recovery_manager {
            recovery.cleanup_old_wal_files()
        } else {
            Err(AsterError::configuration("Recovery not enabled"))
        }
    }

    /// Get the storage manager reference
    pub fn storage(&self) -> &PolyLSM {
        &self.storage
    }

    /// Get the transaction manager reference  
    pub fn transaction_manager(&self) -> &TransactionManager {
        &self.transaction_manager
    }

    /// Check if recovery is enabled
    pub fn recovery_enabled(&self) -> bool {
        self.recovery_manager.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_database_creation() {
        let temp_dir = TempDir::new().unwrap();

        // Test with recovery disabled for simpler setup
        let config = AsterDBConfig {
            enable_recovery: false,
            ..Default::default()
        };

        let db = AsterDB::open_with_config(temp_dir.path(), config)
            .await
            .unwrap();

        // Basic smoke test
        let _graph = db.graph();
        let tx = db.begin_transaction().await.unwrap();

        // Test transaction commit
        db.commit_transaction(tx).await.unwrap();
    }
}
