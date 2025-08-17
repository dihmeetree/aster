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
pub mod validation;

pub use error::{AsterError, Result};
pub use graph::{Edge, Graph, Vertex};
pub use metrics::{DatabaseMetrics, MetricsCollector, MetricsConfig};
pub use query::{
    GremlinContext, GremlinEngine, GremlinResultSet, GremlinTraversal, QueryContext, QueryPlan,
    RangeQueryResult, RangeScanOptimizer,
};
pub use recovery::{RecoveryConfig, RecoveryManager, RecoveryStats};
pub use storage::{PolyLSM, PropertyStore, PropertyStoreConfig};
pub use transaction::{
    ConflictResolution, LockResource, LockType, Transaction, TransactionConfig, TransactionId,
    TransactionManager, TransactionStats,
};
pub use types::{EdgeId, PolyLSMConfig, Properties, PropertyValue, Timestamp, VertexId};
pub use validation::{CostModelValidator, ValidationParameters, ValidationResult};

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for edge registry operations
pub trait EdgeRegistry: Send + Sync {
    fn register_edge(&self, edge_id: EdgeId, source: VertexId, target: VertexId, label: String);
    fn get_outgoing_edges(
        &self,
        vertex_id: VertexId,
        label_filter: Option<&str>,
    ) -> Vec<EdgeRegistryEntry>;
    fn get_incoming_edges(
        &self,
        vertex_id: VertexId,
        label_filter: Option<&str>,
    ) -> Vec<EdgeRegistryEntry>;
}

/// Edge registry entry storing edge metadata
#[derive(Debug, Clone)]
pub struct EdgeRegistryEntry {
    pub edge_id: EdgeId,
    pub source: VertexId,
    pub target: VertexId,
    pub label: String,
}

/// Implementation of EdgeRegistry that wraps the internal edge registry
struct EdgeRegistryImpl {
    registry: Arc<RwLock<HashMap<EdgeId, EdgeRegistryEntry>>>,
}

impl EdgeRegistry for EdgeRegistryImpl {
    fn register_edge(&self, edge_id: EdgeId, source: VertexId, target: VertexId, label: String) {
        let entry = EdgeRegistryEntry {
            edge_id,
            source,
            target,
            label,
        };
        self.registry.write().insert(edge_id, entry);
    }

    fn get_outgoing_edges(
        &self,
        vertex_id: VertexId,
        label_filter: Option<&str>,
    ) -> Vec<EdgeRegistryEntry> {
        let registry = self.registry.read();
        registry
            .values()
            .filter(|entry| {
                entry.source == vertex_id && label_filter.map_or(true, |label| entry.label == label)
            })
            .cloned()
            .collect()
    }

    fn get_incoming_edges(
        &self,
        vertex_id: VertexId,
        label_filter: Option<&str>,
    ) -> Vec<EdgeRegistryEntry> {
        let registry = self.registry.read();
        registry
            .values()
            .filter(|entry| {
                entry.target == vertex_id && label_filter.map_or(true, |label| entry.label == label)
            })
            .cloned()
            .collect()
    }
}

/// Configuration for AsterDB
#[derive(Debug, Clone)]
pub struct AsterDBConfig {
    pub enable_recovery: bool,
    pub recovery_config: RecoveryConfig,
    pub transaction_config: TransactionConfig,
    pub enable_metrics: bool,
    pub metrics_config: MetricsConfig,
    pub enable_properties: bool,
    pub property_store_config: PropertyStoreConfig,
}

impl Default for AsterDBConfig {
    fn default() -> Self {
        Self {
            enable_recovery: true,
            recovery_config: RecoveryConfig::default(),
            transaction_config: TransactionConfig::default(),
            enable_metrics: true,
            metrics_config: MetricsConfig::default(),
            enable_properties: true,
            property_store_config: PropertyStoreConfig::default(),
        }
    }
}

/// Core graph database instance
pub struct AsterDB {
    storage: Arc<PolyLSM>,
    property_store: Option<Arc<PropertyStore>>,
    transaction_manager: Arc<TransactionManager>,
    recovery_manager: Option<Arc<RecoveryManager>>,
    metrics_collector: Option<Arc<MetricsCollector>>,
    gremlin_engine: GremlinEngine,
    range_scan_optimizer: RangeScanOptimizer,
    config: AsterDBConfig,
    /// Global edge registry for tracking edges across query executions
    edge_registry: Arc<RwLock<HashMap<EdgeId, EdgeRegistryEntry>>>,
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
        let path_ref = path.as_ref();
        let storage = Arc::new(PolyLSM::open(path_ref).await?);
        let transaction_manager = Arc::new(TransactionManager::new_with_config(
            config.transaction_config.clone(),
        ));

        // Initialize property store if enabled (as separate column family)
        let property_store = if config.enable_properties {
            let property_path = path_ref.join("properties");
            let prop_store =
                PropertyStore::new(property_path, config.property_store_config.clone())?;
            Some(Arc::new(prop_store))
        } else {
            None
        };

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

        // Initialize Gremlin engine
        let graph = Arc::new(Graph::new(&storage));
        let gremlin_engine = GremlinEngine::new(graph, property_store.clone());

        // Initialize range scan optimizer
        let range_scan_optimizer = RangeScanOptimizer::new(property_store.clone());

        // Create edge registry
        let edge_registry = Arc::new(RwLock::new(HashMap::new()));

        let mut db = Self {
            storage,
            property_store,
            transaction_manager,
            recovery_manager,
            metrics_collector,
            gremlin_engine,
            range_scan_optimizer,
            config,
            edge_registry: edge_registry.clone(),
        };

        // Set edge registry on gremlin engine
        db.gremlin_engine
            .set_edge_registry(Arc::new(EdgeRegistryImpl {
                registry: edge_registry,
            }));

        Ok(db)
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
            let property_stats = if let Some(ref ps) = self.property_store {
                Some(ps.get_stats().await)
            } else {
                None
            };

            // Update the metrics collector with latest stats
            mc.update_from_components(
                None, // Storage manager stats would need to be exposed
                Some(poly_lsm_stats),
                None, // Cache stats would need to be exposed
                Some(tx_stats),
                recovery_stats,
            );

            // Update property store stats if available
            if let Some(prop_stats) = property_stats {
                mc.update_property_stats(prop_stats);
            }

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

    /// Register an edge in the global registry
    pub fn register_edge(
        &self,
        edge_id: EdgeId,
        source: VertexId,
        target: VertexId,
        label: String,
    ) {
        let entry = EdgeRegistryEntry {
            edge_id,
            source,
            target,
            label,
        };
        self.edge_registry.write().insert(edge_id, entry);
    }

    /// Get edges originating from a vertex with optional label filtering
    pub fn get_outgoing_edges(
        &self,
        vertex_id: VertexId,
        label_filter: Option<&str>,
    ) -> Vec<EdgeRegistryEntry> {
        let registry = self.edge_registry.read();
        registry
            .values()
            .filter(|entry| {
                entry.source == vertex_id && label_filter.map_or(true, |label| entry.label == label)
            })
            .cloned()
            .collect()
    }

    /// Get edges targeting a vertex with optional label filtering
    pub fn get_incoming_edges(
        &self,
        vertex_id: VertexId,
        label_filter: Option<&str>,
    ) -> Vec<EdgeRegistryEntry> {
        let registry = self.edge_registry.read();
        registry
            .values()
            .filter(|entry| {
                entry.target == vertex_id && label_filter.map_or(true, |label| entry.label == label)
            })
            .cloned()
            .collect()
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

    /// Get the property store reference if enabled
    pub fn property_store(&self) -> Option<&PropertyStore> {
        self.property_store.as_ref().map(|ps| ps.as_ref())
    }

    /// Get the transaction manager reference  
    pub fn transaction_manager(&self) -> &TransactionManager {
        &self.transaction_manager
    }

    /// Check if recovery is enabled
    pub fn recovery_enabled(&self) -> bool {
        self.recovery_manager.is_some()
    }

    /// Check if properties are enabled
    pub fn properties_enabled(&self) -> bool {
        self.property_store.is_some()
    }

    /// Set properties for a vertex (requires properties to be enabled)
    pub async fn set_vertex_properties(
        &self,
        vertex_id: VertexId,
        properties: Properties,
    ) -> Result<()> {
        if let Some(ref property_store) = self.property_store {
            property_store
                .set_vertex_properties(vertex_id, properties)
                .await
        } else {
            Err(AsterError::configuration("Properties not enabled"))
        }
    }

    /// Get properties for a vertex (requires properties to be enabled)
    pub async fn get_vertex_properties(&self, vertex_id: VertexId) -> Result<Properties> {
        if let Some(ref property_store) = self.property_store {
            property_store.get_vertex_properties(vertex_id).await
        } else {
            Err(AsterError::configuration("Properties not enabled"))
        }
    }

    /// Set properties for an edge (requires properties to be enabled)
    pub async fn set_edge_properties(&self, edge_id: EdgeId, properties: Properties) -> Result<()> {
        if let Some(ref property_store) = self.property_store {
            property_store
                .set_edge_properties(edge_id, properties)
                .await
        } else {
            Err(AsterError::configuration("Properties not enabled"))
        }
    }

    /// Get properties for an edge (requires properties to be enabled)
    pub async fn get_edge_properties(&self, edge_id: EdgeId) -> Result<Properties> {
        if let Some(ref property_store) = self.property_store {
            property_store.get_edge_properties(edge_id).await
        } else {
            Err(AsterError::configuration("Properties not enabled"))
        }
    }

    /// Find vertices by property value (requires properties to be enabled)
    pub async fn find_vertices_by_property(
        &self,
        key: &str,
        value: &PropertyValue,
    ) -> Result<Vec<VertexId>> {
        if let Some(ref property_store) = self.property_store {
            property_store.find_vertices_by_property(key, value).await
        } else {
            Err(AsterError::configuration("Properties not enabled"))
        }
    }

    /// Find vertices by property value range (requires properties to be enabled)
    pub async fn find_vertices_by_property_range(
        &self,
        key: &str,
        min: &PropertyValue,
        max: &PropertyValue,
    ) -> Result<Vec<VertexId>> {
        if let Some(ref property_store) = self.property_store {
            property_store
                .find_vertices_by_property_range(key, min, max)
                .await
        } else {
            Err(AsterError::configuration("Properties not enabled"))
        }
    }

    /// Delete specific properties from a vertex (requires properties to be enabled)
    pub async fn delete_vertex_properties(
        &self,
        vertex_id: VertexId,
        keys: Vec<String>,
    ) -> Result<()> {
        if let Some(ref property_store) = self.property_store {
            property_store
                .delete_vertex_properties(vertex_id, keys)
                .await
        } else {
            Err(AsterError::configuration("Properties not enabled"))
        }
    }

    /// Execute a Gremlin traversal query
    pub async fn gremlin(&self, traversal: &GremlinTraversal) -> Result<GremlinResultSet> {
        let query_context = QueryContext::default();
        let mut gremlin_context = GremlinContext::new(query_context);

        let (results, stats) = self
            .gremlin_engine
            .execute(traversal, &mut gremlin_context)
            .await?;
        Ok(GremlinResultSet::new(results, stats))
    }

    /// Execute a Gremlin traversal query with custom context
    pub async fn gremlin_with_context(
        &self,
        traversal: &GremlinTraversal,
        context: &mut GremlinContext,
    ) -> Result<GremlinResultSet> {
        let (results, stats) = self.gremlin_engine.execute(traversal, context).await?;
        Ok(GremlinResultSet::new(results, stats))
    }

    /// Parse and execute a Gremlin query string
    pub async fn gremlin_query(&self, query: &str) -> Result<GremlinResultSet> {
        let traversal = GremlinEngine::parse_query(query)?;
        self.gremlin(&traversal).await
    }

    /// Create a Gremlin traversal starting with all vertices: g.V()
    pub fn g_v(&self) -> GremlinTraversal {
        GremlinTraversal::v(None)
    }

    /// Create a Gremlin traversal starting with specific vertices: g.V(id1, id2, ...)
    pub fn g_v_ids(&self, ids: Vec<VertexId>) -> GremlinTraversal {
        GremlinTraversal::v(Some(ids))
    }

    /// Create a Gremlin traversal starting with all edges: g.E()
    pub fn g_e(&self) -> GremlinTraversal {
        GremlinTraversal::e(None)
    }

    /// Create a Gremlin traversal starting with specific edges: g.E(id1, id2, ...)
    pub fn g_e_ids(&self, ids: Vec<EdgeId>) -> GremlinTraversal {
        GremlinTraversal::e(Some(ids))
    }

    /// Get the Gremlin engine reference for advanced usage
    pub fn gremlin_engine(&self) -> &GremlinEngine {
        &self.gremlin_engine
    }

    /// Execute an optimized range query with predicates
    pub async fn optimized_range_query(
        &self,
        start_vertex: VertexId,
        end_vertex: VertexId,
        predicates: Vec<query::QueryPredicate>,
    ) -> Result<(RangeQueryResult, query::QueryStats)> {
        let context = QueryContext::default();
        let mut optimizer = self.range_scan_optimizer.clone();

        // Optimize the query
        let plan = optimizer
            .optimize_range_query(start_vertex, end_vertex, predicates, &context)
            .await?;

        // Execute the optimized plan
        optimizer
            .execute_optimized_range_query(&plan, &context, &self.storage)
            .await
    }

    /// Create a query plan for a range query without executing it
    pub async fn plan_range_query(
        &self,
        start_vertex: VertexId,
        end_vertex: VertexId,
        predicates: Vec<query::QueryPredicate>,
    ) -> Result<QueryPlan> {
        let context = QueryContext::default();
        let mut optimizer = self.range_scan_optimizer.clone();

        optimizer
            .optimize_range_query(start_vertex, end_vertex, predicates, &context)
            .await
    }

    /// Execute a pre-created query plan
    pub async fn execute_query_plan(
        &self,
        plan: &QueryPlan,
    ) -> Result<(RangeQueryResult, query::QueryStats)> {
        let context = QueryContext::default();
        let optimizer = self.range_scan_optimizer.clone();

        optimizer
            .execute_optimized_range_query(plan, &context, &self.storage)
            .await
    }

    /// Get vertices in a range with basic filtering (unoptimized)
    pub async fn range_query(
        &self,
        start_vertex: VertexId,
        end_vertex: VertexId,
    ) -> Result<Vec<VertexId>> {
        let range_entries = self.storage.range(start_vertex, end_vertex).await?;
        Ok(range_entries
            .into_iter()
            .map(|(vertex_id, _)| vertex_id)
            .collect())
    }

    /// Get the range scan optimizer reference for advanced usage
    pub fn range_scan_optimizer(&self) -> &RangeScanOptimizer {
        &self.range_scan_optimizer
    }
}

impl EdgeRegistry for AsterDB {
    fn register_edge(&self, edge_id: EdgeId, source: VertexId, target: VertexId, label: String) {
        self.register_edge(edge_id, source, target, label);
    }

    fn get_outgoing_edges(
        &self,
        vertex_id: VertexId,
        label_filter: Option<&str>,
    ) -> Vec<EdgeRegistryEntry> {
        self.get_outgoing_edges(vertex_id, label_filter)
    }

    fn get_incoming_edges(
        &self,
        vertex_id: VertexId,
        label_filter: Option<&str>,
    ) -> Vec<EdgeRegistryEntry> {
        self.get_incoming_edges(vertex_id, label_filter)
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
