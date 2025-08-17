# Aster API Reference

This document provides a comprehensive reference for Aster's public APIs, including graph operations, query interface, transaction management, and configuration options.

## Table of Contents

1. [Database Management](#database-management)
2. [Graph Operations](#graph-operations)
3. [Query Interface](#query-interface)
4. [Transaction Management](#transaction-management)
5. [Configuration](#configuration)
6. [Monitoring and Statistics](#monitoring-and-statistics)
7. [Error Handling](#error-handling)

## Database Management

### AsterDB

The main database handle providing connection management and high-level operations.

```rust
pub struct AsterDB {
    // Internal fields omitted
}

impl AsterDB {
    /// Open a database at the specified path
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self>

    /// Open a database with custom configuration
    pub async fn open_with_config<P: AsRef<Path>>(
        path: P,
        config: AsterDBConfig
    ) -> Result<Self>

    /// Close the database and flush all pending operations
    pub async fn close(self) -> Result<()>

    /// Get a reference to the storage engine
    pub fn storage(&self) -> &Arc<PolyLSM>

    /// Create a new transaction
    pub fn begin_transaction(&self) -> Result<Transaction>

    /// Commit a transaction
    pub async fn commit_transaction(&self, transaction: Transaction) -> Result<()>

    /// Rollback a transaction
    pub async fn rollback_transaction(&self, transaction: Transaction) -> Result<()>
}
```

### Configuration

```rust
#[derive(Debug, Clone)]
pub struct AsterDBConfig {
    pub storage_config: PolyLSMConfig,
    pub query_config: QueryEngineConfig,
    pub transaction_config: TransactionConfig,
    pub metrics_config: MetricsConfig,
}

impl Default for AsterDBConfig {
    fn default() -> Self {
        Self {
            storage_config: PolyLSMConfig::paper_specification(),
            query_config: QueryEngineConfig::default(),
            transaction_config: TransactionConfig::default(),
            metrics_config: MetricsConfig::default(),
        }
    }
}
```

## Graph Operations

### Graph Interface

The primary interface for graph operations, providing vertex and edge management.

```rust
pub struct Graph<'a> {
    storage: &'a Arc<PolyLSM>,
}

impl<'a> Graph<'a> {
    /// Create a new graph interface
    pub fn new(storage: &'a Arc<PolyLSM>) -> Self

    /// Add a vertex with optional properties
    pub async fn add_vertex(
        &self,
        vertex_id: VertexId,
        properties: Option<Properties>
    ) -> Result<()>

    /// Add an edge between two vertices
    pub async fn add_edge(
        &self,
        source: VertexId,
        target: VertexId,
        properties: Option<Properties>
    ) -> Result<()>

    /// Remove a vertex and all its edges
    pub async fn remove_vertex(&self, vertex_id: VertexId) -> Result<()>

    /// Remove an edge between two vertices
    pub async fn remove_edge(&self, source: VertexId, target: VertexId) -> Result<()>

    /// Get all neighbors of a vertex
    pub async fn get_neighbors(&self, vertex_id: VertexId) -> Result<Vec<VertexId>>

    /// Check if a vertex exists
    pub async fn contains_vertex(&self, vertex_id: VertexId) -> Result<bool>

    /// Check if an edge exists
    pub async fn contains_edge(&self, source: VertexId, target: VertexId) -> Result<bool>

    /// Get vertex properties
    pub async fn get_vertex_properties(&self, vertex_id: VertexId) -> Result<Option<Properties>>

    /// Set vertex properties
    pub async fn set_vertex_properties(
        &self,
        vertex_id: VertexId,
        properties: Properties
    ) -> Result<()>

    /// Get edge properties
    pub async fn get_edge_properties(
        &self,
        source: VertexId,
        target: VertexId
    ) -> Result<Option<Properties>>

    /// Set edge properties
    pub async fn set_edge_properties(
        &self,
        source: VertexId,
        target: VertexId,
        properties: Properties
    ) -> Result<()>
}
```

### Vertex and Edge Types

```rust
/// Unique identifier for vertices
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VertexId(u64);

impl VertexId {
    pub fn new(id: u64) -> Self { Self(id) }
    pub fn from_u64(id: u64) -> Self { Self(id) }
    pub fn as_u64(self) -> u64 { self.0 }
}

/// Unique identifier for edges
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId {
    pub source: VertexId,
    pub target: VertexId,
}

/// Property values supported by Aster
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Bytes(Vec<u8>),
}

/// Collection of properties for vertices and edges
pub type Properties = HashMap<String, PropertyValue>;
```

## Query Interface

### Gremlin Query Engine

Aster supports the Apache TinkerPop Gremlin graph traversal language.

```rust
pub struct QueryEngine {
    // Internal fields omitted
}

impl QueryEngine {
    /// Execute a Gremlin query string
    pub async fn execute_gremlin(&self, query: &str) -> Result<Vec<GremlinResult>>

    /// Execute a Gremlin query with parameters
    pub async fn execute_gremlin_with_params(
        &self,
        query: &str,
        params: HashMap<String, PropertyValue>
    ) -> Result<Vec<GremlinResult>>

    /// Prepare a Gremlin query for repeated execution
    pub async fn prepare_gremlin(&self, query: &str) -> Result<PreparedQuery>

    /// Execute a prepared query
    pub async fn execute_prepared(
        &self,
        prepared: &PreparedQuery,
        params: HashMap<String, PropertyValue>
    ) -> Result<Vec<GremlinResult>>
}

/// Result of a Gremlin query
#[derive(Debug, Clone)]
pub enum GremlinResult {
    Vertex(VertexId, Properties),
    Edge(EdgeId, Properties),
    Property(PropertyValue),
    Path(Vec<GremlinResult>),
    Map(HashMap<String, GremlinResult>),
    List(Vec<GremlinResult>),
}
```

### Graph Traversal Builder

For programmatic query construction:

```rust
pub struct GraphTraversal {
    // Internal fields omitted
}

impl GraphTraversal {
    /// Start traversal from all vertices
    pub fn V() -> Self

    /// Start traversal from specific vertices
    pub fn V_with_ids(ids: Vec<VertexId>) -> Self

    /// Start traversal from all edges
    pub fn E() -> Self

    /// Filter vertices by property
    pub fn has(self, key: &str, value: PropertyValue) -> Self

    /// Navigate to outgoing neighbors
    pub fn out(self) -> Self

    /// Navigate to incoming neighbors
    pub fn in_(self) -> Self

    /// Navigate to all neighbors
    pub fn both(self) -> Self

    /// Limit results
    pub fn limit(self, count: usize) -> Self

    /// Skip results
    pub fn skip(self, count: usize) -> Self

    /// Order results
    pub fn order_by(self, key: &str, direction: OrderDirection) -> Self

    /// Group results
    pub fn group_by(self, key: &str) -> Self

    /// Execute the traversal
    pub async fn execute(self) -> Result<Vec<GremlinResult>>
}

#[derive(Debug, Clone)]
pub enum OrderDirection {
    Ascending,
    Descending,
}
```

### Example Gremlin Queries

```rust
// Find all vertices
let results = query_engine.execute_gremlin("g.V()").await?;

// Find vertices with specific property
let results = query_engine.execute_gremlin(
    "g.V().has('name', 'Alice')"
).await?;

// Find neighbors of a vertex
let results = query_engine.execute_gremlin(
    "g.V(1).out()"
).await?;

// Find paths between vertices
let results = query_engine.execute_gremlin(
    "g.V(1).repeat(out()).until(hasId(5)).path()"
).await?;

// Parameterized query
let mut params = HashMap::new();
params.insert("startId".to_string(), PropertyValue::Integer(1));
params.insert("targetName".to_string(), PropertyValue::String("Bob".to_string()));

let results = query_engine.execute_gremlin_with_params(
    "g.V(startId).out().has('name', targetName)",
    params
).await?;
```

## Transaction Management

### Transaction Interface

```rust
pub struct Transaction {
    // Internal fields omitted
}

impl Transaction {
    /// Get transaction ID
    pub fn id(&self) -> TransactionId

    /// Get transaction isolation level
    pub fn isolation_level(&self) -> IsolationLevel

    /// Check if transaction is read-only
    pub fn is_read_only(&self) -> bool

    /// Get transaction start timestamp
    pub fn start_timestamp(&self) -> Timestamp
}

/// Transaction isolation levels
#[derive(Debug, Clone, Copy)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,      // Default: snapshot isolation
}

/// Transaction configuration
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    pub isolation_level: IsolationLevel,
    pub read_only: bool,
    pub timeout_seconds: Option<u64>,
    pub retry_on_conflict: bool,
    pub max_retries: usize,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            isolation_level: IsolationLevel::Snapshot,
            read_only: false,
            timeout_seconds: Some(30),
            retry_on_conflict: true,
            max_retries: 3,
        }
    }
}
```

### Transactional Operations

```rust
impl AsterDB {
    /// Execute operations within a transaction
    pub async fn with_transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Transaction, &Graph) -> Pin<Box<dyn Future<Output = Result<R>> + Send>>,
        R: Send,

    /// Execute read-only operations with snapshot isolation
    pub async fn with_snapshot<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Graph) -> Pin<Box<dyn Future<Output = Result<R>> + Send>>,
        R: Send,
}

// Example usage
let result = db.with_transaction(|tx, graph| {
    Box::pin(async move {
        graph.add_vertex(VertexId::new(1), None).await?;
        graph.add_vertex(VertexId::new(2), None).await?;
        graph.add_edge(VertexId::new(1), VertexId::new(2), None).await?;
        Ok(())
    })
}).await?;
```

## Configuration

### Storage Configuration

```rust
#[derive(Debug, Clone)]
pub struct PolyLSMConfig {
    // Paper-specified parameters
    pub max_levels: usize,                    // L = 4 levels
    pub level_size_ratio: usize,              // T = 10 size ratio
    pub block_size: usize,                    // B = 4KB blocks
    pub memtable_size: usize,                 // MemTable size limit

    // Adaptive strategy parameters
    pub lookup_ratio: f64,                    // Expected lookup/update ratio
    pub degree_sketch_bits_per_vertex: u8,    // Morris counter size

    // Performance tuning
    pub compaction_concurrency: usize,        // Parallel compaction threads
    pub bloom_filter_bits_per_key: usize,     // Bloom filter accuracy
    pub compression_enabled: bool,            // Enable block compression

    // Memory management
    pub block_cache_size: usize,              // Block cache size limit
    pub memory_pool_size: usize,              // Memory pool size limit
}

impl PolyLSMConfig {
    /// Paper-compliant configuration
    pub fn paper_specification() -> Self {
        Self {
            max_levels: 4,
            level_size_ratio: 10,
            block_size: 4 * 1024,
            memtable_size: 64 * 1024 * 1024,
            lookup_ratio: 0.5,
            degree_sketch_bits_per_vertex: 8,
            compaction_concurrency: 2,
            bloom_filter_bits_per_key: 10,
            compression_enabled: true,
            block_cache_size: 256 * 1024 * 1024,
            memory_pool_size: 32 * 1024 * 1024,
        }
    }

    /// High-performance configuration
    pub fn high_performance() -> Self {
        let mut config = Self::paper_specification();
        config.compaction_concurrency = num_cpus::get();
        config.block_cache_size = 1024 * 1024 * 1024; // 1GB
        config.memory_pool_size = 128 * 1024 * 1024;  // 128MB
        config
    }

    /// Memory-constrained configuration
    pub fn low_memory() -> Self {
        let mut config = Self::paper_specification();
        config.memtable_size = 16 * 1024 * 1024;      // 16MB
        config.block_cache_size = 32 * 1024 * 1024;   // 32MB
        config.memory_pool_size = 8 * 1024 * 1024;    // 8MB
        config
    }
}
```

### Query Engine Configuration

```rust
#[derive(Debug, Clone)]
pub struct QueryEngineConfig {
    pub max_query_timeout_seconds: u64,
    pub max_result_size: usize,
    pub enable_query_optimization: bool,
    pub enable_query_caching: bool,
    pub query_cache_size: usize,
    pub max_traversal_depth: usize,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            max_query_timeout_seconds: 300,  // 5 minutes
            max_result_size: 1_000_000,      // 1M results
            enable_query_optimization: true,
            enable_query_caching: true,
            query_cache_size: 1000,          // Cache 1000 queries
            max_traversal_depth: 100,        // Prevent infinite loops
        }
    }
}
```

## Monitoring and Statistics

### Performance Metrics

```rust
/// Comprehensive database statistics
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub storage_stats: PolyLSMStats,
    pub query_stats: QueryStats,
    pub transaction_stats: TransactionStats,
    pub lock_free_stats: LockFreeStats,
}

/// Storage engine statistics
#[derive(Debug, Clone)]
pub struct PolyLSMStats {
    pub active_memtable: MemTableStats,
    pub immutable_memtables: usize,
    pub levels: Vec<LevelStats>,
    pub total_vertices: usize,
    pub adaptive_stats: AdaptiveStats,
}

/// Adaptive strategy performance
#[derive(Debug, Clone)]
pub struct AdaptiveStats {
    pub delta_updates: u64,
    pub pivot_updates: u64,
    pub total_lookups: u64,
    pub avg_lookup_time_delta_ms: f64,
    pub avg_lookup_time_pivot_ms: f64,
    pub effectiveness_score: f64,
    pub current_threshold: u32,
}

/// Lock-free registry performance
#[derive(Debug, Clone)]
pub struct LockFreeStats {
    pub total_acquisitions: usize,
    pub failed_acquisitions: usize,
    pub contention_events: usize,
    pub success_rate: f64,
    pub avg_contention_per_operation: f64,
    pub active_vertices: usize,
}

impl AsterDB {
    /// Get comprehensive database statistics
    pub async fn get_stats(&self) -> DatabaseStats

    /// Get storage engine statistics
    pub async fn get_storage_stats(&self) -> PolyLSMStats

    /// Get adaptive strategy analytics
    pub fn get_adaptive_analytics(&self) -> (WorkloadAnalysis, EffectivenessMetrics)

    /// Get lock-free registry statistics
    pub fn get_lock_free_stats(&self) -> LockFreeStats
}
```

### Monitoring Interface

```rust
pub struct MetricsCollector {
    // Internal fields omitted
}

impl MetricsCollector {
    /// Start collecting metrics
    pub fn start(&self) -> Result<()>

    /// Stop collecting metrics
    pub fn stop(&self) -> Result<()>

    /// Get current metrics snapshot
    pub fn get_current_metrics(&self) -> DatabaseMetrics

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String

    /// Register a custom metric
    pub fn register_metric(&self, name: &str, metric: Box<dyn Metric>) -> Result<()>
}
```

## Error Handling

### Error Types

```rust
/// Main error type for Aster operations
#[derive(Debug, thiserror::Error)]
pub enum AsterError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Configuration error: {0}")]
    Configuration(String),
}

pub type Result<T> = std::result::Result<T, AsterError>;
```

### Error Recovery

```rust
impl AsterDB {
    /// Attempt to recover from database corruption
    pub async fn recover(&self) -> Result<RecoveryReport>

    /// Validate database integrity
    pub async fn validate(&self) -> Result<ValidationReport>

    /// Repair database if possible
    pub async fn repair(&self) -> Result<RepairReport>
}

#[derive(Debug)]
pub struct RecoveryReport {
    pub corrupted_files: Vec<PathBuf>,
    pub recovered_vertices: usize,
    pub recovered_edges: usize,
    pub lost_data_estimate: usize,
}
```

## Usage Examples

### Basic Operations

```rust
use aster_db::{AsterDB, Graph, VertexId, Properties, PropertyValue};

#[tokio::main]
async fn main() -> Result<()> {
    // Open database
    let db = AsterDB::open("./my_graph_db").await?;
    let graph = Graph::new(db.storage());

    // Add vertices
    let alice = VertexId::new(1);
    let bob = VertexId::new(2);

    let mut alice_props = Properties::new();
    alice_props.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
    alice_props.insert("age".to_string(), PropertyValue::Integer(30));

    graph.add_vertex(alice, Some(alice_props)).await?;
    graph.add_vertex(bob, None).await?;

    // Add edge
    graph.add_edge(alice, bob, None).await?;

    // Query neighbors
    let neighbors = graph.get_neighbors(alice).await?;
    println!("Alice's neighbors: {:?}", neighbors);

    Ok(())
}
```

### Transactional Operations

```rust
let result = db.with_transaction(|tx, graph| {
    Box::pin(async move {
        // All operations are atomic
        graph.add_vertex(VertexId::new(3), None).await?;
        graph.add_vertex(VertexId::new(4), None).await?;
        graph.add_edge(VertexId::new(3), VertexId::new(4), None).await?;

        // Transaction commits automatically if Ok is returned
        Ok("Transaction completed".to_string())
    })
}).await?;
```

### Gremlin Queries

```rust
let query_engine = db.query_engine();

// Find all vertices with name "Alice"
let results = query_engine.execute_gremlin(
    "g.V().has('name', 'Alice')"
).await?;

// Find friends of friends
let results = query_engine.execute_gremlin(
    "g.V().has('name', 'Alice').out('friends').out('friends').dedup()"
).await?;

// Complex traversal with filtering
let results = query_engine.execute_gremlin(
    "g.V().has('age', gte(25)).limit(10).values('name')"
).await?;
```

This API reference provides the complete interface for interacting with Aster, from basic graph operations to advanced query processing and performance monitoring.
