# Aster Graph Database

A high-performance graph database built with Rust, featuring the innovative **Poly-LSM** storage engine. Aster is designed for large-scale, evolving graphs with intensive updates and lookups.

## 🚀 Key Features

- **Poly-LSM Storage Engine**: Hybrid vertex-based and edge-based storage with adaptive updates
- **High Performance**: Up to 17x throughput improvement over existing graph databases
- **Scalability**: Handles billion-scale graphs efficiently
- **Adaptive Updates**: Intelligent cost model that selects optimal storage strategy per operation
- **ACID Transactions**: Full transaction support with MVCC
- **Memory Efficient**: Morris Counter-based degree sketching with minimal overhead
- **Advanced Encoding**: Partitioned Elias-Fano compression for neighbor lists
- **Efficient I/O**: Block-based SSTable storage with LZ4 compression
- **Recovery Support**: Write-Ahead Logging (WAL) for durability and crash recovery
- **Query Engine**: Rich graph traversal and pattern matching capabilities
- **Metrics & Monitoring**: Built-in performance monitoring and statistics

## 🏗️ Architecture

### Poly-LSM Storage Engine

- **Hybrid Layout**: Co-existence of vertex-based and edge-based storage
- **Adaptive Mechanism**: Dynamic selection between delta and pivot updates based on vertex degree and workload
- **Degree Sketching**: Space-efficient vertex degree tracking using Morris Counters
- **LSM-Tree Optimization**: Custom LSM-tree design optimized for graph workloads

### Cost Model

The adaptive update mechanism uses a sophisticated cost model from the research paper:

- **Delta Update Cost**: `C_D = (2I·T·L)/B + (θ_L·d)/(θ_U·(T-1))`
- **Pivot Update Cost**: `C_P = 2 + ((d(u)+1)·I)/B + ((d(u)+2)·I·T·L)/B`

Where:

- `I`: Size of vertex ID in bytes
- `T`: Level size ratio in LSM-tree
- `L`: Number of levels
- `B`: Block size
- `θ_L`, `θ_U`: Lookup and update ratios in workload
- `d(u)`: Degree of vertex u

## 🛠️ Usage

### Basic Operations

```rust
use aster_db::{AsterDB, VertexId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database
    let db = AsterDB::open("./graph_data").await?;
    let graph = db.graph();

    // Add vertices with specific IDs
    let v1 = VertexId::from_u64(1);
    let v2 = VertexId::from_u64(2);

    graph.add_vertex(v1, None).await?;
    graph.add_vertex(v2, None).await?;

    // Add edge
    graph.add_edge(v1, v2, None).await?;

    // Query neighbors
    let neighbors = graph.get_neighbors(v1).await?;
    println!("Neighbors of {}: {:?}", v1, neighbors);

    Ok(())
}
```

### Advanced Configuration

```rust
use aster_db::{AsterDB, AsterDBConfig, RecoveryConfig, TransactionConfig, MetricsConfig};

let config = AsterDBConfig {
    enable_recovery: true,
    recovery_config: RecoveryConfig {
        wal_dir: "./wal".into(),
        checkpoint_interval_ms: 30000,
        max_wal_size_mb: 100,
        ..Default::default()
    },
    transaction_config: TransactionConfig {
        max_retries: 3,
        lock_timeout_ms: 5000,
        ..Default::default()
    },
    enable_metrics: true,
    metrics_config: MetricsConfig {
        collection_interval_ms: 1000,
        enable_alerts: true,
        ..Default::default()
    },
};

let db = AsterDB::open_with_config("./data", config).await?;
```

## 📊 Performance

Aster demonstrates exceptional performance improvements:

- **17x throughput** improvement on billion-scale Twitter dataset
- **Adaptive optimization** automatically adjusts to workload patterns
- **Scalable design** maintains performance as graph size increases
- **Memory efficient** degree sketching uses only 8 bits per vertex

## 🎯 Example Applications

### Movie Recommendation Engine

Run the included recommendation engine example:

```bash
cargo run --example recommendation_engine
```

This example demonstrates:

- Real-time collaborative filtering
- High-throughput mixed read/write workloads
- Adaptive storage optimization
- Complex graph traversals

Features:

- User-movie interaction graph
- Similarity-based recommendations
- Performance monitoring
- Realistic workload simulation

## 🧪 Running Tests

```bash
# Run all tests
cargo test

# Run integration tests
cargo test --test integration_tests

# Run specific module tests
cargo test storage::poly_lsm::tests

# Run with output
cargo test -- --nocapture
```

### Integration Tests

Aster includes comprehensive integration tests that cover:

- End-to-end graph operations
- Large-scale data scenarios with compaction
- Adaptive update strategy behavior
- Data persistence and recovery
- Performance under load
- Storage statistics and monitoring
- Edge cases and error handling

## 🔬 Benchmarks

```bash
# Run performance benchmarks
cargo bench

# Generate benchmark reports
cargo bench -- --output-format html
```

## 📁 Project Structure

```
src/
├── lib.rs              # Main library interface
├── error.rs            # Error handling
├── types.rs            # Core data types
├── storage/            # Poly-LSM storage engine
│   ├── mod.rs          # Storage module exports
│   ├── poly_lsm.rs     # Main storage engine
│   ├── adaptive_updates.rs # Adaptive update strategy
│   ├── memtable.rs     # In-memory tables
│   ├── sstable.rs      # Block-based SSTable storage
│   ├── compaction.rs   # Graph-aware compaction strategies
│   ├── block_cache.rs  # Block cache and memory management
│   ├── storage_manager.rs # Storage coordination
│   └── property_store.rs # Property indexing and storage
├── graph/              # Graph database layer
│   ├── mod.rs          # Graph module exports
│   ├── vertex.rs       # Vertex operations
│   ├── edge.rs         # Edge operations
│   └── traversal.rs    # Graph traversal algorithms
├── utils/              # Utility functions
│   ├── mod.rs          # Utils module exports
│   ├── morris_counter.rs # Morris Counter degree sketching
│   ├── encoding.rs     # Adaptive neighbor encoding
│   ├── elias_fano.rs   # Partitioned Elias-Fano compression
│   └── bloom_filter.rs # Bloom filters for SSTable
├── transaction.rs      # MVCC transaction support
├── recovery.rs         # WAL-based recovery system
├── metrics.rs          # Performance monitoring
├── query.rs            # Graph query engine
examples/
├── recommendation_engine.rs # Movie recommendation system
tests/
└── integration_tests.rs # Comprehensive integration tests
```

## 🔄 Recent Improvements

### v0.1.0 Features

- **Comprehensive Integration Tests**: End-to-end testing suite covering all major functionality
- **Fixed API Compatibility**: Corrected method signatures and parameter handling
- **Capacity Optimization**: Resolved Morris Counter overflow issues with sequential vertex IDs
- **Enhanced Documentation**: Updated README with current API and features
- **Example Applications**: Working movie recommendation engine demonstrating real-world usage
- **Robust Error Handling**: Improved error propagation and debugging information

### Performance Optimizations

- **Sequential Vertex IDs**: Prevents Morris Counter capacity overflow in degree sketching
- **Optimized Test Suite**: All integration tests pass with consistent performance
- **Memory Management**: Efficient block cache and memory pool implementations
- **Compilation Fixes**: All warnings addressed, clean compilation process

## 📚 Research Foundation

Aster is based on the research paper:

> **"Aster: Enhancing LSM-structures for Scalable Graph Database"**  
> _Dingheng Mo, Junfeng Liu, Fan Wang, and Siqiang Luo_  
> Proceedings of the ACM on Management of Data, 2025

The implementation faithfully follows the paper's design while adding production-ready features and optimizations.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔍 Citation

If you use Aster in your research, please cite:

```bibtex
@article{mo2025aster,
    title={Aster: Enhancing LSM-structures for Scalable Graph Database},
    author={Mo, Dingheng and Liu, Junfeng and Wang, Fan and Luo, Siqiang},
    journal={Proceedings of the ACM on Management of Data},
    volume={3},
    number={1},
    pages={1--26},
    year={2025},
    publisher={ACM}
}
```
