# Aster Graph Database Documentation

Welcome to the comprehensive documentation for Aster, a high-performance graph database implementation based on the research paper "Aster: An Efficient Multi-Version Graph Store with Adaptive Update Strategies".

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Performance Features](#performance-features)
5. [API Documentation](#api-documentation)
6. [Paper Compliance](#paper-compliance)
7. [Getting Started](#getting-started)

## Overview

Aster is a multi-version graph database that implements adaptive update strategies for optimal performance across different workload patterns. The database combines the efficiency of LSM-tree storage with intelligent neighbor list encoding and degree-aware update selection.

### Key Features

- **Adaptive Update Strategies**: Dynamically selects between delta and pivot updates based on vertex degree and workload characteristics
- **Poly-LSM Storage Engine**: Specialized LSM-tree implementation optimized for graph data
- **Lock-Free Concurrency**: High-performance atomic operations for vertex coordination
- **Degree Sketching**: Morris Counter-based degree estimation for optimization decisions
- **Partitioned Elias-Fano Compression**: Space-efficient neighbor list encoding
- **MVCC Transactions**: Multi-version concurrency control with snapshot isolation
- **Gremlin Query Support**: Apache TinkerPop-compatible graph traversal language

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Layer                         │
├─────────────────────────────────────────────────────────┤
│  Gremlin Query Engine  │  Transaction Manager  │ API   │
├─────────────────────────────────────────────────────────┤
│              Adaptive Update Strategy                   │
├─────────────────────────────────────────────────────────┤
│        Graph Layer (Vertices, Edges, Properties)       │
├─────────────────────────────────────────────────────────┤
│                  Poly-LSM Storage Engine                │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────┐   │
│  │MemTable │ │L0 SSTbs │ │L1 SSTbs │ │...Ln SSTbs  │   │
│  └─────────┘ └─────────┘ └─────────┘ └─────────────┘   │
├─────────────────────────────────────────────────────────┤
│  Block Cache │ Compression │ Bloom Filters │ Encoding  │
└─────────────────────────────────────────────────────────┘
```

## Core Components

### 1. **Poly-LSM Storage Engine** (`src/storage/poly_lsm.rs`)

- Paper-compliant LSM-tree with L=4 levels and T=10 size ratio
- Adaptive update method selection (delta vs pivot)
- Parallel compaction with semaphore-controlled concurrency
- Lock-free vertex coordination using atomic operations

### 2. **Adaptive Update Strategy** (`src/storage/adaptive_updates.rs`)

- Implements Algorithm 1 from the paper for update method selection
- Cost model based on Equations 3, 4, 7, 8 from the research
- Real-time workload analysis and effectiveness metrics
- Dynamic threshold adjustment based on performance feedback

### 3. **Degree Sketching** (`src/utils/morris_counter.rs`)

- Morris Counter implementation with 8-bit structure (4-bit exponent + 4-bit mantissa)
- Space-efficient degree estimation for large graphs
- Used by adaptive strategy for update method selection

### 4. **Neighbor List Encoding** (`src/utils/encoding.rs`, `src/utils/elias_fano.rs`)

- Basic delta compression for small neighbor lists
- Partitioned Elias-Fano compression for large lists
- Achieves 2 + log₂(N_j/t) bits per element compression ratio
- SIMD-ready algorithms for high-performance encoding/decoding

### 5. **Block Cache and Memory Management** (`src/storage/block_cache.rs`)

- Multi-tier memory pooling (0-4KB, 4KB-16KB, 16KB-64KB, 64KB+)
- LRU eviction with configurable size limits
- LZ4 compression for cached blocks
- Performance monitoring with hit/miss ratios

## Performance Features

### Lock-Free Vertex Registry

```rust
/// Uses Compare-And-Swap operations for vertex coordination
pub struct LockFreeVertexRegistry {
    vertex_states: RwLock<HashMap<VertexId, Arc<AtomicVertexState>>>,
    operation_counter: AtomicU64,
    // Performance counters
    total_acquisitions: AtomicUsize,
    contention_events: AtomicUsize,
}
```

### Parallel Compaction

- Concurrent processing of multiple LSM levels
- Semaphore-controlled resource management
- Exponential backoff with jitter for contention reduction

### SIMD-Ready Algorithms

- Chunk-based neighbor encoding for vectorization
- Optimized memory layouts for cache efficiency
- Batch processing patterns throughout the codebase

## Paper Compliance

Aster implements the key algorithms and optimizations described in the research paper:

### Algorithm 1: Adaptive Update Method Selection

```rust
fn select_update_method(&mut self, vertex_id: VertexId, degree: u32) -> UpdateMethod {
    let lookup_cost_delta = self.cost_model.calculate_lookup_cost_delta(degree);
    let lookup_cost_pivot = self.cost_model.calculate_lookup_cost_pivot(degree);

    if lookup_cost_delta <= lookup_cost_pivot {
        UpdateMethod::Delta
    } else {
        UpdateMethod::Pivot
    }
}
```

### Equation 3: Delta Update Lookup Cost

```
L_delta(d) = log₂(F) + d · log₂(B)
```

Where F is the number of files and d is the vertex degree.

### Equation 4: Pivot Update Lookup Cost

```
L_pivot(d) = log₂(F) + log₂(d)
```

### Equation 7: Space Requirements

```
S_delta = d · (log₂(V) + log₂(T))
S_pivot = d · log₂(V)
```

### Equation 8: Update Costs

The implementation includes comprehensive cost modeling for both update methods based on I/O operations and compression efficiency.

## Documentation Structure

- **[Architecture Guide](./architecture.md)**: Detailed system architecture and design decisions
- **[Storage Engine](./storage-engine.md)**: Poly-LSM implementation and paper compliance
- **[Adaptive Updates](./adaptive-updates.md)**: Update strategy algorithms and cost models
- **[Performance Optimization](./performance.md)**: Lock-free structures and parallel processing
- **[Query Engine](./query-engine.md)**: Gremlin implementation and optimization
- **[Transaction System](./transactions.md)**: MVCC and snapshot isolation
- **[API Reference](./api-reference.md)**: Complete API documentation
- **[Configuration](./configuration.md)**: Tuning parameters and paper-specified defaults
- **[Benchmarks](./benchmarks.md)**: Performance characteristics and scalability

## Getting Started

1. **Installation**: See [Getting Started Guide](./getting-started.md)
2. **Configuration**: Review [Configuration Guide](./configuration.md)
3. **Examples**: Explore [Examples Directory](./examples/)
4. **Performance Tuning**: See [Performance Guide](./performance.md)

## Research Paper Reference

This implementation is based on:

> "Aster: An Efficient Multi-Version Graph Store with Adaptive Update Strategies"
> [Paper available at: `aster.pdf`]

Key contributions implemented:

- Adaptive update strategies for graph modifications
- Poly-LSM storage engine optimized for graph workloads
- Degree sketching for efficient vertex degree estimation
- Partitioned Elias-Fano compression for neighbor lists
- Cost models for update method selection

For detailed algorithmic descriptions and theoretical analysis, please refer to the original research paper.
