# Poly-LSM Storage Engine

The Poly-LSM storage engine is the core component of Aster, implementing the research paper's adaptive update strategies within an LSM-tree structure optimized for graph workloads.

## Overview

The Poly-LSM engine combines traditional LSM-tree benefits (write optimization, compaction) with graph-specific optimizations:

- **Adaptive Update Strategies**: Dynamic selection between delta and pivot updates
- **Degree-Aware Optimization**: Uses vertex degree for update method selection
- **Multi-Version Storage**: MVCC support with timestamp-based versioning
- **Lock-Free Concurrency**: Atomic operations for high-performance coordination

## Paper Compliance

### LSM-Tree Configuration (Paper Section 3.1)

```rust
impl PolyLSMConfig {
    pub fn paper_specification() -> Self {
        Self {
            max_levels: 4,                    // L = 4 levels (paper specification)
            level_size_ratio: 10,            // T = 10 (size ratio between levels)
            block_size: 4 * 1024,           // B = 4KB (block size)
            memtable_size: 64 * 1024 * 1024, // 64MB MemTable
            bloom_filter_bits_per_key: 10,   // Standard Bloom filter configuration
            compression_enabled: true,        // LZ4 compression for SSTables
        }
    }
}
```

### Level Structure

The LSM-tree maintains exactly **4 levels (L=4)** with exponential size growth:

```
Level 0: 64MB   (MemTable flush target)
Level 1: 64MB   (Base level)
Level 2: 640MB  (10x growth)
Level 3: 6.4GB  (10x growth)
Level 4: 64GB   (10x growth)
```

## Update Methods

### Delta Updates (Edge-Based)

**When Used**: Low-degree vertices or update-heavy workloads

```rust
async fn add_edge_delta(&self, source: VertexId, target: VertexId) -> Result<()> {
    let neighbors = vec![target];
    let encoded_data = encode_neighbors(&neighbors);
    let entry = MemTableEntry::new_delta(encoded_data, Timestamp::now());

    self.insert_entry(source, entry).await?;

    // Update degree sketch for future decisions
    let mut sketch = self.degree_sketch.write();
    sketch.increment_degree_by_id(source.as_u64());

    Ok(())
}
```

**Advantages**:

- Fast writes (O(1) insertion)
- Minimal data movement
- Good for high-frequency updates

**Lookup Cost** (Paper Equation 3):

```
L_delta(d) = log₂(F) + d · log₂(B)
```

Where:

- F = number of SSTable files containing the vertex
- d = vertex degree
- B = average number of entries per block

### Pivot Updates (Vertex-Based)

**When Used**: High-degree vertices or lookup-heavy workloads

```rust
async fn add_edge_pivot(&self, source: VertexId, target: VertexId) -> Result<()> {
    // Acquire exclusive access using lock-free coordination
    let _guard = self.acquire_vertex_exclusive(source).await?;

    // Read current neighbors and merge with new edge
    let current_neighbors = self.get_neighbors(source).await?;
    let mut all_neighbors = current_neighbors;
    all_neighbors.push(target);
    all_neighbors.sort_by_key(|v| v.as_u64());
    all_neighbors.dedup();

    // Create complete neighbor list entry
    let encoded_data = encode_neighbors(&all_neighbors);
    let entry = MemTableEntry::new_pivot(encoded_data, Timestamp::now());

    self.insert_entry(source, entry).await?;
    Ok(())
}
```

**Advantages**:

- Fast lookups (O(1) neighbor retrieval)
- Optimal for read-heavy workloads
- Eliminates need to merge multiple delta entries

**Lookup Cost** (Paper Equation 4):

```
L_pivot(d) = log₂(F) + log₂(d)
```

## Entry Types and Encoding

### MemTable Entry Structure

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct MemTableEntry {
    pub entry_type: EntryType,
    pub data: Vec<u8>,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    Delta,     // Edge addition/removal
    Pivot,     // Complete neighbor list
    Tombstone, // Vertex deletion marker
}
```

### Neighbor List Encoding

The storage engine supports multiple encoding strategies:

#### Basic Delta Compression

```rust
pub fn encode_neighbors(neighbors: &[VertexId]) -> Vec<u8> {
    // Sort and deduplicate
    let mut sorted_neighbors: Vec<u64> = neighbors.iter().map(|v| v.as_u64()).collect();
    sorted_neighbors.sort_unstable();
    sorted_neighbors.dedup();

    // Delta encode with variable-length integers
    let mut encoded = Vec::new();
    encode_varint(sorted_neighbors.len() as u64, &mut encoded);

    let mut prev = 0u64;
    for &vertex_id in &sorted_neighbors {
        let delta = vertex_id - prev;
        encode_varint(delta, &mut encoded);
        prev = vertex_id;
    }

    encoded
}
```

#### Partitioned Elias-Fano Compression

For large neighbor lists, the engine uses Partitioned Elias-Fano compression achieving **2 + log₂(N_j/t) bits per element**:

```rust
pub fn encode_neighbors_compressed(neighbors: &[VertexId]) -> Result<Vec<u8>> {
    let config = EliasFanoConfig {
        segment_count: calculate_optimal_segments(neighbors.len()),
        max_segment_size: 4096,  // Align with block size
        use_optimal_allocation: true,
    };

    let elias_fano = PartitionedEliasFano::encode(neighbors, config)?;
    Ok(elias_fano.serialize())
}
```

## Adaptive Strategy Integration

### Cost Model Implementation

```rust
impl CostModel {
    /// Paper Equation 3: Delta update lookup cost
    pub fn calculate_lookup_cost_delta(&self, degree: u32) -> f64 {
        let files = self.estimate_files_containing_vertex();
        let blocks_per_file = self.estimate_avg_blocks_per_file();

        files.log2() + (degree as f64) * blocks_per_file.log2()
    }

    /// Paper Equation 4: Pivot update lookup cost
    pub fn calculate_lookup_cost_pivot(&self, degree: u32) -> f64 {
        let files = self.estimate_files_containing_vertex();

        files.log2() + (degree as f64).log2()
    }
}
```

### Update Method Selection

```rust
pub fn select_update_method(&mut self, vertex_id: VertexId, degree: u32) -> UpdateMethod {
    // Calculate expected costs for both methods
    let delta_cost = self.cost_model.calculate_lookup_cost_delta(degree);
    let pivot_cost = self.cost_model.calculate_lookup_cost_pivot(degree);

    // Consider workload characteristics
    let lookup_ratio = self.workload_analyzer.get_current_lookup_ratio();
    let weighted_delta_cost = delta_cost * lookup_ratio;
    let weighted_pivot_cost = pivot_cost * lookup_ratio;

    // Select method with lower expected cost
    if weighted_delta_cost <= weighted_pivot_cost {
        self.record_delta_selection(vertex_id, degree);
        UpdateMethod::Delta
    } else {
        self.record_pivot_selection(vertex_id, degree);
        UpdateMethod::Pivot
    }
}
```

## Lock-Free Concurrency

### Vertex Coordination

The engine uses atomic Compare-And-Swap operations for vertex-level coordination:

```rust
pub struct LockFreeVertexRegistry {
    vertex_states: RwLock<HashMap<VertexId, Arc<AtomicVertexState>>>,
    operation_counter: AtomicU64,
    performance_counters: PerformanceCounters,
}

struct AtomicVertexState {
    operation_id: AtomicU64,    // 0 = available, >0 = in use
    wait_count: AtomicUsize,    // Number of waiting operations
    last_access: AtomicU64,     // Timestamp for cleanup
}
```

### Exclusive Access Protocol

```rust
pub async fn acquire_exclusive(&self, vertex_id: VertexId) -> Result<VertexGuard> {
    let operation_id = self.operation_counter.fetch_add(1, Ordering::SeqCst);

    for retry in 0..max_retries {
        match vertex_state.operation_id.compare_exchange_weak(
            0, operation_id, Ordering::SeqCst, Ordering::Relaxed
        ) {
            Ok(_) => {
                // Successfully acquired exclusive access
                return Ok(VertexGuard { vertex_id, operation_id, registry: self });
            }
            Err(_) => {
                // Exponential backoff with jitter
                let base_delay = Duration::from_micros(1 << min(retry, 10));
                let jitter = Duration::from_micros((operation_id % 100) * 10);
                tokio::time::sleep(base_delay + jitter).await;
            }
        }
    }

    Err(AsterError::storage("Failed to acquire exclusive access"))
}
```

## Compaction Strategy

### Parallel Multi-Level Compaction

```rust
async fn maybe_trigger_compaction(&self) -> Result<()> {
    // Identify levels needing compaction
    let compaction_candidates = self.identify_compaction_candidates();

    // Process multiple levels in parallel
    let mut compaction_tasks = Vec::new();

    for level_num in compaction_candidates {
        if let Ok(_permit) = self.compaction_semaphore.try_acquire() {
            let poly_lsm_clone = self.clone();
            let task = tokio::spawn(async move {
                poly_lsm_clone.compact_level(level_num).await
            });
            compaction_tasks.push(task);
        }
    }

    // Optionally wait for critical compactions
    if compaction_tasks.len() > 1 {
        if let Some(task) = compaction_tasks.first_mut() {
            let _ = task.await;  // Wait for at least one to complete
        }
    }

    Ok(())
}
```

### Entry Merging During Compaction

```rust
async fn merge_entries(&self, mut entries: Vec<MemTableEntry>) -> Result<Vec<VertexId>> {
    // Sort by timestamp (newest first) for MVCC resolution
    entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    let mut current_neighbors = Vec::new();

    for entry in entries {
        match entry.entry_type {
            EntryType::Pivot => {
                // Pivot entry is authoritative - stop processing
                current_neighbors = decode_neighbors(&entry.data)?;
                break;
            }
            EntryType::Delta => {
                // Accumulate delta changes
                let delta_neighbors = decode_neighbors(&entry.data)?;
                current_neighbors.extend(delta_neighbors);
            }
            EntryType::Tombstone => {
                // Vertex deletion - clear neighbors
                current_neighbors.clear();
                break;
            }
        }
    }

    // Final deduplication and sorting
    current_neighbors.sort_by_key(|v| v.as_u64());
    current_neighbors.dedup();

    Ok(current_neighbors)
}
```

## Performance Characteristics

### Memory Usage

```rust
pub struct PolyLSMStats {
    pub active_memtable: MemTableStats,
    pub immutable_memtables: usize,
    pub levels: Vec<LevelStats>,
    pub adaptive_stats: AdaptiveStats,
}

pub struct AdaptiveStats {
    pub delta_updates: u64,
    pub pivot_updates: u64,
    pub total_lookups: u64,
    pub avg_lookup_time_delta_ms: f64,
    pub avg_lookup_time_pivot_ms: f64,
    pub effectiveness_score: f64,
}
```

### Degree Sketching Integration

The storage engine maintains Morris Counters for degree estimation:

```rust
impl PolyLSM {
    pub async fn add_edge(&self, source: VertexId, target: VertexId) -> Result<()> {
        // Get current degree estimate
        let degree = {
            let sketch = self.degree_sketch.read();
            sketch.get_degree_by_id(source.as_u64())
        };

        // Select update method using adaptive strategy
        let update_method = {
            let mut strategy = self.adaptive_strategy.lock();
            strategy.select_update_method(source, degree)
        };

        // Execute the selected update method
        match update_method {
            UpdateMethod::Delta => self.add_edge_delta(source, target).await,
            UpdateMethod::Pivot => self.add_edge_pivot(source, target).await,
        }
    }
}
```

## Configuration and Tuning

### Paper-Specified Parameters

```rust
impl PolyLSMConfig {
    pub fn validate_paper_compliance(&self) -> Result<()> {
        if self.max_levels != 4 {
            return Err(AsterError::validation("Paper specifies L=4 levels"));
        }
        if self.level_size_ratio != 10 {
            return Err(AsterError::validation("Paper specifies T=10 size ratio"));
        }
        if self.block_size != 4 * 1024 {
            return Err(AsterError::validation("Paper specifies B=4KB block size"));
        }
        Ok(())
    }
}
```

### Adaptive Tuning

The engine continuously adjusts parameters based on workload:

```rust
pub fn update_adaptive_strategy(&self) -> Result<()> {
    // Sample degree distribution
    let degrees = self.sample_degree_distribution(1000);

    // Update strategy parameters
    let mut strategy = self.adaptive_strategy.lock();
    strategy.update_degree_distribution(&degrees);
    strategy.recalibrate_thresholds();

    Ok(())
}
```

This storage engine implementation provides the foundation for Aster's high-performance graph processing while maintaining strict compliance with the research paper's algorithms and optimizations.
