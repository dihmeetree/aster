# Adaptive Update Strategies

This document details Aster's implementation of adaptive update strategies, the core research contribution that dynamically selects between delta and pivot update methods based on vertex characteristics and workload patterns.

## Overview

The adaptive update system implements **Algorithm 1** from the research paper, which selects the optimal update method for each vertex modification based on:

- **Vertex Degree**: Estimated using Morris Counters
- **Workload Characteristics**: Lookup vs. update ratio
- **Cost Models**: Mathematical models from the paper (Equations 3, 4, 7, 8)
- **Real-time Feedback**: Performance monitoring and strategy effectiveness

## Core Algorithm Implementation

### Algorithm 1: Update Method Selection

```rust
impl AdaptiveUpdateStrategy {
    /// Core decision algorithm from the research paper
    pub fn select_update_method(&mut self, vertex_id: VertexId, degree: u32) -> UpdateMethod {
        // Record this decision request
        self.total_decisions += 1;

        // Calculate costs using paper equations
        let lookup_cost_delta = self.cost_model.calculate_lookup_cost_delta(degree);
        let lookup_cost_pivot = self.cost_model.calculate_lookup_cost_pivot(degree);

        // Apply workload weighting
        let lookup_ratio = self.workload_analyzer.get_current_lookup_ratio();
        let weighted_delta_cost = lookup_cost_delta * lookup_ratio;
        let weighted_pivot_cost = lookup_cost_pivot * lookup_ratio;

        // Select method with lower expected cost
        let selected_method = if weighted_delta_cost <= weighted_pivot_cost {
            self.delta_selections += 1;
            UpdateMethod::Delta
        } else {
            self.pivot_selections += 1;
            UpdateMethod::Pivot
        };

        // Record decision for effectiveness tracking
        self.record_decision(vertex_id, degree, selected_method);

        selected_method
    }
}
```

### Decision Factors

#### 1. Vertex Degree Estimation

Uses Morris Counters for space-efficient degree tracking:

```rust
/// Get current degree estimate for adaptive decisions
pub fn get_degree_estimate(&self, vertex_id: VertexId) -> u32 {
    let sketch = self.degree_sketch.read();
    sketch.get_degree_by_id(vertex_id.as_u64())
}
```

**Morris Counter Structure** (8-bit total):

```
Bit Layout: EEEE MMMM
- E (4 bits): Exponent (0-15)
- M (4 bits): Mantissa (0-15)
- Value = (16 + M) × 2^E
```

#### 2. Workload Analysis

```rust
pub struct WorkloadAnalyzer {
    lookup_count: AtomicU64,
    update_count: AtomicU64,
    window_size: usize,
    recent_operations: RwLock<VecDeque<OperationType>>,
}

impl WorkloadAnalyzer {
    pub fn get_current_lookup_ratio(&self) -> f64 {
        let lookups = self.lookup_count.load(Ordering::Relaxed);
        let updates = self.update_count.load(Ordering::Relaxed);
        let total = lookups + updates;

        if total == 0 { 0.5 } else { lookups as f64 / total as f64 }
    }
}
```

## Cost Models (Paper Equations)

### Equation 3: Delta Update Lookup Cost

```rust
impl CostModel {
    /// L_delta(d) = log₂(F) + d · log₂(B)
    pub fn calculate_lookup_cost_delta(&self, degree: u32) -> f64 {
        let files = self.estimate_files_containing_vertex() as f64;
        let avg_entries_per_block = self.config.avg_entries_per_block as f64;

        // Paper Equation 3
        files.log2() + (degree as f64) * avg_entries_per_block.log2()
    }
}
```

**Interpretation**:

- `log₂(F)`: Cost to search through F files containing the vertex
- `d · log₂(B)`: Cost scales linearly with degree (d), logarithmically with block size

### Equation 4: Pivot Update Lookup Cost

```rust
impl CostModel {
    /// L_pivot(d) = log₂(F) + log₂(d)
    pub fn calculate_lookup_cost_pivot(&self, degree: u32) -> f64 {
        let files = self.estimate_files_containing_vertex() as f64;

        // Paper Equation 4
        files.log2() + (degree as f64).log2()
    }
}
```

**Interpretation**:

- `log₂(F)`: Same file search cost as delta
- `log₂(d)`: Logarithmic scaling with degree (much better than linear for high degrees)

### Equation 7: Space Requirements

```rust
impl CostModel {
    /// Calculate space overhead for each method
    pub fn calculate_space_requirements(&self, degree: u32) -> (f64, f64) {
        let vertex_id_bits = 64.0; // Assuming 64-bit vertex IDs
        let timestamp_bits = 64.0; // Timestamp overhead

        // S_delta = d · (log₂(V) + log₂(T))
        let space_delta = (degree as f64) * (vertex_id_bits + timestamp_bits);

        // S_pivot = d · log₂(V) (single timestamp for entire neighbor list)
        let space_pivot = (degree as f64) * vertex_id_bits + timestamp_bits;

        (space_delta, space_pivot)
    }
}
```

### Equation 8: Update Costs

```rust
impl CostModel {
    /// Calculate update operation costs
    pub fn calculate_update_costs(&self, degree: u32) -> (f64, f64) {
        let io_cost_per_block = self.config.io_cost_per_block;

        // Delta update: single small write
        let update_cost_delta = io_cost_per_block;

        // Pivot update: read + write entire neighbor list
        let blocks_for_neighbors = ((degree as f64 * 8.0) / self.config.block_size as f64).ceil();
        let update_cost_pivot = blocks_for_neighbors * io_cost_per_block * 2.0; // read + write

        (update_cost_delta, update_cost_pivot)
    }
}
```

## Effectiveness Tracking

### Performance Metrics

```rust
pub struct EffectivenessTracker {
    decision_history: RwLock<VecDeque<Decision>>,
    method_performance: RwLock<HashMap<UpdateMethod, PerformanceStats>>,
    effectiveness_score: AtomicU64, // Fixed-point representation
}

#[derive(Debug, Clone)]
struct Decision {
    vertex_id: VertexId,
    degree: u32,
    method: UpdateMethod,
    timestamp: Instant,
}

#[derive(Debug, Clone)]
struct PerformanceStats {
    total_operations: u64,
    total_time_ns: u64,
    success_rate: f64,
    avg_cost: f64,
}
```

### Real-time Effectiveness Calculation

```rust
impl EffectivenessTracker {
    pub fn calculate_effectiveness(&self) -> f64 {
        let history = self.decision_history.read();
        let performance = self.method_performance.read();

        let mut total_benefit = 0.0;
        let mut total_decisions = 0;

        for decision in history.iter() {
            if let Some(stats) = performance.get(&decision.method) {
                // Calculate actual vs. alternative cost
                let actual_cost = stats.avg_cost;
                let alternative_cost = self.estimate_alternative_cost(decision);
                let benefit = (alternative_cost - actual_cost) / alternative_cost;

                total_benefit += benefit;
                total_decisions += 1;
            }
        }

        if total_decisions > 0 {
            total_benefit / total_decisions as f64
        } else {
            0.0
        }
    }
}
```

## Threshold Adaptation

### Dynamic Threshold Adjustment

```rust
impl AdaptiveUpdateStrategy {
    /// Adjust decision thresholds based on performance feedback
    pub fn recalibrate_thresholds(&mut self) {
        let effectiveness = self.effectiveness_tracker.calculate_effectiveness();
        let current_lookup_ratio = self.workload_analyzer.get_current_lookup_ratio();

        // Adjust degree threshold for method selection
        if effectiveness < self.config.min_effectiveness_threshold {
            // Strategy is underperforming, adjust thresholds
            if current_lookup_ratio > 0.7 {
                // Lookup-heavy workload: favor pivot updates
                self.degree_threshold = (self.degree_threshold as f64 * 0.9) as u32;
            } else {
                // Update-heavy workload: favor delta updates
                self.degree_threshold = (self.degree_threshold as f64 * 1.1) as u32;
            }
        }

        // Update cost model parameters based on recent measurements
        self.cost_model.update_parameters(&self.get_recent_measurements());
    }
}
```

### Workload-Aware Adaptation

```rust
impl WorkloadAnalyzer {
    pub fn analyze_workload_shift(&mut self) -> Option<WorkloadShift> {
        let recent_ops = self.recent_operations.read();
        let window_size = recent_ops.len();

        if window_size < self.min_window_size {
            return None;
        }

        let lookup_ratio = recent_ops.iter()
            .filter(|&op| matches!(op, OperationType::Lookup))
            .count() as f64 / window_size as f64;

        let previous_ratio = self.historical_lookup_ratio;
        let shift_threshold = 0.15; // 15% change threshold

        if (lookup_ratio - previous_ratio).abs() > shift_threshold {
            self.historical_lookup_ratio = lookup_ratio;

            if lookup_ratio > previous_ratio + shift_threshold {
                Some(WorkloadShift::MoreLookupHeavy)
            } else {
                Some(WorkloadShift::MoreUpdateHeavy)
            }
        } else {
            None
        }
    }
}
```

## Integration with Storage Engine

### Update Method Dispatch

```rust
impl PolyLSM {
    pub async fn add_edge(&self, source: VertexId, target: VertexId) -> Result<()> {
        // Get current degree estimate
        let degree = {
            let sketch = self.degree_sketch.read();
            sketch.get_degree_by_id(source.as_u64())
        };

        // Record lookup operation for workload analysis
        {
            let mut strategy = self.adaptive_strategy.lock();
            strategy.record_lookup();
        }

        // Select update method using adaptive strategy
        let update_method = {
            let mut strategy = self.adaptive_strategy.lock();
            strategy.select_update_method(source, degree)
        };

        // Execute the selected method
        let start_time = Instant::now();
        let result = match update_method {
            UpdateMethod::Delta => self.add_edge_delta(source, target).await,
            UpdateMethod::Pivot => self.add_edge_pivot(source, target).await,
        };
        let execution_time = start_time.elapsed();

        // Record performance feedback
        {
            let mut strategy = self.adaptive_strategy.lock();
            strategy.record_execution_time(update_method, execution_time);
        }

        result
    }
}
```

### Degree Sketch Maintenance

```rust
impl PolyLSM {
    /// Update degree sketch with new edge information
    fn update_degree_sketch(&self, vertex_id: VertexId, delta: i32) {
        let mut sketch = self.degree_sketch.write();

        if delta > 0 {
            for _ in 0..delta {
                sketch.increment_degree_by_id(vertex_id.as_u64());
            }
        } else {
            // Handle degree decrements (edge deletions)
            for _ in 0..(-delta) {
                sketch.decrement_degree_by_id(vertex_id.as_u64());
            }
        }
    }
}
```

## Configuration Parameters

### Paper-Specified Defaults

```rust
impl AdaptiveUpdateConfig {
    pub fn paper_specification() -> Self {
        Self {
            initial_degree_threshold: 10,     // Switch to pivot above this degree
            min_effectiveness_threshold: 0.8, // Minimum acceptable effectiveness
            workload_window_size: 1000,       // Operations to track for workload analysis
            recalibration_interval: 10000,    // How often to adjust thresholds
            cost_model_alpha: 0.1,            // Learning rate for cost model updates
        }
    }
}
```

### Tuning Guidelines

#### Lookup-Heavy Workloads

```rust
config.initial_degree_threshold = 5;  // Favor pivot updates earlier
config.lookup_weight = 0.8;           // Higher weight for lookup costs
```

#### Update-Heavy Workloads

```rust
config.initial_degree_threshold = 20; // Favor delta updates longer
config.update_weight = 0.8;           // Higher weight for update costs
```

#### High-Degree Graphs

```rust
config.degree_sketch_accuracy = 16;   // More bits for degree estimation
config.pivot_optimization = true;     // Enable pivot-specific optimizations
```

## Performance Analysis

### Effectiveness Metrics

```rust
pub struct AdaptiveAnalytics {
    pub decision_accuracy: f64,        // % of optimal decisions
    pub cost_reduction: f64,           // Savings vs. static strategy
    pub workload_adaptation_speed: f64, // How quickly it adapts to changes
    pub degree_estimation_error: f64,  // Morris counter accuracy
}
```

### Monitoring and Debugging

```rust
impl AdaptiveUpdateStrategy {
    pub fn get_debug_info(&self) -> AdaptiveDebugInfo {
        AdaptiveDebugInfo {
            current_threshold: self.degree_threshold,
            recent_decisions: self.get_recent_decision_summary(),
            effectiveness_trend: self.effectiveness_tracker.get_trend(),
            workload_characteristics: self.workload_analyzer.get_summary(),
            cost_model_parameters: self.cost_model.get_parameters(),
        }
    }
}
```

This adaptive update system provides the intelligence that makes Aster's storage engine efficient across diverse workload patterns, automatically optimizing for both high-degree and low-degree vertices based on real-time performance feedback and workload characteristics.
