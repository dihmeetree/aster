//! Query processing and execution module for Aster Database
//!
//! This module provides comprehensive query capabilities including:
//! - Native graph traversal algorithms (BFS, DFS, shortest path)
//! - Gremlin query language interface
//! - Graph analytics and pattern matching
//! - Query optimization and caching

pub mod engine;
pub mod gremlin;
pub mod optimizer;

// Re-export commonly used types and functions
pub use engine::{
    AggregationFunction, AnalyticsResult, GraphPattern, Path, QueryContext, QueryEngine,
    QueryPredicate, QueryProjection, QueryResultSet, QueryStats, RangeQueryResult,
};

pub use gremlin::{
    GremlinContext, GremlinEngine, GremlinOrder, GremlinOrderDirection, GremlinPredicate,
    GremlinResult, GremlinResultSet, GremlinStep, GremlinTraversal,
};

pub use optimizer::{
    ExecutionStrategy, IndexType, IndexUsage, MergeStrategy, OptimizationStats, OptimizerConfig,
    QueryCost, QueryPlan, RangePartition, RangeScanOptimizer,
};
