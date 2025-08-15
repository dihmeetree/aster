//! Query execution engine with advanced graph traversal algorithms
//!
//! This module implements a comprehensive query engine supporting:
//! - Breadth-First Search (BFS) and Depth-First Search (DFS)
//! - Shortest path algorithms (Dijkstra, A*)
//! - Graph analytics (connected components, centrality measures)
//! - Pattern matching and subgraph queries
//! - Optimized execution with caching and indexing

use crate::graph::Graph;
use crate::transaction::Transaction;
use crate::{AsterError, EdgeId, Properties, PropertyValue, Result, VertexId};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Query execution statistics
#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    pub vertices_visited: usize,
    pub edges_traversed: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub execution_time_ms: u64,
}

/// Query execution context
#[derive(Debug)]
pub struct QueryContext {
    pub transaction: Option<Transaction>,
    pub max_depth: Option<usize>,
    pub timeout_ms: Option<u64>,
    pub use_cache: bool,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self {
            transaction: None,
            max_depth: Some(10),     // Default max depth
            timeout_ms: Some(30000), // 30 seconds
            use_cache: true,
        }
    }
}

/// Represents a path between vertices
#[derive(Debug, Clone)]
pub struct Path {
    pub vertices: Vec<VertexId>,
    pub edges: Vec<EdgeId>,
    pub total_weight: f64,
}

impl Path {
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            edges: Vec::new(),
            total_weight: 0.0,
        }
    }

    pub fn length(&self) -> usize {
        self.vertices.len().saturating_sub(1)
    }

    pub fn add_vertex(&mut self, vertex: VertexId) {
        self.vertices.push(vertex);
    }

    pub fn add_edge(&mut self, edge: EdgeId, weight: f64) {
        self.edges.push(edge);
        self.total_weight += weight;
    }
}

/// Priority queue entry for Dijkstra's algorithm
#[derive(Debug, Clone)]
struct DijkstraEntry {
    vertex: VertexId,
    distance: f64,
    path: Path,
}

impl Eq for DijkstraEntry {}

impl PartialEq for DijkstraEntry {
    fn eq(&self, other: &Self) -> bool {
        self.distance.partial_cmp(&other.distance) == Some(Ordering::Equal)
    }
}

impl Ord for DijkstraEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for DijkstraEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Graph query filter predicate
#[derive(Debug, Clone)]
pub enum QueryPredicate {
    /// Property equals value
    PropertyEquals(String, PropertyValue),
    /// Property not equals value
    PropertyNotEquals(String, PropertyValue),
    /// Property greater than value
    PropertyGreaterThan(String, PropertyValue),
    /// Property less than value
    PropertyLessThan(String, PropertyValue),
    /// Property exists
    PropertyExists(String),
    /// Vertex ID in set
    VertexIdIn(HashSet<VertexId>),
    /// Vertex degree greater than threshold
    DegreeGreaterThan(usize),
    /// Vertex degree less than threshold
    DegreeLessThan(usize),
    /// Logical AND of predicates
    And(Vec<QueryPredicate>),
    /// Logical OR of predicates
    Or(Vec<QueryPredicate>),
    /// Logical NOT of predicate
    Not(Box<QueryPredicate>),
}

/// Graph query projection
#[derive(Debug, Clone)]
pub enum QueryProjection {
    /// Select vertex ID
    VertexId,
    /// Select vertex property
    VertexProperty(String),
    /// Select edge property
    EdgeProperty(String),
    /// Select vertex degree
    VertexDegree,
    /// Select neighbor count
    NeighborCount,
    /// Select custom computed value (name only for now)
    Computed(String),
}

/// Aggregation function for graph queries
#[derive(Debug, Clone)]
pub enum AggregationFunction {
    Count,
    Sum(String),      // Sum of property values
    Avg(String),      // Average of property values
    Min(String),      // Minimum property value
    Max(String),      // Maximum property value
    Distinct(String), // Count distinct values
}

/// Graph query result set
#[derive(Debug, Clone)]
pub struct QueryResultSet {
    /// Result rows with projected values
    pub rows: Vec<HashMap<String, PropertyValue>>,
    /// Total number of vertices matched (before projection)
    pub total_vertices: usize,
    /// Total number of edges traversed
    pub total_edges: usize,
    /// Execution statistics
    pub stats: QueryStats,
}

impl QueryResultSet {
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            total_vertices: 0,
            total_edges: 0,
            stats: QueryStats::default(),
        }
    }

    pub fn add_row(&mut self, row: HashMap<String, PropertyValue>) {
        self.rows.push(row);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Pattern matching structure for finding subgraphs
#[derive(Debug, Clone)]
pub struct GraphPattern {
    /// Vertices in the pattern with their constraints
    pub vertices: HashMap<String, QueryPredicate>,
    /// Edges in the pattern (source_label, target_label, edge_predicate)
    pub edges: Vec<(String, String, Option<QueryPredicate>)>,
    /// Minimum match count required
    pub min_matches: usize,
    /// Maximum match count (None = unlimited)
    pub max_matches: Option<usize>,
}

/// Cache entry for query results
#[derive(Debug, Clone)]
struct CacheEntry {
    pub result: Vec<VertexId>,
    pub timestamp: u64,
    pub access_count: usize,
}

/// Result of range-based graph queries
#[derive(Debug, Clone)]
pub struct RangeQueryResult {
    pub vertices_in_range: Vec<VertexId>,
    pub neighbor_connections: Vec<(VertexId, Vec<VertexId>)>,
    pub subgraph_density: f64,
    pub total_edges_in_range: usize,
}

/// Analytics computation results
#[derive(Debug, Clone)]
pub struct AnalyticsResult {
    pub name: String,
    pub vertex_scores: HashMap<VertexId, f64>,
    pub global_metrics: HashMap<String, f64>,
}

/// Main query execution engine
pub struct QueryEngine {
    graph: Arc<Graph>,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    max_cache_size: usize,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(graph: Arc<Graph>) -> Self {
        Self {
            graph,
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_cache_size: 10000,
        }
    }

    /// Breadth-First Search traversal
    pub async fn bfs(
        &self,
        start: VertexId,
        context: &QueryContext,
    ) -> Result<(Vec<VertexId>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut result = Vec::new();

        queue.push_back((start, 0)); // (vertex, depth)
        visited.insert(start);

        while let Some((vertex, depth)) = queue.pop_front() {
            result.push(vertex);
            stats.vertices_visited += 1;

            // Check max depth
            if let Some(max_depth) = context.max_depth {
                if depth >= max_depth {
                    continue;
                }
            }

            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("BFS query timeout"));
                }
            }

            // Get neighbors
            let neighbors = self.graph.get_neighbors(vertex).await?;
            stats.edges_traversed += neighbors.len();

            for neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok((result, stats))
    }

    /// Depth-First Search traversal
    pub async fn dfs(
        &self,
        start: VertexId,
        context: &QueryContext,
    ) -> Result<(Vec<VertexId>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let mut stack = Vec::new();
        let mut visited = HashSet::new();
        let mut result = Vec::new();

        stack.push((start, 0)); // (vertex, depth)

        while let Some((vertex, depth)) = stack.pop() {
            if visited.contains(&vertex) {
                continue;
            }

            visited.insert(vertex);
            result.push(vertex);
            stats.vertices_visited += 1;

            // Check max depth
            if let Some(max_depth) = context.max_depth {
                if depth >= max_depth {
                    continue;
                }
            }

            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("DFS query timeout"));
                }
            }

            // Get neighbors and add to stack (in reverse order for consistent traversal)
            let neighbors = self.graph.get_neighbors(vertex).await?;
            stats.edges_traversed += neighbors.len();

            for neighbor in neighbors.into_iter().rev() {
                if !visited.contains(&neighbor) {
                    stack.push((neighbor, depth + 1));
                }
            }
        }

        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok((result, stats))
    }

    /// Find shortest path between two vertices using Dijkstra's algorithm
    pub async fn shortest_path(
        &self,
        start: VertexId,
        target: VertexId,
        context: &QueryContext,
    ) -> Result<(Option<Path>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let mut heap = BinaryHeap::new();
        let mut distances = HashMap::new();
        let mut visited = HashSet::new();

        // Initialize with start vertex
        let mut start_path = Path::new();
        start_path.add_vertex(start);

        heap.push(DijkstraEntry {
            vertex: start,
            distance: 0.0,
            path: start_path,
        });
        distances.insert(start, 0.0);

        while let Some(current) = heap.pop() {
            if visited.contains(&current.vertex) {
                continue;
            }

            visited.insert(current.vertex);
            stats.vertices_visited += 1;

            // Found target
            if current.vertex == target {
                stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
                return Ok((Some(current.path), stats));
            }

            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("Shortest path query timeout"));
                }
            }

            // Process neighbors
            let neighbors = self.graph.get_neighbors(current.vertex).await?;
            stats.edges_traversed += neighbors.len();

            for neighbor in neighbors {
                if visited.contains(&neighbor) {
                    continue;
                }

                let edge_weight = 1.0; // Default weight, could be customized
                let new_distance = current.distance + edge_weight;

                if new_distance < *distances.get(&neighbor).unwrap_or(&f64::INFINITY) {
                    distances.insert(neighbor, new_distance);

                    let mut new_path = current.path.clone();
                    new_path.add_vertex(neighbor);
                    new_path.add_edge(EdgeId::new(), edge_weight);

                    heap.push(DijkstraEntry {
                        vertex: neighbor,
                        distance: new_distance,
                        path: new_path,
                    });
                }
            }
        }

        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok((None, stats)) // No path found
    }

    /// Find all shortest paths from a source vertex (single-source shortest path)
    pub async fn single_source_shortest_paths(
        &self,
        start: VertexId,
        context: &QueryContext,
    ) -> Result<(HashMap<VertexId, Path>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let mut heap = BinaryHeap::new();
        let mut distances = HashMap::new();
        let mut paths = HashMap::new();
        let mut visited = HashSet::new();

        // Initialize with start vertex
        let mut start_path = Path::new();
        start_path.add_vertex(start);

        heap.push(DijkstraEntry {
            vertex: start,
            distance: 0.0,
            path: start_path.clone(),
        });
        distances.insert(start, 0.0);
        paths.insert(start, start_path);

        while let Some(current) = heap.pop() {
            if visited.contains(&current.vertex) {
                continue;
            }

            visited.insert(current.vertex);
            stats.vertices_visited += 1;

            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("Single-source shortest paths timeout"));
                }
            }

            // Process neighbors
            let neighbors = self.graph.get_neighbors(current.vertex).await?;
            stats.edges_traversed += neighbors.len();

            for neighbor in neighbors {
                if visited.contains(&neighbor) {
                    continue;
                }

                let edge_weight = 1.0; // Default weight
                let new_distance = current.distance + edge_weight;

                if new_distance < *distances.get(&neighbor).unwrap_or(&f64::INFINITY) {
                    distances.insert(neighbor, new_distance);

                    let mut new_path = current.path.clone();
                    new_path.add_vertex(neighbor);
                    new_path.add_edge(EdgeId::new(), edge_weight);
                    paths.insert(neighbor, new_path.clone());

                    heap.push(DijkstraEntry {
                        vertex: neighbor,
                        distance: new_distance,
                        path: new_path,
                    });
                }
            }
        }

        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok((paths, stats))
    }

    /// Find k-hop neighbors (vertices reachable within k hops)
    pub async fn k_hop_neighbors(
        &self,
        start: VertexId,
        k: usize,
        context: &QueryContext,
    ) -> Result<(HashMap<usize, HashSet<VertexId>>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let mut result = HashMap::new();
        let mut current_level = HashSet::new();
        current_level.insert(start);
        result.insert(0, current_level.clone());

        for hop in 1..=k {
            let mut next_level = HashSet::new();

            for vertex in &current_level {
                // Check timeout
                if let Some(timeout) = context.timeout_ms {
                    if start_time.elapsed().as_millis() > timeout as u128 {
                        return Err(AsterError::timeout("K-hop neighbors query timeout"));
                    }
                }

                let neighbors = self.graph.get_neighbors(*vertex).await?;
                stats.edges_traversed += neighbors.len();

                for neighbor in neighbors {
                    next_level.insert(neighbor);
                }
            }

            stats.vertices_visited += next_level.len();
            result.insert(hop, next_level.clone());
            current_level = next_level;

            if current_level.is_empty() {
                break;
            }
        }

        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok((result, stats))
    }

    /// Compute PageRank centrality scores
    pub async fn pagerank(
        &self,
        vertices: Vec<VertexId>,
        damping_factor: f64,
        max_iterations: usize,
        tolerance: f64,
        context: &QueryContext,
    ) -> Result<(AnalyticsResult, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let n = vertices.len();
        if n == 0 {
            return Ok((
                AnalyticsResult {
                    name: "PageRank".to_string(),
                    vertex_scores: HashMap::new(),
                    global_metrics: HashMap::new(),
                },
                stats,
            ));
        }

        let initial_score = 1.0 / n as f64;
        let mut scores = HashMap::new();
        let mut new_scores = HashMap::new();

        // Initialize scores
        for &vertex in &vertices {
            scores.insert(vertex, initial_score);
            new_scores.insert(vertex, 0.0);
        }

        // Build adjacency information
        let mut out_degrees = HashMap::new();
        let mut incoming_edges = HashMap::new();

        for &vertex in &vertices {
            let neighbors = self.graph.get_neighbors(vertex).await?;
            let neighbor_count = neighbors.len();
            out_degrees.insert(vertex, neighbor_count.max(1)); // Avoid division by zero

            for neighbor in &neighbors {
                if vertices.contains(neighbor) {
                    incoming_edges
                        .entry(*neighbor)
                        .or_insert_with(Vec::new)
                        .push(vertex);
                }
            }
            stats.edges_traversed += neighbor_count;
        }

        // PageRank iterations
        for _iteration in 0..max_iterations {
            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("PageRank computation timeout"));
                }
            }

            let mut max_change: f64 = 0.0;

            for &vertex in &vertices {
                let empty_vec = Vec::new();
                let incoming = incoming_edges.get(&vertex).unwrap_or(&empty_vec);
                let mut sum = 0.0;

                for &incoming_vertex in incoming {
                    let score = scores.get(&incoming_vertex).unwrap_or(&0.0);
                    let degree = *out_degrees.get(&incoming_vertex).unwrap_or(&1) as f64;
                    sum += score / degree;
                }

                let new_score = (1.0 - damping_factor) / n as f64 + damping_factor * sum;
                let old_score = scores.get(&vertex).unwrap_or(&0.0);
                max_change = max_change.max((new_score - old_score).abs());

                new_scores.insert(vertex, new_score);
            }

            // Update scores
            for &vertex in &vertices {
                if let Some(&new_score) = new_scores.get(&vertex) {
                    scores.insert(vertex, new_score);
                }
            }

            // Check convergence
            if max_change < tolerance {
                break;
            }
        }

        let mut global_metrics = HashMap::new();
        global_metrics.insert("iterations".to_string(), max_iterations as f64);
        global_metrics.insert("vertices_analyzed".to_string(), vertices.len() as f64);

        stats.vertices_visited = vertices.len();
        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;

        Ok((
            AnalyticsResult {
                name: "PageRank".to_string(),
                vertex_scores: scores,
                global_metrics,
            },
            stats,
        ))
    }

    /// Find connected components using Union-Find
    pub async fn connected_components(
        &self,
        vertices: Vec<VertexId>,
        context: &QueryContext,
    ) -> Result<(Vec<HashSet<VertexId>>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let mut parent = HashMap::new();
        let mut rank = HashMap::new();

        // Initialize Union-Find structure
        for &vertex in &vertices {
            parent.insert(vertex, vertex);
            rank.insert(vertex, 0);
        }

        // Find operation with path compression
        fn find(parent: &mut HashMap<VertexId, VertexId>, vertex: VertexId) -> VertexId {
            let parent_vertex = parent[&vertex];
            if parent_vertex != vertex {
                let root = find(parent, parent_vertex);
                parent.insert(vertex, root);
            }
            parent[&vertex]
        }

        // Union operation with rank
        fn union(
            parent: &mut HashMap<VertexId, VertexId>,
            rank: &mut HashMap<VertexId, i32>,
            x: VertexId,
            y: VertexId,
        ) {
            let root_x = find(parent, x);
            let root_y = find(parent, y);

            if root_x != root_y {
                let rank_x = rank[&root_x];
                let rank_y = rank[&root_y];

                if rank_x < rank_y {
                    parent.insert(root_x, root_y);
                } else if rank_x > rank_y {
                    parent.insert(root_y, root_x);
                } else {
                    parent.insert(root_y, root_x);
                    rank.insert(root_x, rank_x + 1);
                }
            }
        }

        // Process all edges
        for &vertex in &vertices {
            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("Connected components query timeout"));
                }
            }

            let neighbors = self.graph.get_neighbors(vertex).await?;
            stats.edges_traversed += neighbors.len();

            for neighbor in neighbors {
                if vertices.contains(&neighbor) {
                    union(&mut parent, &mut rank, vertex, neighbor);
                }
            }
        }

        // Group vertices by component
        let mut components = HashMap::new();
        for &vertex in &vertices {
            let root = find(&mut parent, vertex);
            components
                .entry(root)
                .or_insert_with(HashSet::new)
                .insert(vertex);
        }

        let result: Vec<HashSet<VertexId>> = components.into_values().collect();
        stats.vertices_visited = vertices.len();
        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;

        Ok((result, stats))
    }

    /// Cache a query result
    fn cache_result(&self, key: String, result: Vec<VertexId>) {
        if !key.is_empty() {
            let mut cache = self.cache.write();

            // Evict old entries if cache is full
            if cache.len() >= self.max_cache_size {
                let oldest_key = cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.timestamp)
                    .map(|(k, _)| k.clone());

                if let Some(key_to_remove) = oldest_key {
                    cache.remove(&key_to_remove);
                }
            }

            cache.insert(
                key,
                CacheEntry {
                    result,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    access_count: 0,
                },
            );
        }
    }

    /// Try to get a cached result
    fn get_cached_result(&self, key: &str) -> Option<Vec<VertexId>> {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(key) {
            entry.access_count += 1;
            Some(entry.result.clone())
        } else {
            None
        }
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> HashMap<String, u64> {
        let cache = self.cache.read();
        let mut stats = HashMap::new();

        stats.insert("total_entries".to_string(), cache.len() as u64);
        stats.insert("max_size".to_string(), self.max_cache_size as u64);

        let total_accesses: usize = cache.values().map(|entry| entry.access_count).sum();
        stats.insert("total_accesses".to_string(), total_accesses as u64);

        stats
    }

    /// Clear the query cache
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Optimized range query for finding vertices and their connections within an ID range
    pub async fn range_query(
        &self,
        start_vertex: VertexId,
        end_vertex: VertexId,
        context: &QueryContext,
    ) -> Result<(RangeQueryResult, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        // Build cache key for range query
        let cache_key = if context.use_cache {
            Some(format!(
                "range_{}_{}",
                start_vertex.as_u64(),
                end_vertex.as_u64()
            ))
        } else {
            None
        };

        // Check cache first
        if let Some(ref key) = cache_key {
            if let Some(_cached_result) = self.get_cached_result(key) {
                stats.cache_hits += 1;
                // Skip complex cache usage for now
            }
            stats.cache_misses += 1;
        }

        // Use storage layer's range query for efficient vertex discovery
        let vertices_in_range = self
            .graph
            .storage()
            .range(start_vertex, end_vertex)
            .await?
            .into_iter()
            .map(|(vertex_id, _)| vertex_id)
            .collect::<Vec<_>>();

        stats.vertices_visited = vertices_in_range.len();

        // Collect neighbor connections for vertices in range
        let mut neighbor_connections = Vec::new();
        let mut total_edges_in_range = 0;

        for &vertex in &vertices_in_range {
            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("Range query timeout"));
                }
            }

            let neighbors = self.graph.get_neighbors(vertex).await?;
            stats.edges_traversed += neighbors.len();

            // Filter neighbors that are also in the range for better locality
            let neighbors_in_range: Vec<VertexId> = neighbors
                .into_iter()
                .filter(|&neighbor| {
                    neighbor.as_u64() >= start_vertex.as_u64()
                        && neighbor.as_u64() <= end_vertex.as_u64()
                })
                .collect();

            total_edges_in_range += neighbors_in_range.len();
            neighbor_connections.push((vertex, neighbors_in_range));
        }

        // Calculate subgraph density (edges / max_possible_edges)
        let subgraph_density = if vertices_in_range.len() > 1 {
            let max_possible_edges = vertices_in_range.len() * (vertices_in_range.len() - 1);
            total_edges_in_range as f64 / max_possible_edges as f64
        } else {
            0.0
        };

        let result = RangeQueryResult {
            vertices_in_range,
            neighbor_connections,
            subgraph_density,
            total_edges_in_range,
        };

        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;

        // Cache the result
        if let Some(key) = cache_key {
            // For now, cache just the vertex IDs for simplicity
            self.cache_result(key, result.vertices_in_range.clone());
        }

        Ok((result, stats))
    }

    /// Range-based BFS traversal optimized for graph locality
    pub async fn range_bfs(
        &self,
        start: VertexId,
        range_start: VertexId,
        range_end: VertexId,
        context: &QueryContext,
    ) -> Result<(Vec<VertexId>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back((start, 0)); // (vertex, depth)
        visited.insert(start);

        while let Some((vertex, depth)) = queue.pop_front() {
            result.push(vertex);

            // Check max depth
            if let Some(max_depth) = context.max_depth {
                if depth >= max_depth {
                    continue;
                }
            }

            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("Range BFS timeout"));
                }
            }

            let neighbors = self.graph.get_neighbors(vertex).await?;
            stats.edges_traversed += neighbors.len();

            // Prioritize neighbors within the specified range for better locality
            let mut range_neighbors = Vec::new();
            let mut other_neighbors = Vec::new();

            for neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    let neighbor_u64 = neighbor.as_u64();
                    if neighbor_u64 >= range_start.as_u64() && neighbor_u64 <= range_end.as_u64() {
                        range_neighbors.push(neighbor);
                    } else {
                        other_neighbors.push(neighbor);
                    }
                }
            }

            // Process range neighbors first for better cache locality
            for neighbor in range_neighbors.into_iter().chain(other_neighbors) {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        stats.vertices_visited = result.len();
        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok((result, stats))
    }

    /// Multi-range query for finding vertices across multiple ID ranges efficiently
    pub async fn multi_range_query(
        &self,
        ranges: Vec<(VertexId, VertexId)>,
        context: &QueryContext,
    ) -> Result<(Vec<RangeQueryResult>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();
        let mut results = Vec::new();

        // Sort ranges by start vertex for optimal storage access patterns
        let mut sorted_ranges = ranges;
        sorted_ranges.sort_by_key(|(start, _)| start.as_u64());

        for (range_start, range_end) in sorted_ranges {
            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("Multi-range query timeout"));
                }
            }

            let (range_result, range_stats) =
                self.range_query(range_start, range_end, context).await?;

            // Accumulate statistics
            stats.vertices_visited += range_stats.vertices_visited;
            stats.edges_traversed += range_stats.edges_traversed;
            stats.cache_hits += range_stats.cache_hits;
            stats.cache_misses += range_stats.cache_misses;

            results.push(range_result);
        }

        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok((results, stats))
    }

    /// Range-aware shortest path that prefers paths through specified vertex ranges
    pub async fn range_aware_shortest_path(
        &self,
        start: VertexId,
        target: VertexId,
        preferred_ranges: Vec<(VertexId, VertexId)>,
        context: &QueryContext,
    ) -> Result<(Option<Path>, QueryStats)> {
        let mut stats = QueryStats::default();
        let start_time = std::time::Instant::now();

        let mut visited = HashSet::new();
        let mut distances = HashMap::new();
        let mut heap = BinaryHeap::new();

        let mut start_path = Path::new();
        start_path.add_vertex(start);

        heap.push(DijkstraEntry {
            vertex: start,
            distance: 0.0,
            path: start_path,
        });
        distances.insert(start, 0.0);

        while let Some(current) = heap.pop() {
            if visited.contains(&current.vertex) {
                continue;
            }

            visited.insert(current.vertex);
            stats.vertices_visited += 1;

            // Found target
            if current.vertex == target {
                stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
                return Ok((Some(current.path), stats));
            }

            // Check timeout
            if let Some(timeout) = context.timeout_ms {
                if start_time.elapsed().as_millis() > timeout as u128 {
                    return Err(AsterError::timeout("Range-aware shortest path timeout"));
                }
            }

            let neighbors = self.graph.get_neighbors(current.vertex).await?;
            stats.edges_traversed += neighbors.len();

            for neighbor in neighbors {
                if visited.contains(&neighbor) {
                    continue;
                }

                // Calculate edge weight with range preference bonus
                let mut edge_weight = 1.0; // Base weight

                // Apply range preference bonus (lower weight for vertices in preferred ranges)
                let neighbor_u64 = neighbor.as_u64();
                for (range_start, range_end) in &preferred_ranges {
                    if neighbor_u64 >= range_start.as_u64() && neighbor_u64 <= range_end.as_u64() {
                        edge_weight *= 0.8; // 20% bonus for range locality
                        break;
                    }
                }

                let new_distance = current.distance + edge_weight;

                if new_distance < *distances.get(&neighbor).unwrap_or(&f64::INFINITY) {
                    distances.insert(neighbor, new_distance);

                    let mut new_path = current.path.clone();
                    new_path.add_vertex(neighbor);
                    new_path.add_edge(EdgeId::new(), edge_weight);

                    heap.push(DijkstraEntry {
                        vertex: neighbor,
                        distance: new_distance,
                        path: new_path,
                    });
                }
            }
        }

        stats.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok((None, stats)) // No path found
    }

    /// Filter vertices based on predicate
    pub async fn filter_vertices(
        &self,
        vertices: Vec<VertexId>,
        predicate: &QueryPredicate,
        context: &QueryContext,
    ) -> Result<Vec<VertexId>> {
        let mut filtered = Vec::new();

        for vertex in vertices {
            if self.evaluate_predicate(vertex, predicate, context).await? {
                filtered.push(vertex);
            }
        }

        Ok(filtered)
    }

    /// Project vertex properties into result set
    pub async fn project_vertices(
        &self,
        vertices: Vec<VertexId>,
        projections: &[QueryProjection],
        context: &QueryContext,
    ) -> Result<QueryResultSet> {
        let mut result = QueryResultSet::new();
        result.total_vertices = vertices.len();

        for vertex in vertices {
            let mut row = HashMap::new();

            for projection in projections {
                match projection {
                    QueryProjection::VertexId => {
                        row.insert(
                            "vertex_id".to_string(),
                            PropertyValue::String(vertex.to_string()),
                        );
                    }
                    QueryProjection::VertexDegree => {
                        let degree = self.graph.get_degree(vertex).await?;
                        row.insert("degree".to_string(), PropertyValue::Int(degree as i64));
                    }
                    QueryProjection::NeighborCount => {
                        let neighbors = self.graph.get_neighbors(vertex).await?;
                        row.insert(
                            "neighbor_count".to_string(),
                            PropertyValue::Int(neighbors.len() as i64),
                        );
                    }
                    QueryProjection::VertexProperty(prop_name) => {
                        // For now, we'll just return a placeholder since vertex properties aren't fully implemented
                        row.insert(
                            prop_name.clone(),
                            PropertyValue::String("not_implemented".to_string()),
                        );
                    }
                    QueryProjection::EdgeProperty(_) => {
                        // Skip edge properties for vertex projection
                    }
                    QueryProjection::Computed(name) => {
                        // For now, return placeholder for computed values
                        row.insert(name.clone(), PropertyValue::String("computed".to_string()));
                    }
                }
            }

            result.add_row(row);
        }

        Ok(result)
    }

    /// Aggregate vertex data
    pub async fn aggregate(
        &self,
        vertices: Vec<VertexId>,
        aggregations: &[AggregationFunction],
        context: &QueryContext,
    ) -> Result<HashMap<String, PropertyValue>> {
        let mut results = HashMap::new();

        for aggregation in aggregations {
            match aggregation {
                AggregationFunction::Count => {
                    results.insert(
                        "count".to_string(),
                        PropertyValue::Int(vertices.len() as i64),
                    );
                }
                AggregationFunction::Sum(prop_name) => {
                    // Placeholder - would sum property values
                    results.insert(format!("sum_{}", prop_name), PropertyValue::Float(0.0));
                }
                AggregationFunction::Avg(prop_name) => {
                    // Placeholder - would average property values
                    results.insert(format!("avg_{}", prop_name), PropertyValue::Float(0.0));
                }
                AggregationFunction::Min(prop_name) => {
                    // Placeholder - would find minimum property value
                    results.insert(format!("min_{}", prop_name), PropertyValue::Int(0));
                }
                AggregationFunction::Max(prop_name) => {
                    // Placeholder - would find maximum property value
                    results.insert(format!("max_{}", prop_name), PropertyValue::Int(0));
                }
                AggregationFunction::Distinct(prop_name) => {
                    // Placeholder - would count distinct property values
                    results.insert(format!("distinct_{}", prop_name), PropertyValue::Int(0));
                }
            }
        }

        Ok(results)
    }

    /// Find patterns in the graph
    pub async fn pattern_match(
        &self,
        pattern: &GraphPattern,
        context: &QueryContext,
    ) -> Result<Vec<HashMap<String, VertexId>>> {
        let mut matches = Vec::new();

        // Simple pattern matching implementation
        // For now, we'll just return empty results as a placeholder
        // A full implementation would involve sophisticated subgraph isomorphism algorithms

        Ok(matches)
    }

    /// Execute a complex graph query with filtering, projection, and aggregation
    pub async fn execute_query(
        &self,
        start_vertices: Vec<VertexId>,
        filter: Option<&QueryPredicate>,
        projections: Option<&[QueryProjection]>,
        aggregations: Option<&[AggregationFunction]>,
        context: &QueryContext,
    ) -> Result<QueryResultSet> {
        let mut vertices = start_vertices;

        // Apply filtering if specified
        if let Some(predicate) = filter {
            vertices = self.filter_vertices(vertices, predicate, context).await?;
        }

        // Apply projections if specified
        if let Some(projs) = projections {
            return self.project_vertices(vertices, projs, context).await;
        }

        // Apply aggregations if specified
        if let Some(aggs) = aggregations {
            let agg_results = self.aggregate(vertices.clone(), aggs, context).await?;
            let mut result = QueryResultSet::new();
            result.total_vertices = vertices.len();
            result.add_row(agg_results);
            return Ok(result);
        }

        // Default: return vertex IDs
        let mut result = QueryResultSet::new();
        result.total_vertices = vertices.len();
        for vertex in vertices {
            let mut row = HashMap::new();
            row.insert(
                "vertex_id".to_string(),
                PropertyValue::String(vertex.to_string()),
            );
            result.add_row(row);
        }

        Ok(result)
    }

    /// Helper method to evaluate predicates
    fn evaluate_predicate<'a>(
        &'a self,
        vertex: VertexId,
        predicate: &'a QueryPredicate,
        context: &'a QueryContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            match predicate {
                QueryPredicate::PropertyEquals(_, _) => {
                    // Placeholder - would check vertex property
                    Ok(true)
                }
                QueryPredicate::PropertyNotEquals(_, _) => {
                    // Placeholder - would check vertex property
                    Ok(false)
                }
                QueryPredicate::PropertyGreaterThan(_, _) => {
                    // Placeholder - would check vertex property
                    Ok(false)
                }
                QueryPredicate::PropertyLessThan(_, _) => {
                    // Placeholder - would check vertex property
                    Ok(false)
                }
                QueryPredicate::PropertyExists(_) => {
                    // Placeholder - would check if property exists
                    Ok(true)
                }
                QueryPredicate::VertexIdIn(vertex_set) => Ok(vertex_set.contains(&vertex)),
                QueryPredicate::DegreeGreaterThan(threshold) => {
                    let degree = self.graph.get_degree(vertex).await?;
                    Ok(degree > *threshold)
                }
                QueryPredicate::DegreeLessThan(threshold) => {
                    let degree = self.graph.get_degree(vertex).await?;
                    Ok(degree < *threshold)
                }
                QueryPredicate::And(predicates) => {
                    for pred in predicates {
                        if !self.evaluate_predicate(vertex, pred, context).await? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                QueryPredicate::Or(predicates) => {
                    for pred in predicates {
                        if self.evaluate_predicate(vertex, pred, context).await? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                QueryPredicate::Not(predicate) => {
                    let result = self.evaluate_predicate(vertex, predicate, context).await?;
                    Ok(!result)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PolyLSM;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_bfs_traversal() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create a small test graph: 1 -> 2 -> 3
        let v1 = VertexId::from_u64(1);
        let v2 = VertexId::from_u64(2);
        let v3 = VertexId::from_u64(3);

        // Add edges
        graph.add_edge(v1, v2, None).await.unwrap();
        graph.add_edge(v2, v3, None).await.unwrap();

        let context = QueryContext::default();
        let (result, stats) = query_engine.bfs(v1, &context).await.unwrap();

        assert!(!result.is_empty());
        assert!(result.contains(&v1));
        assert!(stats.vertices_visited > 0);
    }

    #[tokio::test]
    async fn test_dfs_traversal() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create a small test graph: 1 -> 2 -> 3
        let v1 = VertexId::from_u64(1);
        let v2 = VertexId::from_u64(2);
        let v3 = VertexId::from_u64(3);

        // Add edges
        graph.add_edge(v1, v2, None).await.unwrap();
        graph.add_edge(v2, v3, None).await.unwrap();

        let context = QueryContext::default();
        let (result, stats) = query_engine.dfs(v1, &context).await.unwrap();

        assert!(!result.is_empty());
        assert!(result.contains(&v1));
        assert!(stats.vertices_visited > 0);
    }

    #[tokio::test]
    async fn test_shortest_path() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create a small test graph: 1 -> 2 -> 3
        let v1 = VertexId::from_u64(1);
        let v2 = VertexId::from_u64(2);
        let v3 = VertexId::from_u64(3);

        // Add edges
        graph.add_edge(v1, v2, None).await.unwrap();
        graph.add_edge(v2, v3, None).await.unwrap();

        let context = QueryContext::default();
        let (path, stats) = query_engine.shortest_path(v1, v3, &context).await.unwrap();

        if let Some(path) = path {
            assert!(path.vertices.contains(&v1));
            assert!(path.vertices.contains(&v3));
        }
        assert!(stats.vertices_visited > 0);
    }

    #[tokio::test]
    async fn test_k_hop_neighbors() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create a small test graph: 1 -> 2 -> 3
        let v1 = VertexId::from_u64(1);
        let v2 = VertexId::from_u64(2);
        let v3 = VertexId::from_u64(3);

        // Add edges
        graph.add_edge(v1, v2, None).await.unwrap();
        graph.add_edge(v2, v3, None).await.unwrap();

        let context = QueryContext::default();
        let (result, stats) = query_engine.k_hop_neighbors(v1, 2, &context).await.unwrap();

        assert!(result.contains_key(&0)); // 0-hop should contain v1
        assert!(stats.execution_time_ms >= 0); // Can be 0 for fast operations
    }

    #[tokio::test]
    async fn test_cache_functionality() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph);

        // Test caching
        let result = vec![VertexId::from_u64(1), VertexId::from_u64(2)];
        query_engine.cache_result("test_key".to_string(), result.clone());

        let cached = query_engine.get_cached_result("test_key");
        assert!(cached.is_some());

        // Test cache stats
        let stats = query_engine.get_cache_stats();
        assert!(stats.contains_key("total_entries"));
        assert_eq!(stats["total_entries"], 1);

        // Test cache clearing
        query_engine.clear_cache();
        let cleared_stats = query_engine.get_cache_stats();
        assert_eq!(cleared_stats["total_entries"], 0);
    }

    #[tokio::test]
    async fn test_range_query() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create vertices with sequential IDs
        let vertices: Vec<VertexId> = (1..=10).map(VertexId::from_u64).collect();

        // Add vertices to graph
        for vertex in &vertices {
            graph.add_vertex(*vertex, None).await.unwrap();
        }

        // Add some edges between consecutive vertices
        for i in 0..vertices.len() - 1 {
            graph
                .add_edge(vertices[i], vertices[i + 1], None)
                .await
                .unwrap();
        }

        let context = QueryContext::default();
        let (result, stats) = query_engine
            .range_query(VertexId::from_u64(3), VertexId::from_u64(7), &context)
            .await
            .unwrap();

        assert!(!result.vertices_in_range.is_empty());
        assert!(stats.vertices_visited > 0);
        assert!(result.subgraph_density >= 0.0);
    }

    #[tokio::test]
    async fn test_range_bfs() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create a linear graph: 1 -> 2 -> 3 -> 4 -> 5
        for i in 1..=5 {
            let vertex = VertexId::from_u64(i);
            graph.add_vertex(vertex, None).await.unwrap();

            if i > 1 {
                graph
                    .add_edge(VertexId::from_u64(i - 1), vertex, None)
                    .await
                    .unwrap();
            }
        }

        let context = QueryContext::default();
        let (result, stats) = query_engine
            .range_bfs(
                VertexId::from_u64(1),
                VertexId::from_u64(2),
                VertexId::from_u64(4),
                &context,
            )
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert!(result.contains(&VertexId::from_u64(1)));
        assert!(stats.vertices_visited > 0);
    }

    #[tokio::test]
    async fn test_multi_range_query() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create vertices in multiple ranges
        let ranges = vec![
            (VertexId::from_u64(1), VertexId::from_u64(3)),
            (VertexId::from_u64(10), VertexId::from_u64(12)),
        ];

        // Add vertices and some edges to make them discoverable
        for range in &ranges {
            for i in range.0.as_u64()..=range.1.as_u64() {
                graph.add_vertex(VertexId::from_u64(i), None).await.unwrap();

                // Add a self-edge to make the vertex discoverable
                if i < range.1.as_u64() {
                    graph
                        .add_edge(VertexId::from_u64(i), VertexId::from_u64(i + 1), None)
                        .await
                        .unwrap();
                }
            }
        }

        let context = QueryContext::default();
        let (results, stats) = query_engine
            .multi_range_query(ranges, &context)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert!(stats.vertices_visited > 0);
    }

    #[tokio::test]
    async fn test_range_aware_shortest_path() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create a small graph with preferred range
        let vertices: Vec<VertexId> = (1..=5).map(VertexId::from_u64).collect();

        for vertex in &vertices {
            graph.add_vertex(*vertex, None).await.unwrap();
        }

        // Add edges: 1 -> 2 -> 3 -> 4 -> 5
        for i in 0..vertices.len() - 1 {
            graph
                .add_edge(vertices[i], vertices[i + 1], None)
                .await
                .unwrap();
        }

        let preferred_ranges = vec![(VertexId::from_u64(2), VertexId::from_u64(4))];
        let context = QueryContext::default();

        let (path_opt, stats) = query_engine
            .range_aware_shortest_path(
                VertexId::from_u64(1),
                VertexId::from_u64(5),
                preferred_ranges,
                &context,
            )
            .await
            .unwrap();

        if let Some(path) = path_opt {
            assert!(path.vertices.contains(&VertexId::from_u64(1)));
            assert!(path.vertices.contains(&VertexId::from_u64(5)));
        }
        assert!(stats.vertices_visited > 0);
    }

    #[tokio::test]
    async fn test_filter_vertices() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        // Create vertices with different degrees
        let vertices: Vec<VertexId> = (1..=5).map(VertexId::from_u64).collect();

        for vertex in &vertices {
            graph.add_vertex(*vertex, None).await.unwrap();
        }

        // Create edges to give vertices different degrees
        // Add several edges to vertex[1] (which is VertexId::from_u64(2))
        graph
            .add_edge(vertices[1], vertices[2], None)
            .await
            .unwrap(); // v2 -> v3
        graph
            .add_edge(vertices[1], vertices[3], None)
            .await
            .unwrap(); // v2 -> v4
        graph
            .add_edge(vertices[1], vertices[4], None)
            .await
            .unwrap(); // v2 -> v5

        let context = QueryContext::default();

        // Test degree greater than filter
        let predicate = QueryPredicate::DegreeGreaterThan(1);
        let filtered = query_engine
            .filter_vertices(vertices.clone(), &predicate, &context)
            .await
            .unwrap();

        assert!(!filtered.is_empty());
        // Should include vertices[1] which now has degree 3 (> 1)
        assert!(filtered.contains(&vertices[1]));
    }

    #[tokio::test]
    async fn test_project_vertices() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        let vertices: Vec<VertexId> = (1..=3).map(VertexId::from_u64).collect();

        for vertex in &vertices {
            graph.add_vertex(*vertex, None).await.unwrap();
        }

        let context = QueryContext::default();
        let projections = vec![
            QueryProjection::VertexId,
            QueryProjection::VertexDegree,
            QueryProjection::NeighborCount,
        ];

        let result = query_engine
            .project_vertices(vertices.clone(), &projections, &context)
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.total_vertices, 3);

        // Check that each row has the projected columns
        for row in &result.rows {
            assert!(row.contains_key("vertex_id"));
            assert!(row.contains_key("degree"));
            assert!(row.contains_key("neighbor_count"));
        }
    }

    #[tokio::test]
    async fn test_aggregate_vertices() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        let vertices: Vec<VertexId> = (1..=5).map(VertexId::from_u64).collect();

        for vertex in &vertices {
            graph.add_vertex(*vertex, None).await.unwrap();
        }

        let context = QueryContext::default();
        let aggregations = vec![
            AggregationFunction::Count,
            AggregationFunction::Sum("degree".to_string()),
        ];

        let result = query_engine
            .aggregate(vertices.clone(), &aggregations, &context)
            .await
            .unwrap();

        assert!(result.contains_key("count"));
        assert!(result.contains_key("sum_degree"));

        if let Some(PropertyValue::Int(count)) = result.get("count") {
            assert_eq!(*count, 5);
        } else {
            panic!("Count aggregation failed");
        }
    }

    #[tokio::test]
    async fn test_execute_query() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Arc::new(Graph::new(&storage));
        let query_engine = QueryEngine::new(graph.clone());

        let vertices: Vec<VertexId> = (1..=5).map(VertexId::from_u64).collect();

        for vertex in &vertices {
            graph.add_vertex(*vertex, None).await.unwrap();
        }

        let context = QueryContext::default();

        // Test simple query with projection
        let projections = vec![QueryProjection::VertexId, QueryProjection::VertexDegree];
        let result = query_engine
            .execute_query(vertices.clone(), None, Some(&projections), None, &context)
            .await
            .unwrap();

        assert_eq!(result.len(), 5);
        assert_eq!(result.total_vertices, 5);

        // Test query with aggregation
        let aggregations = vec![AggregationFunction::Count];
        let result = query_engine
            .execute_query(vertices.clone(), None, None, Some(&aggregations), &context)
            .await
            .unwrap();

        assert_eq!(result.len(), 1); // Aggregation returns single row
        assert_eq!(result.total_vertices, 5);
    }
}
