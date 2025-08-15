//! Graph traversal algorithms

use crate::{Result, VertexId};
use std::collections::{HashSet, VecDeque};

/// Breadth-first search traversal
pub struct BfsTraversal;

impl BfsTraversal {
    pub fn new() -> Self {
        Self
    }

    /// Perform BFS starting from a vertex
    pub async fn traverse<F>(
        &self,
        start: VertexId,
        max_depth: u32,
        mut get_neighbors: F,
    ) -> Result<Vec<VertexId>>
    where
        F: FnMut(VertexId) -> Result<Vec<VertexId>>,
    {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        queue.push_back((start, 0));
        visited.insert(start);

        while let Some((vertex, depth)) = queue.pop_front() {
            result.push(vertex);

            if depth < max_depth {
                let neighbors = get_neighbors(vertex)?;
                for neighbor in neighbors {
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back((neighbor, depth + 1));
                    }
                }
            }
        }

        Ok(result)
    }
}
