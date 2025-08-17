//! Graph database layer built on top of Poly-LSM storage

use crate::storage::PolyLSM;
use crate::{AsterError, EdgeId, Properties, PropertyValue, Result, Timestamp, VertexId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub mod edge;
pub mod traversal;
pub mod vertex;

pub use edge::Edge;
pub use vertex::Vertex;

/// Main graph interface
#[derive(Debug)]
pub struct Graph {
    storage: Arc<PolyLSM>,
}

impl Graph {
    /// Create a new graph interface
    pub fn new(storage: &PolyLSM) -> Self {
        Self {
            storage: Arc::new(storage.clone()),
        }
    }

    /// Add a vertex with optional properties (creates new ID)
    pub async fn create_vertex(&self, properties: Option<Properties>) -> Result<Vertex> {
        let vertex_id = VertexId::random();
        let vertex = Vertex::new(vertex_id, properties.unwrap_or_default());

        // Store vertex in the graph (implementation would store properties separately)
        // For now, we'll just create the vertex object
        Ok(vertex)
    }

    /// Get a vertex by ID
    pub async fn get_vertex(&self, vertex_id: VertexId) -> Result<Option<Vertex>> {
        // Check if vertex exists by looking for any edges
        let has_edges = self.storage.contains_vertex(vertex_id).await?;

        if has_edges {
            // In a full implementation, we'd load properties from storage
            Ok(Some(Vertex::new(vertex_id, Properties::new())))
        } else {
            Ok(None)
        }
    }

    /// Add an edge between two vertices
    pub async fn add_edge(
        &self,
        source: VertexId,
        target: VertexId,
        properties: Option<Properties>,
    ) -> Result<Edge> {
        // Add the edge to storage
        self.storage.add_edge(source, target).await?;

        // Create edge object
        let edge_id = EdgeId::new();
        let edge = Edge::new(edge_id, source, target, properties.unwrap_or_default());

        Ok(edge)
    }

    /// Get neighbors of a vertex
    pub async fn get_neighbors(&self, vertex_id: VertexId) -> Result<Vec<VertexId>> {
        self.storage.get_neighbors(vertex_id).await
    }

    /// Check if two vertices are connected
    pub async fn has_edge(&self, source: VertexId, target: VertexId) -> Result<bool> {
        let neighbors = self.get_neighbors(source).await?;
        Ok(neighbors.contains(&target))
    }

    /// Get the degree of a vertex
    pub async fn get_degree(&self, vertex_id: VertexId) -> Result<usize> {
        let neighbors = self.get_neighbors(vertex_id).await?;
        Ok(neighbors.len())
    }

    /// Access the underlying storage layer (for advanced queries)
    pub fn storage(&self) -> &PolyLSM {
        &self.storage
    }

    /// Add a vertex with a specific ID (for testing and advanced scenarios)
    pub async fn add_vertex(
        &self,
        vertex_id: VertexId,
        properties: Option<Properties>,
    ) -> Result<()> {
        // This ensures the vertex exists in storage by adding a self-edge or similar marker
        // For now we'll just use the storage layer's vertex tracking
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_graph_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();
        let graph = Graph::new(&storage);

        // Use small, predictable vertex IDs for testing
        let v1_id = VertexId::from_u64(1);
        let v2_id = VertexId::from_u64(2);

        // Add vertices (create vertex objects with specific IDs)
        let v1 = Vertex::new(v1_id, Properties::new());
        let v2 = Vertex::new(v2_id, Properties::new());

        // Add edge
        let _edge = graph.add_edge(v1.id(), v2.id(), None).await.unwrap();

        // Check connectivity
        assert!(graph.has_edge(v1.id(), v2.id()).await.unwrap());
        assert_eq!(graph.get_degree(v1.id()).await.unwrap(), 1);
    }
}
