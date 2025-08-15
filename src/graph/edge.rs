//! Edge representation in the graph

use crate::{EdgeId, Properties, PropertyValue, VertexId};
use serde::{Deserialize, Serialize};

/// An edge in the graph
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    id: EdgeId,
    source: VertexId,
    target: VertexId,
    properties: Properties,
}

impl Edge {
    /// Create a new edge
    pub fn new(id: EdgeId, source: VertexId, target: VertexId, properties: Properties) -> Self {
        Self {
            id,
            source,
            target,
            properties,
        }
    }

    /// Get the edge ID
    pub fn id(&self) -> EdgeId {
        self.id
    }

    /// Get the source vertex ID
    pub fn source(&self) -> VertexId {
        self.source
    }

    /// Get the target vertex ID
    pub fn target(&self) -> VertexId {
        self.target
    }

    /// Get all properties
    pub fn properties(&self) -> &Properties {
        &self.properties
    }

    /// Get a specific property
    pub fn get_property(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(key)
    }

    /// Set a property
    pub fn set_property(&mut self, key: String, value: PropertyValue) {
        self.properties.insert(key, value);
    }

    /// Remove a property
    pub fn remove_property(&mut self, key: &str) -> Option<PropertyValue> {
        self.properties.remove(key)
    }

    /// Check if edge has a property
    pub fn has_property(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }
}

impl std::fmt::Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Edge({}: {} -> {}, {} properties)",
            self.id,
            self.source,
            self.target,
            self.properties.len()
        )
    }
}
