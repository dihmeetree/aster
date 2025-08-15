//! Vertex representation in the graph

use crate::{Properties, PropertyValue, VertexId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A vertex in the graph
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vertex {
    id: VertexId,
    properties: Properties,
}

impl Vertex {
    /// Create a new vertex
    pub fn new(id: VertexId, properties: Properties) -> Self {
        Self { id, properties }
    }

    /// Get the vertex ID
    pub fn id(&self) -> VertexId {
        self.id
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

    /// Check if vertex has a property
    pub fn has_property(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }
}

impl std::fmt::Display for Vertex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Vertex({}, {} properties)",
            self.id,
            self.properties.len()
        )
    }
}
