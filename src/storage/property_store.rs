//! Property storage and indexing system for vertices and edges
//!
//! Implements efficient storage and querying of vertex/edge properties with:
//! - Separate storage for properties to optimize space usage
//! - Secondary indexes for property-based queries
//! - Schema inference and validation
//! - Compression for property values

use crate::storage::{EntryType, MemTable, MemTableEntry, SSTableReader, SSTableWriter};
use crate::types::Properties;
use crate::utils::bloom_filter::BloomFilter;
use crate::{AsterError, EdgeId, PropertyValue, Result, Timestamp, VertexId};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Property storage entry types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyEntryType {
    /// Set properties for an entity
    Set,
    /// Delete specific properties
    Delete,
    /// Delete all properties for an entity
    DeleteAll,
}

/// A property entry in storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyEntry {
    pub entry_type: PropertyEntryType,
    pub properties: Properties,
    pub timestamp: Timestamp,
}

impl PropertyEntry {
    pub fn new_set(properties: Properties, timestamp: Timestamp) -> Self {
        Self {
            entry_type: PropertyEntryType::Set,
            properties,
            timestamp,
        }
    }

    pub fn new_delete(keys: Vec<String>, timestamp: Timestamp) -> Self {
        let mut properties = Properties::new();
        for key in keys {
            properties.insert(key, PropertyValue::Null);
        }

        Self {
            entry_type: PropertyEntryType::Delete,
            properties,
            timestamp,
        }
    }

    pub fn new_delete_all(timestamp: Timestamp) -> Self {
        Self {
            entry_type: PropertyEntryType::DeleteAll,
            properties: Properties::new(),
            timestamp,
        }
    }
}

/// Property storage configuration
#[derive(Debug, Clone)]
pub struct PropertyStoreConfig {
    /// Enable secondary indexes for property queries
    pub enable_indexes: bool,
    /// Maximum number of indexed property keys
    pub max_indexed_keys: usize,
    /// Compression threshold for property values
    pub compression_threshold: usize,
    /// Enable schema validation
    pub enable_schema_validation: bool,
}

impl Default for PropertyStoreConfig {
    fn default() -> Self {
        Self {
            enable_indexes: true,
            max_indexed_keys: 1000,
            compression_threshold: 1024,
            enable_schema_validation: false,
        }
    }
}

/// Property schema information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertySchema {
    /// Property key name
    pub key: String,
    /// Expected value type
    pub value_type: String,
    /// Whether this property is indexed
    pub indexed: bool,
    /// Statistics about this property
    pub stats: PropertyStats,
}

/// Statistics for a property key
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PropertyStats {
    /// Number of entities with this property
    pub count: u64,
    /// Average size of values in bytes
    pub avg_size: f64,
    /// Cardinality (number of distinct values)
    pub cardinality: u64,
    /// Sample values for type inference
    pub sample_values: Vec<String>,
}

/// Secondary index for property queries
#[derive(Debug)]
pub struct PropertyIndex {
    /// Index name (property key)
    key: String,
    /// Value to entity IDs mapping
    value_to_entities: BTreeMap<PropertyValue, BTreeSet<u64>>,
    /// Entity ID to value mapping for fast deletion
    entity_to_value: HashMap<u64, PropertyValue>,
    /// Bloom filter for existence checks
    bloom_filter: BloomFilter,
}

impl PropertyIndex {
    fn new(key: String, capacity: usize) -> Self {
        Self {
            key,
            value_to_entities: BTreeMap::new(),
            entity_to_value: HashMap::new(),
            bloom_filter: BloomFilter::new(capacity, 10),
        }
    }

    /// Add an entity-value mapping to the index
    fn insert(&mut self, entity_id: u64, value: PropertyValue) {
        // Remove old mapping if exists
        if let Some(old_value) = self.entity_to_value.remove(&entity_id) {
            if let Some(entities) = self.value_to_entities.get_mut(&old_value) {
                entities.remove(&entity_id);
                if entities.is_empty() {
                    self.value_to_entities.remove(&old_value);
                }
            }
        }

        // Add new mapping
        self.value_to_entities
            .entry(value.clone())
            .or_default()
            .insert(entity_id);
        self.entity_to_value.insert(entity_id, value.clone());

        // Update bloom filter
        let key = format!(
            "{}:{}",
            entity_id,
            serde_json::to_string(&value).unwrap_or_default()
        );
        self.bloom_filter.insert(&key);
    }

    /// Remove an entity from the index
    fn remove(&mut self, entity_id: u64) {
        if let Some(value) = self.entity_to_value.remove(&entity_id) {
            if let Some(entities) = self.value_to_entities.get_mut(&value) {
                entities.remove(&entity_id);
                if entities.is_empty() {
                    self.value_to_entities.remove(&value);
                }
            }
        }
    }

    /// Find entities with a specific property value
    fn find_entities(&self, value: &PropertyValue) -> Vec<u64> {
        self.value_to_entities
            .get(value)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Find entities with property values in a range
    fn find_entities_in_range(&self, min: &PropertyValue, max: &PropertyValue) -> Vec<u64> {
        let mut result = Vec::new();

        for (value, entities) in self.value_to_entities.range(min..=max) {
            result.extend(entities.iter().copied());
        }

        result
    }

    /// Get all unique values for this property
    fn get_unique_values(&self) -> Vec<PropertyValue> {
        self.value_to_entities.keys().cloned().collect()
    }

    /// Get cardinality (number of unique values)
    fn cardinality(&self) -> usize {
        self.value_to_entities.len()
    }
}

/// Main property storage engine
pub struct PropertyStore {
    config: PropertyStoreConfig,
    data_dir: PathBuf,

    /// In-memory property storage
    vertex_properties: Arc<RwLock<MemTable>>,
    edge_properties: Arc<RwLock<MemTable>>,

    /// Secondary indexes for properties
    vertex_indexes: Arc<RwLock<HashMap<String, PropertyIndex>>>,
    edge_indexes: Arc<RwLock<HashMap<String, PropertyIndex>>>,

    /// Property schemas
    schemas: Arc<RwLock<HashMap<String, PropertySchema>>>,

    /// Next property file ID
    next_file_id: Arc<Mutex<u64>>,
}

impl PropertyStore {
    /// Create a new property store
    pub fn new<P: AsRef<Path>>(data_dir: P, config: PropertyStoreConfig) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        Ok(Self {
            config,
            data_dir,
            vertex_properties: Arc::new(RwLock::new(MemTable::new(64 * 1024 * 1024))),
            edge_properties: Arc::new(RwLock::new(MemTable::new(64 * 1024 * 1024))),
            vertex_indexes: Arc::new(RwLock::new(HashMap::new())),
            edge_indexes: Arc::new(RwLock::new(HashMap::new())),
            schemas: Arc::new(RwLock::new(HashMap::new())),
            next_file_id: Arc::new(Mutex::new(1)),
        })
    }

    /// Set properties for a vertex
    pub async fn set_vertex_properties(
        &self,
        vertex_id: VertexId,
        properties: Properties,
    ) -> Result<()> {
        let entry = PropertyEntry::new_set(properties.clone(), Timestamp::now());
        let encoded_entry = bincode::serialize(&entry)?;
        let mem_entry = MemTableEntry::new_pivot(encoded_entry, entry.timestamp);

        // Store in MemTable
        {
            let vertex_props = self.vertex_properties.read();
            vertex_props.insert(vertex_id, mem_entry)?;
        }

        // Update indexes and schema
        if self.config.enable_indexes {
            self.update_vertex_indexes(vertex_id, &properties).await?;
        }

        self.update_schemas(&properties, true).await?;

        Ok(())
    }

    /// Get properties for a vertex
    pub async fn get_vertex_properties(&self, vertex_id: VertexId) -> Result<Properties> {
        let mut all_entries = Vec::new();

        // Get from MemTable
        {
            let vertex_props = self.vertex_properties.read();
            if let Some(entries) = vertex_props.get(vertex_id) {
                all_entries.extend(entries);
            }
        }

        // Merge entries to get final properties
        self.merge_property_entries(all_entries).await
    }

    /// Set properties for an edge
    pub async fn set_edge_properties(&self, edge_id: EdgeId, properties: Properties) -> Result<()> {
        let entry = PropertyEntry::new_set(properties.clone(), Timestamp::now());
        let encoded_entry = bincode::serialize(&entry)?;
        let mem_entry = MemTableEntry::new_pivot(encoded_entry, entry.timestamp);

        // Convert EdgeId to VertexId for storage compatibility
        let storage_id = VertexId::from_u64(edge_id.as_u64());

        // Store in MemTable
        {
            let edge_props = self.edge_properties.read();
            edge_props.insert(storage_id, mem_entry)?;
        }

        // Update indexes and schema
        if self.config.enable_indexes {
            self.update_edge_indexes(edge_id, &properties).await?;
        }

        self.update_schemas(&properties, false).await?;

        Ok(())
    }

    /// Get properties for an edge
    pub async fn get_edge_properties(&self, edge_id: EdgeId) -> Result<Properties> {
        let storage_id = VertexId::from_u64(edge_id.as_u64());
        let mut all_entries = Vec::new();

        // Get from MemTable
        {
            let edge_props = self.edge_properties.read();
            if let Some(entries) = edge_props.get(storage_id) {
                all_entries.extend(entries);
            }
        }

        // Merge entries to get final properties
        self.merge_property_entries(all_entries).await
    }

    /// Delete specific properties from a vertex
    pub async fn delete_vertex_properties(
        &self,
        vertex_id: VertexId,
        keys: Vec<String>,
    ) -> Result<()> {
        let entry = PropertyEntry::new_delete(keys.clone(), Timestamp::now());
        let encoded_entry = bincode::serialize(&entry)?;
        let mem_entry = MemTableEntry::new_delta(encoded_entry, entry.timestamp);

        // Store deletion entry
        {
            let vertex_props = self.vertex_properties.read();
            vertex_props.insert(vertex_id, mem_entry)?;
        }

        // Update indexes
        if self.config.enable_indexes {
            let mut vertex_indexes = self.vertex_indexes.write();
            for key in keys {
                if let Some(index) = vertex_indexes.get_mut(&key) {
                    index.remove(vertex_id.as_u64());
                }
            }
        }

        Ok(())
    }

    /// Find vertices by property value
    pub async fn find_vertices_by_property(
        &self,
        key: &str,
        value: &PropertyValue,
    ) -> Result<Vec<VertexId>> {
        if !self.config.enable_indexes {
            return Err(AsterError::invalid_operation("Indexes not enabled"));
        }

        let vertex_indexes = self.vertex_indexes.read();
        if let Some(index) = vertex_indexes.get(key) {
            let entity_ids = index.find_entities(value);
            Ok(entity_ids.into_iter().map(VertexId::from_u64).collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Find vertices by property value range
    pub async fn find_vertices_by_property_range(
        &self,
        key: &str,
        min: &PropertyValue,
        max: &PropertyValue,
    ) -> Result<Vec<VertexId>> {
        if !self.config.enable_indexes {
            return Err(AsterError::invalid_operation("Indexes not enabled"));
        }

        let vertex_indexes = self.vertex_indexes.read();
        if let Some(index) = vertex_indexes.get(key) {
            let entity_ids = index.find_entities_in_range(min, max);
            Ok(entity_ids.into_iter().map(VertexId::from_u64).collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Get all property keys for vertices
    pub async fn get_vertex_property_keys(&self) -> Result<Vec<String>> {
        let schemas = self.schemas.read();
        Ok(schemas
            .values()
            .filter(|schema| schema.key.starts_with("vertex:"))
            .map(|schema| schema.key.strip_prefix("vertex:").unwrap().to_string())
            .collect())
    }

    /// Get schema information for a property
    pub async fn get_property_schema(
        &self,
        key: &str,
        is_vertex: bool,
    ) -> Result<Option<PropertySchema>> {
        let prefix = if is_vertex { "vertex:" } else { "edge:" };
        let full_key = format!("{}{}", prefix, key);

        let schemas = self.schemas.read();
        Ok(schemas.get(&full_key).cloned())
    }

    /// Get statistics about the property store
    pub async fn get_stats(&self) -> PropertyStoreStats {
        let vertex_props_stats = {
            let vertex_props = self.vertex_properties.read();
            vertex_props.stats()
        };

        let edge_props_stats = {
            let edge_props = self.edge_properties.read();
            edge_props.stats()
        };

        let num_schemas = {
            let schemas = self.schemas.read();
            schemas.len()
        };

        let num_vertex_indexes = {
            let vertex_indexes = self.vertex_indexes.read();
            vertex_indexes.len()
        };

        let num_edge_indexes = {
            let edge_indexes = self.edge_indexes.read();
            edge_indexes.len()
        };

        PropertyStoreStats {
            vertex_properties: vertex_props_stats.num_vertices,
            edge_properties: edge_props_stats.num_vertices,
            total_schemas: num_schemas,
            vertex_indexes: num_vertex_indexes,
            edge_indexes: num_edge_indexes,
            memory_usage_bytes: vertex_props_stats.size_bytes + edge_props_stats.size_bytes,
        }
    }

    /// Update vertex indexes when properties change
    async fn update_vertex_indexes(
        &self,
        vertex_id: VertexId,
        properties: &Properties,
    ) -> Result<()> {
        let mut vertex_indexes = self.vertex_indexes.write();

        for (key, value) in properties {
            if vertex_indexes.len() >= self.config.max_indexed_keys {
                break;
            }

            let index = vertex_indexes
                .entry(key.clone())
                .or_insert_with(|| PropertyIndex::new(key.clone(), 10000));

            index.insert(vertex_id.as_u64(), value.clone());
        }

        Ok(())
    }

    /// Update edge indexes when properties change
    async fn update_edge_indexes(&self, edge_id: EdgeId, properties: &Properties) -> Result<()> {
        let mut edge_indexes = self.edge_indexes.write();

        for (key, value) in properties {
            if edge_indexes.len() >= self.config.max_indexed_keys {
                break;
            }

            let index = edge_indexes
                .entry(key.clone())
                .or_insert_with(|| PropertyIndex::new(key.clone(), 10000));

            index.insert(edge_id.as_u64(), value.clone());
        }

        Ok(())
    }

    /// Update property schemas
    async fn update_schemas(&self, properties: &Properties, is_vertex: bool) -> Result<()> {
        let mut schemas = self.schemas.write();
        let prefix = if is_vertex { "vertex:" } else { "edge:" };

        for (key, value) in properties {
            let full_key = format!("{}{}", prefix, key);
            let schema = schemas
                .entry(full_key.clone())
                .or_insert_with(|| PropertySchema {
                    key: full_key,
                    value_type: value.type_name().to_string(),
                    indexed: self.config.enable_indexes,
                    stats: PropertyStats::default(),
                });

            // Update statistics
            schema.stats.count += 1;

            // Add sample value for type inference
            if schema.stats.sample_values.len() < 10 {
                schema.stats.sample_values.push(value.to_string());
            }
        }

        Ok(())
    }

    /// Merge property entries to get final properties
    async fn merge_property_entries(&self, mut entries: Vec<MemTableEntry>) -> Result<Properties> {
        if entries.is_empty() {
            return Ok(Properties::new());
        }

        // Sort by timestamp (oldest first) to apply changes chronologically
        entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let mut current_properties = Properties::new();

        for entry in entries {
            let property_entry: PropertyEntry = bincode::deserialize(&entry.data)?;

            match property_entry.entry_type {
                PropertyEntryType::Set => {
                    // Add/update properties
                    for (key, value) in property_entry.properties {
                        current_properties.insert(key, value);
                    }
                }
                PropertyEntryType::Delete => {
                    // Remove specific properties
                    for key in property_entry.properties.keys() {
                        current_properties.remove(key);
                    }
                }
                PropertyEntryType::DeleteAll => {
                    // Clear all properties
                    current_properties.clear();
                    break; // Stop processing older entries
                }
            }
        }

        Ok(current_properties)
    }

    /// Get all vertex IDs that have properties (for Gremlin V() traversals)
    pub async fn get_all_vertex_ids(&self) -> Result<Vec<VertexId>> {
        let vertex_props = self.vertex_properties.read();
        let mut vertex_ids = std::collections::HashSet::new();

        // Collect all unique vertex IDs from the MemTable
        for (vertex_id, _) in vertex_props.iter() {
            vertex_ids.insert(vertex_id);
        }

        Ok(vertex_ids.into_iter().collect())
    }
}

/// Statistics about the property store
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PropertyStoreStats {
    pub vertex_properties: usize,
    pub edge_properties: usize,
    pub total_schemas: usize,
    pub vertex_indexes: usize,
    pub edge_indexes: usize,
    pub memory_usage_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_vertex_property_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = PropertyStoreConfig::default();
        let store = PropertyStore::new(temp_dir.path(), config).unwrap();

        let vertex_id = VertexId::from_u64(1);
        let mut properties = Properties::new();
        properties.insert("name".to_string(), "Alice".into());
        properties.insert("age".to_string(), 30.into());

        // Set properties
        store
            .set_vertex_properties(vertex_id, properties.clone())
            .await
            .unwrap();

        // Get properties
        let retrieved = store.get_vertex_properties(vertex_id).await.unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(retrieved.get("age").unwrap().as_int(), Some(30));
    }

    #[tokio::test]
    async fn test_property_indexing() {
        let temp_dir = TempDir::new().unwrap();
        let config = PropertyStoreConfig::default();
        let store = PropertyStore::new(temp_dir.path(), config).unwrap();

        // Add vertices with properties
        for i in 1..=10 {
            let vertex_id = VertexId::from_u64(i);
            let mut properties = Properties::new();
            properties.insert("type".to_string(), "user".into());
            properties.insert("score".to_string(), ((i * 10) as i64).into());

            store
                .set_vertex_properties(vertex_id, properties)
                .await
                .unwrap();
        }

        // Find vertices by property value
        let users = store
            .find_vertices_by_property("type", &PropertyValue::String("user".to_string()))
            .await
            .unwrap();
        assert_eq!(users.len(), 10);

        // Find vertices by score range
        let high_score = store
            .find_vertices_by_property_range(
                "score",
                &PropertyValue::Int(50),
                &PropertyValue::Int(100),
            )
            .await
            .unwrap();
        assert_eq!(high_score.len(), 6); // vertices 5-10
    }

    #[tokio::test]
    async fn test_property_deletion() {
        let temp_dir = TempDir::new().unwrap();
        let config = PropertyStoreConfig::default();
        let store = PropertyStore::new(temp_dir.path(), config).unwrap();

        let vertex_id = VertexId::from_u64(1);
        let mut properties = Properties::new();
        properties.insert("name".to_string(), "Alice".into());
        properties.insert("age".to_string(), 30.into());
        properties.insert("city".to_string(), "New York".into());

        // Set properties
        store
            .set_vertex_properties(vertex_id, properties)
            .await
            .unwrap();

        // Delete specific properties
        store
            .delete_vertex_properties(vertex_id, vec!["age".to_string()])
            .await
            .unwrap();

        // Check remaining properties
        let remaining = store.get_vertex_properties(vertex_id).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(remaining.contains_key("name"));
        assert!(remaining.contains_key("city"));
        assert!(!remaining.contains_key("age"));
    }
}
