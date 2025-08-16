//! Integration tests for property graph support with separate column family

use aster_db::{
    AsterDB, AsterDBConfig, EdgeId, Properties, PropertyStoreConfig, PropertyValue, VertexId,
};
use tempfile::TempDir;

#[tokio::test]
async fn test_property_store_integration() {
    let temp_dir = TempDir::new().unwrap();

    // Create database with properties enabled
    let config = AsterDBConfig {
        enable_properties: true,
        property_store_config: PropertyStoreConfig::default(),
        enable_recovery: false, // Disable for simpler test
        enable_metrics: false,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Verify properties are enabled
    assert!(db.properties_enabled());

    // Test vertex properties
    let vertex_id = VertexId::from_u64(1);
    let mut vertex_props = Properties::new();
    vertex_props.insert(
        "name".to_string(),
        PropertyValue::String("Alice".to_string()),
    );
    vertex_props.insert("age".to_string(), PropertyValue::Int(30));
    vertex_props.insert("score".to_string(), PropertyValue::Float(95.5));

    // Set vertex properties
    db.set_vertex_properties(vertex_id, vertex_props.clone())
        .await
        .unwrap();

    // Get vertex properties
    let retrieved_props = db.get_vertex_properties(vertex_id).await.unwrap();
    assert_eq!(retrieved_props.len(), 3);
    assert_eq!(
        retrieved_props.get("name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(retrieved_props.get("age").unwrap().as_int(), Some(30));
    assert_eq!(retrieved_props.get("score").unwrap().as_float(), Some(95.5));
}

#[tokio::test]
async fn test_edge_properties() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_properties: true,
        enable_recovery: false,
        enable_metrics: false,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Test edge properties
    let edge_id = EdgeId::from_u64(100);
    let mut edge_props = Properties::new();
    edge_props.insert("weight".to_string(), PropertyValue::Float(0.8));
    edge_props.insert(
        "type".to_string(),
        PropertyValue::String("friendship".to_string()),
    );

    // Set edge properties
    db.set_edge_properties(edge_id, edge_props.clone())
        .await
        .unwrap();

    // Get edge properties
    let retrieved_props = db.get_edge_properties(edge_id).await.unwrap();
    assert_eq!(retrieved_props.len(), 2);
    assert_eq!(retrieved_props.get("weight").unwrap().as_float(), Some(0.8));
    assert_eq!(
        retrieved_props.get("type").unwrap().as_string(),
        Some("friendship")
    );
}

#[tokio::test]
async fn test_property_search() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_properties: true,
        enable_recovery: false,
        enable_metrics: false,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Create multiple vertices with properties
    for i in 1..=10 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert(
            "type".to_string(),
            PropertyValue::String("user".to_string()),
        );
        props.insert("score".to_string(), PropertyValue::Int(i as i64 * 10));
        props.insert("active".to_string(), PropertyValue::Bool(i % 2 == 0));

        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Find vertices by exact property value
    let users = db
        .find_vertices_by_property("type", &PropertyValue::String("user".to_string()))
        .await
        .unwrap();
    assert_eq!(users.len(), 10);

    // Find vertices by property range
    let high_score_users = db
        .find_vertices_by_property_range("score", &PropertyValue::Int(50), &PropertyValue::Int(100))
        .await
        .unwrap();
    assert_eq!(high_score_users.len(), 6); // vertices 5-10
}

#[tokio::test]
async fn test_property_deletion() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_properties: true,
        enable_recovery: false,
        enable_metrics: false,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    let vertex_id = VertexId::from_u64(1);
    let mut props = Properties::new();
    props.insert(
        "name".to_string(),
        PropertyValue::String("Alice".to_string()),
    );
    props.insert("age".to_string(), PropertyValue::Int(30));
    props.insert(
        "city".to_string(),
        PropertyValue::String("New York".to_string()),
    );

    // Set properties
    db.set_vertex_properties(vertex_id, props).await.unwrap();

    // Delete specific properties
    db.delete_vertex_properties(vertex_id, vec!["age".to_string()])
        .await
        .unwrap();

    // Check remaining properties
    let remaining = db.get_vertex_properties(vertex_id).await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.contains_key("name"));
    assert!(remaining.contains_key("city"));
    assert!(!remaining.contains_key("age"));
}

#[tokio::test]
async fn test_properties_disabled() {
    let temp_dir = TempDir::new().unwrap();

    // Create database with properties disabled
    let config = AsterDBConfig {
        enable_properties: false,
        enable_recovery: false,
        enable_metrics: false,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Verify properties are disabled
    assert!(!db.properties_enabled());

    // Attempts to use properties should fail
    let vertex_id = VertexId::from_u64(1);
    let props = Properties::new();

    let result = db.set_vertex_properties(vertex_id, props).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Properties not enabled"));
}

#[tokio::test]
async fn test_property_store_metrics() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_properties: true,
        enable_metrics: true,
        enable_recovery: false,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Add some properties
    for i in 1..=5 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert("id".to_string(), PropertyValue::Int(i as i64));
        props.insert(
            "name".to_string(),
            PropertyValue::String(format!("User{}", i)),
        );

        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Get metrics and verify property stats are included
    let metrics = db.get_metrics().await;
    assert!(metrics.is_some());

    let metrics = metrics.unwrap();
    assert!(metrics.storage.property_stats.vertex_properties > 0);
    assert!(metrics.storage.property_stats.memory_usage_bytes > 0);
}

#[tokio::test]
async fn test_property_column_family_separation() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_properties: true,
        enable_recovery: false,
        enable_metrics: false,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Verify property store has separate directory
    let properties_dir = temp_dir.path().join("properties");
    assert!(
        properties_dir.exists(),
        "Properties directory should exist as separate column family"
    );

    // Add some data and verify it's stored separately
    let vertex_id = VertexId::from_u64(1);
    let mut props = Properties::new();
    props.insert(
        "test".to_string(),
        PropertyValue::String("value".to_string()),
    );

    db.set_vertex_properties(vertex_id, props).await.unwrap();

    // Verify property store can be accessed directly
    let property_store = db.property_store();
    assert!(property_store.is_some());

    let stored_props = property_store
        .unwrap()
        .get_vertex_properties(vertex_id)
        .await
        .unwrap();
    assert_eq!(stored_props.get("test").unwrap().as_string(), Some("value"));
}
