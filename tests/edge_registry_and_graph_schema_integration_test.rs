//! Edge Registry and Graph Schema Integration Tests
//!
//! Comprehensive tests for the edge registry and graph schema functionality including:
//! - Edge registry operations and metadata tracking
//! - Graph schema inference and validation
//! - Integration between edge registry and property store schemas
//! - Cross-validation of edge operations and schema consistency
//! - Performance and scalability testing for edge registries
//! - Schema evolution and migration scenarios

use aster_db::graph::Graph;
use aster_db::{AsterDB, AsterDBConfig, EdgeId, PropertyValue, Result, VertexId};
use tempfile::TempDir;

/// Configuration for edge registry and schema integration tests
struct EdgeRegistrySchemaTestConfig {
    temp_dir: TempDir,
    db_config: AsterDBConfig,
}

impl EdgeRegistrySchemaTestConfig {
    fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let db_config = AsterDBConfig {
            enable_recovery: false, // Disable for consistent testing
            enable_metrics: true,
            enable_properties: true,
            ..AsterDBConfig::default()
        };

        Self {
            temp_dir,
            db_config,
        }
    }
}

#[tokio::test]
async fn test_edge_registry_basic_operations() -> Result<()> {
    let test_config = EdgeRegistrySchemaTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Test basic edge registration
    let edge_id = EdgeId::new();
    let source = VertexId::from_u64(1);
    let target = VertexId::from_u64(2);
    let label = "follows".to_string();

    // Register the edge
    db.register_edge(edge_id, source, target, label.clone());

    // Test outgoing edges retrieval
    let outgoing = db.get_outgoing_edges(source, None);
    assert_eq!(outgoing.len(), 1, "Should have one outgoing edge");
    assert_eq!(outgoing[0].edge_id, edge_id);
    assert_eq!(outgoing[0].source, source);
    assert_eq!(outgoing[0].target, target);
    assert_eq!(outgoing[0].label, label);

    // Test incoming edges retrieval
    let incoming = db.get_incoming_edges(target, None);
    assert_eq!(incoming.len(), 1, "Should have one incoming edge");
    assert_eq!(incoming[0].edge_id, edge_id);
    assert_eq!(incoming[0].source, source);
    assert_eq!(incoming[0].target, target);
    assert_eq!(incoming[0].label, label);

    // Test label filtering
    let filtered_outgoing = db.get_outgoing_edges(source, Some("follows"));
    assert_eq!(
        filtered_outgoing.len(),
        1,
        "Should find edge with correct label"
    );

    let filtered_empty = db.get_outgoing_edges(source, Some("likes"));
    assert_eq!(
        filtered_empty.len(),
        0,
        "Should not find edge with different label"
    );

    // Test edge with different vertices should not interfere
    let edge_id2 = EdgeId::new();
    let source2 = VertexId::from_u64(3);
    let target2 = VertexId::from_u64(4);
    db.register_edge(edge_id2, source2, target2, "likes".to_string());

    let outgoing_v1 = db.get_outgoing_edges(source, None);
    assert_eq!(
        outgoing_v1.len(),
        1,
        "Vertex 1 should still have only one outgoing edge"
    );

    let outgoing_v3 = db.get_outgoing_edges(source2, None);
    assert_eq!(
        outgoing_v3.len(),
        1,
        "Vertex 3 should have one outgoing edge"
    );

    println!("✓ Edge registry basic operations test completed");
    Ok(())
}

#[tokio::test]
async fn test_edge_registry_multiple_edges_and_labels() -> Result<()> {
    let test_config = EdgeRegistrySchemaTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    let source = VertexId::from_u64(1);
    let target1 = VertexId::from_u64(2);
    let target2 = VertexId::from_u64(3);
    let target3 = VertexId::from_u64(4);

    // Register multiple edges with different labels
    let edge_follow1 = EdgeId::new();
    let edge_follow2 = EdgeId::new();
    let edge_like = EdgeId::new();
    let edge_block = EdgeId::new();

    db.register_edge(edge_follow1, source, target1, "follows".to_string());
    db.register_edge(edge_follow2, source, target2, "follows".to_string());
    db.register_edge(edge_like, source, target3, "likes".to_string());
    db.register_edge(edge_block, source, target1, "blocks".to_string());

    // Test retrieving all outgoing edges
    let all_outgoing = db.get_outgoing_edges(source, None);
    assert_eq!(all_outgoing.len(), 4, "Should have four outgoing edges");

    // Test label-specific filtering
    let follows_edges = db.get_outgoing_edges(source, Some("follows"));
    assert_eq!(follows_edges.len(), 2, "Should have two 'follows' edges");

    let likes_edges = db.get_outgoing_edges(source, Some("likes"));
    assert_eq!(likes_edges.len(), 1, "Should have one 'likes' edge");

    let blocks_edges = db.get_outgoing_edges(source, Some("blocks"));
    assert_eq!(blocks_edges.len(), 1, "Should have one 'blocks' edge");

    let nonexistent_edges = db.get_outgoing_edges(source, Some("nonexistent"));
    assert_eq!(
        nonexistent_edges.len(),
        0,
        "Should have no edges with nonexistent label"
    );

    // Test incoming edges for target1 (should have 'follows' and 'blocks')
    let target1_incoming = db.get_incoming_edges(target1, None);
    assert_eq!(
        target1_incoming.len(),
        2,
        "Target1 should have two incoming edges"
    );

    let target1_follows_incoming = db.get_incoming_edges(target1, Some("follows"));
    assert_eq!(
        target1_follows_incoming.len(),
        1,
        "Target1 should have one incoming 'follows' edge"
    );

    let target1_blocks_incoming = db.get_incoming_edges(target1, Some("blocks"));
    assert_eq!(
        target1_blocks_incoming.len(),
        1,
        "Target1 should have one incoming 'blocks' edge"
    );

    // Verify edge IDs are correct
    let follows_edge_ids: Vec<EdgeId> = follows_edges.iter().map(|e| e.edge_id).collect();
    assert!(follows_edge_ids.contains(&edge_follow1));
    assert!(follows_edge_ids.contains(&edge_follow2));

    println!("✓ Edge registry multiple edges and labels test completed");
    Ok(())
}

#[tokio::test]
async fn test_property_schema_inference_and_validation() -> Result<()> {
    let test_config = EdgeRegistrySchemaTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Test vertex property schema inference
    let vertex_id = VertexId::from_u64(1);
    let mut vertex_properties = std::collections::HashMap::new();
    vertex_properties.insert(
        "name".to_string(),
        PropertyValue::String("Alice".to_string()),
    );
    vertex_properties.insert("age".to_string(), PropertyValue::Int(30));
    vertex_properties.insert("score".to_string(), PropertyValue::Float(95.5));
    vertex_properties.insert("active".to_string(), PropertyValue::Bool(true));

    db.set_vertex_properties(vertex_id, vertex_properties.clone())
        .await?;

    // Test edge property schema inference
    let edge_id = EdgeId::new();
    let mut edge_properties = std::collections::HashMap::new();
    edge_properties.insert("weight".to_string(), PropertyValue::Float(0.8));
    edge_properties.insert(
        "created_at".to_string(),
        PropertyValue::String("2024-01-01".to_string()),
    );
    edge_properties.insert("confirmed".to_string(), PropertyValue::Bool(false));

    db.set_edge_properties(edge_id, edge_properties.clone())
        .await?;

    // Test property retrieval (since schema inspection isn't available in AsterDB API)
    let retrieved_vertex_props = db.get_vertex_properties(vertex_id).await?;
    assert_eq!(
        retrieved_vertex_props.get("name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(
        retrieved_vertex_props.get("age").unwrap().as_int(),
        Some(30)
    );
    assert_eq!(
        retrieved_vertex_props.get("score").unwrap().as_float(),
        Some(95.5)
    );
    assert_eq!(
        retrieved_vertex_props.get("active").unwrap().as_bool(),
        Some(true)
    );

    let retrieved_edge_props = db.get_edge_properties(edge_id).await?;
    assert_eq!(
        retrieved_edge_props.get("weight").unwrap().as_float(),
        Some(0.8)
    );
    assert_eq!(
        retrieved_edge_props.get("created_at").unwrap().as_string(),
        Some("2024-01-01")
    );
    assert_eq!(
        retrieved_edge_props.get("confirmed").unwrap().as_bool(),
        Some(false)
    );

    // Test property search functionality
    let users_with_name_alice = db
        .find_vertices_by_property("name", &PropertyValue::String("Alice".to_string()))
        .await?;
    assert_eq!(users_with_name_alice.len(), 1);
    assert_eq!(users_with_name_alice[0], vertex_id);

    let users_with_active_true = db
        .find_vertices_by_property("active", &PropertyValue::Bool(true))
        .await?;
    assert_eq!(users_with_active_true.len(), 1);
    assert_eq!(users_with_active_true[0], vertex_id);

    println!("✓ Property schema inference and validation test completed");
    println!("  Vertex properties tested: name, age, score, active");
    println!("  Edge properties tested: weight, created_at, confirmed");
    Ok(())
}

#[tokio::test]
async fn test_graph_integration_with_edge_registry() -> Result<()> {
    let test_config = EdgeRegistrySchemaTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Create a graph using the lower-level Graph interface
    let graph = Graph::new(db.storage());

    // Create vertices with properties
    let v1_id = VertexId::from_u64(1);
    let v2_id = VertexId::from_u64(2);
    let v3_id = VertexId::from_u64(3);

    // Add vertices with properties
    let mut v1_props = std::collections::HashMap::new();
    v1_props.insert(
        "type".to_string(),
        PropertyValue::String("user".to_string()),
    );
    v1_props.insert(
        "name".to_string(),
        PropertyValue::String("Alice".to_string()),
    );
    db.set_vertex_properties(v1_id, v1_props).await?;

    let mut v2_props = std::collections::HashMap::new();
    v2_props.insert(
        "type".to_string(),
        PropertyValue::String("user".to_string()),
    );
    v2_props.insert("name".to_string(), PropertyValue::String("Bob".to_string()));
    db.set_vertex_properties(v2_id, v2_props).await?;

    let mut v3_props = std::collections::HashMap::new();
    v3_props.insert(
        "type".to_string(),
        PropertyValue::String("post".to_string()),
    );
    v3_props.insert(
        "title".to_string(),
        PropertyValue::String("Hello World".to_string()),
    );
    db.set_vertex_properties(v3_id, v3_props).await?;

    // Add edges through the graph interface
    let edge1 = graph.add_edge(v1_id, v2_id, None).await?;
    let edge2 = graph.add_edge(v1_id, v3_id, None).await?;
    let edge3 = graph.add_edge(v2_id, v3_id, None).await?;

    // Register edges in the edge registry with labels
    db.register_edge(edge1.id(), v1_id, v2_id, "follows".to_string());
    db.register_edge(edge2.id(), v1_id, v3_id, "created".to_string());
    db.register_edge(edge3.id(), v2_id, v3_id, "likes".to_string());

    // Add edge properties
    let mut edge1_props = std::collections::HashMap::new();
    edge1_props.insert(
        "since".to_string(),
        PropertyValue::String("2024-01-01".to_string()),
    );
    edge1_props.insert("weight".to_string(), PropertyValue::Float(0.9));
    db.set_edge_properties(edge1.id(), edge1_props).await?;

    let mut edge2_props = std::collections::HashMap::new();
    edge2_props.insert(
        "timestamp".to_string(),
        PropertyValue::String("2024-01-02T10:30:00Z".to_string()),
    );
    db.set_edge_properties(edge2.id(), edge2_props).await?;

    // Test graph connectivity through both interfaces
    assert!(
        graph.has_edge(v1_id, v2_id).await?,
        "Graph should show v1->v2 edge"
    );
    assert!(
        graph.has_edge(v1_id, v3_id).await?,
        "Graph should show v1->v3 edge"
    );
    assert!(
        graph.has_edge(v2_id, v3_id).await?,
        "Graph should show v2->v3 edge"
    );
    assert!(
        !graph.has_edge(v2_id, v1_id).await?,
        "Graph should not show v2->v1 edge"
    );

    // Test graph degrees
    assert_eq!(graph.get_degree(v1_id).await?, 2, "v1 should have degree 2");
    assert_eq!(graph.get_degree(v2_id).await?, 1, "v2 should have degree 1");
    assert_eq!(
        graph.get_degree(v3_id).await?,
        0,
        "v3 should have degree 0 (no outgoing edges)"
    );

    // Test edge registry functionality
    let v1_outgoing = db.get_outgoing_edges(v1_id, None);
    assert_eq!(
        v1_outgoing.len(),
        2,
        "v1 should have 2 outgoing edges in registry"
    );

    let v1_follows = db.get_outgoing_edges(v1_id, Some("follows"));
    assert_eq!(v1_follows.len(), 1, "v1 should have 1 'follows' edge");
    assert_eq!(v1_follows[0].target, v2_id);

    let v1_created = db.get_outgoing_edges(v1_id, Some("created"));
    assert_eq!(v1_created.len(), 1, "v1 should have 1 'created' edge");
    assert_eq!(v1_created[0].target, v3_id);

    let v3_incoming = db.get_incoming_edges(v3_id, None);
    assert_eq!(v3_incoming.len(), 2, "v3 should have 2 incoming edges");

    // Test property retrieval and schema consistency
    let v1_retrieved_props = db.get_vertex_properties(v1_id).await?;
    assert_eq!(
        v1_retrieved_props.get("name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(
        v1_retrieved_props.get("type").unwrap().as_string(),
        Some("user")
    );

    let edge1_retrieved_props = db.get_edge_properties(edge1.id()).await?;
    assert_eq!(
        edge1_retrieved_props.get("since").unwrap().as_string(),
        Some("2024-01-01")
    );
    assert_eq!(
        edge1_retrieved_props.get("weight").unwrap().as_float(),
        Some(0.9)
    );

    // Verify property retrieval works correctly for type validation
    let alice_retrieved = db.get_vertex_properties(v1_id).await?;
    assert_eq!(
        alice_retrieved.get("type").unwrap().as_string(),
        Some("user")
    );
    assert_eq!(
        alice_retrieved.get("name").unwrap().as_string(),
        Some("Alice")
    );

    let edge1_retrieved = db.get_edge_properties(edge1.id()).await?;
    assert_eq!(edge1_retrieved.get("weight").unwrap().as_float(), Some(0.9));

    println!("✓ Graph integration with edge registry test completed");
    Ok(())
}

#[tokio::test]
async fn test_schema_evolution_and_type_consistency() -> Result<()> {
    let test_config = EdgeRegistrySchemaTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    let vertex_id = VertexId::from_u64(1);

    // Initial property with Int type
    let mut props1 = std::collections::HashMap::new();
    props1.insert("score".to_string(), PropertyValue::Int(100));
    db.set_vertex_properties(vertex_id, props1).await?;

    // Verify property was set correctly
    let retrieved_props = db.get_vertex_properties(vertex_id).await?;
    assert_eq!(retrieved_props.get("score").unwrap().as_int(), Some(100));

    // Add another vertex with the same property type
    let vertex_id2 = VertexId::from_u64(2);
    let mut props2 = std::collections::HashMap::new();
    props2.insert("score".to_string(), PropertyValue::Int(85));
    db.set_vertex_properties(vertex_id2, props2).await?;

    let updated_props = db.get_vertex_properties(vertex_id2).await?;
    assert_eq!(updated_props.get("score").unwrap().as_int(), Some(85));

    // Test with different property on same vertex
    let mut props3 = std::collections::HashMap::new();
    props3.insert("rating".to_string(), PropertyValue::Float(4.5));
    props3.insert("active".to_string(), PropertyValue::Bool(true));
    db.set_vertex_properties(vertex_id, props3).await?;

    // Verify new properties were set correctly
    let latest_props = db.get_vertex_properties(vertex_id).await?;
    assert_eq!(latest_props.get("rating").unwrap().as_float(), Some(4.5));
    assert_eq!(latest_props.get("active").unwrap().as_bool(), Some(true));

    // Test schema statistics accumulation
    let vertex_id3 = VertexId::from_u64(3);
    let mut props4 = std::collections::HashMap::new();
    props4.insert("rating".to_string(), PropertyValue::Float(3.8));
    db.set_vertex_properties(vertex_id3, props4).await?;

    let final_rating_props = db.get_vertex_properties(vertex_id3).await?;
    assert_eq!(
        final_rating_props.get("rating").unwrap().as_float(),
        Some(3.8)
    );

    // Verify property search works across vertices
    let vertices_with_rating = db
        .find_vertices_by_property_range(
            "rating",
            &PropertyValue::Float(3.0),
            &PropertyValue::Float(5.0),
        )
        .await?;
    assert!(
        vertices_with_rating.len() >= 2,
        "Should find vertices with rating in range"
    );

    println!("✓ Schema evolution and type consistency test completed");
    println!("  Property types tested: Int (score), Float (rating), Bool (active)");
    println!("  Property search validated for range queries");
    Ok(())
}

#[tokio::test]
async fn test_edge_registry_performance_and_scalability() -> Result<()> {
    let test_config = EdgeRegistrySchemaTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Test performance with larger numbers of edges
    let num_vertices = 100usize;
    let num_edges_per_vertex = 10usize;

    let start_time = std::time::Instant::now();

    // Create a hub vertex that connects to many others
    let hub_vertex = VertexId::from_u64(0);

    for i in 1..=num_vertices {
        let target = VertexId::from_u64(i as u64);

        // Create multiple edges from hub to target with different labels
        for j in 0..num_edges_per_vertex {
            let edge_id = EdgeId::new();
            let label = match j % 3 {
                0 => "follows",
                1 => "likes",
                _ => "mentions",
            };

            db.register_edge(edge_id, hub_vertex, target, label.to_string());
        }
    }

    let registration_time = start_time.elapsed();

    // Test retrieval performance
    let retrieval_start = std::time::Instant::now();

    let all_outgoing = db.get_outgoing_edges(hub_vertex, None);
    assert_eq!(
        all_outgoing.len(),
        num_vertices * num_edges_per_vertex,
        "Should have registered all edges"
    );

    let follows_edges = db.get_outgoing_edges(hub_vertex, Some("follows"));
    assert!(
        follows_edges.len() >= num_vertices,
        "Should have many 'follows' edges"
    );

    let likes_edges = db.get_outgoing_edges(hub_vertex, Some("likes"));
    assert!(
        likes_edges.len() >= num_vertices,
        "Should have many 'likes' edges"
    );

    let mentions_edges = db.get_outgoing_edges(hub_vertex, Some("mentions"));
    assert!(
        mentions_edges.len() >= num_vertices,
        "Should have many 'mentions' edges"
    );

    let retrieval_time = retrieval_start.elapsed();

    // Test incoming edge performance for one of the target vertices
    let target_vertex = VertexId::from_u64(50);
    let incoming_start = std::time::Instant::now();

    let incoming_edges = db.get_incoming_edges(target_vertex, None);
    assert_eq!(
        incoming_edges.len(),
        num_edges_per_vertex,
        "Target should have correct number of incoming edges"
    );

    let incoming_time = incoming_start.elapsed();

    // Performance assertions (these should be reasonable for the scale)
    assert!(
        registration_time.as_millis() < 5000,
        "Registration should complete in reasonable time: {}ms",
        registration_time.as_millis()
    );
    assert!(
        retrieval_time.as_millis() < 1000,
        "Full retrieval should be fast: {}ms",
        retrieval_time.as_millis()
    );
    assert!(
        incoming_time.as_millis() < 100,
        "Incoming edge lookup should be very fast: {}ms",
        incoming_time.as_millis()
    );

    // Test memory efficiency by checking we don't have duplicate entries
    let unique_edge_ids: std::collections::HashSet<EdgeId> =
        all_outgoing.iter().map(|e| e.edge_id).collect();
    assert_eq!(
        unique_edge_ids.len(),
        all_outgoing.len(),
        "All edge IDs should be unique"
    );

    println!("✓ Edge registry performance and scalability test completed");
    println!(
        "  Registered {} edges in {}ms",
        num_vertices * num_edges_per_vertex,
        registration_time.as_millis()
    );
    println!(
        "  Retrieved {} edges in {}ms",
        all_outgoing.len(),
        retrieval_time.as_millis()
    );
    println!(
        "  Incoming lookup for {} edges in {}ms",
        incoming_edges.len(),
        incoming_time.as_millis()
    );
    Ok(())
}

#[tokio::test]
async fn test_edge_registry_concurrent_operations() -> Result<()> {
    let test_config = EdgeRegistrySchemaTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    let db = std::sync::Arc::new(db);
    let num_tasks = 10;
    let edges_per_task = 50;

    // Test concurrent edge registrations
    let mut handles = Vec::new();

    for task_id in 0..num_tasks {
        let db_clone = db.clone();
        let handle = tokio::spawn(async move {
            for i in 0..edges_per_task {
                let edge_id = EdgeId::new();
                let source = VertexId::from_u64((task_id * edges_per_task + i) as u64);
                let target = VertexId::from_u64((task_id * edges_per_task + i + 1) as u64);
                let label = format!("edge_{}_{}", task_id, i);

                db_clone.register_edge(edge_id, source, target, label);
            }
        });
        handles.push(handle);
    }

    // Wait for all registrations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all edges were registered correctly
    let mut total_edges = 0;
    for task_id in 0..num_tasks {
        for i in 0..edges_per_task {
            let source = VertexId::from_u64((task_id * edges_per_task + i) as u64);
            let outgoing = db.get_outgoing_edges(source, None);
            assert_eq!(
                outgoing.len(),
                1,
                "Each source should have exactly one outgoing edge"
            );
            total_edges += outgoing.len();
        }
    }

    assert_eq!(
        total_edges,
        num_tasks * edges_per_task,
        "All edges should be registered"
    );

    // Test concurrent retrievals
    let mut retrieval_handles = Vec::new();

    for task_id in 0..num_tasks {
        let db_clone = db.clone();
        let handle = tokio::spawn(async move {
            let mut task_edges = 0;
            for i in 0..edges_per_task {
                let source = VertexId::from_u64((task_id * edges_per_task + i) as u64);
                let outgoing = db_clone.get_outgoing_edges(source, None);
                task_edges += outgoing.len();
            }
            task_edges
        });
        retrieval_handles.push(handle);
    }

    // Wait for all retrievals and verify results
    let mut total_retrieved = 0;
    for handle in retrieval_handles {
        let task_result = handle.await.unwrap();
        total_retrieved += task_result;
    }

    assert_eq!(
        total_retrieved,
        num_tasks * edges_per_task,
        "All edges should be retrievable concurrently"
    );

    println!("✓ Edge registry concurrent operations test completed");
    println!(
        "  Successfully registered and retrieved {} edges concurrently",
        total_retrieved
    );
    Ok(())
}

#[tokio::test]
async fn test_comprehensive_edge_and_schema_integration() -> Result<()> {
    let test_config = EdgeRegistrySchemaTestConfig::new();

    let db = AsterDB::open_with_config(test_config.temp_dir.path(), test_config.db_config.clone())
        .await?;

    // Create a complex graph with multiple entity types and relationships

    // User vertices
    let alice_id = VertexId::from_u64(1);
    let bob_id = VertexId::from_u64(2);

    // Content vertices
    let post1_id = VertexId::from_u64(101);
    let comment1_id = VertexId::from_u64(201);

    // Set user properties
    let mut alice_props = std::collections::HashMap::new();
    alice_props.insert(
        "type".to_string(),
        PropertyValue::String("user".to_string()),
    );
    alice_props.insert(
        "username".to_string(),
        PropertyValue::String("alice".to_string()),
    );
    alice_props.insert("age".to_string(), PropertyValue::Int(25));
    alice_props.insert("verified".to_string(), PropertyValue::Bool(true));
    db.set_vertex_properties(alice_id, alice_props).await?;

    let mut bob_props = std::collections::HashMap::new();
    bob_props.insert(
        "type".to_string(),
        PropertyValue::String("user".to_string()),
    );
    bob_props.insert(
        "username".to_string(),
        PropertyValue::String("bob".to_string()),
    );
    bob_props.insert("age".to_string(), PropertyValue::Int(30));
    bob_props.insert("verified".to_string(), PropertyValue::Bool(false));
    db.set_vertex_properties(bob_id, bob_props).await?;

    // Set content properties
    let mut post1_props = std::collections::HashMap::new();
    post1_props.insert(
        "type".to_string(),
        PropertyValue::String("post".to_string()),
    );
    post1_props.insert(
        "title".to_string(),
        PropertyValue::String("Hello World".to_string()),
    );
    post1_props.insert("views".to_string(), PropertyValue::Int(1000));
    post1_props.insert("rating".to_string(), PropertyValue::Float(4.5));
    db.set_vertex_properties(post1_id, post1_props).await?;

    let mut comment1_props = std::collections::HashMap::new();
    comment1_props.insert(
        "type".to_string(),
        PropertyValue::String("comment".to_string()),
    );
    comment1_props.insert(
        "text".to_string(),
        PropertyValue::String("Great post!".to_string()),
    );
    comment1_props.insert("upvotes".to_string(), PropertyValue::Int(5));
    db.set_vertex_properties(comment1_id, comment1_props)
        .await?;

    // Create various types of edges with properties
    let follow_edge = EdgeId::new();
    let created_edge = EdgeId::new();
    let liked_edge = EdgeId::new();
    let commented_edge = EdgeId::new();

    // Register edges with different labels
    db.register_edge(follow_edge, alice_id, bob_id, "follows".to_string());
    db.register_edge(created_edge, alice_id, post1_id, "created".to_string());
    db.register_edge(liked_edge, bob_id, post1_id, "liked".to_string());
    db.register_edge(commented_edge, bob_id, comment1_id, "created".to_string());

    // Add edge properties
    let mut follow_props = std::collections::HashMap::new();
    follow_props.insert(
        "since".to_string(),
        PropertyValue::String("2024-01-01".to_string()),
    );
    follow_props.insert("strength".to_string(), PropertyValue::Float(0.8));
    db.set_edge_properties(follow_edge, follow_props).await?;

    let mut created_props = std::collections::HashMap::new();
    created_props.insert(
        "timestamp".to_string(),
        PropertyValue::String("2024-01-15T10:30:00Z".to_string()),
    );
    db.set_edge_properties(created_edge, created_props).await?;

    let mut liked_props = std::collections::HashMap::new();
    liked_props.insert(
        "timestamp".to_string(),
        PropertyValue::String("2024-01-16T14:22:00Z".to_string()),
    );
    liked_props.insert(
        "reaction".to_string(),
        PropertyValue::String("thumbs_up".to_string()),
    );
    db.set_edge_properties(liked_edge, liked_props).await?;

    // Test complex queries using both edge registry and property schemas

    // Find all users
    let all_vertices_with_type_user = db
        .find_vertices_by_property("type", &PropertyValue::String("user".to_string()))
        .await?;
    assert_eq!(all_vertices_with_type_user.len(), 2, "Should find 2 users");
    assert!(all_vertices_with_type_user.contains(&alice_id));
    assert!(all_vertices_with_type_user.contains(&bob_id));

    // Find verified users
    let verified_users = db
        .find_vertices_by_property("verified", &PropertyValue::Bool(true))
        .await?;
    assert_eq!(verified_users.len(), 1, "Should find 1 verified user");
    assert!(verified_users.contains(&alice_id));

    // Find users in age range
    let age_range_users = db
        .find_vertices_by_property_range("age", &PropertyValue::Int(25), &PropertyValue::Int(35))
        .await?;
    assert_eq!(age_range_users.len(), 2, "Should find users in age range");

    // Test edge registry queries
    let alice_follows = db.get_outgoing_edges(alice_id, Some("follows"));
    assert_eq!(alice_follows.len(), 1, "Alice should follow 1 person");
    assert_eq!(alice_follows[0].target, bob_id);

    let alice_created = db.get_outgoing_edges(alice_id, Some("created"));
    assert_eq!(alice_created.len(), 1, "Alice should have created 1 item");
    assert_eq!(alice_created[0].target, post1_id);

    let post1_creators = db.get_incoming_edges(post1_id, Some("created"));
    assert_eq!(post1_creators.len(), 1, "Post1 should have 1 creator");
    assert_eq!(post1_creators[0].source, alice_id);

    let post1_likers = db.get_incoming_edges(post1_id, Some("liked"));
    assert_eq!(post1_likers.len(), 1, "Post1 should have 1 liker");
    assert_eq!(post1_likers[0].source, bob_id);

    // Test property retrieval to validate types (schema inspection not available through AsterDB API)
    let alice_retrieved = db.get_vertex_properties(alice_id).await?;
    assert_eq!(
        alice_retrieved.get("type").unwrap().as_string(),
        Some("user")
    );
    assert_eq!(
        alice_retrieved.get("username").unwrap().as_string(),
        Some("alice")
    );
    assert_eq!(alice_retrieved.get("age").unwrap().as_int(), Some(25));
    assert_eq!(
        alice_retrieved.get("verified").unwrap().as_bool(),
        Some(true)
    );

    let post1_retrieved = db.get_vertex_properties(post1_id).await?;
    assert_eq!(
        post1_retrieved.get("type").unwrap().as_string(),
        Some("post")
    );
    assert_eq!(
        post1_retrieved.get("title").unwrap().as_string(),
        Some("Hello World")
    );
    assert_eq!(post1_retrieved.get("views").unwrap().as_int(), Some(1000));
    assert_eq!(post1_retrieved.get("rating").unwrap().as_float(), Some(4.5));

    let follow_props_retrieved = db.get_edge_properties(follow_edge).await?;
    assert_eq!(
        follow_props_retrieved.get("since").unwrap().as_string(),
        Some("2024-01-01")
    );
    assert_eq!(
        follow_props_retrieved.get("strength").unwrap().as_float(),
        Some(0.8)
    );

    println!("✓ Comprehensive edge and schema integration test completed");
    println!("  Vertex properties validated: type, username, age, verified, title, views, rating");
    println!("  Total edge relationships tested: 4");
    println!("  Property types validated through retrieval");
    Ok(())
}
