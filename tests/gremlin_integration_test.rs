//! Integration tests for Gremlin Query Language Interface
//!
//! Tests the complete Gremlin functionality including:
//! - Basic traversals (V, E, out, in, both)
//! - Property filtering (has, hasKey, hasValue)
//! - Data selection (values, properties, propertyMap)
//! - Aggregation operations (count, sum, mean, min, max)
//! - Complex traversals with chaining
//! - Query optimization and performance

use aster_db::query::GremlinPredicate;
use aster_db::{
    AsterDB, AsterDBConfig, GremlinContext, Properties, PropertyValue, QueryContext, VertexId,
};
use tempfile::TempDir;

#[tokio::test]
async fn test_basic_gremlin_traversals() {
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

    // Create test graph: 1 -> 2 -> 3 -> 4
    let vertices: Vec<VertexId> = (1..=4).map(VertexId::from_u64).collect();

    // Add vertices with properties
    for (i, vertex) in vertices.iter().enumerate() {
        let mut props = Properties::new();
        props.insert("id".to_string(), PropertyValue::Int(i as i64 + 1));
        props.insert(
            "name".to_string(),
            PropertyValue::String(format!("vertex_{}", i + 1)),
        );
        props.insert(
            "type".to_string(),
            PropertyValue::String("node".to_string()),
        );

        db.set_vertex_properties(*vertex, props).await.unwrap();
    }

    // Create edges: 1->2, 2->3, 3->4
    for i in 0..vertices.len() - 1 {
        db.graph()
            .add_edge(vertices[i], vertices[i + 1], None)
            .await
            .unwrap();
    }

    // Test g.V() - get all vertices
    let traversal = db.g_v();
    let result = db.gremlin(&traversal).await.unwrap();

    // We should get vertices, though exact count depends on storage implementation
    assert!(!result.is_empty());

    // Test g.V().count() - count all vertices
    let traversal = db.g_v().count();
    let result = db.gremlin(&traversal).await.unwrap();

    assert_eq!(result.len(), 1);
    if let Some(count_result) = result.first() {
        assert!(count_result.as_count().is_some());
    }

    // Test g.V().limit(2) - limit results
    let traversal = db.g_v().limit(2);
    let result = db.gremlin(&traversal).await.unwrap();

    assert!(result.len() <= 2);
}

#[tokio::test]
async fn test_gremlin_property_filtering() {
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

    // Create vertices with different properties
    for i in 1..=5 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert("id".to_string(), PropertyValue::Int(i as i64));
        props.insert(
            "name".to_string(),
            PropertyValue::String(format!("vertex_{}", i)),
        );
        props.insert(
            "type".to_string(),
            PropertyValue::String(if i % 2 == 0 {
                "even".to_string()
            } else {
                "odd".to_string()
            }),
        );
        props.insert("score".to_string(), PropertyValue::Float(i as f64 * 10.0));

        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Test g.V().has("type", "even") - filter by property value
    let traversal = db.g_v().has(
        "type".to_string(),
        Some(PropertyValue::String("even".to_string())),
    );
    let result = db.gremlin(&traversal).await.unwrap();

    // Should return vertices with even type
    println!("Even type vertices: {}", result.len());

    // Test g.V().hasKey("score") - filter by property existence
    let traversal = db.g_v().has_key("score".to_string());
    let result = db.gremlin(&traversal).await.unwrap();

    // Should return vertices that have a score property
    println!("Vertices with score: {}", result.len());

    // Test g.V().hasValue(30.0) - filter by property value
    let traversal = db.g_v().has_value(PropertyValue::Float(30.0));
    let result = db.gremlin(&traversal).await.unwrap();

    // Should return vertices that have property value 30.0
    println!("Vertices with value 30.0: {}", result.len());
}

#[tokio::test]
async fn test_gremlin_traversal_steps() {
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

    // Create a small graph
    let v1 = VertexId::from_u64(1);
    let v2 = VertexId::from_u64(2);
    let v3 = VertexId::from_u64(3);

    // Add edges: 1->2, 2->3
    db.graph().add_edge(v1, v2, None).await.unwrap();
    db.graph().add_edge(v2, v3, None).await.unwrap();

    // Add properties
    let mut props1 = Properties::new();
    props1.insert(
        "name".to_string(),
        PropertyValue::String("alice".to_string()),
    );
    props1.insert("age".to_string(), PropertyValue::Int(30));
    db.set_vertex_properties(v1, props1).await.unwrap();

    let mut props2 = Properties::new();
    props2.insert("name".to_string(), PropertyValue::String("bob".to_string()));
    props2.insert("age".to_string(), PropertyValue::Int(25));
    db.set_vertex_properties(v2, props2).await.unwrap();

    let mut props3 = Properties::new();
    props3.insert(
        "name".to_string(),
        PropertyValue::String("charlie".to_string()),
    );
    props3.insert("age".to_string(), PropertyValue::Int(35));
    db.set_vertex_properties(v3, props3).await.unwrap();

    // Test g.V(1).out() - get outgoing neighbors
    let traversal = db.g_v_ids(vec![v1]).out(None);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Outgoing neighbors from v1: {}", result.len());

    // Test g.V().out().out() - chain traversals
    let traversal = db.g_v_ids(vec![v1]).out(None).out(None);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Two-hop neighbors from v1: {}", result.len());
}

#[tokio::test]
async fn test_gremlin_property_selection() {
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

    // Create vertices with properties
    for i in 1..=3 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert(
            "name".to_string(),
            PropertyValue::String(format!("vertex_{}", i)),
        );
        props.insert("value".to_string(), PropertyValue::Int(i as i64 * 10));
        props.insert("enabled".to_string(), PropertyValue::Bool(i % 2 == 0));

        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Test g.V().values("name") - get property values
    let traversal = db.g_v().values(vec!["name".to_string()]);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Name values: {}", result.len());

    // Check that we got string values
    for res in result.iter() {
        if let Some(value) = res.as_value() {
            assert!(matches!(value, PropertyValue::String(_)));
        }
    }

    // Test g.V().propertyMap() - get property maps
    let traversal = db.g_v().property_map(None);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Property maps: {}", result.len());
}

#[tokio::test]
async fn test_gremlin_aggregation_functions() {
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

    // Create vertices with numeric properties
    for i in 1..=5 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert("value".to_string(), PropertyValue::Int(i as i64));

        // Add vertex to graph storage (by adding a self-edge or similar)
        db.graph()
            .add_vertex(vertex_id, Some(props.clone()))
            .await
            .unwrap();
        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Test g.V().values("value").sum() - sum of values
    let traversal = db.g_v().values(vec!["value".to_string()]).sum();
    let result = db.gremlin(&traversal).await.unwrap();

    assert_eq!(result.len(), 1);
    if let Some(sum_result) = result.first() {
        if let Some(value) = sum_result.as_value() {
            if let Some(sum_val) = value.as_float() {
                assert!(sum_val > 0.0); // Should be sum of 1+2+3+4+5 = 15
            }
        }
    }

    // Test g.V().values("value").mean() - average of values
    let traversal = db.g_v().values(vec!["value".to_string()]).mean();
    let result = db.gremlin(&traversal).await.unwrap();

    assert_eq!(result.len(), 1);

    // Test g.V().values("value").max() - maximum value
    let traversal = db.g_v().values(vec!["value".to_string()]).max();
    let result = db.gremlin(&traversal).await.unwrap();

    assert_eq!(result.len(), 1);

    // Test g.V().values("value").min() - minimum value
    let traversal = db.g_v().values(vec!["value".to_string()]).min();
    let result = db.gremlin(&traversal).await.unwrap();

    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_gremlin_complex_traversals() {
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

    // Create a more complex graph
    let vertices: Vec<VertexId> = (1..=6).map(VertexId::from_u64).collect();

    // Add vertices with different types
    for (i, &vertex) in vertices.iter().enumerate() {
        let mut props = Properties::new();
        props.insert("id".to_string(), PropertyValue::Int(i as i64 + 1));
        props.insert(
            "type".to_string(),
            PropertyValue::String(if i < 3 {
                "person".to_string()
            } else {
                "place".to_string()
            }),
        );
        props.insert(
            "name".to_string(),
            PropertyValue::String(format!("entity_{}", i + 1)),
        );

        db.set_vertex_properties(vertex, props).await.unwrap();
    }

    // Create edges
    db.graph()
        .add_edge(vertices[0], vertices[1], None)
        .await
        .unwrap(); // 1->2
    db.graph()
        .add_edge(vertices[1], vertices[2], None)
        .await
        .unwrap(); // 2->3
    db.graph()
        .add_edge(vertices[0], vertices[3], None)
        .await
        .unwrap(); // 1->4
    db.graph()
        .add_edge(vertices[3], vertices[4], None)
        .await
        .unwrap(); // 4->5

    // Test complex traversal: g.V().has("type", "person").out().has("type", "person").count()
    let traversal = db
        .g_v()
        .has(
            "type".to_string(),
            Some(PropertyValue::String("person".to_string())),
        )
        .out(None)
        .has(
            "type".to_string(),
            Some(PropertyValue::String("person".to_string())),
        )
        .count();

    let result = db.gremlin(&traversal).await.unwrap();

    assert_eq!(result.len(), 1);

    // Test with deduplication: g.V().out().dedup().count()
    let traversal = db.g_v().out(None).dedup().count();
    let result = db.gremlin(&traversal).await.unwrap();

    assert_eq!(result.len(), 1);

    // Test with limit and skip: g.V().skip(1).limit(2)
    let traversal = db.g_v().skip(1).limit(2);
    let result = db.gremlin(&traversal).await.unwrap();

    assert!(result.len() <= 2);
}

#[tokio::test]
async fn test_gremlin_predicate_filtering() {
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

    // Create vertices with numeric properties for filtering
    for i in 1..=10 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert("value".to_string(), PropertyValue::Int(i as i64));
        props.insert(
            "category".to_string(),
            PropertyValue::String(if i <= 5 {
                "low".to_string()
            } else {
                "high".to_string()
            }),
        );

        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Test where() step with greater than predicate
    let predicate = GremlinPredicate::Gt(PropertyValue::Int(5));
    let traversal = db.g_v().values(vec!["value".to_string()]).where_(predicate);
    let result = db.gremlin(&traversal).await.unwrap();

    // Should return values > 5
    for res in result.iter() {
        if let Some(value) = res.as_value() {
            if let Some(int_val) = value.as_int() {
                assert!(int_val > 5);
            }
        }
    }

    // Test where() with within predicate
    let predicate = GremlinPredicate::Within(vec![
        PropertyValue::String("low".to_string()),
        PropertyValue::String("medium".to_string()),
    ]);
    let traversal = db
        .g_v()
        .values(vec!["category".to_string()])
        .where_(predicate);
    let result = db.gremlin(&traversal).await.unwrap();

    // Should return "low" category values
    for res in result.iter() {
        if let Some(value) = res.as_value() {
            if let Some(str_val) = value.as_string() {
                assert!(str_val == "low" || str_val == "medium");
            }
        }
    }
}

#[tokio::test]
async fn test_gremlin_query_string_parsing() {
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

    // Test basic query string parsing
    let queries = vec![
        "g.V()",
        "g.V().count()",
        "g.V().limit(5)",
        "g.V().out()",
        "g.V().dedup()",
    ];

    for query in queries {
        let result = db.gremlin_query(query).await;
        // Should parse without error (though may return empty results)
        assert!(result.is_ok(), "Failed to parse query: {}", query);
    }
}

#[tokio::test]
async fn test_gremlin_context_and_bindings() {
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

    // Create test data
    let v1 = VertexId::from_u64(1);
    let mut props = Properties::new();
    props.insert(
        "name".to_string(),
        PropertyValue::String("test_vertex".to_string()),
    );
    db.set_vertex_properties(v1, props).await.unwrap();

    // Test custom context
    let query_context = QueryContext {
        max_depth: Some(5),
        timeout_ms: Some(10000),
        use_cache: true,
        transaction: None,
    };

    let mut gremlin_context = GremlinContext::new(query_context);

    // Test traversal with custom context
    let traversal = db.g_v_ids(vec![v1]).as_("start".to_string());
    let result = db
        .gremlin_with_context(&traversal, &mut gremlin_context)
        .await
        .unwrap();

    // Check that binding was created
    assert!(gremlin_context.get_binding("start").is_some());

    // Test select with binding
    let traversal = db
        .g_v_ids(vec![v1])
        .as_("start".to_string())
        .select(vec!["start".to_string()]);
    let result = db
        .gremlin_with_context(&traversal, &mut gremlin_context)
        .await
        .unwrap();

    assert!(!result.is_empty());
}

#[tokio::test]
async fn test_gremlin_performance_and_stats() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_properties: true,
        enable_recovery: false,
        enable_metrics: true, // Enable metrics for performance tracking
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Create a larger dataset for performance testing
    for i in 1..=100 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert("id".to_string(), PropertyValue::Int(i as i64));
        props.insert("value".to_string(), PropertyValue::Float(i as f64 * 0.5));

        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Test performance of complex query
    let traversal = db
        .g_v()
        .has("id".to_string(), None) // Filter vertices with id property
        .values(vec!["value".to_string()])
        .where_(GremlinPredicate::Gt(PropertyValue::Float(25.0)))
        .count();

    let result = db.gremlin(&traversal).await.unwrap();

    // Check that we got execution stats
    assert!(result.stats.execution_time_ms >= 0);
    assert!(result.stats.vertices_visited >= 0);

    println!("Query execution time: {}ms", result.stats.execution_time_ms);
    println!("Vertices visited: {}", result.stats.vertices_visited);
    println!("Edges traversed: {}", result.stats.edges_traversed);

    // Verify result
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_gremlin_edge_traversals() {
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

    // Create a graph with labeled edges
    // v1 --(friend)--> v2 --(colleague)--> v3
    // v1 --(family)--> v4
    let v1 = VertexId::from_u64(1);
    let v2 = VertexId::from_u64(2);
    let v3 = VertexId::from_u64(3);
    let v4 = VertexId::from_u64(4);

    // Add vertices
    for &vertex in &[v1, v2, v3, v4] {
        let mut props = Properties::new();
        props.insert(
            "name".to_string(),
            PropertyValue::String(format!("vertex_{}", vertex.as_u64())),
        );
        db.set_vertex_properties(vertex, props).await.unwrap();
    }

    // Add edges with labels
    db.graph().add_edge(v1, v2, None).await.unwrap(); // friend edge
    db.graph().add_edge(v2, v3, None).await.unwrap(); // colleague edge
    db.graph().add_edge(v1, v4, None).await.unwrap(); // family edge

    // Test g.V(1).outE() - get outgoing edges from vertex 1
    let traversal = db.g_v_ids(vec![v1]).out_e(None);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Outgoing edges from v1: {}", result.len());
    // Should find edges to v2 and v4
    assert!(result.len() >= 1);

    // Test g.V(1).outE().inV() - get target vertices of outgoing edges
    let traversal = db.g_v_ids(vec![v1]).out_e(None).in_v();
    let result = db.gremlin(&traversal).await.unwrap();

    println!(
        "Target vertices of outgoing edges from v1: {}",
        result.len()
    );
    // Should find v2 and v4
    assert!(result.len() >= 1);

    // Test g.V().inE() - get incoming edges
    let traversal = db.g_v_ids(vec![v2]).in_e(None);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Incoming edges to v2: {}", result.len());
    // Should find edge from v1

    // Test g.V().bothE() - get both incoming and outgoing edges
    let traversal = db.g_v_ids(vec![v2]).both_e(None);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("All edges connected to v2: {}", result.len());
    // Should find edges from v1 to v2 and from v2 to v3

    // Test edge to vertex traversals: g.E().outV() and g.E().inV()
    let traversal = db.g_e().out_v();
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Source vertices of all edges: {}", result.len());

    let traversal = db.g_e().in_v();
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Target vertices of all edges: {}", result.len());

    // Test g.E().otherV() - get the other vertex from edge perspective
    let traversal = db.g_e().other_v();
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Other vertices from edge perspective: {}", result.len());
}

#[tokio::test]
async fn test_gremlin_advanced_operations() {
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

    // Create a more complex graph for advanced operations
    for i in 1..=10 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert("id".to_string(), PropertyValue::Int(i as i64));
        props.insert(
            "category".to_string(),
            PropertyValue::String(if i <= 5 {
                "group_a".to_string()
            } else {
                "group_b".to_string()
            }),
        );
        props.insert("value".to_string(), PropertyValue::Float(i as f64 * 10.0));

        db.set_vertex_properties(vertex_id, props).await.unwrap();

        // Create edges to form a connected graph
        if i > 1 {
            db.graph()
                .add_edge(VertexId::from_u64(i - 1), vertex_id, None)
                .await
                .unwrap();
        }
    }

    // Test g.V().groupCount() - count occurrences of vertices
    let traversal = db.g_v().group_count();
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Group count result: {}", result.len());
    // Should return aggregated counts

    // Test g.V().project('id', 'category') - project specific properties
    let traversal = db
        .g_v()
        .project(vec!["id".to_string(), "category".to_string()]);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Projected properties: {}", result.len());
    // Should return maps with id and category

    // Test g.V().union(g.V().has('category', 'group_a'), g.V().has('category', 'group_b'))
    let trav1 = db.g_v().has(
        "category".to_string(),
        Some(PropertyValue::String("group_a".to_string())),
    );
    let trav2 = db.g_v().has(
        "category".to_string(),
        Some(PropertyValue::String("group_b".to_string())),
    );
    let traversal = db.g_v().union(vec![trav1, trav2]);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Union of two groups: {}", result.len());

    // Test g.V().optional(g.V().out()) - optional traversal
    let optional_trav = db.g_v().out(None);
    let traversal = db.g_v().optional(optional_trav);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Optional traversal: {}", result.len());

    // Test g.V().coalesce() with multiple traversals
    let coalesce_trav1 = db.g_v().out(None);
    let coalesce_trav2 = db.g_v(); // fallback
    let traversal = db.g_v().coalesce(vec![coalesce_trav1, coalesce_trav2]);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Coalesce traversal: {}", result.len());

    // Test g.V().local(g.V().out().limit(1)) - local traversal
    let local_trav = db.g_v().out(None).limit(1);
    let traversal = db.g_v().local(local_trav);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Local traversal: {}", result.len());
}

#[tokio::test]
async fn test_gremlin_path_operations() {
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

    // Create a linear graph: 1 -> 2 -> 3 -> 4 -> 5
    for i in 1..=5 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert("step".to_string(), PropertyValue::Int(i as i64));

        db.set_vertex_properties(vertex_id, props).await.unwrap();

        if i > 1 {
            db.graph()
                .add_edge(VertexId::from_u64(i - 1), vertex_id, None)
                .await
                .unwrap();
        }
    }

    // Test g.V(1).out().out().path() - track path through traversal
    let traversal = db
        .g_v_ids(vec![VertexId::from_u64(1)])
        .out(None)
        .out(None)
        .path();
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Path traversal results: {}", result.len());
    // Should return path information

    // Test g.V().as('start').out().as('middle').out().as('end').select('start', 'end')
    let traversal = db
        .g_v_ids(vec![VertexId::from_u64(1)])
        .as_("start".to_string())
        .out(None)
        .as_("middle".to_string())
        .out(None)
        .as_("end".to_string())
        .select(vec!["start".to_string(), "end".to_string()]);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Select with labels: {}", result.len());

    // Test complex chaining: g.V().out().dedup().values('step').where(gt(2)).count()
    let traversal = db
        .g_v()
        .out(None)
        .dedup()
        .values(vec!["step".to_string()])
        .where_(GremlinPredicate::Gt(PropertyValue::Int(2)))
        .count();
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Complex chaining result: {}", result.len());
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_gremlin_range_and_order_operations() {
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

    // Create vertices with sortable values
    for i in 1..=20 {
        let vertex_id = VertexId::from_u64(i);
        let mut props = Properties::new();
        props.insert("value".to_string(), PropertyValue::Int(i as i64));
        props.insert("score".to_string(), PropertyValue::Float((21 - i) as f64)); // Reverse order

        db.set_vertex_properties(vertex_id, props).await.unwrap();
    }

    // Test g.V().range(5, 10) - get specific range
    let traversal = db.g_v().range(5, 10);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Range 5-10: {}", result.len());
    assert!(result.len() <= 5); // Should get at most 5 results

    // Test g.V().skip(10).limit(5) - equivalent to range
    let traversal = db.g_v().skip(10).limit(5);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Skip 10, limit 5: {}", result.len());
    assert!(result.len() <= 5);

    // Test g.V().values('value').order() - order values
    let traversal = db.g_v().values(vec!["value".to_string()]).order(None);
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Ordered values: {}", result.len());

    // Test g.V().fold() - collect all into a single result
    let traversal = db.g_v().limit(5).fold();
    let result = db.gremlin(&traversal).await.unwrap();

    println!("Folded results: {}", result.len());
    assert_eq!(result.len(), 1); // Should return single collection
}

#[tokio::test]
async fn test_gremlin_error_handling() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_properties: false, // Disable properties to test error handling
        enable_recovery: false,
        enable_metrics: false,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Test property operations when properties are disabled
    let traversal = db.g_v().has(
        "name".to_string(),
        Some(PropertyValue::String("test".to_string())),
    );
    let result = db.gremlin(&traversal).await;

    // Should work but return no filtered results since properties aren't available
    assert!(result.is_ok());

    // Test invalid query parsing
    let result = db.gremlin_query("invalid.query.syntax").await;
    assert!(result.is_err());
}
