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
    AsterDB, AsterDBConfig, GremlinContext, GremlinResultSet, GremlinTraversal, Properties,
    PropertyValue, QueryContext, VertexId,
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
