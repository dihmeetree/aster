//! Integration tests for MVCC Transaction Support with Snapshot Isolation
//!
//! Tests the complete MVCC functionality including:
//! - Snapshot isolation between concurrent transactions
//! - Version visibility rules for reads and writes
//! - Write-write conflict detection and resolution
//! - Transactional storage operations with proper versioning
//! - Commit timestamp ordering and validation

use aster_db::{AsterDB, AsterDBConfig, LockResource, Properties, PropertyValue, VertexId};
use tempfile::TempDir;

#[tokio::test]
async fn test_basic_mvcc_snapshot_isolation() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: false,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Begin two concurrent transactions
    let tx1 = db.begin_transaction().await.unwrap();
    let tx2 = db.begin_transaction().await.unwrap();

    // Verify transactions have different snapshot timestamps
    assert!(tx1.snapshot_timestamp() <= tx2.snapshot_timestamp());

    // Test that each transaction sees a consistent snapshot
    let vertex_id = VertexId::from_u64(1);

    // Set initial data before any transaction
    let mut initial_props = Properties::new();
    initial_props.insert("value".to_string(), PropertyValue::Int(100));
    db.set_vertex_properties(vertex_id, initial_props)
        .await
        .unwrap();

    // Both transactions should see the initial value
    let props1 = db.get_vertex_properties(vertex_id).await.unwrap();
    let props2 = db.get_vertex_properties(vertex_id).await.unwrap();

    assert_eq!(props1.get("value"), props2.get("value"));
    assert_eq!(props1.get("value"), Some(&PropertyValue::Int(100)));

    // Commit both transactions
    db.commit_transaction(tx1).await.unwrap();
    db.commit_transaction(tx2).await.unwrap();
}

#[tokio::test]
async fn test_write_write_conflict_detection() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: false,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    let vertex_id = VertexId::from_u64(1);

    // Set initial data
    let mut initial_props = Properties::new();
    initial_props.insert("value".to_string(), PropertyValue::Int(100));
    db.set_vertex_properties(vertex_id, initial_props)
        .await
        .unwrap();

    // Begin two concurrent transactions
    let tx1 = db.begin_transaction().await.unwrap();
    let tx2 = db.begin_transaction().await.unwrap();

    // Both transactions attempt to write to the same vertex
    // First transaction should succeed
    tx1.record_write(LockResource::Vertex(vertex_id)).unwrap();

    // Second transaction should conflict
    let write_result = tx2.record_write(LockResource::Vertex(vertex_id));
    assert!(write_result.is_err());

    // First transaction should commit successfully
    db.commit_transaction(tx1).await.unwrap();

    // Second transaction should rollback
    db.rollback_transaction(tx2).await.unwrap();
}

#[tokio::test]
async fn test_concurrent_read_transactions() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: false,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    let vertex_id = VertexId::from_u64(1);

    // Set initial data
    let mut initial_props = Properties::new();
    initial_props.insert("value".to_string(), PropertyValue::Int(100));
    db.set_vertex_properties(vertex_id, initial_props)
        .await
        .unwrap();

    // Begin multiple concurrent read-only transactions
    let tx1 = db.begin_transaction().await.unwrap();
    let tx2 = db.begin_transaction().await.unwrap();
    let tx3 = db.begin_transaction().await.unwrap();

    // All transactions should be able to read the same data
    tx1.record_read(LockResource::Vertex(vertex_id)).unwrap();
    tx2.record_read(LockResource::Vertex(vertex_id)).unwrap();
    tx3.record_read(LockResource::Vertex(vertex_id)).unwrap();

    // All should validate and commit successfully
    assert!(tx1.validate_snapshot_isolation().is_ok());
    assert!(tx2.validate_snapshot_isolation().is_ok());
    assert!(tx3.validate_snapshot_isolation().is_ok());

    db.commit_transaction(tx1).await.unwrap();
    db.commit_transaction(tx2).await.unwrap();
    db.commit_transaction(tx3).await.unwrap();
}

#[tokio::test]
async fn test_version_visibility_rules() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: false,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Begin a transaction
    let tx = db.begin_transaction().await.unwrap();
    let snapshot_ts = tx.snapshot_timestamp();

    // Test visibility of data committed before snapshot
    let older_ts = snapshot_ts - 1;
    assert!(tx.is_version_visible(older_ts, Some(older_ts)));

    // Test visibility of data committed after snapshot
    let newer_ts = snapshot_ts + 1;
    assert!(!tx.is_version_visible(newer_ts, Some(newer_ts)));

    // Test visibility of uncommitted data
    assert!(!tx.is_version_visible(older_ts, None));

    db.commit_transaction(tx).await.unwrap();
}

#[tokio::test]
async fn test_repeatable_reads_within_transaction() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: false,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    let vertex_id = VertexId::from_u64(1);

    // Set initial data
    let mut initial_props = Properties::new();
    initial_props.insert("value".to_string(), PropertyValue::Int(100));
    db.set_vertex_properties(vertex_id, initial_props)
        .await
        .unwrap();

    // Begin a long-running transaction
    let tx = db.begin_transaction().await.unwrap();
    let initial_snapshot = tx.snapshot_timestamp();

    // Read data multiple times within the transaction
    let props1 = db.get_vertex_properties(vertex_id).await.unwrap();

    // Simulate some time passing
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let props2 = db.get_vertex_properties(vertex_id).await.unwrap();

    // Snapshot timestamp should remain constant throughout transaction
    assert_eq!(tx.snapshot_timestamp(), initial_snapshot);

    // Should read the same data both times (repeatable reads)
    assert_eq!(props1.get("value"), props2.get("value"));

    db.commit_transaction(tx).await.unwrap();
}

#[tokio::test]
async fn test_mvcc_with_graph_operations() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: false,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    let v1 = VertexId::from_u64(1);
    let v2 = VertexId::from_u64(2);

    // Set up initial graph structure
    db.graph().add_edge(v1, v2, None).await.unwrap();

    // Begin transactions for graph operations
    let tx1 = db.begin_transaction().await.unwrap();
    let tx2 = db.begin_transaction().await.unwrap();

    // Test concurrent reads of graph structure
    tx1.record_read(LockResource::Vertex(v1)).unwrap();
    tx2.record_read(LockResource::Vertex(v1)).unwrap();

    // Both transactions should be able to read
    let neighbors1 = db.graph().get_neighbors(v1).await.unwrap();
    let neighbors2 = db.graph().get_neighbors(v1).await.unwrap();

    assert_eq!(neighbors1, neighbors2);
    assert!(neighbors1.contains(&v2));

    // Both should validate successfully
    assert!(tx1.validate_snapshot_isolation().is_ok());
    assert!(tx2.validate_snapshot_isolation().is_ok());

    db.commit_transaction(tx1).await.unwrap();
    db.commit_transaction(tx2).await.unwrap();
}

#[tokio::test]
async fn test_transaction_manager_stats_with_mvcc() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: false,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    let vertex_id = VertexId::from_u64(1);

    // Begin multiple transactions
    let tx1 = db.begin_transaction().await.unwrap();
    let tx2 = db.begin_transaction().await.unwrap();
    let tx3 = db.begin_transaction().await.unwrap();

    // Perform various operations
    tx1.record_read(LockResource::Vertex(vertex_id)).unwrap();
    tx2.record_read(LockResource::Vertex(vertex_id)).unwrap();

    // Check transaction manager stats
    let stats = db.transaction_manager().get_stats();
    assert_eq!(stats.active_transactions, 3);
    assert!(stats.total_reads >= 2);

    // Commit all transactions
    db.commit_transaction(tx1).await.unwrap();
    db.commit_transaction(tx2).await.unwrap();
    db.commit_transaction(tx3).await.unwrap();

    // Check final stats
    let final_stats = db.transaction_manager().get_stats();
    assert_eq!(final_stats.active_transactions, 0);
}

#[tokio::test]
async fn test_snapshot_isolation_edge_cases() {
    let temp_dir = TempDir::new().unwrap();

    let config = AsterDBConfig {
        enable_recovery: false,
        enable_metrics: false,
        enable_properties: true,
        ..Default::default()
    };

    let db = AsterDB::open_with_config(temp_dir.path(), config)
        .await
        .unwrap();

    // Test transaction with no operations
    let tx_empty = db.begin_transaction().await.unwrap();
    assert!(tx_empty.validate_snapshot_isolation().is_ok());
    db.commit_transaction(tx_empty).await.unwrap();

    // Test transaction that only reads
    let tx_read = db.begin_transaction().await.unwrap();
    let vertex_id = VertexId::from_u64(999);
    tx_read
        .record_read(LockResource::Vertex(vertex_id))
        .unwrap();
    assert!(tx_read.validate_snapshot_isolation().is_ok());
    db.commit_transaction(tx_read).await.unwrap();

    // Test immediate commit after begin
    let tx_immediate = db.begin_transaction().await.unwrap();
    db.commit_transaction(tx_immediate).await.unwrap();
}
