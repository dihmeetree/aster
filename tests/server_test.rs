//! Basic server functionality test

use aster_db::server::{AsterServer, ServerConfig};
use tempfile::TempDir;

#[tokio::test]
async fn test_server_creation() {
    let temp_dir = TempDir::new().unwrap();

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0, // Use port 0 to get any available port
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        db_config: aster_db::AsterDBConfig {
            enable_recovery: false,
            enable_metrics: false,
            ..Default::default()
        },
        enable_cors: true,
        request_timeout_secs: 30,
    };

    // Just test that we can create a server instance
    let server = AsterServer::new(config).await.unwrap();
    let _router = server.create_router();

    // Basic test that the server can be created
    assert!(true);
}
