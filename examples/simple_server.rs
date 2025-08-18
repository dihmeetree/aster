//! Simple AsterDB HTTP Server Example

use aster_db::server::{AsterServer, ServerConfig};
use aster_db::AsterDBConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 8080,
        data_dir: "./example-data".to_string(),
        db_config: AsterDBConfig {
            enable_recovery: false, // Simplified for example
            enable_metrics: true,
            enable_properties: true,
            ..Default::default()
        },
        enable_cors: true,
        request_timeout_secs: 30,
    };

    let server = AsterServer::new(config).await?;
    println!("Starting AsterDB server on http://127.0.0.1:8080");
    println!("Try: curl -X POST http://127.0.0.1:8080/query -H 'Content-Type: application/json' -d '{{\"query\":\"g.V().count()\"}}'");

    server.serve().await?;
    Ok(())
}
