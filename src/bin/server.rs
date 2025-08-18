//! AsterDB HTTP Server Binary
//!
//! A standalone HTTP server for AsterDB that provides REST API access
//! to the graph database functionality.

use aster_db::server::{AsterServer, ServerConfig};
use aster_db::{AsterDBConfig, MetricsConfig, RecoveryConfig, TransactionConfig};
use clap::{Arg, Command};
use std::path::PathBuf;
use tracing::{error, info};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let matches = Command::new("aster-server")
        .version("0.1.0")
        .author("Aster Team")
        .about("AsterDB HTTP Server - Graph Database REST API")
        .arg(
            Arg::new("host")
                .long("host")
                .value_name("HOST")
                .help("Host to bind to")
                .default_value("127.0.0.1"),
        )
        .arg(
            Arg::new("port")
                .long("port")
                .short('p')
                .value_name("PORT")
                .help("Port to bind to")
                .default_value("8080"),
        )
        .arg(
            Arg::new("data-dir")
                .long("data-dir")
                .short('d')
                .value_name("DIR")
                .help("Database data directory")
                .default_value("./aster-data"),
        )
        .arg(
            Arg::new("disable-recovery")
                .long("disable-recovery")
                .help("Disable database recovery features")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("disable-metrics")
                .long("disable-metrics")
                .help("Disable metrics collection")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("disable-properties")
                .long("disable-properties")
                .help("Disable property storage")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("disable-cors")
                .long("disable-cors")
                .help("Disable CORS for web clients")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .value_name("SECONDS")
                .help("Request timeout in seconds")
                .default_value("30"),
        )
        .arg(
            Arg::new("log-level")
                .long("log-level")
                .value_name("LEVEL")
                .help("Log level (trace, debug, info, warn, error)")
                .default_value("info"),
        )
        .get_matches();

    // Parse configuration from command line arguments
    let host = matches.get_one::<String>("host").unwrap().clone();
    let port = matches
        .get_one::<String>("port")
        .unwrap()
        .parse::<u16>()
        .unwrap_or_else(|_| {
            eprintln!("Invalid port number");
            std::process::exit(1);
        });

    let data_dir = matches.get_one::<String>("data-dir").unwrap().clone();
    let disable_recovery = matches.get_flag("disable-recovery");
    let disable_metrics = matches.get_flag("disable-metrics");
    let disable_properties = matches.get_flag("disable-properties");
    let disable_cors = matches.get_flag("disable-cors");
    let timeout = matches
        .get_one::<String>("timeout")
        .unwrap()
        .parse::<u64>()
        .unwrap_or_else(|_| {
            eprintln!("Invalid timeout value");
            std::process::exit(1);
        });

    // Ensure data directory exists
    let data_path = PathBuf::from(&data_dir);
    if !data_path.exists() {
        if let Err(e) = std::fs::create_dir_all(&data_path) {
            eprintln!("Failed to create data directory: {}", e);
            std::process::exit(1);
        }
        info!(data_dir = %data_dir, "Created data directory");
    }

    // Build database configuration
    let db_config = AsterDBConfig {
        enable_recovery: !disable_recovery,
        recovery_config: RecoveryConfig::default(),
        transaction_config: TransactionConfig::default(),
        enable_metrics: !disable_metrics,
        metrics_config: MetricsConfig::default(),
        enable_properties: !disable_properties,
        property_store_config: aster_db::PropertyStoreConfig::default(),
    };

    // Build server configuration
    let server_config = ServerConfig {
        host,
        port,
        db_config,
        data_dir,
        enable_cors: !disable_cors,
        request_timeout_secs: timeout,
    };

    info!(
        config = ?server_config,
        "Starting AsterDB server with configuration"
    );

    // Create and start the server
    let server = match AsterServer::new(server_config).await {
        Ok(server) => server,
        Err(e) => {
            error!(error = %e, "Failed to initialize AsterDB server");
            return Err(Box::new(e) as Box<dyn std::error::Error>);
        }
    };

    info!("AsterDB server initialized successfully");

    // Start serving requests
    if let Err(e) = server.serve().await {
        eprintln!("Server error: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error>);
    }

    Ok(())
}
