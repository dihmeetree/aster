//! HTTP Server for AsterDB
//!
//! Provides a REST API for interacting with the AsterDB graph database:
//! - Gremlin query execution
//! - Database metrics and health
//! - Transaction management
//! - Administrative operations

use crate::{AsterDB, AsterDBConfig, AsterError, GremlinResultSet, Result};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::Json as ResponseJson,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, warn};

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Host to bind to
    pub host: String,
    /// Port to bind to
    pub port: u16,
    /// Database configuration
    pub db_config: AsterDBConfig,
    /// Database data directory
    pub data_dir: String,
    /// Enable CORS for web clients
    pub enable_cors: bool,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            db_config: AsterDBConfig::default(),
            data_dir: "./data".to_string(),
            enable_cors: true,
            request_timeout_secs: 30,
        }
    }
}

/// Server application state
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<AsterDB>,
    pub config: ServerConfig,
}

/// Request body for Gremlin queries
#[derive(Debug, Deserialize)]
pub struct GremlinQueryRequest {
    /// The Gremlin query string
    pub query: String,
    /// Optional query bindings/parameters
    pub bindings: Option<HashMap<String, serde_json::Value>>,
    /// Optional timeout in milliseconds
    pub timeout_ms: Option<u64>,
}

/// Response for successful Gremlin queries
#[derive(Debug, Serialize)]
pub struct GremlinQueryResponse {
    /// Query results
    pub results: Vec<serde_json::Value>,
    /// Query execution statistics
    pub stats: QueryStatsResponse,
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
}

/// Query statistics in the response
#[derive(Debug, Serialize)]
pub struct QueryStatsResponse {
    pub vertices_visited: usize,
    pub edges_traversed: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub database_status: String,
    pub uptime_seconds: u64,
    pub active_connections: u64,
}

/// Error response format
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub error_type: String,
    pub details: Option<String>,
}

impl From<AsterError> for ErrorResponse {
    fn from(err: AsterError) -> Self {
        let error_type = match &err {
            AsterError::Storage { .. } => "StorageError",
            AsterError::Transaction { .. } => "TransactionError",
            AsterError::Internal { .. } => "InternalError",
            AsterError::InvalidOperation { .. } => "InvalidOperationError",
            AsterError::Configuration { .. } => "ConfigurationError",
            AsterError::Conflict { .. } => "ConflictError",
            AsterError::Recovery { .. } => "RecoveryError",
            AsterError::Corruption { .. } => "CorruptionError",
            AsterError::Timeout { .. } => "TimeoutError",
            AsterError::VertexNotFound { .. } => "VertexNotFoundError",
            AsterError::EdgeNotFound { .. } => "EdgeNotFoundError",
            AsterError::Serialization(_) => "SerializationError",
            AsterError::Json(_) => "JsonError",
            AsterError::Io(_) => "IoError",
        };

        Self {
            error: err.to_string(),
            error_type: error_type.to_string(),
            details: None,
        }
    }
}

/// Main AsterDB HTTP server
pub struct AsterServer {
    config: ServerConfig,
    app_state: AppState,
}

impl AsterServer {
    /// Create a new server instance
    pub async fn new(config: ServerConfig) -> Result<Self> {
        info!(
            host = %config.host,
            port = config.port,
            data_dir = %config.data_dir,
            "Initializing AsterDB server"
        );

        // Initialize the database
        let db =
            Arc::new(AsterDB::open_with_config(&config.data_dir, config.db_config.clone()).await?);

        let app_state = AppState {
            db,
            config: config.clone(),
        };

        Ok(Self { config, app_state })
    }

    /// Build the Axum router with all routes
    pub fn create_router(&self) -> Router {
        let mut router = Router::new()
            // Health and status endpoints
            .route("/health", get(health_check))
            .route("/status", get(database_status))
            .route("/metrics", get(get_metrics))
            // Query endpoint
            .route("/query", post(execute_gremlin_query))
            // Administrative endpoints
            .route("/admin/checkpoint", post(create_checkpoint))
            .route("/admin/cleanup", post(cleanup_logs))
            // Property endpoints (if enabled)
            .route("/vertex/:id/properties", get(get_vertex_properties))
            .route("/vertex/:id/properties", post(set_vertex_properties))
            .route("/edge/:id/properties", get(get_edge_properties))
            .route("/edge/:id/properties", post(set_edge_properties))
            .with_state(self.app_state.clone());

        // Add middleware
        let service = ServiceBuilder::new().layer(TraceLayer::new_for_http());

        if self.config.enable_cors {
            router = router.layer(CorsLayer::permissive());
        }

        router.layer(service.into_inner())
    }

    /// Start the server
    pub async fn serve(self) -> Result<()> {
        let app = self.create_router();
        let addr = format!("{}:{}", self.config.host, self.config.port);

        info!(address = %addr, "Starting AsterDB HTTP server");

        let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| {
            AsterError::Io(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                format!("Failed to bind to {}: {}", addr, e),
            ))
        })?;

        info!(address = %addr, "AsterDB server listening");

        axum::serve(listener, app).await.map_err(|e| {
            AsterError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Server error: {}", e),
            ))
        })
    }
}

// Handler functions

/// Health check endpoint
async fn health_check(
    State(state): State<AppState>,
) -> std::result::Result<ResponseJson<HealthResponse>, StatusCode> {
    let health = HealthResponse {
        status: "healthy".to_string(),
        database_status: "active".to_string(),
        uptime_seconds: 0,     // TODO: Track actual uptime
        active_connections: 1, // TODO: Track actual connections
    };

    state.db.record_read_operation(0.0); // Record this as a read operation
    Ok(ResponseJson(health))
}

/// Database status endpoint
async fn database_status(
    State(state): State<AppState>,
) -> std::result::Result<ResponseJson<serde_json::Value>, StatusCode> {
    match state.db.get_metrics().await {
        Some(metrics) => Ok(ResponseJson(
            serde_json::to_value(metrics).unwrap_or_default(),
        )),
        None => {
            let basic_status = serde_json::json!({
                "status": "active",
                "properties_enabled": state.db.properties_enabled(),
                "recovery_enabled": state.db.recovery_enabled()
            });
            Ok(ResponseJson(basic_status))
        }
    }
}

/// Get database metrics
async fn get_metrics(
    State(state): State<AppState>,
) -> std::result::Result<ResponseJson<serde_json::Value>, (StatusCode, ResponseJson<ErrorResponse>)>
{
    match state.db.export_prometheus_metrics() {
        Some(metrics) => {
            let response = serde_json::json!({
                "prometheus": metrics,
                "database_metrics": state.db.get_metrics().await
            });
            Ok(ResponseJson(response))
        }
        None => {
            let error = ErrorResponse {
                error: "Metrics not available".to_string(),
                error_type: "ConfigurationError".to_string(),
                details: Some("Metrics collection is not enabled".to_string()),
            };
            Err((StatusCode::SERVICE_UNAVAILABLE, ResponseJson(error)))
        }
    }
}

/// Execute a Gremlin query
async fn execute_gremlin_query(
    State(state): State<AppState>,
    Json(request): Json<GremlinQueryRequest>,
) -> std::result::Result<ResponseJson<serde_json::Value>, (StatusCode, ResponseJson<ErrorResponse>)>
{
    let start_time = std::time::Instant::now();

    let query = request.query.clone();
    info!(query = %query, "Executing Gremlin query");

    // Execute the query in a spawn_blocking to avoid Send issues
    let db = Arc::clone(&state.db);
    let result = match tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(db.gremlin_query(&query))
    })
    .await
    {
        Ok(query_result) => query_result,
        Err(join_error) => {
            warn!(error = %join_error, "Task join error");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                ResponseJson(ErrorResponse {
                    error: "Internal server error".to_string(),
                    error_type: "InternalError".to_string(),
                    details: Some(format!("Task join error: {}", join_error)),
                }),
            ));
        }
    };

    let execution_time = start_time.elapsed();
    state
        .db
        .record_query_operation(execution_time.as_millis() as f64);

    match result {
        Ok(result_set) => {
            let response = serde_json::json!({
                "results": convert_gremlin_results(&result_set),
                "stats": {
                    "vertices_visited": result_set.stats.vertices_visited,
                    "edges_traversed": result_set.stats.edges_traversed,
                    "cache_hits": result_set.stats.cache_hits,
                    "cache_misses": result_set.stats.cache_misses,
                },
                "execution_time_ms": execution_time.as_millis() as u64,
            });
            Ok(ResponseJson(response))
        }
        Err(err) => {
            warn!(error = %err, query = %request.query, "Gremlin query failed");
            state.db.record_error();
            Err((
                StatusCode::BAD_REQUEST,
                ResponseJson(ErrorResponse::from(err)),
            ))
        }
    }
}

/// Get vertex properties
async fn get_vertex_properties(
    State(state): State<AppState>,
    Path(vertex_id): Path<u64>,
) -> std::result::Result<ResponseJson<serde_json::Value>, (StatusCode, ResponseJson<ErrorResponse>)>
{
    let vertex_id = crate::VertexId::from_u64(vertex_id);

    match state.db.get_vertex_properties(vertex_id).await {
        Ok(properties) => {
            let json_properties: HashMap<String, serde_json::Value> = properties
                .into_iter()
                .map(|(k, v)| (k, property_value_to_json(v)))
                .collect();
            Ok(ResponseJson(
                serde_json::to_value(json_properties).unwrap_or_default(),
            ))
        }
        Err(err) => {
            let error = ErrorResponse::from(err);
            let status = if error.error_type == "ConfigurationError" {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::NOT_FOUND
            };
            Err((status, ResponseJson(error)))
        }
    }
}

/// Set vertex properties
async fn set_vertex_properties(
    State(state): State<AppState>,
    Path(vertex_id): Path<u64>,
    Json(properties): Json<HashMap<String, serde_json::Value>>,
) -> std::result::Result<ResponseJson<serde_json::Value>, (StatusCode, ResponseJson<ErrorResponse>)>
{
    let vertex_id = crate::VertexId::from_u64(vertex_id);

    // Convert JSON values to PropertyValues
    let mut prop_map = crate::Properties::new();
    for (key, value) in properties {
        if let Some(prop_value) = json_to_property_value(value) {
            prop_map.insert(key, prop_value);
        }
    }

    match state.db.set_vertex_properties(vertex_id, prop_map).await {
        Ok(_) => Ok(ResponseJson(serde_json::json!({"status": "success"}))),
        Err(err) => {
            let error = ErrorResponse::from(err);
            let status = if error.error_type == "ConfigurationError" {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((status, ResponseJson(error)))
        }
    }
}

/// Get edge properties
async fn get_edge_properties(
    State(state): State<AppState>,
    Path(edge_id): Path<u64>,
) -> std::result::Result<ResponseJson<serde_json::Value>, (StatusCode, ResponseJson<ErrorResponse>)>
{
    let edge_id = crate::EdgeId::from_u64(edge_id);

    match state.db.get_edge_properties(edge_id).await {
        Ok(properties) => {
            let json_properties: HashMap<String, serde_json::Value> = properties
                .into_iter()
                .map(|(k, v)| (k, property_value_to_json(v)))
                .collect();
            Ok(ResponseJson(
                serde_json::to_value(json_properties).unwrap_or_default(),
            ))
        }
        Err(err) => {
            let error = ErrorResponse::from(err);
            let status = if error.error_type == "ConfigurationError" {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::NOT_FOUND
            };
            Err((status, ResponseJson(error)))
        }
    }
}

/// Set edge properties
async fn set_edge_properties(
    State(state): State<AppState>,
    Path(edge_id): Path<u64>,
    Json(properties): Json<HashMap<String, serde_json::Value>>,
) -> std::result::Result<ResponseJson<serde_json::Value>, (StatusCode, ResponseJson<ErrorResponse>)>
{
    let edge_id = crate::EdgeId::from_u64(edge_id);

    // Convert JSON values to PropertyValues
    let mut prop_map = crate::Properties::new();
    for (key, value) in properties {
        if let Some(prop_value) = json_to_property_value(value) {
            prop_map.insert(key, prop_value);
        }
    }

    match state.db.set_edge_properties(edge_id, prop_map).await {
        Ok(_) => Ok(ResponseJson(serde_json::json!({"status": "success"}))),
        Err(err) => {
            let error = ErrorResponse::from(err);
            let status = if error.error_type == "ConfigurationError" {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((status, ResponseJson(error)))
        }
    }
}

/// Create a checkpoint
async fn create_checkpoint(
    State(state): State<AppState>,
) -> std::result::Result<ResponseJson<serde_json::Value>, (StatusCode, ResponseJson<ErrorResponse>)>
{
    match state.db.create_checkpoint().await {
        Ok(checkpoint_id) => Ok(Json(serde_json::json!({"checkpoint_id": checkpoint_id}))),
        Err(err) => {
            let error = ErrorResponse::from(err);
            let status = if error.error_type == "ConfigurationError" {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            Err((status, ResponseJson(error)))
        }
    }
}

/// Cleanup old logs
async fn cleanup_logs(
    State(state): State<AppState>,
) -> std::result::Result<ResponseJson<serde_json::Value>, (StatusCode, ResponseJson<ErrorResponse>)>
{
    match state.db.cleanup_recovery_logs() {
        Ok(_) => Ok(ResponseJson(serde_json::json!({"status": "success"}))),
        Err(err) => {
            let error = ErrorResponse::from(err);
            let status = if error.error_type == "ConfigurationError" {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            Err((status, ResponseJson(error)))
        }
    }
}

// Helper functions

/// Convert GremlinResultSet to JSON values
fn convert_gremlin_results(result_set: &GremlinResultSet) -> Vec<serde_json::Value> {
    result_set
        .results
        .iter()
        .map(|result| {
            match result {
                crate::query::GremlinResult::Vertex(vertex_id) => {
                    serde_json::json!({"type": "vertex", "id": vertex_id.as_u64()})
                }
                crate::query::GremlinResult::Edge(edge_id) => {
                    serde_json::json!({"type": "edge", "id": edge_id.as_u64()})
                }
                crate::query::GremlinResult::PropertyMap(properties) => {
                    let json_properties: HashMap<String, serde_json::Value> = properties
                        .iter()
                        .map(|(k, v)| (k.clone(), property_value_to_json(v.clone())))
                        .collect();
                    serde_json::json!({"type": "properties", "value": json_properties})
                }
                crate::query::GremlinResult::Value(value) => {
                    property_value_to_json(value.clone())
                }
                crate::query::GremlinResult::Count(count) => {
                    serde_json::json!({"type": "count", "value": count})
                }
                crate::query::GremlinResult::Path(elements) => {
                    serde_json::json!({"type": "path", "elements": elements.iter().map(|e| gremlin_result_to_json(e)).collect::<Vec<_>>()})
                }
                crate::query::GremlinResult::Map(map) => {
                    let json_map: HashMap<String, serde_json::Value> = map
                        .iter()
                        .map(|(k, v)| (k.clone(), gremlin_result_to_json(v)))
                        .collect();
                    serde_json::json!({"type": "map", "value": json_map})
                }
                crate::query::GremlinResult::List(list) => {
                    serde_json::json!({"type": "list", "value": list.iter().map(|e| gremlin_result_to_json(e)).collect::<Vec<_>>()})
                }
                crate::query::GremlinResult::Null => {
                    serde_json::Value::Null
                }
                crate::query::GremlinResult::IncompleteEdge(_, _) => {
                    serde_json::json!({"type": "incomplete_edge", "error": "Incomplete edge result"})
                }
            }
        })
        .collect()
}

/// Convert a single GremlinResult to JSON
fn gremlin_result_to_json(result: &crate::query::GremlinResult) -> serde_json::Value {
    match result {
        crate::query::GremlinResult::Vertex(vertex_id) => {
            serde_json::json!({"type": "vertex", "id": vertex_id.as_u64()})
        }
        crate::query::GremlinResult::Edge(edge_id) => {
            serde_json::json!({"type": "edge", "id": edge_id.as_u64()})
        }
        crate::query::GremlinResult::PropertyMap(properties) => {
            let json_properties: HashMap<String, serde_json::Value> = properties
                .iter()
                .map(|(k, v)| (k.clone(), property_value_to_json(v.clone())))
                .collect();
            serde_json::json!({"type": "properties", "value": json_properties})
        }
        crate::query::GremlinResult::Value(value) => property_value_to_json(value.clone()),
        crate::query::GremlinResult::Count(count) => {
            serde_json::json!(count)
        }
        crate::query::GremlinResult::Path(elements) => {
            serde_json::json!(elements
                .iter()
                .map(|e| gremlin_result_to_json(e))
                .collect::<Vec<_>>())
        }
        crate::query::GremlinResult::Map(map) => {
            let json_map: HashMap<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), gremlin_result_to_json(v)))
                .collect();
            serde_json::to_value(json_map).unwrap_or_default()
        }
        crate::query::GremlinResult::List(list) => {
            serde_json::json!(list
                .iter()
                .map(|e| gremlin_result_to_json(e))
                .collect::<Vec<_>>())
        }
        crate::query::GremlinResult::Null => serde_json::Value::Null,
        crate::query::GremlinResult::IncompleteEdge(_, _) => {
            serde_json::json!({"type": "incomplete_edge", "error": "Incomplete edge result"})
        }
    }
}

/// Convert PropertyValue to JSON
fn property_value_to_json(value: crate::PropertyValue) -> serde_json::Value {
    match value {
        crate::PropertyValue::String(s) => serde_json::Value::String(s),
        crate::PropertyValue::Int(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        crate::PropertyValue::Float(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        crate::PropertyValue::Bool(b) => serde_json::Value::Bool(b),
        crate::PropertyValue::Null => serde_json::Value::Null,
        crate::PropertyValue::Bytes(bytes) => {
            // Convert bytes to base64 string for JSON serialization
            serde_json::json!({"type": "bytes", "data": base64_encode(&bytes)})
        }
        crate::PropertyValue::List(list) => {
            let json_list: Vec<serde_json::Value> =
                list.into_iter().map(property_value_to_json).collect();
            serde_json::Value::Array(json_list)
        }
        crate::PropertyValue::Map(map) => {
            let json_map: HashMap<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k, property_value_to_json(v)))
                .collect();
            serde_json::to_value(json_map).unwrap_or_default()
        }
    }
}

/// Simple base64 encoding for bytes
fn base64_encode(bytes: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let mut i = 0;

    while i < bytes.len() {
        let b1 = bytes[i];
        let b2 = if i + 1 < bytes.len() { bytes[i + 1] } else { 0 };
        let b3 = if i + 2 < bytes.len() { bytes[i + 2] } else { 0 };

        let triple = ((b1 as u32) << 16) | ((b2 as u32) << 8) | (b3 as u32);

        result.push(CHARS[((triple >> 18) & 63) as usize] as char);
        result.push(CHARS[((triple >> 12) & 63) as usize] as char);
        result.push(if i + 1 < bytes.len() {
            CHARS[((triple >> 6) & 63) as usize] as char
        } else {
            '='
        });
        result.push(if i + 2 < bytes.len() {
            CHARS[(triple & 63) as usize] as char
        } else {
            '='
        });

        i += 3;
    }

    result
}

/// Convert JSON to PropertyValue
fn json_to_property_value(value: serde_json::Value) -> Option<crate::PropertyValue> {
    match value {
        serde_json::Value::String(s) => Some(crate::PropertyValue::String(s)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(crate::PropertyValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Some(crate::PropertyValue::Float(f))
            } else {
                None
            }
        }
        serde_json::Value::Bool(b) => Some(crate::PropertyValue::Bool(b)),
        serde_json::Value::Null => Some(crate::PropertyValue::Null),
        serde_json::Value::Array(arr) => {
            let mut prop_list = Vec::new();
            for item in arr {
                if let Some(prop_val) = json_to_property_value(item) {
                    prop_list.push(prop_val);
                } else {
                    return None; // If any item fails to convert, fail the whole array
                }
            }
            Some(crate::PropertyValue::List(prop_list))
        }
        serde_json::Value::Object(obj) => {
            let mut prop_map = HashMap::new();
            for (key, val) in obj {
                if let Some(prop_val) = json_to_property_value(val) {
                    prop_map.insert(key, prop_val);
                } else {
                    return None; // If any value fails to convert, fail the whole object
                }
            }
            Some(crate::PropertyValue::Map(prop_map))
        }
    }
}
