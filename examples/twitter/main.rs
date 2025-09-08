mod database;
mod handlers;
mod models;
mod seeder;

use aster_db::{AsterDB, AsterDBConfig};
use axum::{
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Router,
};
use database::TwitterDatabase;
use handlers::*;
use seeder::{DatabaseSeeder, SeedConfig};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceBuilder;
use tower_http::{
    cors::CorsLayer,
    trace::{DefaultMakeSpan, TraceLayer},
};
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Twitter clone with Aster database...");

    // Initialize the database
    let data_dir = "./twitter_data";
    std::fs::create_dir_all(data_dir)?;

    // Quick check for existing data before building expensive user index
    let config = AsterDBConfig {
        enable_properties: true,
        enable_recovery: true,
        enable_metrics: true,
        ..Default::default()
    };
    let aster_db = AsterDB::open_with_config(data_dir, config).await?;

    info!("Checking if database has existing data...");
    let should_seed = !TwitterDatabase::has_any_data(&aster_db).await?;

    if should_seed {
        info!("Database is empty, will seed after initialization");
    } else {
        info!("Database already contains data, will skip seeding");
    }

    // Now create the TwitterDatabase (with expensive index building)
    let db = TwitterDatabase::new(data_dir).await?;
    info!("Connected to Aster database at {}", data_dir);

    if should_seed {
        // Seed the database with sample data
        info!("Seeding database with sample data...");
        let mut seeder = DatabaseSeeder::new(db.clone());

        // Choose seeding scale based on environment variable or default to small
        let seed_scale = std::env::var("SEED_SCALE").unwrap_or_else(|_| "small".to_string());
        let config = match seed_scale.as_str() {
            "demo" => {
                info!("Using demo scale: 100 users, 500 posts, 1K comments");
                SeedConfig::demo_scale()
            }
            "small" => {
                info!("Using small scale: 10K users, 50K posts, 100K comments");
                SeedConfig::small_scale()
            }
            "medium" => {
                info!("Using medium scale: 100K users, 500K posts, 1M comments");
                SeedConfig::medium_scale()
            }
            "large" => {
                info!("Using large scale: 1M users, 5M posts, 10M comments");
                SeedConfig::large_scale()
            }
            _ => {
                info!("Using small scale (default): 10K users, 50K posts, 100K comments");
                SeedConfig::small_scale()
            }
        };

        if let Err(e) = seeder.seed_database(&config).await {
            tracing::warn!(
                "Failed to seed database: {}. Continuing with existing data.",
                e
            );
        }
    }

    // Wrap database in Arc<RwLock> for sharing across handlers
    let app_state = Arc::new(RwLock::new(db));

    // Build our application with routes
    let app = Router::new()
        .route("/", get(home))
        .route("/login", get(login_form))
        .route("/login", post(login))
        .route("/logout", get(logout))
        .route("/profile/:username", get(profile))
        .route("/post/:id", get(view_post))
        .route("/post", post(create_post))
        .route("/comment", post(create_comment))
        .route("/like/:id", get(like_post))
        .route("/follow/:username", get(follow_user))
        .route("/api/recommendations", get(api_recommendations))
        .route("/api/search", get(api_search_users))
        .fallback(not_found)
        .with_state(app_state)
        .layer(
            ServiceBuilder::new()
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::default().include_headers(true)),
                )
                .layer(CorsLayer::permissive()),
        );

    // Start the server
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("0.0.0.0:{}", port);

    info!("🚀 Server starting on http://{}", addr);
    info!("🐦 Twitter clone is ready!");
    info!("📚 Visit http://localhost:{} to get started", port);
    info!("🔐 Click 'Login' and enter any username to create/login");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn not_found() -> Result<Html<String>, StatusCode> {
    let html = r#"
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>404 - Page Not Found</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background-color: #000;
                color: #fff;
                display: flex;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                margin: 0;
                text-align: center;
            }
            .container {
                background-color: #16181c;
                border: 1px solid #2f3336;
                border-radius: 15px;
                padding: 40px;
                max-width: 400px;
            }
            h1 { font-size: 72px; margin: 0; color: #1d9bf0; }
            h2 { margin: 20px 0; }
            p { color: #536471; margin: 20px 0; }
            a {
                color: #1d9bf0;
                text-decoration: none;
                background-color: #1d9bf0;
                color: white;
                padding: 12px 24px;
                border-radius: 25px;
                display: inline-block;
                margin-top: 20px;
            }
            a:hover { background-color: #1a8cd8; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>404</h1>
            <h2>Page Not Found</h2>
            <p>The page you're looking for doesn't exist.</p>
            <a href="/">Go Home</a>
        </div>
    </body>
    </html>
    "#;

    Ok(Html(html.to_string()))
}
