//! Recommendation Engine Example using Aster Graph Database
//!
//! This example demonstrates how to build a movie recommendation system using Aster.
//! It showcases:
//! - High-performance graph operations with Poly-LSM
//! - Real-time recommendations with collaborative filtering
//! - Handling large-scale user-item interactions
//! - Adaptive updates optimizing for different workload patterns

use aster_db::{AsterDB, Properties, PropertyValue, Result, VertexId};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

/// User in the recommendation system
#[derive(Debug, Clone)]
struct User {
    id: VertexId,
    name: String,
    age: u32,
    preferences: Vec<String>, // genres they like
}

/// Movie in the system
#[derive(Debug, Clone)]
struct Movie {
    id: VertexId,
    title: String,
    genres: Vec<String>,
    year: u32,
    rating: f64,
}

/// User interaction with a movie
#[derive(Debug, Clone)]
struct Interaction {
    user_id: VertexId,
    movie_id: VertexId,
    rating: f64,
    timestamp: u64,
}

/// Main recommendation engine
struct RecommendationEngine {
    db: AsterDB,
    users: HashMap<VertexId, User>,
    movies: HashMap<VertexId, Movie>,
    next_user_id: u64,
    next_movie_id: u64,
}

impl RecommendationEngine {
    /// Create a new recommendation engine
    async fn new(data_path: &str) -> Result<Self> {
        let db = AsterDB::open(data_path).await?;

        Ok(Self {
            db,
            users: HashMap::new(),
            movies: HashMap::new(),
            next_user_id: 1,
            next_movie_id: 1000, // Start movies at 1000 to avoid conflicts
        })
    }

    /// Add a user to the system
    async fn add_user(
        &mut self,
        name: String,
        age: u32,
        preferences: Vec<String>,
    ) -> Result<VertexId> {
        let mut properties = Properties::new();
        properties.insert("type".to_string(), "user".into());
        properties.insert("name".to_string(), name.clone().into());
        properties.insert("age".to_string(), (age as i64).into());
        properties.insert(
            "preferences".to_string(),
            PropertyValue::List(
                preferences
                    .iter()
                    .map(|s| PropertyValue::String(s.clone()))
                    .collect(),
            ),
        );

        let graph = self.db.graph();
        let user_id = VertexId::from_u64(self.next_user_id);
        self.next_user_id += 1;
        graph.add_vertex(user_id, Some(properties)).await?;

        let user = User {
            id: user_id,
            name,
            age,
            preferences,
        };

        self.users.insert(user_id, user);
        info!("Added user: {}", user_id);

        Ok(user_id)
    }

    /// Add a movie to the system
    async fn add_movie(
        &mut self,
        title: String,
        genres: Vec<String>,
        year: u32,
        rating: f64,
    ) -> Result<VertexId> {
        let mut properties = Properties::new();
        properties.insert("type".to_string(), "movie".into());
        properties.insert("title".to_string(), title.clone().into());
        properties.insert(
            "genres".to_string(),
            PropertyValue::List(
                genres
                    .iter()
                    .map(|s| PropertyValue::String(s.clone()))
                    .collect(),
            ),
        );
        properties.insert("year".to_string(), (year as i64).into());
        properties.insert("rating".to_string(), rating.into());

        let graph = self.db.graph();
        let movie_id = VertexId::from_u64(self.next_movie_id);
        self.next_movie_id += 1;
        graph.add_vertex(movie_id, Some(properties)).await?;

        let movie = Movie {
            id: movie_id,
            title: title.clone(),
            genres,
            year,
            rating,
        };

        info!("Added movie: {} ({})", title, movie_id);
        self.movies.insert(movie_id, movie);

        Ok(movie_id)
    }

    /// Record a user interaction (rating) with a movie
    async fn add_interaction(
        &self,
        user_id: VertexId,
        movie_id: VertexId,
        rating: f64,
    ) -> Result<()> {
        let mut properties = Properties::new();
        properties.insert("rating".to_string(), rating.into());
        properties.insert(
            "timestamp".to_string(),
            (chrono::Utc::now().timestamp() as i64).into(),
        );

        let graph = self.db.graph();

        // Add edge from user to movie with rating as property
        let _edge = graph.add_edge(user_id, movie_id, Some(properties)).await?;

        info!("User {} rated movie {} with {}", user_id, movie_id, rating);
        Ok(())
    }

    /// Get movies that a user has interacted with
    async fn get_user_movies(&self, user_id: VertexId) -> Result<Vec<VertexId>> {
        let graph = self.db.graph();
        graph.get_neighbors(user_id).await
    }

    /// Find similar users using collaborative filtering
    async fn find_similar_users(
        &self,
        target_user_id: VertexId,
        limit: usize,
    ) -> Result<Vec<(VertexId, f64)>> {
        let target_movies = self.get_user_movies(target_user_id).await?;
        let target_movie_set: HashSet<VertexId> = target_movies.into_iter().collect();

        if target_movie_set.is_empty() {
            return Ok(Vec::new());
        }

        let mut similarities = Vec::new();

        // Calculate Jaccard similarity with all other users
        for (&other_user_id, _) in &self.users {
            if other_user_id == target_user_id {
                continue;
            }

            let other_movies = self.get_user_movies(other_user_id).await?;
            let other_movie_set: HashSet<VertexId> = other_movies.into_iter().collect();

            if other_movie_set.is_empty() {
                continue;
            }

            // Jaccard similarity: |A ∩ B| / |A ∪ B|
            let intersection: HashSet<_> =
                target_movie_set.intersection(&other_movie_set).collect();
            let union: HashSet<_> = target_movie_set.union(&other_movie_set).collect();

            let similarity = intersection.len() as f64 / union.len() as f64;

            if similarity > 0.0 {
                similarities.push((other_user_id, similarity));
            }
        }

        // Sort by similarity (descending) and limit results
        similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        similarities.truncate(limit);

        Ok(similarities)
    }

    /// Generate movie recommendations for a user
    async fn recommend_movies(
        &self,
        user_id: VertexId,
        limit: usize,
    ) -> Result<Vec<(VertexId, f64)>> {
        // Get movies the user has already seen
        let user_movies = self.get_user_movies(user_id).await?;
        let seen_movies: HashSet<VertexId> = user_movies.into_iter().collect();

        // Find similar users
        let similar_users = self.find_similar_users(user_id, 50).await?;

        if similar_users.is_empty() {
            return self.recommend_popular_movies(limit, &seen_movies).await;
        }

        // Collect movie recommendations from similar users
        let mut movie_scores: HashMap<VertexId, f64> = HashMap::new();

        for (similar_user_id, similarity) in similar_users {
            let similar_user_movies = self.get_user_movies(similar_user_id).await?;

            for movie_id in similar_user_movies {
                if !seen_movies.contains(&movie_id) {
                    *movie_scores.entry(movie_id).or_insert(0.0) += similarity;
                }
            }
        }

        // Sort recommendations by score
        let mut recommendations: Vec<_> = movie_scores.into_iter().collect();
        recommendations.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        recommendations.truncate(limit);

        Ok(recommendations)
    }

    /// Fallback: recommend popular movies for new users
    async fn recommend_popular_movies(
        &self,
        limit: usize,
        seen_movies: &HashSet<VertexId>,
    ) -> Result<Vec<(VertexId, f64)>> {
        let mut movie_popularity: HashMap<VertexId, u32> = HashMap::new();

        // Count how many users have interacted with each movie
        for &user_id in self.users.keys() {
            let user_movies = self.get_user_movies(user_id).await?;
            for movie_id in user_movies {
                if !seen_movies.contains(&movie_id) {
                    *movie_popularity.entry(movie_id).or_insert(0) += 1;
                }
            }
        }

        let mut recommendations: Vec<_> = movie_popularity
            .into_iter()
            .map(|(movie_id, count)| (movie_id, count as f64))
            .collect();

        recommendations.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        recommendations.truncate(limit);

        Ok(recommendations)
    }

    /// Print recommendations for a user
    async fn print_recommendations(&self, user_id: VertexId) -> Result<()> {
        if let Some(user) = self.users.get(&user_id) {
            println!("\n🎬 Recommendations for {}:", user.name);

            let recommendations = self.recommend_movies(user_id, 5).await?;

            if recommendations.is_empty() {
                println!("   No recommendations available yet.");
                return Ok(());
            }

            for (i, (movie_id, score)) in recommendations.iter().enumerate() {
                if let Some(movie) = self.movies.get(movie_id) {
                    println!(
                        "   {}. {} ({}) - Score: {:.2}",
                        i + 1,
                        movie.title,
                        movie.year,
                        score
                    );
                    println!("      Genres: {}", movie.genres.join(", "));
                    println!("      Rating: {:.1}/10", movie.rating);
                }
            }
        }

        Ok(())
    }

    /// Generate sample data for testing
    async fn generate_sample_data(&mut self) -> Result<()> {
        info!("Generating sample data...");

        // Sample movies
        let movies = vec![
            (
                "The Shawshank Redemption",
                vec!["Drama".to_string()],
                1994,
                9.3,
            ),
            (
                "The Godfather",
                vec!["Crime", "Drama"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                1972,
                9.2,
            ),
            (
                "The Dark Knight",
                vec!["Action", "Crime", "Drama"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                2008,
                9.0,
            ),
            (
                "Pulp Fiction",
                vec!["Crime", "Drama"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                1994,
                8.9,
            ),
            (
                "Forrest Gump",
                vec!["Drama", "Romance"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                1994,
                8.8,
            ),
            (
                "Inception",
                vec!["Action", "Sci-Fi", "Thriller"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                2010,
                8.8,
            ),
            (
                "The Matrix",
                vec!["Action", "Sci-Fi"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                1999,
                8.7,
            ),
            (
                "Goodfellas",
                vec!["Biography", "Crime", "Drama"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                1990,
                8.7,
            ),
            (
                "The Lord of the Rings: The Return of the King",
                vec!["Adventure", "Drama", "Fantasy"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                2003,
                8.9,
            ),
            (
                "Avatar",
                vec!["Action", "Adventure", "Fantasy"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                2009,
                7.8,
            ),
        ];

        let mut movie_ids = Vec::new();
        for (title, genres, year, rating) in movies {
            let movie_id = self
                .add_movie(title.to_string(), genres, year, rating)
                .await?;
            movie_ids.push(movie_id);
        }

        // Sample users with different preferences
        let users = vec![
            ("Alice", 25, vec!["Drama", "Romance"]),
            ("Bob", 30, vec!["Action", "Sci-Fi"]),
            ("Carol", 28, vec!["Crime", "Drama"]),
            ("David", 35, vec!["Fantasy", "Adventure"]),
            ("Eve", 22, vec!["Thriller", "Sci-Fi"]),
            ("Frank", 40, vec!["Biography", "Drama"]),
        ];

        let mut user_ids = Vec::new();
        for (name, age, prefs) in users {
            let preferences: Vec<String> = prefs.into_iter().map(|s| s.to_string()).collect();
            let user_id = self.add_user(name.to_string(), age, preferences).await?;
            user_ids.push(user_id);
        }

        // Generate random interactions
        let mut rng = rand::thread_rng();
        info!("Generating user interactions...");

        for &user_id in &user_ids {
            // Each user rates 3-7 movies
            let num_ratings = rng.gen_range(3..=7);
            let mut rated_movies = HashSet::new();

            for _ in 0..num_ratings {
                // Pick a random movie
                let movie_idx = rng.gen_range(0..movie_ids.len());
                let movie_id = movie_ids[movie_idx];

                if rated_movies.contains(&movie_id) {
                    continue; // Skip if already rated
                }

                rated_movies.insert(movie_id);

                // Generate rating (biased towards higher ratings)
                let rating = if rng.gen_bool(0.7) {
                    rng.gen_range(7.0..10.0) // 70% chance of high rating
                } else {
                    rng.gen_range(4.0..7.0) // 30% chance of medium rating
                };

                self.add_interaction(user_id, movie_id, rating).await?;

                // Small delay to simulate real-time behavior
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        info!("Sample data generation complete!");
        Ok(())
    }

    /// Simulate real-time updates and show performance
    async fn simulate_real_time_workload(&self, duration_seconds: u64) -> Result<()> {
        info!(
            "Starting real-time simulation for {} seconds...",
            duration_seconds
        );

        let start_time = Instant::now();
        let mut operation_count = 0u64;
        let mut rng = rand::thread_rng();

        let user_ids: Vec<_> = self.users.keys().copied().collect();
        let movie_ids: Vec<_> = self.movies.keys().copied().collect();

        if user_ids.is_empty() || movie_ids.is_empty() {
            warn!("No users or movies available for simulation");
            return Ok(());
        }

        while start_time.elapsed().as_secs() < duration_seconds {
            // 70% reads (getting recommendations), 30% writes (new ratings)
            if rng.gen_bool(0.7) {
                // Read operation: get recommendations
                let user_id = user_ids[rng.gen_range(0..user_ids.len())];
                let _recommendations = self.recommend_movies(user_id, 5).await?;
            } else {
                // Write operation: new rating
                let user_id = user_ids[rng.gen_range(0..user_ids.len())];
                let movie_id = movie_ids[rng.gen_range(0..movie_ids.len())];
                let rating = rng.gen_range(1.0..10.0);

                let _ = self.add_interaction(user_id, movie_id, rating).await;
            }

            operation_count += 1;

            // Print stats every 1000 operations
            if operation_count % 1000 == 0 {
                let elapsed = start_time.elapsed().as_secs_f64();
                let ops_per_second = operation_count as f64 / elapsed;
                info!(
                    "Operations: {}, Rate: {:.2} ops/sec",
                    operation_count, ops_per_second
                );
            }

            // Small delay to prevent overwhelming the system
            tokio::time::sleep(Duration::from_micros(100)).await;
        }

        let total_time = start_time.elapsed();
        let final_ops_per_second = operation_count as f64 / total_time.as_secs_f64();

        info!("Simulation complete!");
        info!("Total operations: {}", operation_count);
        info!("Total time: {:.2}s", total_time.as_secs_f64());
        info!("Average throughput: {:.2} ops/sec", final_ops_per_second);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_level(true)
        .with_target(false)
        .init();

    println!("🚀 Aster Graph Database - Movie Recommendation Engine");
    println!("====================================================");

    // Initialize the recommendation engine
    let mut engine = RecommendationEngine::new("./recommendation_data").await?;

    // Generate sample data
    engine.generate_sample_data().await?;

    println!("\n📊 Database Statistics:");
    println!("Users: {}", engine.users.len());
    println!("Movies: {}", engine.movies.len());

    // Show recommendations for each user
    for user_id in engine.users.keys().copied() {
        engine.print_recommendations(user_id).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("\n🔄 Starting real-time workload simulation...");
    println!("This demonstrates Aster's performance under mixed read/write workloads.");
    println!("The adaptive update mechanism optimizes storage based on usage patterns.\n");

    // Run real-time simulation for 30 seconds
    engine.simulate_real_time_workload(30).await?;

    // Show final recommendations after all the activity
    println!("\n🎯 Final Recommendations (after real-time activity):");
    if let Some(&first_user_id) = engine.users.keys().next() {
        engine.print_recommendations(first_user_id).await?;
    }

    println!("\n✨ Demo completed! Aster successfully handled:");
    println!("   • Real-time graph updates with adaptive optimization");
    println!("   • Complex graph traversals for similarity computation");
    println!("   • Mixed read/write workloads with high throughput");
    println!("   • Efficient storage using Poly-LSM's hybrid layout");

    Ok(())
}
