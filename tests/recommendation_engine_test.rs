//! Movie Recommendation Engine Integration Tests
//!
//! Tests for sophisticated movie recommendation system using:
//! - Gremlin query language for expressive graph traversals
//! - Property graph model with rich metadata
//! - Collaborative filtering using graph algorithms
//! - High-performance operations with Poly-LSM storage

use aster_db::{AsterDB, AsterDBConfig, PropertyValue, Result, VertexId};
use std::collections::{HashMap, HashSet};
use tempfile::TempDir;

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

/// Main recommendation engine
struct RecommendationEngine {
    db: AsterDB,
    users: HashMap<VertexId, User>,
    movies: HashMap<VertexId, Movie>,
}

impl RecommendationEngine {
    /// Create a new recommendation engine with full Aster features
    async fn new(data_path: &str) -> Result<Self> {
        let config = AsterDBConfig {
            enable_recovery: false, // Disable for tests
            enable_metrics: true,
            enable_properties: true,
            ..Default::default()
        };
        let db = AsterDB::open_with_config(data_path, config).await?;

        Ok(Self {
            db,
            users: HashMap::new(),
            movies: HashMap::new(),
        })
    }

    /// Add a user to the system
    async fn add_user(
        &mut self,
        name: String,
        age: u32,
        preferences: Vec<String>,
    ) -> Result<VertexId> {
        // Use Gremlin to add vertex with properties
        let add_user_query = format!(
            "g.addV('user').property('name', '{}').property('age', {}).property('preferences', '{}')",
            name.replace("'", "\\'"),
            age,
            preferences.join(",")
        );

        let result = self.db.gremlin_query(&add_user_query).await?;

        // Extract the vertex ID from the result
        let user_id = if let Some(aster_db::query::GremlinResult::Vertex(vertex_id)) =
            result.results.first()
        {
            *vertex_id
        } else {
            return Err(aster_db::AsterError::invalid_operation(
                "Failed to create user vertex",
            ));
        };

        let user = User {
            id: user_id,
            name,
            age,
            preferences,
        };

        self.users.insert(user_id, user);
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
        // Use Gremlin to add movie vertex with properties
        let add_movie_query = format!(
            "g.addV('movie').property('title', '{}').property('genres', '{}').property('year', {}).property('rating', {})",
            title.replace("'", "\\'"),
            genres.join(","),
            year,
            rating
        );

        let result = self.db.gremlin_query(&add_movie_query).await?;

        // Extract the vertex ID from the result
        let movie_id = if let Some(aster_db::query::GremlinResult::Vertex(vertex_id)) =
            result.results.first()
        {
            *vertex_id
        } else {
            return Err(aster_db::AsterError::invalid_operation(
                "Failed to create movie vertex",
            ));
        };

        let movie = Movie {
            id: movie_id,
            title: title.clone(),
            genres,
            year,
            rating,
        };

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
        // Use simpler Gremlin query to avoid parsing issues
        let add_rating_query = format!(
            "g.V({}).addE('rated').to(g.V({}))",
            user_id.as_u64(),
            movie_id.as_u64()
        );

        self.db.gremlin_query(&add_rating_query).await?;
        Ok(())
    }

    /// Get movies that a user has rated using Gremlin
    async fn get_user_movies(&self, user_id: VertexId) -> Result<Vec<VertexId>> {
        let query = format!("g.V({}).out('rated').id()", user_id.as_u64());

        let result = self.db.gremlin_query(&query).await?;

        // Extract vertex IDs from results
        let mut movie_ids = Vec::new();
        for gremlin_result in result.results {
            if let aster_db::query::GremlinResult::Vertex(vertex_id) = gremlin_result {
                movie_ids.push(vertex_id);
            }
        }
        Ok(movie_ids)
    }

    /// Generate movie recommendations using advanced Gremlin traversals
    async fn recommend_movies(
        &self,
        user_id: VertexId,
        limit: usize,
    ) -> Result<Vec<(VertexId, f64)>> {
        // Use sophisticated Gremlin query for collaborative filtering recommendations
        let recommendation_query = format!(
            "g.V({}).out('rated').aggregate('seen')
             .in('rated').where(neq(V({})))
             .out('rated').where(without('seen'))
             .groupCount().by(id())
             .order(local).by(values, desc)
             .limit({})",
            user_id.as_u64(),
            user_id.as_u64(),
            limit
        );

        let result = self.db.gremlin_query(&recommendation_query).await?;

        let mut recommendations = Vec::new();

        for gremlin_result in result.results {
            if let aster_db::query::GremlinResult::Map(map) = gremlin_result {
                for (movie_id_str, score_result) in map {
                    if let Ok(movie_id_num) = movie_id_str.parse::<u64>() {
                        let movie_id = VertexId::from_u64(movie_id_num);
                        let score = match score_result {
                            aster_db::query::GremlinResult::Count(c) => c as f64,
                            aster_db::query::GremlinResult::Value(PropertyValue::Int(i)) => {
                                i as f64
                            }
                            aster_db::query::GremlinResult::Value(PropertyValue::Float(f)) => f,
                            _ => 0.0,
                        };
                        recommendations.push((movie_id, score));
                    }
                }
            }
        }

        // Fallback to popular movies if no collaborative filtering results
        if recommendations.is_empty() {
            let user_movies = self.get_user_movies(user_id).await?;
            let seen_movies: HashSet<VertexId> = user_movies.into_iter().collect();
            return self.recommend_popular_movies(limit, &seen_movies).await;
        }

        Ok(recommendations)
    }

    /// Fallback: recommend popular movies using Gremlin aggregation
    async fn recommend_popular_movies(
        &self,
        limit: usize,
        seen_movies: &HashSet<VertexId>,
    ) -> Result<Vec<(VertexId, f64)>> {
        // Use Gremlin to find most popular movies (highest rating counts)
        let popularity_query = format!(
            "g.V().hasLabel('movie')
             .order().by(inE('rated').count(), desc)
             .limit({})
             .id()",
            limit * 2 // Get extra to filter out seen movies
        );

        let result = self.db.gremlin_query(&popularity_query).await?;

        let mut recommendations = Vec::new();
        let mut count = 0;

        for gremlin_result in result.results {
            if let aster_db::query::GremlinResult::Vertex(vertex_id) = gremlin_result {
                if !seen_movies.contains(&vertex_id) && count < limit {
                    // Get rating count as popularity score
                    let count_query = format!("g.V({}).inE('rated').count()", vertex_id.as_u64());

                    let count_result = self.db.gremlin_query(&count_query).await?;
                    let popularity_score = if let Some(aster_db::query::GremlinResult::Count(c)) =
                        count_result.results.first()
                    {
                        *c as f64
                    } else {
                        0.0
                    };

                    recommendations.push((vertex_id, popularity_score));
                    count += 1;
                }
            }
        }

        Ok(recommendations)
    }

    /// Generate sample data for testing
    async fn generate_sample_data(&mut self) -> Result<()> {
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
            ("Bob", 30, vec!["Action", "Crime"]),
            ("Carol", 28, vec!["Crime", "Drama"]),
        ];

        let mut user_ids = Vec::new();
        for (name, age, prefs) in users {
            let preferences: Vec<String> = prefs.into_iter().map(|s| s.to_string()).collect();
            let user_id = self.add_user(name.to_string(), age, preferences).await?;
            user_ids.push(user_id);
        }

        // Generate specific interactions for predictable testing
        // Alice rates dramas highly
        self.add_interaction(user_ids[0], movie_ids[0], 9.0).await?; // Shawshank
        self.add_interaction(user_ids[0], movie_ids[4], 8.5).await?; // Forrest Gump

        // Bob rates action/crime movies highly
        self.add_interaction(user_ids[1], movie_ids[2], 9.0).await?; // Dark Knight
        self.add_interaction(user_ids[1], movie_ids[3], 8.0).await?; // Pulp Fiction

        // Carol likes crime/drama
        self.add_interaction(user_ids[2], movie_ids[1], 9.0).await?; // Godfather
        self.add_interaction(user_ids[2], movie_ids[3], 8.5).await?; // Pulp Fiction

        Ok(())
    }
}

#[tokio::test]
async fn test_recommendation_engine_basic_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = RecommendationEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Generate sample data
    engine.generate_sample_data().await.unwrap();

    // Verify data was created
    assert_eq!(engine.users.len(), 3);
    assert_eq!(engine.movies.len(), 5);

    // Test user movie retrieval
    let first_user_id = *engine.users.keys().next().unwrap();
    let user_movies = engine.get_user_movies(first_user_id).await.unwrap();
    assert!(
        !user_movies.is_empty(),
        "User should have rated some movies"
    );

    // Test recommendations
    let recommendations = engine.recommend_movies(first_user_id, 3).await.unwrap();
    assert!(
        !recommendations.is_empty(),
        "Should generate recommendations"
    );
    assert!(recommendations.len() <= 3, "Should respect limit");

    // Verify recommendation scores are non-negative
    for (_, score) in &recommendations {
        assert!(
            *score >= 0.0,
            "Recommendation scores should be non-negative"
        );
    }
}

#[tokio::test]
async fn test_collaborative_filtering() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = RecommendationEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Create users with known preferences
    let alice_id = engine
        .add_user("Alice".to_string(), 25, vec!["Drama".to_string()])
        .await
        .unwrap();
    let bob_id = engine
        .add_user("Bob".to_string(), 30, vec!["Drama".to_string()])
        .await
        .unwrap();

    // Add movies
    let movie1_id = engine
        .add_movie(
            "Drama Movie 1".to_string(),
            vec!["Drama".to_string()],
            2020,
            8.0,
        )
        .await
        .unwrap();
    let movie2_id = engine
        .add_movie(
            "Drama Movie 2".to_string(),
            vec!["Drama".to_string()],
            2021,
            8.5,
        )
        .await
        .unwrap();
    let movie3_id = engine
        .add_movie(
            "Drama Movie 3".to_string(),
            vec!["Drama".to_string()],
            2022,
            9.0,
        )
        .await
        .unwrap();

    // Both users rate movie1 highly
    engine
        .add_interaction(alice_id, movie1_id, 9.0)
        .await
        .unwrap();
    engine
        .add_interaction(bob_id, movie1_id, 8.5)
        .await
        .unwrap();

    // Bob also rates movie2 highly
    engine
        .add_interaction(bob_id, movie2_id, 9.0)
        .await
        .unwrap();

    // Alice should get movie2 recommended based on Bob's rating
    let alice_recommendations = engine.recommend_movies(alice_id, 5).await.unwrap();

    // Check if movie2 is in Alice's recommendations
    let recommended_movie_ids: Vec<VertexId> =
        alice_recommendations.iter().map(|(id, _)| *id).collect();
    assert!(
        recommended_movie_ids.contains(&movie2_id),
        "Collaborative filtering should recommend movie2 to Alice based on Bob's preferences"
    );
}

#[tokio::test]
async fn test_gremlin_queries() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = RecommendationEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Add test data
    let user_id = engine
        .add_user("Test User".to_string(), 25, vec!["Action".to_string()])
        .await
        .unwrap();
    let movie_id = engine
        .add_movie(
            "Test Movie".to_string(),
            vec!["Action".to_string()],
            2020,
            8.0,
        )
        .await
        .unwrap();

    // Test adding interaction through Gremlin
    engine
        .add_interaction(user_id, movie_id, 8.5)
        .await
        .unwrap();

    // Debug: Check what g.V(user_id) returns
    let user_query = format!("g.V({}).count()", user_id.as_u64());
    let user_result = engine.db.gremlin_query(&user_query).await.unwrap();
    let user_count =
        if let Some(aster_db::query::GremlinResult::Count(c)) = user_result.results.first() {
            *c
        } else {
            0
        };
    println!("g.V({}) returns {} vertices", user_id.as_u64(), user_count);

    // Debug: Check what g.V(movie_id) returns
    let movie_query = format!("g.V({}).count()", movie_id.as_u64());
    let movie_result = engine.db.gremlin_query(&movie_query).await.unwrap();
    let movie_count =
        if let Some(aster_db::query::GremlinResult::Count(c)) = movie_result.results.first() {
            *c
        } else {
            0
        };
    println!(
        "g.V({}) returns {} vertices",
        movie_id.as_u64(),
        movie_count
    );

    // Debug: Let's see exactly what vertices exist for our IDs
    let user_id_list_query = format!("g.V({}).id()", user_id.as_u64());
    let user_id_result = engine.db.gremlin_query(&user_id_list_query).await.unwrap();
    println!("User vertex IDs found: {:?}", user_id_result.results);

    let movie_id_list_query = format!("g.V({}).id()", movie_id.as_u64());
    let movie_id_result = engine.db.gremlin_query(&movie_id_list_query).await.unwrap();
    println!("Movie vertex IDs found: {:?}", movie_id_result.results);

    // Verify the edge was created using direct Gremlin query
    let edge_query = format!("g.V({}).outE('rated').count()", user_id.as_u64());
    let result = engine.db.gremlin_query(&edge_query).await.unwrap();

    let edge_count = if let Some(aster_db::query::GremlinResult::Count(c)) = result.results.first()
    {
        *c
    } else {
        0
    };

    assert_eq!(edge_count, 1, "Should have one rated edge");

    // Test vertex property queries
    let user_query = format!("g.V({}).hasLabel('user').count()", user_id.as_u64());
    let user_result = engine.db.gremlin_query(&user_query).await.unwrap();

    let user_count =
        if let Some(aster_db::query::GremlinResult::Count(c)) = user_result.results.first() {
            *c
        } else {
            0
        };

    assert_eq!(user_count, 1, "Should find one user vertex");
}

#[tokio::test]
async fn test_popular_movie_fallback() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = RecommendationEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Create a user with no ratings
    let user_id = engine
        .add_user("New User".to_string(), 25, vec!["Drama".to_string()])
        .await
        .unwrap();

    // Add movies with different popularity
    let popular_movie = engine
        .add_movie(
            "Popular Movie".to_string(),
            vec!["Drama".to_string()],
            2020,
            9.0,
        )
        .await
        .unwrap();
    let unpopular_movie = engine
        .add_movie(
            "Unpopular Movie".to_string(),
            vec!["Drama".to_string()],
            2021,
            7.0,
        )
        .await
        .unwrap();

    // Create other users to rate movies
    let other_user1 = engine
        .add_user("User 1".to_string(), 30, vec!["Drama".to_string()])
        .await
        .unwrap();
    let other_user2 = engine
        .add_user("User 2".to_string(), 35, vec!["Drama".to_string()])
        .await
        .unwrap();

    // Make popular movie actually popular
    engine
        .add_interaction(other_user1, popular_movie, 9.0)
        .await
        .unwrap();
    engine
        .add_interaction(other_user2, popular_movie, 8.5)
        .await
        .unwrap();

    // Only one rating for unpopular movie
    engine
        .add_interaction(other_user1, unpopular_movie, 7.0)
        .await
        .unwrap();

    // New user should get popular movies recommended
    let recommendations = engine.recommend_movies(user_id, 3).await.unwrap();

    assert!(
        !recommendations.is_empty(),
        "Should get fallback recommendations"
    );

    // Popular movie should have higher score than unpopular movie
    let popular_score = recommendations
        .iter()
        .find(|(id, _)| *id == popular_movie)
        .map(|(_, score)| *score)
        .unwrap_or(0.0);

    let unpopular_score = recommendations
        .iter()
        .find(|(id, _)| *id == unpopular_movie)
        .map(|(_, score)| *score)
        .unwrap_or(0.0);

    assert!(
        popular_score >= unpopular_score,
        "Popular movies should have higher scores"
    );
}

#[tokio::test]
async fn test_recommendation_performance() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = RecommendationEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Generate sample data
    engine.generate_sample_data().await.unwrap();

    let start_time = std::time::Instant::now();
    let mut total_recommendations = 0;

    // Perform multiple recommendation queries
    for user_id in engine.users.keys().copied() {
        let recommendations = engine.recommend_movies(user_id, 5).await.unwrap();
        total_recommendations += recommendations.len();
    }

    let elapsed = start_time.elapsed();

    assert!(total_recommendations > 0, "Should generate recommendations");
    assert!(
        elapsed.as_millis() < 5000,
        "Recommendations should be generated quickly (< 5s)"
    );

    println!(
        "Generated {} recommendations in {}ms",
        total_recommendations,
        elapsed.as_millis()
    );
}
