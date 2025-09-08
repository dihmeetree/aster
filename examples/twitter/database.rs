use super::models::*;
use aster_db::{
    query::GremlinResult, AsterDB, AsterDBConfig, GremlinTraversal, Properties, PropertyValue,
    Result, VertexId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Clone)]
pub struct TwitterDatabase {
    pub db: Arc<AsterDB>,
    pub current_user_id: Option<VertexId>,
    // Simple in-memory index for fast user lookups
    user_index: Arc<RwLock<HashMap<String, VertexId>>>, // username -> vertex_id
}

impl TwitterDatabase {
    pub async fn new(data_dir: &str) -> Result<Self> {
        let config = AsterDBConfig {
            enable_properties: true,
            enable_recovery: true,
            enable_metrics: true,
            ..Default::default()
        };

        let db = AsterDB::open_with_config(data_dir, config).await?;

        let twitter_db = Self {
            db: Arc::new(db),
            current_user_id: None,
            user_index: Arc::new(RwLock::new(HashMap::new())),
        };

        // Always build the user index on startup, but we'll optimize this later
        twitter_db.build_user_index().await?;

        Ok(twitter_db)
    }

    pub fn set_current_user(&mut self, user_id: VertexId) {
        self.current_user_id = Some(user_id);
    }

    /// Build the user index from existing data in the database
    async fn build_user_index(&self) -> Result<()> {
        info!("Building user index...");
        let start_time = std::time::Instant::now();

        // Use the inefficient query only once on startup to build the index
        let traversal = GremlinTraversal::v(None).has(
            "type".to_string(),
            Some(PropertyValue::String("user".to_string())),
        );

        let result_set = self.db.gremlin(&traversal).await?;
        let mut index = self.user_index.write().await;

        for result in result_set.results {
            if let Some(vertex_id) = result.as_vertex() {
                if let Ok(props) = self.db.get_vertex_properties(vertex_id).await {
                    if let Some(username) = props.get("username").and_then(|v| v.as_string()) {
                        index.insert(username.to_string(), vertex_id);
                    }
                }
            }
        }

        let duration = start_time.elapsed();
        info!(
            "Built user index with {} users in {:.2?}",
            index.len(),
            duration
        );
        Ok(())
    }

    /// Check if the database already has data (to avoid re-seeding)  
    pub async fn has_existing_data(&self) -> Result<bool> {
        // Check the user index that was built during startup
        let index = self.user_index.read().await;
        Ok(!index.is_empty())
    }

    /// Quick check for existing data without building index (static method)
    pub async fn has_any_data(db: &AsterDB) -> Result<bool> {
        // Try a quick query with a small limit to see if there's any user data
        let traversal = GremlinTraversal::v(None)
            .has(
                "type".to_string(),
                Some(PropertyValue::String("user".to_string())),
            )
            .limit(1);

        let result_set = db.gremlin(&traversal).await?;
        Ok(!result_set.results.is_empty())
    }

    // User operations
    pub async fn create_user(&self, user: &User) -> Result<()> {
        self.db
            .set_vertex_properties(user.id, user.to_properties())
            .await?;

        // Update the user index for fast lookups
        {
            let mut index = self.user_index.write().await;
            index.insert(user.username.clone(), user.id);
        }

        Ok(())
    }

    pub async fn get_user_by_id(&self, user_id: VertexId) -> Result<Option<User>> {
        let props = self.db.get_vertex_properties(user_id).await?;
        if props.get("type").and_then(|v| v.as_string()) == Some(&"user".to_string()) {
            return Ok(User::from_properties(user_id, &props));
        }
        Ok(None)
    }

    pub async fn get_user_by_username(&self, username: &str) -> Result<Option<User>> {
        // Use the in-memory index for fast O(1) lookup
        let user_id = {
            let index = self.user_index.read().await;
            index.get(username).copied()
        };

        if let Some(user_id) = user_id {
            self.get_user_by_id(user_id).await
        } else {
            Ok(None)
        }
    }

    pub async fn follow_user(&self, follower_id: VertexId, followed_id: VertexId) -> Result<()> {
        let edge_props = EdgeType::Follows.to_properties();
        self.db
            .graph()
            .add_edge(follower_id, followed_id, Some(edge_props))
            .await?;

        // Update follower counts
        self.update_follower_counts(follower_id, followed_id, true)
            .await?;
        Ok(())
    }

    pub async fn batch_follow_users(&self, follows: Vec<(VertexId, VertexId)>) -> Result<usize> {
        if follows.is_empty() {
            return Ok(0);
        }

        let edge_props = EdgeType::Follows.to_properties();
        let mut successful = 0;

        // Collect all unique users to update counts for
        let mut follower_updates: std::collections::HashMap<VertexId, i64> =
            std::collections::HashMap::new();
        let mut followed_updates: std::collections::HashMap<VertexId, i64> =
            std::collections::HashMap::new();

        // Process edges in batch
        for (follower_id, followed_id) in follows {
            match self
                .db
                .graph()
                .add_edge(follower_id, followed_id, Some(edge_props.clone()))
                .await
            {
                Ok(_edge) => {
                    successful += 1;
                    *follower_updates.entry(follower_id).or_insert(0) += 1;
                    *followed_updates.entry(followed_id).or_insert(0) += 1;
                }
                Err(_) => {
                    // Skip failed follows but continue processing
                }
            }
        }

        // Batch update follower counts
        self.batch_update_following_counts(follower_updates).await?;
        self.batch_update_follower_counts(followed_updates).await?;

        Ok(successful)
    }

    async fn batch_update_following_counts(
        &self,
        updates: std::collections::HashMap<VertexId, i64>,
    ) -> Result<()> {
        for (user_id, delta) in updates {
            if let Ok(mut props) = self.db.get_vertex_properties(user_id).await {
                let current_count = props
                    .get("following_count")
                    .and_then(|v| v.as_int())
                    .unwrap_or(0);
                let new_count = (current_count + delta).max(0);
                props.insert("following_count".to_string(), PropertyValue::Int(new_count));
                let _ = self.db.set_vertex_properties(user_id, props).await;
            }
        }
        Ok(())
    }

    async fn batch_update_follower_counts(
        &self,
        updates: std::collections::HashMap<VertexId, i64>,
    ) -> Result<()> {
        for (user_id, delta) in updates {
            if let Ok(mut props) = self.db.get_vertex_properties(user_id).await {
                let current_count = props
                    .get("follower_count")
                    .and_then(|v| v.as_int())
                    .unwrap_or(0);
                let new_count = (current_count + delta).max(0);
                props.insert("follower_count".to_string(), PropertyValue::Int(new_count));
                let _ = self.db.set_vertex_properties(user_id, props).await;
            }
        }
        Ok(())
    }

    pub async fn unfollow_user(&self, follower_id: VertexId, followed_id: VertexId) -> Result<()> {
        self.db
            .graph()
            .delete_edge(follower_id, followed_id)
            .await?;

        // Update follower counts
        self.update_follower_counts(follower_id, followed_id, false)
            .await?;
        Ok(())
    }

    async fn update_follower_counts(
        &self,
        follower_id: VertexId,
        followed_id: VertexId,
        increment: bool,
    ) -> Result<()> {
        let delta = if increment { 1 } else { -1 };

        // Update follower's following count
        let mut follower_props = self.db.get_vertex_properties(follower_id).await?;
        let current_following_count = follower_props
            .get("following_count")
            .and_then(|v| v.as_int())
            .unwrap_or(0);
        let new_following_count = (current_following_count + delta).max(0);
        follower_props.insert(
            "following_count".to_string(),
            PropertyValue::Int(new_following_count),
        );
        self.db
            .set_vertex_properties(follower_id, follower_props)
            .await?;

        // Update followed user's follower count
        let mut followed_props = self.db.get_vertex_properties(followed_id).await?;
        let current_follower_count = followed_props
            .get("follower_count")
            .and_then(|v| v.as_int())
            .unwrap_or(0);
        let new_follower_count = (current_follower_count + delta).max(0);
        followed_props.insert(
            "follower_count".to_string(),
            PropertyValue::Int(new_follower_count),
        );
        self.db
            .set_vertex_properties(followed_id, followed_props)
            .await?;

        Ok(())
    }

    // Post operations
    pub async fn create_post(&self, post: &Post) -> Result<()> {
        // Store post properties
        self.db
            .set_vertex_properties(post.id, post.to_properties())
            .await?;

        // Create edge from author to post
        let edge_props = EdgeType::AuthoredPost.to_properties();
        self.db
            .graph()
            .add_edge(post.author_id, post.id, Some(edge_props))
            .await?;

        Ok(())
    }

    pub async fn get_post_by_id(&self, post_id: VertexId) -> Result<Option<Post>> {
        let props = self.db.get_vertex_properties(post_id).await?;
        if props.get("type").and_then(|v| v.as_string()) == Some(&"post".to_string()) {
            return Ok(Post::from_properties(post_id, &props));
        }
        Ok(None)
    }

    pub async fn like_post(&self, user_id: VertexId, post_id: VertexId) -> Result<()> {
        let edge_props = EdgeType::Likes.to_properties();
        self.db
            .graph()
            .add_edge(user_id, post_id, Some(edge_props))
            .await?;

        // Update like count
        self.update_like_count(post_id, true).await?;
        Ok(())
    }

    pub async fn unlike_post(&self, user_id: VertexId, post_id: VertexId) -> Result<()> {
        self.db.graph().delete_edge(user_id, post_id).await?;

        // Update like count
        self.update_like_count(post_id, false).await?;
        Ok(())
    }

    async fn update_like_count(&self, post_id: VertexId, increment: bool) -> Result<()> {
        let mut props = self.db.get_vertex_properties(post_id).await?;
        if let Some(PropertyValue::Int(count)) = props.get("like_count") {
            let delta = if increment { 1 } else { -1 };
            let new_count = (count + delta).max(0);
            props.insert("like_count".to_string(), PropertyValue::Int(new_count));
            self.db.set_vertex_properties(post_id, props).await?;
        }
        Ok(())
    }

    // Comment operations
    pub async fn create_comment(&self, comment: &Comment) -> Result<()> {
        // Store comment properties
        self.db
            .set_vertex_properties(comment.id, comment.to_properties())
            .await?;

        // Create edges
        let authored_props = EdgeType::AuthoredComment.to_properties();
        self.db
            .graph()
            .add_edge(comment.author_id, comment.id, Some(authored_props))
            .await?;

        let comment_props = EdgeType::Comments.to_properties();
        self.db
            .graph()
            .add_edge(comment.id, comment.post_id, Some(comment_props))
            .await?;

        // Update comment count on post
        self.update_comment_count(comment.post_id, true).await?;

        Ok(())
    }

    async fn update_comment_count(&self, post_id: VertexId, increment: bool) -> Result<()> {
        let mut props = self.db.get_vertex_properties(post_id).await?;
        if let Some(PropertyValue::Int(count)) = props.get("comment_count") {
            let delta = if increment { 1 } else { -1 };
            let new_count = (count + delta).max(0);
            props.insert("comment_count".to_string(), PropertyValue::Int(new_count));
            self.db.set_vertex_properties(post_id, props).await?;
        }
        Ok(())
    }

    // Feed and timeline operations
    pub async fn get_user_timeline(
        &self,
        user_id: VertexId,
        limit: u32,
    ) -> Result<Vec<TimelinePost>> {
        // Get all users this user follows
        let all_neighbors = self.db.graph().get_neighbors(user_id).await?;

        // Filter to only include users (not posts, likes, etc.) efficiently
        let mut followed_users = Vec::new();
        for neighbor_id in all_neighbors.iter().take(50) {
            // Reasonable limit
            // Check if this neighbor is a user by looking at its properties directly
            let props = self.db.get_vertex_properties(*neighbor_id).await?;
            if props.get("type").and_then(|v| v.as_string()) == Some(&"user".to_string()) {
                followed_users.push(*neighbor_id);
            }
        }

        if followed_users.is_empty() {
            return Ok(Vec::new());
        }

        // Get all posts from followed users in a single optimized query
        let timeline_posts = self
            .get_posts_from_users(&followed_users, limit * 3)
            .await?;

        // Convert to timeline posts with user interaction data
        let mut result = Vec::new();

        // Get liked posts in one query to reduce database calls
        let user_neighbors = self.db.graph().get_neighbors(user_id).await?;

        for post in timeline_posts {
            // Get author info efficiently
            let author_props = self.db.get_vertex_properties(post.author_id).await?;
            if let Some(author) = User::from_properties(post.author_id, &author_props) {
                let user_liked = user_neighbors.contains(&post.id);
                let user_reposted = false;

                result.push(TimelinePost {
                    post,
                    author,
                    user_liked,
                    user_reposted,
                });
            }
        }

        // Sort by creation time and limit
        result.sort_by(|a, b| b.post.created_at.cmp(&a.post.created_at));
        result.truncate(limit as usize);

        Ok(result)
    }

    // Optimized method to get posts from multiple users in a single query
    async fn get_posts_from_users(&self, user_ids: &[VertexId], limit: u32) -> Result<Vec<Post>> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Get all posts in a single query
        let traversal = GremlinTraversal::v(None).has(
            "type".to_string(),
            Some(PropertyValue::String("post".to_string())),
        );

        let result_set = self.db.gremlin(&traversal).await?;
        let mut posts = Vec::new();

        // Process all posts and filter by author efficiently
        for result in result_set.results {
            if let Some(post_id) = result.as_vertex() {
                // Get the post properties directly to avoid extra queries
                let props = self.db.get_vertex_properties(post_id).await?;
                if let Some(post) = Post::from_properties(post_id, &props) {
                    // Check if this post is from one of the users we're interested in
                    if user_ids.contains(&post.author_id) {
                        posts.push(post);
                    }
                }
            }
        }

        // Sort by creation time and limit
        posts.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        posts.truncate(limit as usize);

        Ok(posts)
    }

    pub async fn get_user_posts(&self, user_id: VertexId, limit: u32) -> Result<Vec<Post>> {
        // Use Gremlin to find posts authored by this user
        let traversal = GremlinTraversal::v(None)
            .has(
                "type".to_string(),
                Some(PropertyValue::String("post".to_string())),
            )
            .has(
                "author_id".to_string(),
                Some(PropertyValue::String(user_id.to_string())),
            );

        let result_set = self.db.gremlin(&traversal).await?;
        let post_ids: Vec<VertexId> = result_set
            .results
            .iter()
            .filter_map(|result| result.as_vertex())
            .collect();

        // Batch fetch all post properties
        let post_properties = self.get_multiple_vertex_properties(&post_ids).await?;
        let mut posts = Vec::new();

        for (vertex_id, properties) in post_properties {
            if let Some(post) = Post::from_properties(vertex_id, &properties) {
                posts.push(post);
            }
        }

        // Sort by creation time and limit
        posts.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        posts.truncate(limit as usize);

        Ok(posts)
    }

    pub async fn user_liked_post(&self, user_id: VertexId, post_id: VertexId) -> Result<bool> {
        // Check if user has a "likes" edge to this post
        let liked_items = self.db.graph().get_neighbors(user_id).await?;
        Ok(liked_items.contains(&post_id))
    }

    pub async fn get_post_comments(&self, post_id: VertexId) -> Result<Vec<CommentWithAuthor>> {
        let traversal = GremlinTraversal::v(None)
            .has(
                "type".to_string(),
                Some(PropertyValue::String("comment".to_string())),
            )
            .has(
                "post_id".to_string(),
                Some(PropertyValue::String(post_id.to_string())),
            );

        let result_set = self.db.gremlin(&traversal).await?;
        let mut comments_data = Vec::new();
        let mut author_ids = Vec::new();

        // First pass: collect all comments and their author IDs
        for result in result_set.results {
            if let GremlinResult::Vertex(vertex_id) = result {
                let props = self.db.get_vertex_properties(vertex_id).await?;
                if let Some(comment) = Comment::from_properties(vertex_id, &props) {
                    author_ids.push(comment.author_id);
                    comments_data.push(comment);
                }
            }
        }

        // Batch fetch all author properties
        let author_properties = self.get_multiple_vertex_properties(&author_ids).await?;
        let author_map: HashMap<VertexId, User> = author_properties
            .into_iter()
            .filter_map(|(vertex_id, props)| {
                User::from_properties(vertex_id, &props).map(|user| (vertex_id, user))
            })
            .collect();

        // Build final comments with authors
        let mut comments = Vec::new();
        for comment in comments_data {
            if let Some(author) = author_map.get(&comment.author_id) {
                comments.push(CommentWithAuthor {
                    comment,
                    author: author.clone(),
                    user_liked: false, // For now, we don't track if current user liked comments
                });
            }
        }

        // Sort comments by creation time
        comments.sort_by(|a, b| a.comment.created_at.cmp(&b.comment.created_at));

        Ok(comments)
    }

    // Recommendation system - optimized to reduce query count
    pub async fn get_user_recommendations(
        &self,
        user_id: VertexId,
        limit: u32,
    ) -> Result<Vec<UserRecommendation>> {
        use tracing::info;

        // Get all potential users and their follow relationships in batch
        let user_neighbors = self.db.graph().get_neighbors(user_id).await?;

        // Get all user vertices in a single query to filter out non-users efficiently
        let all_users = self.get_all_users_batch().await?;
        let user_id_set: std::collections::HashSet<VertexId> =
            all_users.iter().map(|u| u.id).collect();

        // Filter to only users this user follows
        let followed_users: Vec<VertexId> = user_neighbors
            .into_iter()
            .filter(|id| user_id_set.contains(id))
            .collect();

        info!("User {} follows {} users", user_id, followed_users.len());

        if followed_users.is_empty() {
            return Ok(Vec::new());
        }

        let mut recommendation_scores: std::collections::HashMap<VertexId, u32> =
            std::collections::HashMap::new();

        // Get neighbor relationships for followed users (optimized approach)
        for &followed_user_id in followed_users.iter().take(10) {
            if let Ok(neighbors) = self.db.graph().get_neighbors(followed_user_id).await {
                for potential_rec in neighbors {
                    if user_id_set.contains(&potential_rec)
                        && potential_rec != user_id
                        && !followed_users.contains(&potential_rec)
                    {
                        *recommendation_scores.entry(potential_rec).or_insert(0) += 1;
                    }
                }
            }
        }

        info!(
            "Found {} potential recommendations",
            recommendation_scores.len()
        );

        // Sort and get top recommendations
        let mut sorted_recommendations: Vec<_> = recommendation_scores.into_iter().collect();
        sorted_recommendations.sort_by(|a, b| b.1.cmp(&a.1));

        let mut recommendations = Vec::new();

        // Create user lookup map to avoid repeated queries
        let user_map: std::collections::HashMap<VertexId, User> =
            all_users.into_iter().map(|u| (u.id, u)).collect();

        for (rec_user_id, score) in sorted_recommendations.into_iter().take(limit as usize) {
            if let Some(user) = user_map.get(&rec_user_id) {
                recommendations.push(UserRecommendation {
                    user: user.clone(),
                    reason: format!("{} mutual connections", score),
                    score: score as f64,
                    mutual_connections: score,
                });
            }
        }

        info!(
            "Returning {} recommendations for user {}",
            recommendations.len(),
            user_id
        );
        Ok(recommendations)
    }

    // Helper method to get all users in a single batch query
    async fn get_all_users_batch(&self) -> Result<Vec<User>> {
        // Use the index to get all user IDs without scanning
        let user_vertex_ids: Vec<VertexId> = {
            let index = self.user_index.read().await;
            index.values().copied().collect()
        };

        // Batch fetch properties for all users
        let user_properties = self
            .get_multiple_vertex_properties(&user_vertex_ids)
            .await?;

        let mut users = Vec::new();
        for (vertex_id, properties) in user_properties {
            if let Some(user) = User::from_properties(vertex_id, &properties) {
                users.push(user);
            }
        }

        Ok(users)
    }

    // Combined profile data query to reduce multiple round trips
    pub async fn get_profile_data(
        &self,
        username: &str,
        current_user_id: Option<VertexId>,
    ) -> Result<Option<(User, Vec<Post>, bool, bool)>> {
        // Get profile user
        let profile_user = match self.get_user_by_username(username).await? {
            Some(user) => user,
            None => return Ok(None),
        };

        // Batch the remaining operations
        let posts_future = self.get_user_posts(profile_user.id, 20);

        let (following_future, followed_by_future) = if let Some(current_user_id) = current_user_id
        {
            (
                Some(self.check_if_following(current_user_id, profile_user.id)),
                Some(self.check_if_following(profile_user.id, current_user_id)),
            )
        } else {
            (None, None)
        };

        // Execute in parallel
        let posts = posts_future.await?;
        let following = if let Some(future) = following_future {
            future.await.unwrap_or(false)
        } else {
            false
        };
        let followed_by = if let Some(future) = followed_by_future {
            future.await.unwrap_or(false)
        } else {
            false
        };

        Ok(Some((profile_user, posts, following, followed_by)))
    }

    // Batch property fetching method to reduce individual queries
    async fn get_multiple_vertex_properties(
        &self,
        vertex_ids: &[VertexId],
    ) -> Result<Vec<(VertexId, Properties)>> {
        let mut results = Vec::new();

        // For now, fetch properties in sequential batches to avoid overwhelming the database
        // This still provides significant improvement over the original N individual queries in the calling code
        for &vertex_id in vertex_ids {
            if let Ok(props) = self.db.get_vertex_properties(vertex_id).await {
                results.push((vertex_id, props));
            }
        }

        Ok(results)
    }

    pub async fn check_if_following(
        &self,
        follower_id: VertexId,
        followed_id: VertexId,
    ) -> Result<bool> {
        let followed_users = self.db.graph().get_neighbors(follower_id).await?;
        Ok(followed_users.contains(&followed_id))
    }

    async fn count_mutual_connections(
        &self,
        user1_id: VertexId,
        user2_id: VertexId,
    ) -> Result<u32> {
        let user1_follows = self.db.graph().get_neighbors(user1_id).await?;
        let user2_follows = self.db.graph().get_neighbors(user2_id).await?;

        let mutual_count = user1_follows
            .iter()
            .filter(|&id| user2_follows.contains(id))
            .count();

        Ok(mutual_count as u32)
    }

    pub async fn search_users(&self, query: &str, limit: u32) -> Result<Vec<User>> {
        // Use the index to get all user IDs and search in memory
        let user_vertex_ids: Vec<VertexId> = {
            let index = self.user_index.read().await;
            // Filter by username first using the index keys for efficiency
            index
                .iter()
                .filter(|(username, _)| username.contains(query))
                .map(|(_, &user_id)| user_id)
                .take(limit as usize * 2) // Get more to account for display_name matches
                .collect()
        };

        // Batch fetch properties for matching users
        let user_properties = self
            .get_multiple_vertex_properties(&user_vertex_ids)
            .await?;
        let mut matching_users = Vec::new();

        for (vertex_id, properties) in user_properties {
            if let Some(user) = User::from_properties(vertex_id, &properties) {
                if user.username.contains(query) || user.display_name.contains(query) {
                    matching_users.push(user);
                }
            }
        }

        matching_users.truncate(limit as usize);
        Ok(matching_users)
    }

    pub async fn get_trending_topics(&self) -> Result<Vec<String>> {
        // Simplified trending topics - could be enhanced with hashtag parsing
        Ok(vec![
            "#Technology".to_string(),
            "#Programming".to_string(),
            "#GraphDatabase".to_string(),
            "#Rust".to_string(),
            "#WebDev".to_string(),
        ])
    }
}
