use super::models::*;
use aster_db::{
    query::GremlinResult, AsterDB, AsterDBConfig, GremlinTraversal, PropertyValue, Result, VertexId,
};
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct TwitterDatabase {
    pub db: Arc<AsterDB>,
    pub current_user_id: Option<VertexId>,
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

        Ok(Self {
            db: Arc::new(db),
            current_user_id: None,
        })
    }

    pub fn set_current_user(&mut self, user_id: VertexId) {
        self.current_user_id = Some(user_id);
    }

    // User operations
    pub async fn create_user(&self, user: &User) -> Result<()> {
        self.db
            .set_vertex_properties(user.id, user.to_properties())
            .await?;
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
        // For now, we'll use a simple approach and iterate through vertices
        // In a real implementation, we'd use proper indexing
        let traversal = GremlinTraversal::v(None)
            .has(
                "type".to_string(),
                Some(PropertyValue::String("user".to_string())),
            )
            .has(
                "username".to_string(),
                Some(PropertyValue::String(username.to_string())),
            );

        let result_set = self.db.gremlin(&traversal).await?;

        if let Some(result) = result_set.results.first() {
            if let Some(vertex_id) = result.as_vertex() {
                return self.get_user_by_id(vertex_id).await;
            }
        }
        Ok(None)
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
        if let Some(PropertyValue::Int(count)) = follower_props.get("following_count") {
            let new_count = (count + delta).max(0);
            follower_props.insert("following_count".to_string(), PropertyValue::Int(new_count));
            self.db
                .set_vertex_properties(follower_id, follower_props)
                .await?;
        }

        // Update followed user's follower count
        let mut followed_props = self.db.get_vertex_properties(followed_id).await?;
        if let Some(PropertyValue::Int(count)) = followed_props.get("follower_count") {
            let new_count = (count + delta).max(0);
            followed_props.insert("follower_count".to_string(), PropertyValue::Int(new_count));
            self.db
                .set_vertex_properties(followed_id, followed_props)
                .await?;
        }

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
        // For now, get all users this user follows
        let followed_users = self.db.graph().get_neighbors(user_id).await?;

        let mut timeline_posts = Vec::new();

        // Get posts from followed users
        for followed_user_id in followed_users.iter().take(20) {
            // Limit for performance
            let user_posts = self.get_user_posts(*followed_user_id, 5).await?;

            for post in user_posts {
                if let Some(author) = self.get_user_by_id(post.author_id).await? {
                    let user_liked = self.user_liked_post(user_id, post.id).await?;
                    let user_reposted = false;

                    timeline_posts.push(TimelinePost {
                        post,
                        author,
                        user_liked,
                        user_reposted,
                    });
                }
            }
        }

        // Sort by creation time and limit
        timeline_posts.sort_by(|a, b| b.post.created_at.cmp(&a.post.created_at));
        timeline_posts.truncate(limit as usize);

        Ok(timeline_posts)
    }

    pub async fn get_user_posts(&self, user_id: VertexId, limit: u32) -> Result<Vec<Post>> {
        // Get post IDs authored by this user
        let post_ids = self.db.graph().get_neighbors(user_id).await?;

        let mut posts = Vec::new();

        for post_id in post_ids {
            if let Some(post) = self.get_post_by_id(post_id).await? {
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
        let mut comments = Vec::new();

        for result in result_set.results {
            if let GremlinResult::Vertex(vertex_id) = result {
                let props = self.db.get_vertex_properties(vertex_id).await?;
                if let Some(comment) = Comment::from_properties(vertex_id, &props) {
                    if let Some(author) = self.get_user_by_id(comment.author_id).await? {
                        // For now, we don't track if current user liked comments, so set to false
                        comments.push(CommentWithAuthor {
                            comment,
                            author,
                            user_liked: false,
                        });
                    }
                }
            }
        }

        // Sort comments by creation time
        comments.sort_by(|a, b| a.comment.created_at.cmp(&b.comment.created_at));

        Ok(comments)
    }

    // Recommendation system
    pub async fn get_user_recommendations(
        &self,
        user_id: VertexId,
        limit: u32,
    ) -> Result<Vec<UserRecommendation>> {
        use tracing::info;

        // Simplified recommendation: get people followed by people you follow
        let all_neighbors = self.db.graph().get_neighbors(user_id).await?;
        let mut followed_users = Vec::new();

        // Filter neighbors to only include users (not posts, likes, etc.)
        for neighbor_id in all_neighbors {
            if let Some(_) = self.get_user_by_id(neighbor_id).await? {
                followed_users.push(neighbor_id);
            }
        }

        info!("User {} follows {} users", user_id, followed_users.len());

        let mut recommendation_scores: std::collections::HashMap<VertexId, u32> =
            std::collections::HashMap::new();

        // For each user we follow, see who they follow
        for followed_user_id in followed_users.iter().take(10) {
            // Limit for performance
            let all_their_neighbors = self.db.graph().get_neighbors(*followed_user_id).await?;

            // Filter to only user vertices
            let mut their_follows = Vec::new();
            for neighbor_id in all_their_neighbors {
                if let Some(_) = self.get_user_by_id(neighbor_id).await? {
                    their_follows.push(neighbor_id);
                }
            }

            for potential_rec in their_follows {
                if potential_rec != user_id && !followed_users.contains(&potential_rec) {
                    *recommendation_scores.entry(potential_rec).or_insert(0) += 1;
                }
            }
        }

        info!(
            "Found {} potential recommendations",
            recommendation_scores.len()
        );

        // Sort recommendation scores by score first, then take the top ones
        let mut sorted_recommendations: Vec<_> = recommendation_scores.iter().collect();
        sorted_recommendations.sort_by(|a, b| b.1.cmp(a.1)); // Sort by score descending

        let mut recommendations = Vec::new();

        for (rec_user_id, score) in sorted_recommendations.iter().take(limit as usize) {
            if let Some(user) = self.get_user_by_id(**rec_user_id).await? {
                recommendations.push(UserRecommendation {
                    user,
                    reason: format!("{} mutual connections", score),
                    score: **score as f64,
                    mutual_connections: **score,
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
        // Simplified search - in practice, we'd use proper text indexing
        let traversal = GremlinTraversal::v(None).has(
            "type".to_string(),
            Some(PropertyValue::String("user".to_string())),
        );

        let result_set = self.db.gremlin(&traversal).await?;
        let mut matching_users = Vec::new();

        for result in result_set.results {
            if let Some(user_id) = result.as_vertex() {
                if let Some(user) = self.get_user_by_id(user_id).await? {
                    if user.username.contains(query) || user.display_name.contains(query) {
                        matching_users.push(user);
                    }
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
