use aster_db::{Properties, PropertyValue, VertexId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: VertexId,
    pub username: String,
    pub display_name: String,
    pub bio: String,
    pub follower_count: u32,
    pub following_count: u32,
    pub created_at: DateTime<Utc>,
}

impl User {
    pub fn new(username: String, display_name: String, bio: String) -> Self {
        Self {
            id: VertexId::random(),
            username,
            display_name,
            bio,
            follower_count: 0,
            following_count: 0,
            created_at: Utc::now(),
        }
    }

    pub fn to_properties(&self) -> Properties {
        let mut props = Properties::new();
        props.insert(
            "type".to_string(),
            PropertyValue::String("user".to_string()),
        );
        props.insert(
            "username".to_string(),
            PropertyValue::String(self.username.clone()),
        );
        props.insert(
            "display_name".to_string(),
            PropertyValue::String(self.display_name.clone()),
        );
        props.insert("bio".to_string(), PropertyValue::String(self.bio.clone()));
        props.insert(
            "follower_count".to_string(),
            PropertyValue::Int(self.follower_count as i64),
        );
        props.insert(
            "following_count".to_string(),
            PropertyValue::Int(self.following_count as i64),
        );
        props.insert(
            "created_at".to_string(),
            PropertyValue::String(self.created_at.to_rfc3339()),
        );
        props
    }

    pub fn from_properties(id: VertexId, props: &Properties) -> Option<Self> {
        Some(Self {
            id,
            username: props.get("username")?.as_string()?.to_string(),
            display_name: props.get("display_name")?.as_string()?.to_string(),
            bio: props.get("bio")?.as_string()?.to_string(),
            follower_count: props.get("follower_count")?.as_int()? as u32,
            following_count: props.get("following_count")?.as_int()? as u32,
            created_at: DateTime::parse_from_rfc3339(props.get("created_at")?.as_string()?)
                .ok()?
                .with_timezone(&Utc),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Post {
    pub id: VertexId,
    pub author_id: VertexId,
    pub content: String,
    pub like_count: u32,
    pub comment_count: u32,
    pub repost_count: u32,
    pub created_at: DateTime<Utc>,
}

impl Post {
    pub fn new(author_id: VertexId, content: String) -> Self {
        Self {
            id: VertexId::random(),
            author_id,
            content,
            like_count: 0,
            comment_count: 0,
            repost_count: 0,
            created_at: Utc::now(),
        }
    }

    pub fn to_properties(&self) -> Properties {
        let mut props = Properties::new();
        props.insert(
            "type".to_string(),
            PropertyValue::String("post".to_string()),
        );
        props.insert(
            "author_id".to_string(),
            PropertyValue::String(self.author_id.to_string()),
        );
        props.insert(
            "content".to_string(),
            PropertyValue::String(self.content.clone()),
        );
        props.insert(
            "like_count".to_string(),
            PropertyValue::Int(self.like_count as i64),
        );
        props.insert(
            "comment_count".to_string(),
            PropertyValue::Int(self.comment_count as i64),
        );
        props.insert(
            "repost_count".to_string(),
            PropertyValue::Int(self.repost_count as i64),
        );
        props.insert(
            "created_at".to_string(),
            PropertyValue::String(self.created_at.to_rfc3339()),
        );
        props
    }

    pub fn from_properties(id: VertexId, props: &Properties) -> Option<Self> {
        Some(Self {
            id,
            author_id: VertexId::from_string(props.get("author_id")?.as_string()?)?,
            content: props.get("content")?.as_string()?.to_string(),
            like_count: props.get("like_count")?.as_int()? as u32,
            comment_count: props.get("comment_count")?.as_int()? as u32,
            repost_count: props.get("repost_count")?.as_int()? as u32,
            created_at: DateTime::parse_from_rfc3339(props.get("created_at")?.as_string()?)
                .ok()?
                .with_timezone(&Utc),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Comment {
    pub id: VertexId,
    pub post_id: VertexId,
    pub author_id: VertexId,
    pub content: String,
    pub like_count: u32,
    pub created_at: DateTime<Utc>,
}

impl Comment {
    pub fn new(post_id: VertexId, author_id: VertexId, content: String) -> Self {
        Self {
            id: VertexId::random(),
            post_id,
            author_id,
            content,
            like_count: 0,
            created_at: Utc::now(),
        }
    }

    pub fn to_properties(&self) -> Properties {
        let mut props = Properties::new();
        props.insert(
            "type".to_string(),
            PropertyValue::String("comment".to_string()),
        );
        props.insert(
            "post_id".to_string(),
            PropertyValue::String(self.post_id.to_string()),
        );
        props.insert(
            "author_id".to_string(),
            PropertyValue::String(self.author_id.to_string()),
        );
        props.insert(
            "content".to_string(),
            PropertyValue::String(self.content.clone()),
        );
        props.insert(
            "like_count".to_string(),
            PropertyValue::Int(self.like_count as i64),
        );
        props.insert(
            "created_at".to_string(),
            PropertyValue::String(self.created_at.to_rfc3339()),
        );
        props
    }

    pub fn from_properties(id: VertexId, props: &Properties) -> Option<Self> {
        Some(Self {
            id,
            post_id: VertexId::from_string(props.get("post_id")?.as_string()?)?,
            author_id: VertexId::from_string(props.get("author_id")?.as_string()?)?,
            content: props.get("content")?.as_string()?.to_string(),
            like_count: props.get("like_count")?.as_int()? as u32,
            created_at: DateTime::parse_from_rfc3339(props.get("created_at")?.as_string()?)
                .ok()?
                .with_timezone(&Utc),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelinePost {
    pub post: Post,
    pub author: User,
    pub user_liked: bool,
    pub user_reposted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostWithComments {
    pub post: Post,
    pub author: User,
    pub comments: Vec<CommentWithAuthor>,
    pub user_liked: bool,
    pub user_reposted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentWithAuthor {
    pub comment: Comment,
    pub author: User,
    pub user_liked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfile {
    pub user: User,
    pub posts: Vec<Post>,
    pub following: bool,
    pub followed_by: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecommendation {
    pub user: User,
    pub reason: String,
    pub score: f64,
    pub mutual_connections: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedData {
    pub posts: Vec<TimelinePost>,
    pub recommendations: Vec<UserRecommendation>,
    pub trending_topics: Vec<String>,
}

// Edge types for the graph
pub enum EdgeType {
    Follows,
    Likes,
    Comments,
    Reposts,
    AuthoredPost,
    AuthoredComment,
}

impl EdgeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EdgeType::Follows => "follows",
            EdgeType::Likes => "likes",
            EdgeType::Comments => "comments",
            EdgeType::Reposts => "reposts",
            EdgeType::AuthoredPost => "authored_post",
            EdgeType::AuthoredComment => "authored_comment",
        }
    }

    pub fn to_properties(&self) -> Properties {
        let mut props = Properties::new();
        props.insert(
            "edge_type".to_string(),
            PropertyValue::String(self.as_str().to_string()),
        );
        props.insert(
            "created_at".to_string(),
            PropertyValue::String(Utc::now().to_rfc3339()),
        );
        props
    }
}

// Query parameters for different views
#[derive(Debug, Clone, Deserialize)]
pub struct PaginationParams {
    pub page: Option<u32>,
    pub limit: Option<u32>,
}

impl Default for PaginationParams {
    fn default() -> Self {
        Self {
            page: Some(1),
            limit: Some(20),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserParams {
    pub username: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostParams {
    pub content: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CommentParams {
    pub post_id: String,
    pub content: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LikeParams {
    pub target_id: String,
    pub target_type: String, // "post" or "comment"
}

#[derive(Debug, Clone, Deserialize)]
pub struct FollowParams {
    pub username: String,
}
