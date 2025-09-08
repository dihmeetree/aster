use super::database::TwitterDatabase;
use super::models::*;
use askama::Template;
use aster_db::VertexId;
use axum::{
    extract::{Form, Path, Query, State},
    http::StatusCode,
    response::{Html, Redirect},
    Json,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

pub type AppState = Arc<RwLock<TwitterDatabase>>;

#[derive(Template)]
#[template(path = "layout.html")]
pub struct Layout<'a> {
    pub title: &'a str,
    pub content: &'a str,
}

#[derive(Template)]
#[template(path = "home.html")]
pub struct HomeTemplate {
    pub posts: Vec<TimelinePost>,
    pub recommendations: Vec<UserRecommendation>,
    pub trending: Vec<String>,
    pub current_user: Option<User>,
    pub is_logged_in: bool,
    pub current_username: String,
}

#[derive(Template)]
#[template(path = "profile.html")]
pub struct ProfileTemplate {
    pub profile_user: User,
    pub posts: Vec<Post>,
    pub following: bool,
    pub followed_by: bool,
    pub current_user: Option<User>,
    pub is_logged_in: bool,
    pub current_username: String,
}

#[derive(Template)]
#[template(path = "post.html")]
pub struct PostTemplate {
    pub post: Post,
    pub author: User,
    pub comments: Vec<CommentWithAuthor>,
    pub user_liked: bool,
    pub current_user: Option<User>,
    pub is_logged_in: bool,
    pub current_username: String,
}

#[derive(Template)]
#[template(path = "login.html")]
pub struct LoginTemplate {
    pub error: String,
}

pub async fn home(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Html<String>, StatusCode> {
    let (current_user_id, db_clone) = {
        let db = state.read().await;
        (db.current_user_id, db.clone())
    };

    let current_user = if let Some(user_id) = current_user_id {
        db_clone.get_user_by_id(user_id).await.map_err(|e| {
            error!("Failed to get current user: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
    } else {
        None
    };

    let posts = if let Some(user_id) = current_user_id {
        let limit = params.limit.unwrap_or(20);
        db_clone
            .get_user_timeline(user_id, limit)
            .await
            .map_err(|e| {
                error!("Failed to get timeline: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
    } else {
        Vec::new()
    };

    let recommendations = if let Some(user_id) = current_user_id {
        db_clone
            .get_user_recommendations(user_id, 5)
            .await
            .map_err(|e| {
                error!("Failed to get recommendations: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
    } else {
        Vec::new()
    };

    let trending = db_clone.get_trending_topics().await.map_err(|e| {
        error!("Failed to get trending topics: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let (is_logged_in, current_username) = match &current_user {
        Some(user) => (true, user.username.clone()),
        None => (false, String::new()),
    };

    let template = HomeTemplate {
        posts,
        recommendations,
        trending,
        current_user,
        is_logged_in,
        current_username,
    };

    Ok(Html(template.render().map_err(|e| {
        error!("Template render error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?))
}

pub async fn profile(
    State(state): State<AppState>,
    Path(username): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Html<String>, StatusCode> {
    let (current_user_id, db_clone) = {
        let db = state.read().await;
        (db.current_user_id, db.clone())
    };

    // Use combined query to reduce database round trips
    let profile_data = db_clone
        .get_profile_data(&username, current_user_id)
        .await
        .map_err(|e| {
            error!("Failed to get profile data: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let (profile_user, posts, following, followed_by) = profile_data;

    let current_user = if let Some(user_id) = current_user_id {
        db_clone.get_user_by_id(user_id).await.map_err(|e| {
            error!("Failed to get current user: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
    } else {
        None
    };

    let (is_logged_in, current_username) = match &current_user {
        Some(user) => (true, user.username.clone()),
        None => (false, String::new()),
    };

    let template = ProfileTemplate {
        profile_user,
        posts,
        following,
        followed_by,
        current_user,
        is_logged_in,
        current_username,
    };

    Ok(Html(template.render().map_err(|e| {
        error!("Template render error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?))
}

pub async fn view_post(
    State(state): State<AppState>,
    Path(post_id_str): Path<String>,
) -> Result<Html<String>, StatusCode> {
    let (current_user_id, db_clone) = {
        let db = state.read().await;
        (db.current_user_id, db.clone())
    };

    let post_id = VertexId::from_string(&post_id_str).ok_or(StatusCode::BAD_REQUEST)?;

    let post = db_clone
        .get_post_by_id(post_id)
        .await
        .map_err(|e| {
            error!("Failed to get post: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let author = db_clone
        .get_user_by_id(post.author_id)
        .await
        .map_err(|e| {
            error!("Failed to get post author: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let current_user = if let Some(user_id) = current_user_id {
        db_clone.get_user_by_id(user_id).await.map_err(|e| {
            error!("Failed to get current user: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
    } else {
        None
    };

    let comments = db_clone.get_post_comments(post_id).await.map_err(|e| {
        error!("Failed to get post comments: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let user_liked = if let Some(user_id) = current_user_id {
        db_clone
            .user_liked_post(user_id, post_id)
            .await
            .unwrap_or(false)
    } else {
        false
    };

    let (is_logged_in, current_username) = match &current_user {
        Some(user) => (true, user.username.clone()),
        None => (false, String::new()),
    };

    let template = PostTemplate {
        post,
        author,
        comments,
        user_liked,
        current_user,
        is_logged_in,
        current_username,
    };

    Ok(Html(template.render().map_err(|e| {
        error!("Template render error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?))
}

pub async fn login_form() -> Result<Html<String>, StatusCode> {
    let template = LoginTemplate {
        error: String::new(),
    };
    Ok(Html(template.render().map_err(|e| {
        error!("Template render error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?))
}

pub async fn login(
    State(state): State<AppState>,
    Form(params): Form<UserParams>,
) -> Result<Redirect, StatusCode> {
    let user = {
        let db_clone = {
            let db = state.read().await;
            db.clone()
        };
        db_clone
            .get_user_by_username(&params.username)
            .await
            .map_err(|e| {
                error!("Failed to get user by username: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
    };

    if let Some(user) = user {
        {
            let mut db = state.write().await;
            db.set_current_user(user.id);
        }
        info!("User {} logged in", user.username);
        Ok(Redirect::to("/"))
    } else {
        // For demo purposes, create the user if they don't exist
        let new_user = User::new(
            params.username.clone(),
            params.username.clone(),
            format!("Bio for {}", params.username),
        );

        {
            let db_clone = {
                let db = state.read().await;
                db.clone()
            };
            db_clone.create_user(&new_user).await.map_err(|e| {
                error!("Failed to create user: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        }

        {
            let mut db = state.write().await;
            db.set_current_user(new_user.id);
        }

        info!("Created and logged in user {}", new_user.username);
        Ok(Redirect::to("/"))
    }
}

pub async fn logout(State(state): State<AppState>) -> Result<Redirect, StatusCode> {
    let mut db = state.write().await;
    db.current_user_id = None;
    info!("User logged out");
    Ok(Redirect::to("/login"))
}

pub async fn create_post(
    State(state): State<AppState>,
    Form(params): Form<PostParams>,
) -> Result<Redirect, StatusCode> {
    let (current_user_id, db_clone) = {
        let db = state.read().await;
        let current_user_id = db.current_user_id.ok_or(StatusCode::UNAUTHORIZED)?;
        (current_user_id, db.clone())
    };

    let post = Post::new(current_user_id, params.content);

    db_clone.create_post(&post).await.map_err(|e| {
        error!("Failed to create post: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    info!("Created post: {}", post.id);
    Ok(Redirect::to(&format!("/post/{}", post.id)))
}

pub async fn like_post(
    State(state): State<AppState>,
    Path(post_id_str): Path<String>,
) -> Result<Redirect, StatusCode> {
    let (current_user_id, db_clone) = {
        let db = state.read().await;
        let current_user_id = db.current_user_id.ok_or(StatusCode::UNAUTHORIZED)?;
        (current_user_id, db.clone())
    };

    let post_id = VertexId::from_string(&post_id_str).ok_or(StatusCode::BAD_REQUEST)?;

    // Test just the user_liked_post method
    let already_liked = db_clone
        .user_liked_post(current_user_id, post_id)
        .await
        .unwrap_or(false);

    if already_liked {
        db_clone
            .unlike_post(current_user_id, post_id)
            .await
            .map_err(|e| {
                error!("Failed to unlike post: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        info!("User {} unliked post {}", current_user_id, post_id);
    } else {
        db_clone
            .like_post(current_user_id, post_id)
            .await
            .map_err(|e| {
                error!("Failed to like post: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        info!("User {} liked post {}", current_user_id, post_id);
    }

    Ok(Redirect::to(&format!("/post/{}", post_id_str)))
}

pub async fn create_comment(
    State(state): State<AppState>,
    Form(params): Form<CommentParams>,
) -> Result<Redirect, StatusCode> {
    let (current_user_id, db_clone) = {
        let db = state.read().await;
        let current_user_id = db.current_user_id.ok_or(StatusCode::UNAUTHORIZED)?;
        (current_user_id, db.clone())
    };

    let post_id = VertexId::from_string(&params.post_id).ok_or(StatusCode::BAD_REQUEST)?;

    let comment = Comment::new(post_id, current_user_id, params.content);

    db_clone.create_comment(&comment).await.map_err(|e| {
        error!("Failed to create comment: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    info!("Created comment: {}", comment.id);
    Ok(Redirect::to(&format!("/post/{}", params.post_id)))
}

pub async fn follow_user(
    State(state): State<AppState>,
    Path(username): Path<String>,
) -> Result<Redirect, StatusCode> {
    let (current_user_id, db_clone) = {
        let db = state.read().await;
        let current_user_id = db.current_user_id.ok_or(StatusCode::UNAUTHORIZED)?;
        (current_user_id, db.clone())
    };

    let target_user = db_clone
        .get_user_by_username(&username)
        .await
        .map_err(|e| {
            error!("Failed to get user by username: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let already_following = db_clone
        .check_if_following(current_user_id, target_user.id)
        .await
        .unwrap_or(false);

    if already_following {
        db_clone
            .unfollow_user(current_user_id, target_user.id)
            .await
            .map_err(|e| {
                error!("Failed to unfollow user: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        info!("User {} unfollowed {}", current_user_id, target_user.id);
    } else {
        db_clone
            .follow_user(current_user_id, target_user.id)
            .await
            .map_err(|e| {
                error!("Failed to follow user: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        info!("User {} followed {}", current_user_id, target_user.id);
    }

    Ok(Redirect::to(&format!("/profile/{}", username)))
}

// API endpoints for AJAX requests
pub async fn api_recommendations(
    State(state): State<AppState>,
) -> Result<Json<Vec<UserRecommendation>>, StatusCode> {
    let (current_user_id, db_clone) = {
        let db = state.read().await;
        let current_user_id = db.current_user_id.ok_or(StatusCode::UNAUTHORIZED)?;
        (current_user_id, db.clone())
    };

    let recommendations = db_clone
        .get_user_recommendations(current_user_id, 5)
        .await
        .map_err(|e| {
            error!("Failed to get recommendations: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(recommendations))
}

pub async fn api_search_users(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<User>>, StatusCode> {
    let db_clone = {
        let db = state.read().await;
        db.clone()
    };

    let query = params.get("q").ok_or(StatusCode::BAD_REQUEST)?;

    let users = db_clone.search_users(query, 10).await.map_err(|e| {
        error!("Failed to search users: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(users))
}
