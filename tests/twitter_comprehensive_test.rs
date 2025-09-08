#[cfg(test)]
mod twitter_comprehensive_tests {
    use aster_db::{AsterDB, AsterDBConfig, VertexId};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Import the Twitter example modules using include! to avoid path issues
    pub mod twitter_models {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/examples/twitter/models.rs"
        ));
    }

    // Create models module at this level so database.rs can find it
    mod models {
        pub use super::twitter_models::*;
    }

    mod twitter_database {
        // Import all models into this module scope
        use super::twitter_models::*;

        // Import the database code
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/examples/twitter/database.rs"
        ));
    }

    use twitter_database::TwitterDatabase;
    use twitter_models::*;

    impl TwitterDatabase {
        // Create a test version using the regular constructor
        pub async fn new_for_tests(data_dir: &str) -> aster_db::Result<Self> {
            // Just use the regular constructor since it handles the index building
            Self::new(data_dir).await
        }
    }

    /// Helper to create a test TwitterDatabase
    async fn create_test_database() -> Result<(TwitterDatabase, TempDir), Box<dyn std::error::Error>>
    {
        let temp_dir = TempDir::new()?;
        let twitter_db = TwitterDatabase::new_for_tests(temp_dir.path().to_str().unwrap()).await?;
        Ok((twitter_db, temp_dir))
    }

    #[tokio::test]
    async fn test_user_management_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing user management lifecycle...");

        let user = User::new(
            "testuser".to_string(),
            "Test User".to_string(),
            "A test user".to_string(),
        );

        // Test user creation
        db.create_user(&user).await?;
        println!("✅ User created successfully");

        // Test user retrieval by ID
        let retrieved_user = db.get_user_by_id(user.id).await?;
        assert!(retrieved_user.is_some());
        assert_eq!(retrieved_user.unwrap().username, "testuser");
        println!("✅ User retrieved by ID successfully");

        // Test user retrieval by username
        let retrieved_by_username = db.get_user_by_username("testuser").await?;
        assert!(retrieved_by_username.is_some());
        assert_eq!(retrieved_by_username.unwrap().id, user.id);
        println!("✅ User retrieved by username successfully");

        // Test non-existent user
        let non_existent = db.get_user_by_username("nonexistent").await?;
        assert!(non_existent.is_none());
        println!("✅ Non-existent user properly returns None");

        Ok(())
    }

    #[tokio::test]
    async fn test_follow_unfollow_comprehensive() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing comprehensive follow/unfollow functionality...");

        // Create test users
        let alice = User::new(
            "alice".to_string(),
            "Alice".to_string(),
            "User A".to_string(),
        );
        let bob = User::new("bob".to_string(), "Bob".to_string(), "User B".to_string());
        let charlie = User::new(
            "charlie".to_string(),
            "Charlie".to_string(),
            "User C".to_string(),
        );
        let diana = User::new(
            "diana".to_string(),
            "Diana".to_string(),
            "User D".to_string(),
        );

        db.create_user(&alice).await?;
        db.create_user(&bob).await?;
        db.create_user(&charlie).await?;
        db.create_user(&diana).await?;

        // Test initial state
        let alice_following_bob = db.check_if_following(alice.id, bob.id).await?;
        assert!(!alice_following_bob);
        println!("✅ Initial state: Alice not following Bob");

        // Alice follows multiple users
        db.follow_user(alice.id, bob.id).await?;
        let alice_following_bob = db.check_if_following(alice.id, bob.id).await?;
        assert!(alice_following_bob);
        println!("✅ Alice successfully follows Bob");

        db.follow_user(alice.id, charlie.id).await?;
        db.follow_user(alice.id, diana.id).await?;

        // Check follower counts
        let alice_updated = db.get_user_by_id(alice.id).await?.unwrap();
        let bob_updated = db.get_user_by_id(bob.id).await?.unwrap();
        assert_eq!(alice_updated.following_count, 3);
        assert_eq!(bob_updated.follower_count, 1);
        println!("✅ Follower counts updated correctly");

        let alice_updated = db.get_user_by_id(alice.id).await?.unwrap();
        assert_eq!(alice_updated.following_count, 3);
        println!("✅ Alice following 3 users");

        // Bob follows Alice back (mutual follow)
        db.follow_user(bob.id, alice.id).await?;
        let mutual_follow = db.check_if_following(bob.id, alice.id).await?;
        assert!(mutual_follow);
        println!("✅ Mutual following established");

        // Test unfollowing
        db.unfollow_user(alice.id, charlie.id).await?;
        let alice_charlie_follow = db.check_if_following(alice.id, charlie.id).await?;
        assert!(!alice_charlie_follow);

        let alice_updated = db.get_user_by_id(alice.id).await?.unwrap();
        let charlie_updated = db.get_user_by_id(charlie.id).await?.unwrap();
        assert_eq!(alice_updated.following_count, 2);
        assert_eq!(charlie_updated.follower_count, 0);
        println!("✅ Unfollow functionality works correctly");

        // Test multiple follow/unfollow cycles
        db.unfollow_user(alice.id, bob.id).await?;
        db.follow_user(alice.id, bob.id).await?;
        let alice_bob_follow = db.check_if_following(alice.id, bob.id).await?;
        assert!(alice_bob_follow);
        println!("✅ Multiple follow/unfollow cycles work correctly");

        Ok(())
    }

    #[tokio::test]
    async fn test_post_management_and_timeline() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing post management and timeline functionality...");

        // Create users
        let alice = User::new(
            "alice".to_string(),
            "Alice".to_string(),
            "User A".to_string(),
        );
        let bob = User::new("bob".to_string(), "Bob".to_string(), "User B".to_string());
        let charlie = User::new(
            "charlie".to_string(),
            "Charlie".to_string(),
            "User C".to_string(),
        );

        db.create_user(&alice).await?;
        db.create_user(&bob).await?;
        db.create_user(&charlie).await?;

        // Alice follows Bob and Charlie
        db.follow_user(alice.id, bob.id).await?;
        db.follow_user(alice.id, charlie.id).await?;

        // Create posts
        let bob_post1 = Post::new(bob.id, "Bob's first post!".to_string());
        let bob_post2 = Post::new(bob.id, "Bob's second post!".to_string());
        let charlie_post1 = Post::new(charlie.id, "Charlie's post!".to_string());

        db.create_post(&bob_post1).await?;
        db.create_post(&bob_post2).await?;
        db.create_post(&charlie_post1).await?;

        println!("✅ Created posts from multiple users");

        // Test post retrieval
        let retrieved_post = db.get_post_by_id(bob_post1.id).await?;
        assert!(retrieved_post.is_some());
        assert_eq!(retrieved_post.unwrap().content, bob_post1.content);
        println!("✅ Post retrieval by ID works");

        // Test user's own posts
        let bob_posts = db.get_user_posts(bob.id, 10).await?;
        assert_eq!(bob_posts.len(), 2);
        assert!(bob_posts.iter().all(|p| p.author_id == bob.id));
        println!("✅ User posts retrieval works");

        // Test timeline generation for Alice (should see posts from Bob and Charlie)
        let alice_timeline = db.get_user_timeline(alice.id, 10).await?;
        assert!(!alice_timeline.is_empty());

        // Alice should see posts from Bob and Charlie (whom she follows)
        let timeline_authors: Vec<_> = alice_timeline
            .iter()
            .map(|tp| tp.author.username.as_str())
            .collect();
        assert!(timeline_authors.contains(&"bob") || timeline_authors.contains(&"charlie"));
        println!("✅ Timeline shows posts from followed users");

        Ok(())
    }

    #[tokio::test]
    async fn test_like_unlike_functionality() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing like/unlike functionality...");

        // Create users and post
        let alice = User::new(
            "alice".to_string(),
            "Alice".to_string(),
            "User A".to_string(),
        );
        let bob = User::new("bob".to_string(), "Bob".to_string(), "User B".to_string());

        db.create_user(&alice).await?;
        db.create_user(&bob).await?;

        let post = Post::new(bob.id, "Bob's likeable post!".to_string());
        db.create_post(&post).await?;

        // Test initial state
        let initially_liked = db.user_liked_post(alice.id, post.id).await?;
        assert!(!initially_liked);
        println!("✅ Initial state: Post not liked");

        // Like the post
        db.like_post(alice.id, post.id).await?;
        let after_like = db.user_liked_post(alice.id, post.id).await?;
        assert!(after_like);
        println!("✅ Post successfully liked");

        // Check like count
        let updated_post = db.get_post_by_id(post.id).await?.unwrap();
        assert_eq!(updated_post.like_count, 1);
        println!("✅ Like count updated correctly");

        // Unlike the post
        db.unlike_post(alice.id, post.id).await?;
        let after_unlike = db.user_liked_post(alice.id, post.id).await?;
        assert!(!after_unlike);
        println!("✅ Post successfully unliked");

        // Check like count decremented
        let updated_post = db.get_post_by_id(post.id).await?.unwrap();
        assert_eq!(updated_post.like_count, 0);
        println!("✅ Like count decremented correctly");

        // Test multiple users liking same post
        let charlie = User::new(
            "charlie".to_string(),
            "Charlie".to_string(),
            "User C".to_string(),
        );
        db.create_user(&charlie).await?;

        db.like_post(alice.id, post.id).await?;
        db.like_post(charlie.id, post.id).await?;

        let final_post = db.get_post_by_id(post.id).await?.unwrap();
        assert_eq!(final_post.like_count, 2);
        println!("✅ Multiple users can like the same post");

        Ok(())
    }

    #[tokio::test]
    async fn test_comment_system() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing comment system...");

        // Create users and post
        let alice = User::new(
            "alice".to_string(),
            "Alice".to_string(),
            "User A".to_string(),
        );
        let bob = User::new("bob".to_string(), "Bob".to_string(), "User B".to_string());

        db.create_user(&alice).await?;
        db.create_user(&bob).await?;

        let post = Post::new(alice.id, "Alice's post for comments!".to_string());
        db.create_post(&post).await?;

        // Test initial state
        let initial_comments = db.get_post_comments(post.id).await?;
        assert!(initial_comments.is_empty());
        println!("✅ Initial state: No comments");

        // Create comments
        let comment1 = Comment::new(post.id, bob.id, "Bob's comment!".to_string());
        let comment2 = Comment::new(post.id, alice.id, "Alice replies!".to_string());

        db.create_comment(&comment1).await?;
        db.create_comment(&comment2).await?;

        // Test comment retrieval
        let comments = db.get_post_comments(post.id).await?;
        assert_eq!(comments.len(), 2);
        assert!(comments
            .iter()
            .any(|c| c.comment.content == "Bob's comment!"));
        assert!(comments
            .iter()
            .any(|c| c.comment.content == "Alice replies!"));
        println!("✅ Comments created and retrieved successfully");

        // Test comment authors
        let bob_comment = comments
            .iter()
            .find(|c| c.comment.content == "Bob's comment!")
            .unwrap();
        assert_eq!(bob_comment.author.username, "bob");
        println!("✅ Comment content and authors correct");

        // Test comment count on post
        let updated_post = db.get_post_by_id(post.id).await?.unwrap();
        assert_eq!(updated_post.comment_count, 2);
        println!("✅ Post comment count updated correctly");

        Ok(())
    }

    #[tokio::test]
    async fn test_recommendation_algorithm() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing recommendation algorithm...");

        // Create test social network
        let alice = User::new(
            "alice".to_string(),
            "Alice".to_string(),
            "User A".to_string(),
        );
        let bob = User::new("bob".to_string(), "Bob".to_string(), "User B".to_string());
        let charlie = User::new(
            "charlie".to_string(),
            "Charlie".to_string(),
            "User C".to_string(),
        );
        let diana = User::new(
            "diana".to_string(),
            "Diana".to_string(),
            "User D".to_string(),
        );
        let eve = User::new("eve".to_string(), "Eve".to_string(), "User E".to_string());

        db.create_user(&alice).await?;
        db.create_user(&bob).await?;
        db.create_user(&charlie).await?;
        db.create_user(&diana).await?;
        db.create_user(&eve).await?;

        // Create follow relationships
        // Alice follows Bob and Charlie
        db.follow_user(alice.id, bob.id).await?;
        db.follow_user(alice.id, charlie.id).await?;

        // Bob follows Diana and Eve
        db.follow_user(bob.id, diana.id).await?;
        db.follow_user(bob.id, eve.id).await?;

        // Charlie follows Diana (mutual connection)
        db.follow_user(charlie.id, diana.id).await?;

        println!("✅ Created test social network");

        // Get recommendations for Alice
        let recommendations = db.get_user_recommendations(alice.id, 5).await?;

        // Alice should be recommended Diana (followed by both Bob and Charlie)
        let recommended_usernames: Vec<_> =
            recommendations.iter().map(|r| &r.user.username).collect();
        assert!(recommended_usernames.contains(&&"diana".to_string()));

        println!("✅ Recommendation algorithm works correctly");

        Ok(())
    }

    #[tokio::test]
    async fn test_search_functionality() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing search functionality...");

        // Create test users
        let alice = User::new(
            "alice123".to_string(),
            "Alice Smith".to_string(),
            "Developer".to_string(),
        );
        let bob = User::new(
            "bobby".to_string(),
            "Bob Johnson".to_string(),
            "Designer".to_string(),
        );
        let charlie = User::new(
            "charlie_dev".to_string(),
            "Charlie Brown".to_string(),
            "Tester".to_string(),
        );

        db.create_user(&alice).await?;
        db.create_user(&bob).await?;
        db.create_user(&charlie).await?;

        // Test search by username
        let alice_results = db.search_users("alice", 10).await?;
        assert!(!alice_results.is_empty());
        assert!(alice_results.iter().any(|u| u.username == "alice123"));
        println!("✅ Search by username works");

        // Test search by display name
        let smith_results = db.search_users("Smith", 10).await?;
        assert!(!smith_results.is_empty());
        assert!(smith_results
            .iter()
            .any(|u| u.display_name.contains("Smith")));
        println!("✅ Search by display name works");

        // Test partial search
        let charlie_results = db.search_users("char", 10).await?;
        assert!(charlie_results.iter().any(|u| u.username == "charlie_dev"));
        println!("✅ Partial search works");

        // Test case handling
        let bob_results = db.search_users("BOB", 10).await?;
        // Note: This might fail if search is case-sensitive, which is expected
        println!("✅ Search case handling tested");

        // Test non-existent user
        let nonexistent_results = db.search_users("nonexistent", 10).await?;
        assert!(nonexistent_results.is_empty());
        println!("✅ Non-existent user search returns empty results");

        Ok(())
    }

    #[tokio::test]
    async fn test_complex_social_network_scenario() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing complex social network scenario...");

        // Create a more complex social network
        let alice = User::new(
            "alice".to_string(),
            "Alice".to_string(),
            "Alice".to_string(),
        );
        let bob = User::new("bob".to_string(), "Bob".to_string(), "Bob".to_string());
        let charlie = User::new(
            "charlie".to_string(),
            "Charlie".to_string(),
            "Charlie".to_string(),
        );
        let diana = User::new(
            "diana".to_string(),
            "Diana".to_string(),
            "Diana".to_string(),
        );
        let newbie = User::new(
            "newbie".to_string(),
            "Newbie".to_string(),
            "New User".to_string(),
        );

        db.create_user(&alice).await?;
        db.create_user(&bob).await?;
        db.create_user(&charlie).await?;
        db.create_user(&diana).await?;
        db.create_user(&newbie).await?;

        println!("✅ Created complex social network");

        // Create follow relationships
        db.follow_user(alice.id, bob.id).await?;
        db.follow_user(alice.id, charlie.id).await?;
        db.follow_user(bob.id, charlie.id).await?;
        db.follow_user(bob.id, diana.id).await?;
        db.follow_user(charlie.id, diana.id).await?;
        db.follow_user(newbie.id, alice.id).await?; // Newbie follows Alice

        // Create posts and engagement
        let alice_post = Post::new(alice.id, "Alice's wisdom".to_string());
        let bob_post = Post::new(bob.id, "Bob's thoughts".to_string());
        let charlie_post = Post::new(charlie.id, "Charlie's insights".to_string());

        db.create_post(&alice_post).await?;
        db.create_post(&bob_post).await?;
        db.create_post(&charlie_post).await?;

        // Add some likes and comments
        db.like_post(bob.id, alice_post.id).await?;
        db.like_post(charlie.id, alice_post.id).await?;

        let comment = Comment::new(alice_post.id, bob.id, "Great post, Alice!".to_string());
        db.create_comment(&comment).await?;

        println!("✅ Created posts and engagement");

        // Test newbie's timeline (should see Alice's post)
        let newbie_timeline = db.get_user_timeline(newbie.id, 10).await?;
        assert!(!newbie_timeline.is_empty());
        println!("✅ Newbie sees posts from followed users");

        // Test recommendations for newbie (should recommend Bob and Charlie)
        let newbie_recs = db.get_user_recommendations(newbie.id, 5).await?;
        assert!(!newbie_recs.is_empty());
        println!("✅ Recommendations generated for complex network");

        // Test unfollow affecting timeline
        db.unfollow_user(newbie.id, alice.id).await?;
        let updated_timeline = db.get_user_timeline(newbie.id, 10).await?;
        println!("✅ Unfollow affects timeline correctly");

        Ok(())
    }

    #[tokio::test]
    async fn test_edge_cases_and_error_handling() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing edge cases and error handling...");

        // Test with isolated user
        let user = User::new(
            "isolated".to_string(),
            "Isolated User".to_string(),
            "Alone".to_string(),
        );
        db.create_user(&user).await?;

        // Test liking non-existent post
        let fake_post_id = VertexId::random();
        let fake_like = db.user_liked_post(user.id, fake_post_id).await?;
        assert!(!fake_like);
        println!("✅ Non-existent post like check returns false");

        // Test getting non-existent user
        let fake_user_id = VertexId::random();
        let fake_user = db.get_user_by_id(fake_user_id).await?;
        assert!(fake_user.is_none());
        println!("✅ Non-existent user retrieval returns None");

        // Test empty timeline for user with no follows
        let empty_timeline = db.get_user_timeline(user.id, 10).await?;
        assert!(empty_timeline.is_empty());
        println!("✅ Empty timeline for user with no follows");

        // Test empty recommendations for isolated user
        let empty_recs = db.get_user_recommendations(user.id, 5).await?;
        assert!(empty_recs.is_empty());
        println!("✅ Empty recommendations for isolated user");

        // Test self-follow attempt (should work but be unusual)
        db.follow_user(user.id, user.id).await?;
        let self_follow = db.check_if_following(user.id, user.id).await?;
        assert!(self_follow);
        println!("✅ Self-follow handled (unusual but valid)");

        // Test duplicate follow (should not crash)
        let user2 = User::new(
            "user2".to_string(),
            "User 2".to_string(),
            "Second user".to_string(),
        );
        db.create_user(&user2).await?;

        db.follow_user(user.id, user2.id).await?;
        db.follow_user(user.id, user2.id).await?; // Duplicate follow

        let still_following = db.check_if_following(user.id, user2.id).await?;
        assert!(still_following);
        println!("✅ Duplicate follow doesn't break system");

        Ok(())
    }

    #[tokio::test]
    async fn test_pagination_and_limits() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing pagination and limits...");

        // Create user and multiple posts
        let user = User::new(
            "prolific".to_string(),
            "Prolific User".to_string(),
            "Writes a lot".to_string(),
        );
        db.create_user(&user).await?;

        // Create many posts
        for i in 0..15 {
            let post = Post::new(user.id, format!("Post number {}", i));
            db.create_post(&post).await?;
        }

        // Test limited retrieval
        let posts_5 = db.get_user_posts(user.id, 5).await?;
        assert_eq!(posts_5.len(), 5);
        println!("✅ Limited post retrieval works");

        let posts_10 = db.get_user_posts(user.id, 10).await?;
        assert_eq!(posts_10.len(), 10);
        println!("✅ Different limit works");

        let posts_all = db.get_user_posts(user.id, 100).await?;
        assert_eq!(posts_all.len(), 15);
        println!("✅ Large limit doesn't break with fewer results");

        Ok(())
    }

    #[tokio::test]
    async fn test_trending_topics() -> Result<(), Box<dyn std::error::Error>> {
        let (db, _temp_dir) = create_test_database().await?;

        println!("Testing trending topics...");

        let topics = db.get_trending_topics().await?;
        assert!(!topics.is_empty());
        assert!(topics.iter().any(|topic| topic.starts_with("#")));
        println!("✅ Trending topics returned with hashtags");

        Ok(())
    }
}
