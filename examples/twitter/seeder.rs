use super::database::TwitterDatabase;
use super::models::*;
use aster_db::VertexId;
use rand::prelude::*;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{info, warn};

pub struct DatabaseSeeder {
    db: TwitterDatabase,
    rng: StdRng,
}

#[derive(Debug, Clone)]
pub struct SeedConfig {
    pub num_users: usize,
    pub num_posts: usize,
    pub num_comments: usize,
    pub avg_follows_per_user: usize,
    pub avg_likes_per_post: usize,
    pub batch_size: usize,
    pub enable_progress: bool,
}

impl Default for SeedConfig {
    fn default() -> Self {
        Self {
            num_users: 1_000_000,      // 1M users
            num_posts: 5_000_000,      // 5M posts
            num_comments: 10_000_000,  // 10M comments
            avg_follows_per_user: 50,  // Average follows per user
            avg_likes_per_post: 25,    // Average likes per post
            batch_size: 1000,          // Process in batches of 1000
            enable_progress: true,
        }
    }
}

impl SeedConfig {
    pub fn small_scale() -> Self {
        Self {
            num_users: 10_000,
            num_posts: 50_000,
            num_comments: 100_000,
            avg_follows_per_user: 20,
            avg_likes_per_post: 10,
            batch_size: 500,
            enable_progress: true,
        }
    }

    pub fn medium_scale() -> Self {
        Self {
            num_users: 100_000,
            num_posts: 500_000,
            num_comments: 1_000_000,
            avg_follows_per_user: 30,
            avg_likes_per_post: 15,
            batch_size: 1000,
            enable_progress: true,
        }
    }

    pub fn large_scale() -> Self {
        Self::default()
    }

    pub fn demo_scale() -> Self {
        Self {
            num_users: 100,
            num_posts: 500,
            num_comments: 1_000,
            avg_follows_per_user: 10,
            avg_likes_per_post: 5,
            batch_size: 50,
            enable_progress: true,
        }
    }
}

impl DatabaseSeeder {
    pub fn new(db: TwitterDatabase) -> Self {
        let rng = StdRng::from_entropy();
        Self { db, rng }
    }

    pub async fn seed_database(&mut self, config: &SeedConfig) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting database seeding with config: {:?}", config);
        let total_start = Instant::now();

        // Phase 1: Create users
        info!("Phase 1: Creating {} users...", config.num_users);
        let users = self.create_users(config).await?;
        info!("✅ Created {} users", users.len());

        // Phase 2: Create follow relationships
        info!("Phase 2: Creating follow relationships...");
        self.create_follows(&users, config).await?;
        info!("✅ Created follow relationships");

        // Phase 3: Create posts
        info!("Phase 3: Creating {} posts...", config.num_posts);
        let posts = self.create_posts(&users, config).await?;
        info!("✅ Created {} posts", posts.len());

        // Phase 4: Create likes
        info!("Phase 4: Creating likes...");
        self.create_likes(&users, &posts, config).await?;
        info!("✅ Created likes");

        // Phase 5: Create comments
        info!("Phase 5: Creating {} comments...", config.num_comments);
        self.create_comments(&users, &posts, config).await?;
        info!("✅ Created comments");

        let total_duration = total_start.elapsed();
        info!("🎉 Database seeding completed in {:.2?}", total_duration);
        
        // Print summary statistics
        self.print_statistics(config, total_duration).await?;

        Ok(())
    }

    // Legacy interface for compatibility
    pub async fn seed_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let config = SeedConfig::demo_scale();
        self.seed_database(&config).await
    }

    async fn create_users(&mut self, config: &SeedConfig) -> Result<Vec<User>, Box<dyn std::error::Error>> {
        let mut users = Vec::with_capacity(config.num_users);
        let start_time = Instant::now();

        for i in 0..config.num_users {
            let username = self.generate_username(i);
            let display_name = self.generate_display_name(&username);
            let bio = self.generate_bio();

            let user = User::new(username, display_name, bio);
            
            if let Err(e) = self.db.create_user(&user).await {
                warn!("Failed to create user {}: {}", i, e);
                continue;
            }

            users.push(user);

            // Progress reporting and throttling
            if config.enable_progress && (i + 1) % config.batch_size == 0 {
                let elapsed = start_time.elapsed();
                let rate = (i + 1) as f64 / elapsed.as_secs_f64();
                info!("Created {}/{} users ({:.0} users/sec)", i + 1, config.num_users, rate);
                
                // Small delay every batch to reduce write pressure
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        Ok(users)
    }

    async fn create_follows(&mut self, users: &[User], config: &SeedConfig) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        let mut total_follows = 0;
        let follow_batch_size = 5_000; // Process 5,000 follows at a time
        let mut pending_follows = Vec::with_capacity(follow_batch_size);

        info!("Generating follow relationships for {} users...", users.len());

        for (i, user) in users.iter().enumerate() {
            // Each user follows a random number of other users
            let num_follows = self.rng.gen_range(1..=(config.avg_follows_per_user * 2));
            
            let mut followed_users = std::collections::HashSet::new();
            
            for _ in 0..num_follows {
                // Pick a random user to follow (avoid self-follows)
                let mut target_idx = self.rng.gen_range(0..users.len());
                while target_idx == i || followed_users.contains(&target_idx) {
                    target_idx = self.rng.gen_range(0..users.len());
                }
                
                followed_users.insert(target_idx);
                let target_user = &users[target_idx];

                // Add to batch instead of executing immediately
                pending_follows.push((user.id, target_user.id));
                
                // Process batch when it reaches the limit
                if pending_follows.len() >= follow_batch_size {
                    let batch_start = Instant::now();
                    let batch_processed = self.process_follow_batch(&mut pending_follows).await;
                    total_follows += batch_processed;
                    
                    let batch_duration = batch_start.elapsed();
                    let batch_rate = batch_processed as f64 / batch_duration.as_secs_f64();
                    
                    if config.enable_progress {
                        info!("Processed batch of {} follows in {:.2?} ({:.0} follows/sec)", 
                              batch_processed, batch_duration, batch_rate);
                    }
                    
                    // Small delay to prevent overwhelming the database
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }

            // Progress reporting for users processed
            if config.enable_progress && (i + 1) % (config.batch_size / 10).max(1) == 0 {
                let elapsed = start_time.elapsed();
                let user_rate = (i + 1) as f64 / elapsed.as_secs_f64();
                info!("Processed follows for {}/{} users ({:.0} users/sec, {} total follows queued)", 
                      i + 1, users.len(), user_rate, total_follows + pending_follows.len());
            }
        }

        // Process any remaining follows in the final batch
        if !pending_follows.is_empty() {
            let final_batch_start = Instant::now();
            let final_processed = self.process_follow_batch(&mut pending_follows).await;
            total_follows += final_processed;
            
            let final_duration = final_batch_start.elapsed();
            let final_rate = final_processed as f64 / final_duration.as_secs_f64();
            
            info!("Processed final batch of {} follows in {:.2?} ({:.0} follows/sec)", 
                  final_processed, final_duration, final_rate);
        }

        let total_duration = start_time.elapsed();
        let overall_rate = total_follows as f64 / total_duration.as_secs_f64();
        info!("✅ Created {} total follow relationships in {:.2?} ({:.0} follows/sec overall)", 
              total_follows, total_duration, overall_rate);
        
        Ok(())
    }

    async fn process_follow_batch(&self, follows: &mut Vec<(aster_db::VertexId, aster_db::VertexId)>) -> usize {
        if follows.is_empty() {
            return 0;
        }
        
        let follows_to_process = follows.drain(..).collect();
        match self.db.batch_follow_users(follows_to_process).await {
            Ok(successful) => successful,
            Err(_) => 0, // If batch fails, return 0 successful
        }
    }

    async fn create_posts(&mut self, users: &[User], config: &SeedConfig) -> Result<Vec<Post>, Box<dyn std::error::Error>> {
        let mut posts = Vec::with_capacity(config.num_posts);
        let start_time = Instant::now();

        for i in 0..config.num_posts {
            // Pick a random user to author the post
            let author = &users[self.rng.gen_range(0..users.len())];
            let content = self.generate_post_content();

            let post = Post::new(author.id, content);
            
            if let Err(e) = self.db.create_post(&post).await {
                warn!("Failed to create post {}: {}", i, e);
                continue;
            }

            posts.push(post);

            // Progress reporting and throttling
            if config.enable_progress && (i + 1) % config.batch_size == 0 {
                let elapsed = start_time.elapsed();
                let rate = (i + 1) as f64 / elapsed.as_secs_f64();
                info!("Created {}/{} posts ({:.0} posts/sec)", i + 1, config.num_posts, rate);
                
                // Small delay every batch to reduce write pressure
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        Ok(posts)
    }

    async fn create_likes(&mut self, users: &[User], posts: &[Post], config: &SeedConfig) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        let mut total_likes = 0;
        let like_batch_size = 1_000; // Process 1,000 likes at a time
        let mut pending_likes = Vec::with_capacity(like_batch_size);

        info!("Generating likes for {} posts...", posts.len());

        for (i, post) in posts.iter().enumerate() {
            // Each post gets a random number of likes
            let num_likes = self.rng.gen_range(0..=(config.avg_likes_per_post * 2));
            
            let mut liked_users = std::collections::HashSet::new();
            
            for _ in 0..num_likes {
                // Pick a random user to like the post
                let user_idx = self.rng.gen_range(0..users.len());
                
                // Avoid duplicate likes
                if liked_users.contains(&user_idx) {
                    continue;
                }
                
                liked_users.insert(user_idx);
                let user = &users[user_idx];

                // Add to batch instead of executing immediately
                pending_likes.push((user.id, post.id));

                // Process batch when it reaches the limit
                if pending_likes.len() >= like_batch_size {
                    let batch_start = Instant::now();
                    let batch_processed = self.process_like_batch(&mut pending_likes).await;
                    total_likes += batch_processed;
                    
                    let batch_duration = batch_start.elapsed();
                    let batch_rate = batch_processed as f64 / batch_duration.as_secs_f64();
                    
                    if config.enable_progress {
                        info!("Processed batch of {} likes in {:.2?} ({:.0} likes/sec)", 
                              batch_processed, batch_duration, batch_rate);
                    }
                    
                    // Small delay to prevent overwhelming the database
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }

            // Progress reporting for posts processed
            if config.enable_progress && (i + 1) % config.batch_size == 0 {
                let elapsed = start_time.elapsed();
                let rate = (i + 1) as f64 / elapsed.as_secs_f64();
                info!("Processed likes for {}/{} posts ({:.0} posts/sec, {} total likes queued)", 
                      i + 1, posts.len(), rate, total_likes + pending_likes.len());
            }
        }

        // Process any remaining likes in the final batch
        if !pending_likes.is_empty() {
            let final_batch_start = Instant::now();
            let final_processed = self.process_like_batch(&mut pending_likes).await;
            total_likes += final_processed;
            
            let final_duration = final_batch_start.elapsed();
            let final_rate = final_processed as f64 / final_duration.as_secs_f64();
            
            info!("Processed final batch of {} likes in {:.2?} ({:.0} likes/sec)", 
                  final_processed, final_duration, final_rate);
        }

        let total_duration = start_time.elapsed();
        let overall_rate = total_likes as f64 / total_duration.as_secs_f64();
        info!("✅ Created {} total likes in {:.2?} ({:.0} likes/sec overall)", 
              total_likes, total_duration, overall_rate);
        
        Ok(())
    }

    async fn process_like_batch(&self, likes: &mut Vec<(aster_db::VertexId, aster_db::VertexId)>) -> usize {
        let mut successful = 0;
        
        for (user_id, post_id) in likes.drain(..) {
            if let Ok(()) = self.db.like_post(user_id, post_id).await {
                successful += 1;
            } else {
                // Don't warn for individual failures to avoid spam - this is normal in large datasets
            }
        }
        
        successful
    }

    async fn create_comments(&mut self, users: &[User], posts: &[Post], config: &SeedConfig) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        for i in 0..config.num_comments {
            // Pick random post and user for the comment
            let post = &posts[self.rng.gen_range(0..posts.len())];
            let author = &users[self.rng.gen_range(0..users.len())];
            let content = self.generate_comment_content();

            let comment = Comment::new(post.id, author.id, content);
            
            if let Err(e) = self.db.create_comment(&comment).await {
                warn!("Failed to create comment {}: {}", i, e);
                continue;
            }

            // Progress reporting and throttling
            if config.enable_progress && (i + 1) % config.batch_size == 0 {
                let elapsed = start_time.elapsed();
                let rate = (i + 1) as f64 / elapsed.as_secs_f64();
                info!("Created {}/{} comments ({:.0} comments/sec)", i + 1, config.num_comments, rate);
                
                // Small delay every batch to reduce write pressure
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        Ok(())
    }

    async fn print_statistics(&self, config: &SeedConfig, duration: std::time::Duration) -> Result<(), Box<dyn std::error::Error>> {
        info!("📊 SEEDING STATISTICS:");
        info!("  Duration: {:.2?}", duration);
        info!("  Users: {}", config.num_users);
        info!("  Posts: {}", config.num_posts);
        info!("  Comments: {}", config.num_comments);
        info!("  Avg follows per user: {}", config.avg_follows_per_user);
        info!("  Avg likes per post: {}", config.avg_likes_per_post);
        
        let total_operations = config.num_users + config.num_posts + config.num_comments + 
                              (config.num_users * config.avg_follows_per_user) + 
                              (config.num_posts * config.avg_likes_per_post);
        let ops_per_sec = total_operations as f64 / duration.as_secs_f64();
        info!("  Overall rate: {:.0} operations/sec", ops_per_sec);

        Ok(())
    }

    // Content generation methods
    fn generate_username(&mut self, index: usize) -> String {
        let prefixes = ["user", "dev", "coder", "hacker", "ninja", "guru", "pro", "elite", "master", "wizard"];
        let suffixes = ["2024", "x", "dev", "code", "tech", "ai", "bot", "prime", "ultra", "max"];
        
        if self.rng.gen_bool(0.3) {
            format!("{}_{}", prefixes[self.rng.gen_range(0..prefixes.len())], index)
        } else {
            format!("{}{}_{}", 
                   prefixes[self.rng.gen_range(0..prefixes.len())],
                   suffixes[self.rng.gen_range(0..suffixes.len())],
                   index)
        }
    }

    fn generate_display_name(&mut self, username: &str) -> String {
        let adjectives = ["Amazing", "Awesome", "Cool", "Epic", "Great", "Super", "Ultra", "Mega", "Pro", "Elite"];
        
        if self.rng.gen_bool(0.4) {
            format!("{} {}", adjectives[self.rng.gen_range(0..adjectives.len())], username)
        } else {
            username.to_string()
        }
    }

    fn generate_bio(&mut self) -> String {
        let bios = [
            "Software developer passionate about technology",
            "Building the future one line of code at a time",
            "Graph database enthusiast and performance optimizer",
            "Open source contributor and tech blogger",
            "Database architect specializing in graph systems",
            "Full-stack developer with a love for distributed systems",
            "AI/ML engineer working on next-gen applications",
            "System architect focused on scalable solutions",
            "Performance engineer optimizing high-throughput systems",
            "Tech lead building innovative database solutions",
        ];
        
        bios[self.rng.gen_range(0..bios.len())].to_string()
    }

    fn generate_post_content(&mut self) -> String {
        let templates = [
            "Just discovered an amazing optimization technique that improved performance by {}%! #TechTips",
            "Working on a new graph database feature. The query execution time dropped from {}ms to {}ms! 🚀",
            "Excited to share that our latest release handles {} million operations per second! #Performance",
            "Been experimenting with distributed systems. Consistency vs availability is always a fun challenge! 🤔",
            "Performance tip: Batch your database queries! Reduced our API response time by {}ms today.",
            "Graph traversals are fascinating. Just implemented a new algorithm that's {}x faster! 📊",
            "Database optimization is an art form. Every microsecond counts when you're processing millions of records.",
            "Love working with Rust! Memory safety and performance - best of both worlds 🦀",
            "Today's achievement: Optimized our recommendation engine to handle {} users in real-time!",
            "Building scalable systems requires thinking differently about data structures and algorithms.",
        ];
        
        let template = templates[self.rng.gen_range(0..templates.len())];
        
        // Fill in placeholders with random numbers
        if template.contains("{}") {
            let mut result = template.to_string();
            while result.contains("{}") {
                let value = match self.rng.gen_range(0..4) {
                    0 => self.rng.gen_range(10..100).to_string(),  // 10-99
                    1 => self.rng.gen_range(100..1000).to_string(), // 100-999
                    2 => self.rng.gen_range(1000..10000).to_string(), // 1K-10K
                    _ => self.rng.gen_range(1..50).to_string(),     // 1-49
                };
                result = result.replacen("{}", &value, 1);
            }
            result
        } else {
            template.to_string()
        }
    }

    fn generate_comment_content(&mut self) -> String {
        let comments = [
            "Great post! Thanks for sharing this insight.",
            "This is exactly what I needed to solve my performance issue!",
            "Impressive results! How did you measure the improvement?",
            "Love seeing these kind of optimizations. Keep up the great work!",
            "This approach is brilliant. Going to try it in my project.",
            "Wow, those numbers are incredible! 🔥",
            "Thanks for the detailed explanation. Very helpful!",
            "I've been struggling with this exact problem. Thank you!",
            "Fantastic work on the optimization!",
            "This is why I love the tech community. Great knowledge sharing!",
            "Could you share more details about the implementation?",
            "Amazing performance gains! What was the bottleneck before?",
            "This deserves more visibility. Excellent work!",
            "I'm definitely going to implement this in our system.",
            "Such a clever solution! Thanks for sharing.",
        ];
        
        comments[self.rng.gen_range(0..comments.len())].to_string()
    }
}

// Legacy compatibility - maintain the Seeder name
pub type Seeder = DatabaseSeeder;
