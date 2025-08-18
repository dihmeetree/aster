use super::database::TwitterDatabase;
use super::models::*;
use rand::prelude::*;
use tracing::info;

pub struct Seeder {
    db: TwitterDatabase,
}

impl Seeder {
    pub fn new(db: TwitterDatabase) -> Self {
        Self { db }
    }

    pub async fn seed_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting database seeding...");

        let users = self.create_sample_users().await?;
        info!("Created {} users", users.len());

        self.create_follow_relationships(&users).await?;
        info!("Created follow relationships");

        let posts = self.create_sample_posts(&users).await?;
        info!("Created {} posts", posts.len());

        self.create_likes(&users, &posts).await?;
        info!("Created likes");

        self.create_comments(&users, &posts).await?;
        info!("Created comments");

        info!("Database seeding completed!");
        Ok(())
    }

    async fn create_sample_users(&self) -> Result<Vec<User>, Box<dyn std::error::Error>> {
        let sample_users = vec![
            ("alice_codes", "Alice Johnson", "Full-stack developer passionate about Rust and graph databases. Building the future one commit at a time! 🦀"),
            ("bob_data", "Bob Smith", "Data scientist exploring the intersection of AI and graph theory. Love visualizing complex networks."),
            ("charlie_dev", "Charlie Brown", "Backend engineer at a tech startup. Coffee enthusiast ☕ and weekend rock climber 🧗‍♂️"),
            ("diana_design", "Diana Williams", "UX/UI designer crafting beautiful user experiences. Believes good design is invisible design."),
            ("eve_security", "Eve Davis", "Cybersecurity researcher. Breaking things so others can fix them. Security through obscurity is not security!"),
            ("frank_mobile", "Frank Miller", "Mobile app developer. Building iOS and Android apps that people actually want to use 📱"),
            ("grace_ml", "Grace Lee", "Machine learning engineer working on recommendation systems. Teaching computers to understand humans."),
            ("henry_devops", "Henry Wilson", "DevOps engineer keeping the servers happy. If it's not automated, it's broken."),
            ("iris_frontend", "Iris Chen", "Frontend developer creating responsive and accessible web applications. Semantic HTML for the win!"),
            ("jack_blockchain", "Jack Taylor", "Blockchain developer building the decentralized future. Not all heroes wear capes, some write smart contracts."),
            ("kate_product", "Kate Anderson", "Product manager bridging the gap between business and technology. User stories are my love language."),
            ("liam_game", "Liam Murphy", "Game developer bringing virtual worlds to life. Currently working on an indie RPG set in space."),
            ("maya_research", "Maya Patel", "Computer science researcher focusing on distributed systems. PhD in making things scale."),
            ("noah_startup", "Noah Garcia", "Startup founder building the next big thing in social media. Always looking for the next unicorn 🦄"),
            ("olivia_open", "Olivia Rodriguez", "Open source contributor and maintainer. If you're not sharing, you're not caring."),
            ("paul_quantum", "Paul Thompson", "Quantum computing researcher. Superposition is not just a physics concept, it's a lifestyle."),
            ("quinn_web3", "Quinn Kim", "Web3 developer building on the blockchain. Decentralization is the future of the internet."),
            ("rachel_ai", "Rachel White", "AI ethics researcher ensuring artificial intelligence serves humanity. With great power comes great responsibility."),
            ("sam_systems", "Sam Johnson", "Systems programmer optimizing performance at the lowest level. Assembly is poetry in motion."),
            ("tina_testing", "Tina Brown", "QA engineer ensuring software quality. Bugs fear me, developers respect me, managers need me."),
        ];

        let mut users = Vec::new();

        for (username, display_name, bio) in sample_users {
            let user = User::new(
                username.to_string(),
                display_name.to_string(),
                bio.to_string(),
            );
            self.db.create_user(&user).await?;
            users.push(user);
        }

        Ok(users)
    }

    async fn create_follow_relationships(
        &self,
        users: &[User],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut rng = rand::thread_rng();

        // Create a realistic social network with some users being more popular
        let popular_users = &users[0..8]; // First 8 users are popular (increased from 5)
        let regular_users = &users[8..];

        // Popular users follow each other - make this more likely
        for i in 0..popular_users.len() {
            for j in 0..popular_users.len() {
                if i != j && rng.gen_bool(0.95) {
                    // 95% chance popular users follow each other (increased from 80%)
                    self.db
                        .follow_user(popular_users[i].id, popular_users[j].id)
                        .await?;
                }
            }
        }

        // Regular users follow popular users - make this more likely
        for regular_user in regular_users {
            for popular_user in popular_users {
                if rng.gen_bool(0.85) {
                    // 85% chance regular users follow popular ones (increased from 60%)
                    self.db
                        .follow_user(regular_user.id, popular_user.id)
                        .await?;
                }
            }

            // Also make regular users follow some other popular users they haven't followed yet
            let additional_follows = rng.gen_range(1..4);
            for _ in 0..additional_follows {
                let popular_idx = rng.gen_range(0..popular_users.len());
                self.db
                    .follow_user(regular_user.id, popular_users[popular_idx].id)
                    .await
                    .unwrap_or(());
            }
        }

        // Regular users follow some other regular users - increase connections
        for i in 0..regular_users.len() {
            let follow_count = rng.gen_range(5..12); // Each user follows 5-11 others (increased from 2-7)
            let mut followed = std::collections::HashSet::new();

            for _ in 0..follow_count {
                let target_idx = rng.gen_range(0..regular_users.len());
                if target_idx != i && !followed.contains(&target_idx) {
                    self.db
                        .follow_user(regular_users[i].id, regular_users[target_idx].id)
                        .await?;
                    followed.insert(target_idx);
                }
            }
        }

        // Add some random cross-connections for more variety
        for user in users {
            let random_follows = rng.gen_range(2..6);
            for _ in 0..random_follows {
                let target_idx = rng.gen_range(0..users.len());
                if users[target_idx].id != user.id {
                    self.db
                        .follow_user(user.id, users[target_idx].id)
                        .await
                        .unwrap_or(());
                }
            }
        }

        Ok(())
    }

    async fn create_sample_posts(
        &self,
        users: &[User],
    ) -> Result<Vec<Post>, Box<dyn std::error::Error>> {
        let sample_posts = vec![
            "Just discovered the power of graph databases! The way Aster handles complex relationships is incredible. Time to rebuild everything! 🚀",
            "Working on a new recommendation algorithm using Gremlin queries. The traversal language is so intuitive once you get the hang of it.",
            "Hot take: Graph databases are the future of social media. Traditional relational DBs just can't handle the complexity of modern social networks.",
            "Spent the weekend implementing a Twitter clone with Aster. The Poly-LSM storage engine is surprisingly fast for complex graph queries!",
            "Anyone else think that social media algorithms should be more transparent? Users deserve to know why they see what they see.",
            "The intersection of distributed systems and graph theory is fascinating. Building scalable graph databases is an art form.",
            "Just pushed my latest open source project to GitHub. Building developer tools that don't suck is my passion project.",
            "Conference season is coming up! Who's excited for the latest developments in database technology? Graph DBs are having a moment.",
            "Debugging distributed systems is like solving a mystery where the evidence keeps changing. But when it works... *chef's kiss*",
            "The beauty of functional programming is that it makes complex data transformations feel elegant. Rust + FP = ❤️",
            "Building a recommendation engine that actually respects user privacy. It's harder than it sounds but absolutely necessary.",
            "Graph algorithms in the morning, coffee in the afternoon, and contemplating the heat death of the universe in the evening.",
            "Just realized I've been overthinking this problem for weeks. Sometimes the simplest solution is the best solution.",
            "The amount of data we generate daily is staggering. Good thing we have graph databases to make sense of it all!",
            "Working from a coffee shop today. Nothing beats the ambient noise of espresso machines and keyboard clicking.",
            "Microservices are great until you need to debug them. Then you realize you've just created a distributed monolith.",
            "The future of computing is edge + cloud + graph. Mark my words, this combination will change everything.",
            "Security should be built in, not bolted on. Yet here we are, patching systems that were insecure by design.",
            "Graph neural networks are blowing my mind. The intersection of AI and graph theory is where the magic happens.",
            "Documentation is love. If you're not documenting your code, you're not loving your future self (or your teammates).",
            "The best part about working in tech? Every day brings new challenges and opportunities to learn something new.",
            "Building scalable systems is like playing Tetris at scale. Everything needs to fit perfectly or it all falls apart.",
            "Graph databases make complex queries feel simple. It's like having a conversation with your data instead of interrogating it.",
            "The more I learn about distributed consensus algorithms, the more I appreciate the complexity of building reliable systems.",
            "Performance optimization is an art. You can't just throw more hardware at the problem and hope it goes away.",
            "Open source is the backbone of modern software development. We stand on the shoulders of giants, and we should give back.",
            "The hardest part about machine learning isn't the algorithms, it's getting clean, representative data.",
            "Graph visualization tools are game-changers for understanding complex networks. Seeing is believing!",
            "Building developer tools requires empathy. You need to understand the pain points developers face every day.",
            "The cloud is just someone else's computer, but at least it's someone else's problem when it breaks.",
            "Refactoring legacy code is like archaeology. You're constantly discovering artifacts from previous civilizations of developers.",
            "The best APIs are invisible. They just work, and you don't have to think about them.",
            "Graph databases excel at answering questions you didn't know you had. The exploratory nature is addictive.",
            "Testing in production is inevitable. The question is whether you do it intentionally or by accident.",
            "The hardest problems in computer science: cache invalidation, naming things, and explaining why everything is broken.",
            "Real-time analytics on graph data is where things get interesting. Watching patterns emerge in live data is mesmerizing.",
            "The beauty of graph traversals is that they mirror how we naturally think about relationships and connections.",
            "Building resilient systems requires thinking about failure at every level. Murphy's law is not just a suggestion.",
            "The best conferences are the ones where you leave with more questions than answers. That means you're learning!",
            "Graph databases make fraud detection so much more effective. Following the money has never been easier.",
        ];

        let mut posts = Vec::new();
        let mut rng = rand::thread_rng();

        // Create posts with realistic distribution
        for user in users {
            let post_count = if users.iter().position(|u| u.id == user.id).unwrap() < 5 {
                rng.gen_range(5..12) // Popular users post more
            } else {
                rng.gen_range(1..6) // Regular users post less
            };

            for _ in 0..post_count {
                let content = sample_posts.choose(&mut rng).unwrap().to_string();
                let post = Post::new(user.id, content);
                self.db.create_post(&post).await?;
                posts.push(post);
            }
        }

        Ok(posts)
    }

    async fn create_likes(
        &self,
        users: &[User],
        posts: &[Post],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut rng = rand::thread_rng();

        for user in users {
            // Each user likes 10-30 random posts
            let like_count = rng.gen_range(10..30);
            let mut liked_posts = std::collections::HashSet::new();

            for _ in 0..like_count {
                let post_idx = rng.gen_range(0..posts.len());
                let post = &posts[post_idx];

                // Don't like your own posts (most of the time)
                if post.author_id != user.id && !liked_posts.contains(&post.id) {
                    self.db.like_post(user.id, post.id).await?;
                    liked_posts.insert(post.id);
                }
            }
        }

        Ok(())
    }

    async fn create_comments(
        &self,
        users: &[User],
        posts: &[Post],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let sample_comments = vec![
            "Great point! This really resonates with me.",
            "I totally agree with this perspective.",
            "This is exactly what I've been thinking about lately.",
            "Interesting take! I'd love to hear more about this.",
            "Thanks for sharing this insight!",
            "This is spot on. Well said!",
            "I had a similar experience recently.",
            "This deserves more attention!",
            "Couldn't agree more with this.",
            "This is why I love this community.",
            "Such a thoughtful post!",
            "You've articulated this perfectly.",
            "This is incredibly insightful.",
            "Thanks for breaking this down so clearly.",
            "This is exactly the kind of content I'm here for.",
            "Mind blown! 🤯",
            "This thread is pure gold.",
            "Adding this to my reading list.",
            "This should be required reading for everyone in tech.",
            "So much wisdom in this post.",
        ];

        let mut rng = rand::thread_rng();

        // Add comments to random posts
        for post in posts {
            let comment_count = rng.gen_range(0..8); // 0-7 comments per post

            for _ in 0..comment_count {
                let commenter = users.choose(&mut rng).unwrap();
                // Don't comment on your own posts (most of the time)
                if commenter.id != post.author_id || rng.gen_bool(0.1) {
                    let content = sample_comments.choose(&mut rng).unwrap().to_string();
                    let comment = Comment::new(post.id, commenter.id, content);
                    self.db.create_comment(&comment).await?;
                }
            }
        }

        Ok(())
    }
}
