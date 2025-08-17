//! Property Graph Analytics Integration Tests
//!
//! Tests for advanced property graph analytics using:
//! - Rich property-based queries with complex filtering
//! - Aggregation and analytical queries across graph properties
//! - Property indexing for high-performance searches
//! - Multi-dimensional analysis of graph data

use aster_db::{AsterDB, AsterDBConfig, Properties, PropertyValue, Result, VertexId};
use std::collections::HashMap;
use tempfile::TempDir;

/// Product in an e-commerce graph
#[derive(Debug, Clone)]
struct Product {
    id: VertexId,
    name: String,
    category: String,
    price: f64,
    brand: String,
    rating: f64,
    inventory: u32,
}

/// Customer in the system
#[derive(Debug, Clone)]
struct Customer {
    id: VertexId,
    name: String,
    age: u32,
    city: String,
    total_spent: f64,
    loyalty_tier: String,
}

/// Property analytics engine
struct PropertyAnalyticsEngine {
    db: AsterDB,
    products: HashMap<VertexId, Product>,
    customers: HashMap<VertexId, Customer>,
    next_product_id: u64,
    next_customer_id: u64,
}

impl PropertyAnalyticsEngine {
    /// Create a new property analytics engine
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
            products: HashMap::new(),
            customers: HashMap::new(),
            next_product_id: 1000,
            next_customer_id: 1,
        })
    }

    /// Add a product to the system
    async fn add_product(
        &mut self,
        name: String,
        category: String,
        price: f64,
        brand: String,
        rating: f64,
        inventory: u32,
    ) -> Result<VertexId> {
        let product_id = VertexId::from_u64(self.next_product_id);
        self.next_product_id += 1;

        // Use Gremlin to add product vertex with rich properties
        let add_product_query = format!(
            "g.addV('product')\
             .property('name', '{}')\
             .property('category', '{}')\
             .property('price', {})\
             .property('brand', '{}')\
             .property('rating', {})\
             .property('inventory', {})",
            name.replace("'", "\\'"),
            category.replace("'", "\\'"),
            price,
            brand.replace("'", "\\'"),
            rating,
            inventory
        );

        let query_result = self.db.gremlin_query(&add_product_query).await?;

        // Extract the actual vertex ID from the query result
        let actual_vertex_id = if let Some(aster_db::query::GremlinResult::Vertex(vertex_id)) =
            query_result.results.first()
        {
            *vertex_id
        } else {
            return Err(aster_db::AsterError::internal(
                "Failed to get vertex ID from addV query",
            ));
        };

        let product = Product {
            id: actual_vertex_id,
            name: name.clone(),
            category,
            price,
            brand,
            rating,
            inventory,
        };

        self.products.insert(actual_vertex_id, product);
        Ok(actual_vertex_id)
    }

    /// Add a customer to the system
    async fn add_customer(
        &mut self,
        name: String,
        age: u32,
        city: String,
        loyalty_tier: String,
    ) -> Result<VertexId> {
        let customer_id = VertexId::from_u64(self.next_customer_id);
        self.next_customer_id += 1;

        // Use Gremlin to add customer vertex with properties
        let add_customer_query = format!(
            "g.addV('customer')\
             .property('name', '{}')\
             .property('age', {})\
             .property('city', '{}')\
             .property('total_spent', 0.0)\
             .property('loyalty_tier', '{}')",
            name.replace("'", "\\'"),
            age,
            city.replace("'", "\\'"),
            loyalty_tier.replace("'", "\\'")
        );

        let query_result = self.db.gremlin_query(&add_customer_query).await?;

        // Extract the actual vertex ID from the query result
        let actual_vertex_id = if let Some(aster_db::query::GremlinResult::Vertex(vertex_id)) =
            query_result.results.first()
        {
            *vertex_id
        } else {
            return Err(aster_db::AsterError::internal(
                "Failed to get vertex ID from addV query",
            ));
        };

        let customer = Customer {
            id: actual_vertex_id,
            name: name.clone(),
            age,
            city: city.clone(),
            total_spent: 0.0,
            loyalty_tier,
        };

        self.customers.insert(actual_vertex_id, customer);
        Ok(actual_vertex_id)
    }

    /// Record a purchase transaction
    async fn add_purchase(
        &mut self,
        customer_id: VertexId,
        product_id: VertexId,
        quantity: u32,
    ) -> Result<()> {
        let (customer_name, product_name, amount) = {
            if let (Some(customer), Some(product)) = (
                self.customers.get(&customer_id),
                self.products.get(&product_id),
            ) {
                let amount = product.price * quantity as f64;
                (customer.name.clone(), product.name.clone(), amount)
            } else {
                return Ok(());
            }
        };

        let timestamp = chrono::Utc::now().timestamp() as u64;

        // Add purchase edge with transaction properties
        let add_purchase_query = format!(
            "g.V({}).addE('purchased')\
             .to(g.V({}))\
             .property('amount', {})\
             .property('quantity', {})\
             .property('timestamp', {})",
            customer_id.as_u64(),
            product_id.as_u64(),
            amount,
            quantity,
            timestamp
        );

        self.db.gremlin_query(&add_purchase_query).await?;

        // Get current total spent before updating
        let current_total = self
            .customers
            .get(&customer_id)
            .map(|c| c.total_spent)
            .unwrap_or(0.0);

        // Update customer's total spent
        let update_spent_query = format!(
            "g.V({}).property('total_spent', {})",
            customer_id.as_u64(),
            current_total + amount
        );

        self.db.gremlin_query(&update_spent_query).await?;

        // Update local state
        if let Some(customer_mut) = self.customers.get_mut(&customer_id) {
            customer_mut.total_spent += amount;
        }

        Ok(())
    }

    /// Find products by category and price range
    async fn find_products_by_criteria(
        &self,
        category: Option<&str>,
        min_price: Option<f64>,
        max_price: Option<f64>,
        min_rating: Option<f64>,
    ) -> Result<Vec<VertexId>> {
        let mut query = "g.V().hasLabel('product')".to_string();

        if let Some(cat) = category {
            query.push_str(&format!(".has('category', '{}')", cat.replace("'", "\\'")));
        }

        if let Some(min_p) = min_price {
            query.push_str(&format!(".has('price', gte({}))", min_p));
        }

        if let Some(max_p) = max_price {
            query.push_str(&format!(".has('price', lte({}))", max_p));
        }

        if let Some(min_r) = min_rating {
            query.push_str(&format!(".has('rating', gte({}))", min_r));
        }

        query.push_str(".id()");
        let result = self.db.gremlin_query(&query).await?;

        let mut product_ids = Vec::new();
        for gremlin_result in result.results {
            if let aster_db::query::GremlinResult::Vertex(vertex_id) = gremlin_result {
                product_ids.push(vertex_id);
            }
        }
        Ok(product_ids)
    }

    /// Analyze sales by category
    async fn analyze_sales_by_category(&self) -> Result<HashMap<String, (f64, u32)>> {
        // Process results to aggregate by category
        let mut category_sales: HashMap<String, (f64, u32)> = HashMap::new();

        // In a real implementation, we'd process the projection results
        // For this example, we'll use a simpler approach
        for category in ["Electronics", "Clothing", "Books", "Home", "Sports"] {
            let category_sales_query = format!(
                "g.V().hasLabel('product').has('category', '{}')\
                 .inE('purchased')\
                 .values('amount').sum()",
                category
            );

            let amount_result = self.db.gremlin_query(&category_sales_query).await?;
            let total_amount =
                if let Some(aster_db::query::GremlinResult::Value(PropertyValue::Float(f))) =
                    amount_result.results.first()
                {
                    *f
                } else if let Some(aster_db::query::GremlinResult::Value(PropertyValue::Int(i))) =
                    amount_result.results.first()
                {
                    *i as f64
                } else {
                    0.0
                };

            let category_quantity_query = format!(
                "g.V().hasLabel('product').has('category', '{}')\
                 .inE('purchased')\
                 .values('quantity').sum()",
                category
            );

            let quantity_result = self.db.gremlin_query(&category_quantity_query).await?;
            let total_quantity =
                if let Some(aster_db::query::GremlinResult::Value(PropertyValue::Int(i))) =
                    quantity_result.results.first()
                {
                    *i as u32
                } else {
                    0
                };

            if total_amount > 0.0 {
                category_sales.insert(category.to_string(), (total_amount, total_quantity));
            }
        }

        Ok(category_sales)
    }

    /// Find top customers by spending
    async fn find_top_customers(&self, limit: usize) -> Result<Vec<(VertexId, f64)>> {
        let top_customers_query = format!(
            "g.V().hasLabel('customer')\
             .order().by('total_spent', desc)\
             .limit({})\
             .project('id', 'spent')\
             .by(id())\
             .by(values('total_spent'))",
            limit
        );

        let result = self.db.gremlin_query(&top_customers_query).await?;

        let mut top_customers = Vec::new();

        // Process projected map results
        for gremlin_result in result.results {
            if let aster_db::query::GremlinResult::Map(map) = gremlin_result {
                // Extract vertex ID from the "id" field
                if let Some(aster_db::query::GremlinResult::Vertex(vertex_id)) = map.get("id") {
                    if let Some(customer) = self.customers.get(vertex_id) {
                        top_customers.push((*vertex_id, customer.total_spent));
                    }
                }
            }
        }

        // Sort by spending (descending)
        top_customers.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        top_customers.truncate(limit);

        Ok(top_customers)
    }

    /// Generate comprehensive sample data
    async fn generate_sample_data(&mut self) -> Result<()> {
        // Sample products across different categories
        let products = vec![
            ("iPhone 14", "Electronics", 999.99, "Apple", 4.8, 50),
            ("MacBook Pro", "Electronics", 2399.99, "Apple", 4.9, 20),
            ("Samsung TV", "Electronics", 799.99, "Samsung", 4.6, 30),
            ("Nike Air Max", "Clothing", 129.99, "Nike", 4.5, 100),
            ("Levi's Jeans", "Clothing", 79.99, "Levi's", 4.3, 75),
            ("Python Programming", "Books", 39.99, "O'Reilly", 4.7, 500),
            ("Design Patterns", "Books", 54.99, "Pearson", 4.6, 300),
            ("Coffee Maker", "Home", 89.99, "Breville", 4.5, 40),
        ];

        let mut product_ids = Vec::new();
        for (name, category, price, brand, rating, inventory) in products {
            let product_id = self
                .add_product(
                    name.to_string(),
                    category.to_string(),
                    price,
                    brand.to_string(),
                    rating,
                    inventory,
                )
                .await?;
            product_ids.push(product_id);
        }

        // Sample customers with diverse profiles
        let customers = vec![
            ("Alice Johnson", 28, "New York", "Gold"),
            ("Bob Smith", 35, "San Francisco", "Silver"),
            ("Carol Davis", 42, "Los Angeles", "Platinum"),
            ("David Wilson", 29, "Chicago", "Bronze"),
        ];

        let mut customer_ids = Vec::new();
        for (name, age, city, loyalty_tier) in customers {
            let customer_id = self
                .add_customer(
                    name.to_string(),
                    age,
                    city.to_string(),
                    loyalty_tier.to_string(),
                )
                .await?;
            customer_ids.push(customer_id);
        }

        // Generate purchase transactions
        // Alice buys electronics
        self.add_purchase(customer_ids[0], product_ids[0], 1)
            .await?; // iPhone
        self.add_purchase(customer_ids[0], product_ids[1], 1)
            .await?; // MacBook

        // Bob buys clothing
        self.add_purchase(customer_ids[1], product_ids[3], 2)
            .await?; // Nike shoes
        self.add_purchase(customer_ids[1], product_ids[4], 1)
            .await?; // Jeans

        // Carol buys books
        self.add_purchase(customer_ids[2], product_ids[5], 3)
            .await?; // Python book
        self.add_purchase(customer_ids[2], product_ids[6], 2)
            .await?; // Design patterns

        // David buys home items
        self.add_purchase(customer_ids[3], product_ids[7], 1)
            .await?; // Coffee maker

        Ok(())
    }
}

#[tokio::test]
async fn test_property_analytics_basic_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PropertyAnalyticsEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Generate sample data
    engine.generate_sample_data().await.unwrap();

    // Verify data was created
    assert_eq!(engine.products.len(), 8);
    assert_eq!(engine.customers.len(), 4);

    // Test product criteria search
    let electronics = engine
        .find_products_by_criteria(Some("Electronics"), None, None, None)
        .await
        .unwrap();
    assert_eq!(electronics.len(), 3, "Should find 3 electronics products");

    // Test price range filtering
    let expensive_products = engine
        .find_products_by_criteria(None, Some(1000.0), None, None)
        .await
        .unwrap();
    assert_eq!(
        expensive_products.len(),
        1,
        "Should find 1 expensive product (MacBook Pro >= $1000)"
    );

    // Test rating filtering
    let high_rated = engine
        .find_products_by_criteria(None, None, None, Some(4.7))
        .await
        .unwrap();
    assert!(
        high_rated.len() >= 2,
        "Should find at least 2 high-rated products"
    );
}

#[tokio::test]
async fn test_sales_analytics() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PropertyAnalyticsEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Generate sample data
    engine.generate_sample_data().await.unwrap();

    // Test sales by category
    let category_sales = engine.analyze_sales_by_category().await.unwrap();

    // Should have sales in Electronics category (Alice's purchases)
    assert!(
        category_sales.contains_key("Electronics"),
        "Should have electronics sales"
    );
    let electronics_sales = category_sales.get("Electronics").unwrap();
    assert!(
        electronics_sales.0 > 3000.0,
        "Electronics sales should be substantial (iPhone + MacBook)"
    );

    // Should have sales in Clothing category (Bob's purchases)
    assert!(
        category_sales.contains_key("Clothing"),
        "Should have clothing sales"
    );
    let clothing_sales = category_sales.get("Clothing").unwrap();
    assert!(
        clothing_sales.0 > 300.0,
        "Clothing sales should include shoes and jeans"
    );

    // Should have sales in Books category (Carol's purchases)
    assert!(
        category_sales.contains_key("Books"),
        "Should have book sales"
    );
    let books_sales = category_sales.get("Books").unwrap();
    assert!(
        books_sales.0 > 200.0,
        "Book sales should include multiple purchases"
    );
}

#[tokio::test]
async fn test_top_customers() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PropertyAnalyticsEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Generate sample data
    engine.generate_sample_data().await.unwrap();

    // Find top customers
    let top_customers = engine.find_top_customers(3).await.unwrap();

    assert!(!top_customers.is_empty(), "Should find top customers");
    assert!(top_customers.len() <= 3, "Should respect limit");

    // Verify customers are sorted by spending (descending)
    for i in 1..top_customers.len() {
        assert!(
            top_customers[i - 1].1 >= top_customers[i].1,
            "Customers should be sorted by spending in descending order"
        );
    }

    // Alice should be the top customer (bought iPhone + MacBook)
    let top_customer_id = top_customers[0].0;
    let alice_id = engine
        .customers
        .values()
        .find(|c| c.name == "Alice Johnson")
        .unwrap()
        .id;
    assert_eq!(
        top_customer_id, alice_id,
        "Alice should be the top customer"
    );
    assert!(
        top_customers[0].1 > 3000.0,
        "Top customer should have spent substantial amount"
    );
}

#[tokio::test]
async fn test_complex_property_queries() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PropertyAnalyticsEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Add specific test products
    let laptop_id = engine
        .add_product(
            "Gaming Laptop".to_string(),
            "Electronics".to_string(),
            1500.0,
            "ASUS".to_string(),
            4.8,
            10,
        )
        .await
        .unwrap();

    let book_id = engine
        .add_product(
            "Cheap Book".to_string(),
            "Books".to_string(),
            15.0,
            "Publisher".to_string(),
            4.2,
            100,
        )
        .await
        .unwrap();

    let phone_id = engine
        .add_product(
            "Budget Phone".to_string(),
            "Electronics".to_string(),
            300.0,
            "Generic".to_string(),
            3.5,
            50,
        )
        .await
        .unwrap();

    // Test multiple criteria filtering
    let high_end_electronics = engine
        .find_products_by_criteria(
            Some("Electronics"),
            Some(1000.0), // min price
            None,         // max price
            Some(4.5),    // min rating
        )
        .await
        .unwrap();

    assert_eq!(
        high_end_electronics.len(),
        1,
        "Should find only the gaming laptop"
    );
    assert!(
        high_end_electronics.contains(&laptop_id),
        "Should find the gaming laptop"
    );

    // Test that budget phone is excluded (low rating)
    let premium_electronics = engine
        .find_products_by_criteria(Some("Electronics"), None, None, Some(4.0))
        .await
        .unwrap();

    assert!(
        !premium_electronics.contains(&phone_id),
        "Budget phone should be excluded due to low rating"
    );

    // Test price range filtering
    let mid_range_products = engine
        .find_products_by_criteria(
            None,
            Some(100.0), // min price
            Some(500.0), // max price
            None,
        )
        .await
        .unwrap();

    assert!(
        mid_range_products.contains(&phone_id),
        "Budget phone should be in mid-range"
    );
    assert!(
        !mid_range_products.contains(&laptop_id),
        "Gaming laptop should be too expensive"
    );
    assert!(
        !mid_range_products.contains(&book_id),
        "Book should be too cheap"
    );
}

#[tokio::test]
async fn test_gremlin_property_operations() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PropertyAnalyticsEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Add test data
    let product_id = engine
        .add_product(
            "Test Product".to_string(),
            "Test Category".to_string(),
            100.0,
            "Test Brand".to_string(),
            4.5,
            20,
        )
        .await
        .unwrap();

    let customer_id = engine
        .add_customer(
            "Test Customer".to_string(),
            30,
            "Test City".to_string(),
            "Gold".to_string(),
        )
        .await
        .unwrap();

    // Test vertex count by label
    let product_count_query = "g.V().hasLabel('product').count()";
    let result = engine.db.gremlin_query(product_count_query).await.unwrap();
    let product_count =
        if let Some(aster_db::query::GremlinResult::Count(c)) = result.results.first() {
            *c
        } else {
            0
        };
    assert_eq!(product_count, 1, "Should have one product");

    // Test property-based filtering
    let test_category_query = "g.V().hasLabel('product').has('category', 'Test Category').count()";
    let category_result = engine.db.gremlin_query(test_category_query).await.unwrap();
    let category_count =
        if let Some(aster_db::query::GremlinResult::Count(c)) = category_result.results.first() {
            *c
        } else {
            0
        };
    assert_eq!(category_count, 1, "Should find product by category");

    // Test numerical property filtering
    let price_query = "g.V().hasLabel('product').has('price', gte(50.0)).count()";
    let price_result = engine.db.gremlin_query(price_query).await.unwrap();
    let price_count =
        if let Some(aster_db::query::GremlinResult::Count(c)) = price_result.results.first() {
            *c
        } else {
            0
        };
    assert_eq!(price_count, 1, "Should find product by price range");

    // Add a purchase relationship
    engine
        .add_purchase(customer_id, product_id, 2)
        .await
        .unwrap();

    // Test edge traversal
    let purchase_query = format!("g.V({}).outE('purchased').count()", customer_id.as_u64());
    let purchase_result = engine.db.gremlin_query(&purchase_query).await.unwrap();
    let purchase_count =
        if let Some(aster_db::query::GremlinResult::Count(c)) = purchase_result.results.first() {
            *c
        } else {
            0
        };
    assert_eq!(purchase_count, 1, "Should have one purchase edge");

    // Test edge property access
    let amount_query = format!(
        "g.V({}).outE('purchased').values('amount')",
        customer_id.as_u64()
    );
    let amount_result = engine.db.gremlin_query(&amount_query).await.unwrap();
    let amount = match amount_result.results.first() {
        Some(aster_db::query::GremlinResult::Value(PropertyValue::Float(f))) => *f,
        Some(aster_db::query::GremlinResult::Value(PropertyValue::Int(i))) => *i as f64,
        _ => 0.0,
    };
    assert_eq!(amount, 200.0, "Purchase amount should be 100.0 * 2 = 200.0");
}

#[tokio::test]
async fn test_analytics_performance() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = PropertyAnalyticsEngine::new(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Generate sample data
    engine.generate_sample_data().await.unwrap();

    let start_time = std::time::Instant::now();

    // Perform multiple analytics operations
    let _electronics = engine
        .find_products_by_criteria(Some("Electronics"), None, None, None)
        .await
        .unwrap();
    let _top_customers = engine.find_top_customers(5).await.unwrap();
    let _category_sales = engine.analyze_sales_by_category().await.unwrap();
    let _expensive_products = engine
        .find_products_by_criteria(None, Some(500.0), None, None)
        .await
        .unwrap();

    let elapsed = start_time.elapsed();

    assert!(
        elapsed.as_millis() < 3000,
        "Analytics operations should complete quickly (< 3s)"
    );

    println!(
        "Analytics operations completed in {}ms",
        elapsed.as_millis()
    );
}
