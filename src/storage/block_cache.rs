//! Block cache implementation for efficient SSTable data access
//!
//! This module provides a sophisticated multi-level caching system for SSTable blocks:
//! - LRU eviction policy with configurable size limits
//! - Block compression and decompression
//! - Memory pool management for reduced allocations
//! - Statistics tracking for cache performance analysis
//! - Thread-safe concurrent access

use crate::{AsterError, Result, VertexId};
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Unique identifier for a block
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockId {
    pub sstable_id: u64,
    pub block_offset: u64,
}

impl BlockId {
    pub fn new(sstable_id: u64, block_offset: u64) -> Self {
        Self {
            sstable_id,
            block_offset,
        }
    }
}

/// Cache entry containing block data and metadata
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub data: Vec<u8>,
    pub last_accessed: Instant,
    pub access_count: u64,
    pub size_bytes: usize,
    pub is_compressed: bool,
}

impl CacheEntry {
    pub fn new(data: Vec<u8>, is_compressed: bool) -> Self {
        let size_bytes = data.len();
        Self {
            data,
            last_accessed: Instant::now(),
            access_count: 1,
            size_bytes,
            is_compressed,
        }
    }

    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count = self.access_count.saturating_add(1);
    }
}

/// Cache statistics for performance monitoring
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub total_size_bytes: usize,
    pub entry_count: usize,
    pub average_access_count: f64,
    pub hit_ratio: f64,
}

impl CacheStats {
    pub fn update_hit_ratio(&mut self) {
        let total = self.hits + self.misses;
        self.hit_ratio = if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        };
    }
}

/// Configuration for the block cache
#[derive(Debug, Clone)]
pub struct BlockCacheConfig {
    pub max_size_bytes: usize,
    pub max_entries: usize,
    pub enable_compression: bool,
    pub compression_threshold: usize,
    pub ttl_seconds: Option<u64>,
    pub enable_memory_pool: bool,
    pub memory_pool_size: usize,
}

impl Default for BlockCacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 256 * 1024 * 1024, // 256MB
            max_entries: 10000,
            enable_compression: true,
            compression_threshold: 4 * 1024, // 4KB
            ttl_seconds: Some(3600),         // 1 hour
            enable_memory_pool: true,
            memory_pool_size: 32 * 1024 * 1024, // 32MB
        }
    }
}

/// Memory pool for reducing allocations
#[derive(Debug)]
struct MemoryPool {
    free_blocks: Vec<Vec<u8>>,
    total_size: usize,
    max_size: usize,
}

impl MemoryPool {
    fn new(max_size: usize) -> Self {
        Self {
            free_blocks: Vec::new(),
            total_size: 0,
            max_size,
        }
    }

    fn get_block(&mut self, min_size: usize) -> Vec<u8> {
        // Try to find a suitable block from the pool
        for i in 0..self.free_blocks.len() {
            if self.free_blocks[i].capacity() >= min_size {
                let mut block = self.free_blocks.swap_remove(i);
                self.total_size -= block.capacity();
                block.clear();
                return block;
            }
        }

        // No suitable block found, allocate new one
        Vec::with_capacity(min_size.max(4096))
    }

    fn return_block(&mut self, mut block: Vec<u8>) {
        if self.total_size + block.capacity() <= self.max_size {
            block.clear();
            self.total_size += block.capacity();
            self.free_blocks.push(block);
        }
        // Otherwise, let it drop and be deallocated
    }
}

/// Main block cache implementation
#[derive(Debug)]
pub struct BlockCache {
    config: BlockCacheConfig,
    cache: Arc<RwLock<HashMap<BlockId, CacheEntry>>>,
    access_order: Arc<Mutex<BTreeMap<Instant, BlockId>>>,
    stats: Arc<Mutex<CacheStats>>,
    memory_pool: Arc<Mutex<MemoryPool>>,
}

impl BlockCache {
    /// Create a new block cache with the given configuration
    pub fn new(config: BlockCacheConfig) -> Self {
        let memory_pool = if config.enable_memory_pool {
            Arc::new(Mutex::new(MemoryPool::new(config.memory_pool_size)))
        } else {
            Arc::new(Mutex::new(MemoryPool::new(0)))
        };

        Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
            access_order: Arc::new(Mutex::new(BTreeMap::new())),
            stats: Arc::new(Mutex::new(CacheStats::default())),
            memory_pool,
        }
    }

    /// Get a block from the cache
    pub fn get(&self, block_id: BlockId) -> Option<Vec<u8>> {
        let mut cache = self.cache.write();
        let mut stats = self.stats.lock();

        if let Some(entry) = cache.get_mut(&block_id) {
            // Cache hit
            stats.hits += 1;
            entry.touch();

            // Update access order
            let mut access_order = self.access_order.lock();
            access_order.insert(entry.last_accessed, block_id);

            // Decompress if needed
            let data = if entry.is_compressed {
                self.decompress(&entry.data)
                    .unwrap_or_else(|_| entry.data.clone())
            } else {
                entry.data.clone()
            };

            stats.update_hit_ratio();
            Some(data)
        } else {
            // Cache miss
            stats.misses += 1;
            stats.update_hit_ratio();
            None
        }
    }

    /// Put a block into the cache
    pub fn put(&self, block_id: BlockId, data: Vec<u8>) -> Result<()> {
        let compressed_data =
            if self.config.enable_compression && data.len() >= self.config.compression_threshold {
                match self.compress(&data) {
                    Ok(compressed) if compressed.len() < data.len() => (compressed, true),
                    _ => (data, false),
                }
            } else {
                (data, false)
            };

        let entry = CacheEntry::new(compressed_data.0, compressed_data.1);
        let entry_size = entry.size_bytes;

        {
            let mut cache = self.cache.write();
            let mut stats = self.stats.lock();
            let mut access_order = self.access_order.lock();

            // Check if we need to evict entries
            while (stats.total_size_bytes + entry_size > self.config.max_size_bytes)
                || (stats.entry_count >= self.config.max_entries)
            {
                if let Some((oldest_access, oldest_id)) = access_order.iter().next() {
                    let oldest_access = *oldest_access;
                    let oldest_id = *oldest_id;

                    if let Some(old_entry) = cache.remove(&oldest_id) {
                        stats.total_size_bytes =
                            stats.total_size_bytes.saturating_sub(old_entry.size_bytes);
                        stats.entry_count = stats.entry_count.saturating_sub(1);
                        stats.evictions += 1;

                        // Return memory to pool if enabled
                        if self.config.enable_memory_pool {
                            self.memory_pool.lock().return_block(old_entry.data);
                        }
                    }
                    access_order.remove(&oldest_access);
                } else {
                    break;
                }
            }

            // Insert new entry
            access_order.insert(entry.last_accessed, block_id);
            stats.total_size_bytes += entry_size;
            stats.entry_count += 1;
            cache.insert(block_id, entry);
        }

        Ok(())
    }

    /// Remove a block from the cache
    pub fn remove(&self, block_id: &BlockId) -> Option<Vec<u8>> {
        let mut cache = self.cache.write();
        let mut stats = self.stats.lock();

        if let Some(entry) = cache.remove(block_id) {
            stats.total_size_bytes = stats.total_size_bytes.saturating_sub(entry.size_bytes);
            stats.entry_count = stats.entry_count.saturating_sub(1);

            // Clean up access order
            let mut access_order = self.access_order.lock();
            access_order.retain(|_, id| id != block_id);

            // Return decompressed data
            let data = if entry.is_compressed {
                self.decompress(&entry.data).unwrap_or(entry.data)
            } else {
                entry.data
            };

            Some(data)
        } else {
            None
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        let mut stats = self.stats.lock();
        let mut access_order = self.access_order.lock();

        // Return all memory to pool if enabled
        if self.config.enable_memory_pool {
            let mut pool = self.memory_pool.lock();
            for entry in cache.values() {
                pool.return_block(entry.data.clone());
            }
        }

        cache.clear();
        access_order.clear();
        stats.total_size_bytes = 0;
        stats.entry_count = 0;
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> CacheStats {
        let mut stats = self.stats.lock().clone();
        let cache = self.cache.read();

        // Calculate average access count
        if !cache.is_empty() {
            let total_accesses: u64 = cache.values().map(|e| e.access_count).sum();
            stats.average_access_count = total_accesses as f64 / cache.len() as f64;
        }

        stats
    }

    /// Cleanup expired entries based on TTL
    pub fn cleanup_expired(&self) {
        if let Some(ttl_seconds) = self.config.ttl_seconds {
            let now = Instant::now();
            let ttl = Duration::from_secs(ttl_seconds);

            let mut cache = self.cache.write();
            let mut stats = self.stats.lock();
            let mut access_order = self.access_order.lock();

            let expired_keys: Vec<BlockId> = cache
                .iter()
                .filter_map(|(id, entry)| {
                    if now.duration_since(entry.last_accessed) > ttl {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect();

            for key in expired_keys {
                if let Some(entry) = cache.remove(&key) {
                    stats.total_size_bytes =
                        stats.total_size_bytes.saturating_sub(entry.size_bytes);
                    stats.entry_count = stats.entry_count.saturating_sub(1);
                    stats.evictions += 1;

                    // Return memory to pool
                    if self.config.enable_memory_pool {
                        self.memory_pool.lock().return_block(entry.data);
                    }
                }
            }

            // Clean up access order
            access_order.retain(|time, _| now.duration_since(*time) <= ttl);
        }
    }

    /// Compress data using LZ4
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use lz4::EncoderBuilder;
        use std::io::Write;

        let mut encoder = EncoderBuilder::new().level(1).build(Vec::new())?;
        encoder.write_all(data)?;
        let (compressed, result) = encoder.finish();
        result?;
        Ok(compressed)
    }

    /// Decompress data using LZ4
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use lz4::Decoder;
        use std::io::Read;

        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    /// Get memory pool statistics
    pub fn get_memory_pool_stats(&self) -> HashMap<String, usize> {
        let pool = self.memory_pool.lock();
        let mut stats = HashMap::new();
        stats.insert("free_blocks".to_string(), pool.free_blocks.len());
        stats.insert("total_size".to_string(), pool.total_size);
        stats.insert("max_size".to_string(), pool.max_size);
        stats
    }

    /// Resize the cache
    pub fn resize(&self, max_size_bytes: usize, max_entries: usize) {
        let mut config = self.config.clone();
        config.max_size_bytes = max_size_bytes;
        config.max_entries = max_entries;

        // Trigger eviction if necessary
        let current_stats = self.get_stats();
        if current_stats.total_size_bytes > max_size_bytes
            || current_stats.entry_count > max_entries
        {
            self.evict_to_limits(max_size_bytes, max_entries);
        }
    }

    /// Evict entries to meet size and count limits
    fn evict_to_limits(&self, max_size_bytes: usize, max_entries: usize) {
        let mut cache = self.cache.write();
        let mut stats = self.stats.lock();
        let mut access_order = self.access_order.lock();

        while (stats.total_size_bytes > max_size_bytes) || (stats.entry_count > max_entries) {
            if let Some((oldest_access, oldest_id)) = access_order.iter().next() {
                let oldest_access = *oldest_access;
                let oldest_id = *oldest_id;

                if let Some(old_entry) = cache.remove(&oldest_id) {
                    stats.total_size_bytes =
                        stats.total_size_bytes.saturating_sub(old_entry.size_bytes);
                    stats.entry_count = stats.entry_count.saturating_sub(1);
                    stats.evictions += 1;

                    // Return memory to pool
                    if self.config.enable_memory_pool {
                        self.memory_pool.lock().return_block(old_entry.data);
                    }
                }
                access_order.remove(&oldest_access);
            } else {
                break;
            }
        }
    }

    /// Prefetch blocks into cache
    pub async fn prefetch(
        &self,
        block_ids: Vec<BlockId>,
        loader: impl Fn(BlockId) -> Result<Vec<u8>>,
    ) -> Result<usize> {
        let mut prefetched = 0;

        for block_id in block_ids {
            if self.get(block_id).is_none() {
                match loader(block_id) {
                    Ok(data) => {
                        self.put(block_id, data)?;
                        prefetched += 1;
                    }
                    Err(_) => continue,
                }
            }
        }

        Ok(prefetched)
    }
}

/// Block manager for coordinating SSTable block operations
#[derive(Debug)]
pub struct BlockManager {
    cache: Arc<BlockCache>,
    block_size: usize,
}

impl BlockManager {
    pub fn new(cache: Arc<BlockCache>, block_size: usize) -> Self {
        Self { cache, block_size }
    }

    /// Load a block from storage with caching
    pub async fn load_block(
        &self,
        sstable_id: u64,
        block_offset: u64,
        loader: impl Fn(u64, u64) -> Result<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let block_id = BlockId::new(sstable_id, block_offset);

        // Try cache first
        if let Some(data) = self.cache.get(block_id) {
            return Ok(data);
        }

        // Load from storage
        let data = loader(sstable_id, block_offset)?;

        // Cache the loaded data
        self.cache.put(block_id, data.clone())?;

        Ok(data)
    }

    /// Invalidate cached blocks for an SSTable
    pub fn invalidate_sstable(&self, sstable_id: u64) {
        let cache = self.cache.cache.read();
        let keys_to_remove: Vec<BlockId> = cache
            .keys()
            .filter(|id| id.sstable_id == sstable_id)
            .copied()
            .collect();

        drop(cache);

        for key in keys_to_remove {
            self.cache.remove(&key);
        }
    }

    /// Get block size
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Get cache reference
    pub fn cache(&self) -> &BlockCache {
        &self.cache
    }

    /// Calculate block offset for a given position
    pub fn block_offset(&self, position: u64) -> u64 {
        (position / self.block_size as u64) * self.block_size as u64
    }

    /// Calculate position within block
    pub fn offset_in_block(&self, position: u64) -> usize {
        (position % self.block_size as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_block_cache_basic_operations() {
        let config = BlockCacheConfig::default();
        let cache = BlockCache::new(config);

        let block_id = BlockId::new(1, 0);
        let data = vec![1, 2, 3, 4, 5];

        // Test put and get
        cache.put(block_id, data.clone()).unwrap();
        let retrieved = cache.get(block_id).unwrap();
        assert_eq!(data, retrieved);

        // Test remove
        let removed = cache.remove(&block_id).unwrap();
        assert_eq!(data, removed);
        assert!(cache.get(block_id).is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let config = BlockCacheConfig {
            max_size_bytes: 100,
            max_entries: 2,
            ..Default::default()
        };
        let cache = BlockCache::new(config);

        // Add entries that exceed limits
        cache.put(BlockId::new(1, 0), vec![0; 40]).unwrap();
        thread::sleep(StdDuration::from_millis(1));

        cache.put(BlockId::new(2, 0), vec![1; 40]).unwrap();
        thread::sleep(StdDuration::from_millis(1));

        cache.put(BlockId::new(3, 0), vec![2; 40]).unwrap();

        let stats = cache.get_stats();
        assert_eq!(stats.entry_count, 2);
        assert!(stats.evictions > 0);

        // First block should be evicted (LRU)
        assert!(cache.get(BlockId::new(1, 0)).is_none());
        assert!(cache.get(BlockId::new(2, 0)).is_some());
        assert!(cache.get(BlockId::new(3, 0)).is_some());
    }

    #[test]
    fn test_cache_compression() {
        let config = BlockCacheConfig {
            enable_compression: true,
            compression_threshold: 10,
            ..Default::default()
        };
        let cache = BlockCache::new(config);

        let large_data = vec![42; 1000]; // Repeating pattern compresses well
        let block_id = BlockId::new(1, 0);

        cache.put(block_id, large_data.clone()).unwrap();
        let retrieved = cache.get(block_id).unwrap();

        assert_eq!(large_data, retrieved);

        // Check that compression actually happened by looking at stored size
        let stats = cache.get_stats();
        assert!(stats.total_size_bytes < large_data.len());
    }

    #[test]
    fn test_ttl_expiration() {
        let config = BlockCacheConfig {
            ttl_seconds: Some(1), // 1 second TTL
            ..Default::default()
        };
        let cache = BlockCache::new(config);

        let block_id = BlockId::new(1, 0);
        let data = vec![1, 2, 3];

        cache.put(block_id, data.clone()).unwrap();
        assert!(cache.get(block_id).is_some());

        // Wait for expiration
        thread::sleep(StdDuration::from_secs(2));
        cache.cleanup_expired();

        assert!(cache.get(block_id).is_none());
        let stats = cache.get_stats();
        assert_eq!(stats.entry_count, 0);
    }

    #[tokio::test]
    async fn test_block_manager() {
        let cache = Arc::new(BlockCache::new(BlockCacheConfig::default()));
        let manager = BlockManager::new(cache.clone(), 4096);

        let loader = |sstable_id: u64, block_offset: u64| -> Result<Vec<u8>> {
            Ok(vec![(sstable_id + block_offset) as u8; 100])
        };

        // Load block (should cache it)
        let data1 = manager.load_block(1, 0, &loader).await.unwrap();
        assert_eq!(data1.len(), 100);

        // Load same block again (should hit cache)
        let data2 = manager.load_block(1, 0, &loader).await.unwrap();
        assert_eq!(data1, data2);

        let stats = cache.get_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_memory_pool() {
        let mut pool = MemoryPool::new(8192); // Increased max_size to accommodate block capacity

        // Get a block
        let block = pool.get_block(100);
        assert!(block.capacity() >= 100);

        // Return the block
        pool.return_block(block);
        assert_eq!(pool.free_blocks.len(), 1);

        // Get another block (should reuse)
        let block2 = pool.get_block(50);
        assert!(block2.capacity() >= 50);
        assert_eq!(pool.free_blocks.len(), 0);
    }

    #[test]
    fn test_cache_resize() {
        let config = BlockCacheConfig {
            max_size_bytes: 1000,
            max_entries: 10,
            ..Default::default()
        };
        let cache = BlockCache::new(config);

        // Fill cache
        for i in 0..5 {
            cache.put(BlockId::new(i, 0), vec![0; 100]).unwrap();
        }

        let stats = cache.get_stats();
        assert_eq!(stats.entry_count, 5);

        // Resize to smaller limits
        cache.resize(300, 2);

        let new_stats = cache.get_stats();
        assert_eq!(new_stats.entry_count, 2);
        assert!(new_stats.total_size_bytes <= 300);
    }
}
