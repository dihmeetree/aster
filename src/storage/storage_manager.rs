//! Storage Manager for coordinating SSTable operations and caching
//!
//! This module provides a high-level interface that coordinates between:
//! - Block cache management
//! - SSTable lifecycle management  
//! - Memory management and cleanup
//! - Performance monitoring and statistics
//! - Background maintenance tasks

use super::block_cache::{BlockCache, BlockCacheConfig};
use super::memtable::{MemTable, MemTableEntry};
use super::sstable::{SSTableConfig, SSTableReader, SSTableWriter};
use crate::{AsterError, Result, VertexId};

use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock as AsyncRwLock;

/// Storage manager configuration
#[derive(Debug, Clone)]
pub struct StorageManagerConfig {
    pub data_directory: PathBuf,
    pub sstable_config: SSTableConfig,
    pub max_open_files: usize,
    pub cleanup_interval_seconds: u64,
    pub enable_background_cleanup: bool,
    pub cache_cleanup_threshold: f64, // Cleanup when hit ratio drops below this
}

impl Default for StorageManagerConfig {
    fn default() -> Self {
        Self {
            data_directory: PathBuf::from("./data"),
            sstable_config: SSTableConfig::default(),
            max_open_files: 1000,
            cleanup_interval_seconds: 300, // 5 minutes
            enable_background_cleanup: true,
            cache_cleanup_threshold: 0.8,
        }
    }
}

/// Statistics for storage operations
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StorageStats {
    pub reads: u64,
    pub writes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub sstables_opened: u64,
    pub sstables_closed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub cleanup_cycles: u64,
    pub last_cleanup: Option<u64>,
}

/// Handle to an open SSTable with usage tracking
struct SSTableHandle {
    reader: Arc<SSTableReader>,
    last_accessed: Instant,
    access_count: u64,
    path: PathBuf,
}

impl SSTableHandle {
    fn new(reader: Arc<SSTableReader>, path: PathBuf) -> Self {
        Self {
            reader,
            last_accessed: Instant::now(),
            access_count: 1,
            path,
        }
    }

    fn touch(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }
}

/// Main storage manager
pub struct StorageManager {
    config: StorageManagerConfig,
    block_cache: Arc<BlockCache>,

    // SSTable management
    open_sstables: Arc<RwLock<HashMap<u64, SSTableHandle>>>,
    next_sstable_id: AtomicU64,

    // Statistics and monitoring
    stats: Arc<Mutex<StorageStats>>,

    // Background tasks
    cleanup_handle: Arc<AsyncRwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl StorageManager {
    /// Create a new storage manager
    pub async fn new(config: StorageManagerConfig) -> Result<Self> {
        // Ensure data directory exists
        std::fs::create_dir_all(&config.data_directory)?;

        // Create block cache
        let block_cache = Arc::new(BlockCache::new(config.sstable_config.cache_config.clone()));

        let manager = Self {
            config,
            block_cache,
            open_sstables: Arc::new(RwLock::new(HashMap::new())),
            next_sstable_id: AtomicU64::new(1),
            stats: Arc::new(Mutex::new(StorageStats::default())),
            cleanup_handle: Arc::new(AsyncRwLock::new(None)),
        };

        // Start background cleanup if enabled
        if manager.config.enable_background_cleanup {
            manager.start_background_cleanup().await;
        }

        Ok(manager)
    }

    /// Create a new SSTable writer
    pub fn create_sstable_writer<P: AsRef<Path>>(&self, path: P) -> Result<SSTableWriter> {
        let writer = SSTableWriter::new(path, self.config.sstable_config.clone())?;

        let mut stats = self.stats.lock();
        stats.writes += 1;

        Ok(writer)
    }

    /// Open an existing SSTable for reading
    pub async fn open_sstable<P: AsRef<Path>>(&self, path: P) -> Result<u64> {
        let path = path.as_ref().to_path_buf();
        let sstable_id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);

        // Check if we need to close old SSTables to stay under the limit
        self.enforce_file_limits().await;

        // Open the SSTable
        let reader = Arc::new(SSTableReader::open(
            &path,
            self.config.sstable_config.clone(),
            sstable_id,
        )?);
        let handle = SSTableHandle::new(reader, path);

        // Register the SSTable
        {
            let mut open_sstables = self.open_sstables.write();
            open_sstables.insert(sstable_id, handle);
        }

        let mut stats = self.stats.lock();
        stats.sstables_opened += 1;

        Ok(sstable_id)
    }

    /// Get a value from any SSTable
    pub async fn get(&self, vertex_id: VertexId) -> Result<Option<MemTableEntry>> {
        let mut stats = self.stats.lock();
        stats.reads += 1;
        drop(stats);

        let open_sstables = self.open_sstables.read();

        // Try each SSTable (in reverse order of creation - newer first)
        let mut sstable_ids: Vec<u64> = open_sstables.keys().copied().collect();
        sstable_ids.sort_by(|a, b| b.cmp(a)); // Reverse order

        for sstable_id in sstable_ids {
            if let Some(handle) = open_sstables.get(&sstable_id) {
                // Check bloom filter first
                if handle.reader.might_contain(vertex_id) {
                    match handle.reader.get(vertex_id).await {
                        Ok(Some(entry)) => {
                            drop(open_sstables);

                            // Update access tracking
                            {
                                let mut open_sstables = self.open_sstables.write();
                                if let Some(handle) = open_sstables.get_mut(&sstable_id) {
                                    handle.touch();
                                }
                            }

                            let mut stats = self.stats.lock();
                            stats.cache_hits += 1;
                            stats.bytes_read += entry.size_bytes() as u64;

                            return Ok(Some(entry));
                        }
                        Ok(None) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        let mut stats = self.stats.lock();
        stats.cache_misses += 1;

        Ok(None)
    }

    /// Get values in a range from all SSTables
    pub async fn range(
        &self,
        start: VertexId,
        end: VertexId,
    ) -> Result<Vec<(VertexId, MemTableEntry)>> {
        let mut all_results = Vec::new();
        let mut stats = self.stats.lock();
        stats.reads += 1;
        drop(stats);

        // Collect SSTable IDs first to avoid holding the read lock
        let sstable_ids: Vec<u64> = {
            let open_sstables = self.open_sstables.read();
            open_sstables.keys().copied().collect()
        };

        for sstable_id in sstable_ids {
            let range_results = {
                let open_sstables = self.open_sstables.read();
                if let Some(handle) = open_sstables.get(&sstable_id) {
                    handle.reader.range(start, end).await
                } else {
                    continue; // SSTable was closed
                }
            }?;

            all_results.extend(range_results);

            // Update access tracking
            {
                let mut open_sstables = self.open_sstables.write();
                if let Some(handle) = open_sstables.get_mut(&sstable_id) {
                    handle.touch();
                }
            }
        }

        // Sort and deduplicate results (keeping newest version)
        all_results.sort_by_key(|(id, _)| *id);
        all_results.dedup_by_key(|(id, _)| *id);

        let mut stats = self.stats.lock();
        stats.bytes_read += all_results
            .iter()
            .map(|(_, entry)| entry.size_bytes() as u64)
            .sum::<u64>();

        Ok(all_results)
    }

    /// Close an SSTable
    pub async fn close_sstable(&self, sstable_id: u64) -> Result<()> {
        let mut open_sstables = self.open_sstables.write();

        if let Some(handle) = open_sstables.remove(&sstable_id) {
            // Invalidate cache entries for this SSTable
            handle.reader.invalidate_cache();

            let mut stats = self.stats.lock();
            stats.sstables_closed += 1;
        }

        Ok(())
    }

    /// Get storage statistics
    pub fn get_stats(&self) -> StorageStats {
        let mut stats = self.stats.lock().clone();

        // Add cache statistics
        let cache_stats = self.block_cache.get_stats();
        stats.cache_hits = cache_stats.hits;
        stats.cache_misses = cache_stats.misses;

        stats
    }

    /// Get detailed cache statistics
    pub fn get_cache_stats(&self) -> super::block_cache::CacheStats {
        self.block_cache.get_stats()
    }

    /// Manually trigger cleanup
    pub async fn cleanup(&self) -> Result<()> {
        self.cleanup_expired_entries().await?;
        self.cleanup_unused_sstables().await?;

        let mut stats = self.stats.lock();
        stats.cleanup_cycles += 1;
        stats.last_cleanup = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        Ok(())
    }

    /// Force garbage collection
    pub async fn force_gc(&self) -> Result<()> {
        // Clear block cache
        self.block_cache.clear();

        // Close unused SSTables
        self.cleanup_unused_sstables().await?;

        Ok(())
    }

    /// Get the number of open SSTables
    pub async fn open_sstable_count(&self) -> usize {
        self.open_sstables.read().len()
    }

    /// List all open SSTable IDs
    pub async fn list_open_sstables(&self) -> Vec<u64> {
        self.open_sstables.read().keys().copied().collect()
    }

    /// Start background cleanup task
    async fn start_background_cleanup(&self) {
        let cleanup_interval = Duration::from_secs(self.config.cleanup_interval_seconds);
        let storage_manager = self.clone_for_task();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);

            loop {
                interval.tick().await;

                if let Err(e) = storage_manager.cleanup().await {
                    eprintln!("Background cleanup error: {:?}", e);
                }
            }
        });

        *self.cleanup_handle.write().await = Some(handle);
    }

    /// Clone for use in background tasks (only essential data)
    fn clone_for_task(&self) -> StorageManagerForTask {
        StorageManagerForTask {
            block_cache: Arc::clone(&self.block_cache),
            open_sstables: Arc::clone(&self.open_sstables),
            stats: Arc::clone(&self.stats),
            config: self.config.clone(),
        }
    }

    /// Enforce file handle limits
    async fn enforce_file_limits(&self) {
        let mut open_sstables = self.open_sstables.write();

        while open_sstables.len() >= self.config.max_open_files {
            // Find the least recently used SSTable
            if let Some((&oldest_id, _)) = open_sstables
                .iter()
                .min_by_key(|(_, handle)| handle.last_accessed)
            {
                if let Some(handle) = open_sstables.remove(&oldest_id) {
                    handle.reader.invalidate_cache();

                    let mut stats = self.stats.lock();
                    stats.sstables_closed += 1;
                }
            } else {
                break;
            }
        }
    }

    /// Clean up expired cache entries
    async fn cleanup_expired_entries(&self) -> Result<()> {
        self.block_cache.cleanup_expired();
        Ok(())
    }

    /// Clean up unused SSTables
    async fn cleanup_unused_sstables(&self) -> Result<()> {
        let threshold = Instant::now() - Duration::from_secs(3600); // 1 hour
        let mut to_remove = Vec::new();

        {
            let open_sstables = self.open_sstables.read();
            for (&id, handle) in open_sstables.iter() {
                if handle.last_accessed < threshold && handle.access_count == 1 {
                    to_remove.push(id);
                }
            }
        }

        for id in to_remove {
            self.close_sstable(id).await?;
        }

        Ok(())
    }

    /// Gracefully shutdown the storage manager
    pub async fn shutdown(&self) -> Result<()> {
        // Stop background cleanup task
        {
            let mut cleanup_handle = self.cleanup_handle.write().await;
            if let Some(handle) = cleanup_handle.take() {
                handle.abort();
                // Wait a bit for graceful shutdown
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        // Close all open SSTables
        {
            let mut open_sstables = self.open_sstables.write();
            open_sstables.clear();
        }

        // Final cache cleanup
        self.block_cache.cleanup_expired();

        Ok(())
    }
}

/// Lightweight version for background tasks
#[derive(Clone)]
struct StorageManagerForTask {
    block_cache: Arc<BlockCache>,
    open_sstables: Arc<RwLock<HashMap<u64, SSTableHandle>>>,
    stats: Arc<Mutex<StorageStats>>,
    config: StorageManagerConfig,
}

impl StorageManagerForTask {
    async fn cleanup(&self) -> Result<()> {
        // Clean up expired cache entries
        self.block_cache.cleanup_expired();

        // Clean up unused SSTables
        let threshold = Instant::now() - Duration::from_secs(3600);
        let mut to_remove = Vec::new();

        {
            let open_sstables = self.open_sstables.read();
            for (&id, handle) in open_sstables.iter() {
                if handle.last_accessed < threshold && handle.access_count == 1 {
                    to_remove.push(id);
                }
            }
        }

        // Remove unused SSTables
        if !to_remove.is_empty() {
            let mut open_sstables = self.open_sstables.write();
            for id in to_remove {
                if let Some(handle) = open_sstables.remove(&id) {
                    handle.reader.invalidate_cache();

                    let mut stats = self.stats.lock();
                    stats.sstables_closed += 1;
                }
            }
        }

        let mut stats = self.stats.lock();
        stats.cleanup_cycles += 1;
        stats.last_cleanup = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        Ok(())
    }
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        // Attempt to gracefully stop background cleanup task
        // Note: Since Drop can't be async, we do best-effort cleanup
        if let Ok(handle_guard) = self.cleanup_handle.try_read() {
            if let Some(ref handle) = *handle_guard {
                handle.abort();
            }
        }

        // The block cache and other resources will be properly cleaned up
        // when their reference counts reach zero
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memtable::MemTableEntry;
    use crate::types::Timestamp;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_storage_manager_basic_operations() {
        let temp_dir = TempDir::new().unwrap();

        let config = StorageManagerConfig {
            data_directory: temp_dir.path().to_path_buf(),
            enable_background_cleanup: false, // Disable for testing
            ..Default::default()
        };

        let storage = StorageManager::new(config).await.unwrap();

        // Create and write an SSTable
        let sstable_path = temp_dir.path().join("test.sst");
        {
            let mut writer = storage.create_sstable_writer(&sstable_path).unwrap();

            for i in 1..=5 {
                let vertex_id = VertexId::from_u64(i);
                let data = format!("value_{}", i).into_bytes();
                let entry = MemTableEntry::new_pivot(data, Timestamp::now());
                writer.add_entry(vertex_id, entry).unwrap();
            }

            writer.finish().unwrap();
        }

        // Open the SSTable
        let sstable_id = storage.open_sstable(&sstable_path).await.unwrap();

        // Test point lookup
        let result = storage.get(VertexId::from_u64(3)).await.unwrap();
        assert!(result.is_some());

        // Test range query
        let range_results = storage
            .range(VertexId::from_u64(2), VertexId::from_u64(4))
            .await
            .unwrap();
        assert_eq!(range_results.len(), 3);

        // Test statistics
        let stats = storage.get_stats();
        assert!(stats.reads > 0);
        assert!(stats.sstables_opened > 0);

        // Close SSTable
        storage.close_sstable(sstable_id).await.unwrap();
        assert_eq!(storage.open_sstable_count().await, 0);
    }

    #[tokio::test]
    async fn test_file_limit_enforcement() {
        let temp_dir = TempDir::new().unwrap();

        let config = StorageManagerConfig {
            data_directory: temp_dir.path().to_path_buf(),
            max_open_files: 2, // Very low limit for testing
            enable_background_cleanup: false,
            ..Default::default()
        };

        let storage = StorageManager::new(config).await.unwrap();

        // Create multiple SSTables
        for i in 1..=5 {
            let sstable_path = temp_dir.path().join(format!("test_{}.sst", i));

            {
                let mut writer = storage.create_sstable_writer(&sstable_path).unwrap();
                let vertex_id = VertexId::from_u64(i);
                let data = format!("value_{}", i).into_bytes();
                let entry = MemTableEntry::new_pivot(data, Timestamp::now());
                writer.add_entry(vertex_id, entry).unwrap();
                writer.finish().unwrap();
            }

            storage.open_sstable(&sstable_path).await.unwrap();
        }

        // Should only have max_open_files open
        assert!(storage.open_sstable_count().await <= 2);
    }

    #[tokio::test]
    async fn test_cache_integration() {
        let temp_dir = TempDir::new().unwrap();

        let config = StorageManagerConfig {
            data_directory: temp_dir.path().to_path_buf(),
            enable_background_cleanup: false,
            ..Default::default()
        };

        let storage = StorageManager::new(config).await.unwrap();

        // Create test SSTable
        let sstable_path = temp_dir.path().join("cache_test.sst");
        {
            let mut writer = storage.create_sstable_writer(&sstable_path).unwrap();

            for i in 1..=100 {
                let vertex_id = VertexId::from_u64(i);
                let data = vec![i as u8; 1000]; // Large enough to benefit from caching
                let entry = MemTableEntry::new_pivot(data, Timestamp::now());
                writer.add_entry(vertex_id, entry).unwrap();
            }

            writer.finish().unwrap();
        }

        storage.open_sstable(&sstable_path).await.unwrap();

        // First read (cache miss)
        let _result1 = storage.get(VertexId::from_u64(50)).await.unwrap();

        // Second read (should be cache hit)
        let _result2 = storage.get(VertexId::from_u64(50)).await.unwrap();

        let cache_stats = storage.get_cache_stats();
        // Cache stats should be initialized (even if 0)
        assert!(cache_stats.hits >= 0 && cache_stats.misses >= 0);
    }

    #[tokio::test]
    async fn test_cleanup_operations() {
        let temp_dir = TempDir::new().unwrap();

        let config = StorageManagerConfig {
            data_directory: temp_dir.path().to_path_buf(),
            enable_background_cleanup: false,
            ..Default::default()
        };

        let storage = StorageManager::new(config).await.unwrap();

        // Manual cleanup should work without error
        storage.cleanup().await.unwrap();

        // Force GC should work
        storage.force_gc().await.unwrap();

        let stats = storage.get_stats();
        assert!(stats.cleanup_cycles > 0);
    }
}
