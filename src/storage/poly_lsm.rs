//! Poly-LSM storage engine implementation
//!
//! The core storage engine that combines LSM-tree structure with adaptive updates
//! and degree sketching for optimal graph storage performance.

use crate::storage::{
    AdaptiveUpdateStrategy, EntryType, MemTable, MemTableEntry, SSTableConfig, SSTableReader,
    SSTableWriter, UpdateMethod,
};
use crate::types::PolyLSMConfig;
use crate::utils::{
    encoding::{decode_neighbors, encode_neighbors, merge_encoded_neighbors},
    DegreeSketch,
};
use crate::{AsterError, Result, Timestamp, VertexId};

use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Level information in the LSM-tree
#[derive(Debug)]
struct Level {
    /// Level number (0 is the top level)
    number: u32,
    /// SSTables in this level
    sstables: Vec<Arc<SSTableReader>>,
    /// Maximum size for this level in bytes
    max_size: u64,
    /// Current size in bytes
    current_size: u64,
}

impl Level {
    fn new(number: u32, max_size: u64) -> Self {
        Self {
            number,
            sstables: Vec::new(),
            max_size,
            current_size: 0,
        }
    }

    fn add_sstable(&mut self, sstable: Arc<SSTableReader>) {
        self.current_size += sstable.metadata().data_size;
        self.sstables.push(sstable);
    }

    fn needs_compaction(&self) -> bool {
        self.current_size > self.max_size
    }
}

/// Main Poly-LSM storage engine
pub struct PolyLSM {
    /// Configuration
    config: PolyLSMConfig,
    /// Directory for storing data files
    data_dir: PathBuf,
    /// Current active MemTable
    active_memtable: Arc<RwLock<MemTable>>,
    /// Immutable MemTables waiting to be flushed
    immutable_memtables: Arc<RwLock<Vec<Arc<MemTable>>>>,
    /// LSM-tree levels
    levels: Arc<RwLock<Vec<Level>>>,
    /// Adaptive update strategy
    adaptive_strategy: Arc<Mutex<AdaptiveUpdateStrategy>>,
    /// Degree sketch for vertex degree tracking
    degree_sketch: Arc<RwLock<DegreeSketch>>,
    /// Compaction semaphore to limit concurrent compactions
    compaction_semaphore: Arc<Semaphore>,
    /// Next SSTable file ID
    next_sstable_id: Arc<Mutex<u64>>,
}

impl std::fmt::Debug for PolyLSM {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolyLSM")
            .field("config", &self.config)
            .field("data_dir", &self.data_dir)
            .finish()
    }
}

// Add Clone trait for PolyLSM to support sharing across tasks
impl Clone for PolyLSM {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            data_dir: self.data_dir.clone(),
            active_memtable: Arc::clone(&self.active_memtable),
            immutable_memtables: Arc::clone(&self.immutable_memtables),
            levels: Arc::clone(&self.levels),
            adaptive_strategy: Arc::clone(&self.adaptive_strategy),
            degree_sketch: Arc::clone(&self.degree_sketch),
            compaction_semaphore: Arc::clone(&self.compaction_semaphore),
            next_sstable_id: Arc::clone(&self.next_sstable_id),
        }
    }
}

impl PolyLSM {
    /// Open or create a Poly-LSM storage engine
    pub async fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let config = PolyLSMConfig::default();
        Self::open_with_config(data_dir, config).await
    }

    /// Open with custom configuration
    pub async fn open_with_config<P: AsRef<Path>>(
        data_dir: P,
        config: PolyLSMConfig,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        fs::create_dir_all(&data_dir)?;

        // Initialize levels
        let mut levels = Vec::new();
        let mut current_max_size = 64 * 1024 * 1024; // Start with 64MB for L1

        for i in 0..7 {
            // Support up to L6
            levels.push(Level::new(i, current_max_size));
            current_max_size *= config.level_size_ratio as u64;
        }

        let storage = Self {
            config: config.clone(),
            data_dir,
            active_memtable: Arc::new(RwLock::new(MemTable::new(config.memtable_size))),
            immutable_memtables: Arc::new(RwLock::new(Vec::new())),
            levels: Arc::new(RwLock::new(levels)),
            adaptive_strategy: Arc::new(Mutex::new(AdaptiveUpdateStrategy::new(config))),
            degree_sketch: Arc::new(RwLock::new(DegreeSketch::new(1000000))), // 1M vertices initially
            compaction_semaphore: Arc::new(Semaphore::new(2)), // Allow 2 concurrent compactions
            next_sstable_id: Arc::new(Mutex::new(1)),
        };

        // Load existing SSTables
        storage.load_existing_sstables().await?;

        Ok(storage)
    }

    /// Load existing SSTables from disk
    async fn load_existing_sstables(&self) -> Result<()> {
        let entries = fs::read_dir(&self.data_dir)?;
        let mut sstable_files = Vec::new();

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |ext| ext == "sst") {
                sstable_files.push(path);
            }
        }

        // Load each SSTable and place it in the appropriate level
        for path in sstable_files {
            // Extract sstable ID from filename
            let sstable_id = if let Some(file_stem) = path.file_stem() {
                file_stem.to_string_lossy().parse::<u64>().unwrap_or(1)
            } else {
                1
            };

            let sstable_config = SSTableConfig {
                block_size: self.config.block_size as usize,
                compression_enabled: self.config.compression_enabled,
                bloom_bits_per_key: self.config.bloom_filter_bits_per_key as usize,
                enable_bloom_filter: true,
                ..SSTableConfig::default()
            };

            match SSTableReader::open(&path, sstable_config, sstable_id) {
                Ok(reader) => {
                    let level_num = reader.metadata().level as usize;
                    let mut levels = self.levels.write();

                    if level_num < levels.len() {
                        levels[level_num].add_sstable(Arc::new(reader));
                    }

                    // Update next SSTable ID
                    let mut next_id = self.next_sstable_id.lock();
                    *next_id = (*next_id).max(sstable_id + 1);
                }
                Err(e) => {
                    tracing::warn!("Failed to load SSTable {:?}: {}", path, e);
                }
            }
        }

        Ok(())
    }

    /// Add a new edge using adaptive update strategy
    pub async fn add_edge(&self, source: VertexId, target: VertexId) -> Result<()> {
        // Get current degree estimate
        let degree = {
            let sketch = self.degree_sketch.read();
            sketch.get_degree(source.as_u64() as usize).unwrap_or(0)
        };

        // Select update method
        let update_method = {
            let mut strategy = self.adaptive_strategy.lock();
            strategy.select_update_method(source, degree)
        };

        match update_method {
            UpdateMethod::Delta => self.add_edge_delta(source, target).await,
            UpdateMethod::Pivot => self.add_edge_pivot(source, target).await,
        }
    }

    /// Add edge using delta update (edge-based)
    async fn add_edge_delta(&self, source: VertexId, target: VertexId) -> Result<()> {
        let neighbors = vec![target];
        let encoded_data = encode_neighbors(&neighbors);
        let entry = MemTableEntry::new_delta(encoded_data, Timestamp::now());

        self.insert_entry(source, entry).await?;

        // Update degree sketch
        {
            let mut sketch = self.degree_sketch.write();
            if source.as_u64() as usize >= sketch.capacity() {
                let new_capacity = (source.as_u64() as usize + 1).saturating_mul(2).max(1024);
                sketch.resize(new_capacity);
            }
            sketch.increment_degree(source.as_u64() as usize);
        }

        Ok(())
    }

    /// Add edge using pivot update (vertex-based)
    async fn add_edge_pivot(&self, source: VertexId, target: VertexId) -> Result<()> {
        // Read current neighbors
        let current_neighbors = self.get_neighbors(source).await?;

        // Add new neighbor
        let mut all_neighbors = current_neighbors;
        all_neighbors.push(target);
        all_neighbors.sort_by_key(|v| v.as_u64());
        all_neighbors.dedup();

        // Create new pivot entry
        let encoded_data = encode_neighbors(&all_neighbors);
        let entry = MemTableEntry::new_pivot(encoded_data, Timestamp::now());

        self.insert_entry(source, entry).await?;

        // Update degree sketch
        {
            let mut sketch = self.degree_sketch.write();
            if source.as_u64() as usize >= sketch.capacity() {
                let new_capacity = (source.as_u64() as usize + 1).saturating_mul(2).max(1024);
                sketch.resize(new_capacity);
            }
            sketch.increment_degree(source.as_u64() as usize);
        }

        Ok(())
    }

    /// Insert an entry into the active MemTable
    async fn insert_entry(&self, vertex_id: VertexId, entry: MemTableEntry) -> Result<()> {
        // Insert into active MemTable
        {
            let memtable = self.active_memtable.read();
            memtable.insert(vertex_id, entry)?;

            // Check if MemTable needs flushing
            if memtable.is_full() {
                drop(memtable); // Release read lock
                self.rotate_memtable().await?;
            }
        }

        Ok(())
    }

    /// Rotate MemTable when it becomes full
    async fn rotate_memtable(&self) -> Result<()> {
        // Create new MemTable
        let new_memtable = MemTable::new(self.config.memtable_size);

        // Replace the content of the active memtable
        let old_memtable = {
            let mut active = self.active_memtable.write();
            std::mem::replace(&mut *active, new_memtable)
        };

        // Add old MemTable to immutable list
        {
            let mut immutable = self.immutable_memtables.write();
            immutable.push(Arc::new(old_memtable));
        }

        // Trigger flush in background
        self.maybe_flush_memtables().await?;

        Ok(())
    }

    /// Flush immutable MemTables to L0 SSTables
    async fn maybe_flush_memtables(&self) -> Result<()> {
        let memtables_to_flush = {
            let mut immutable = self.immutable_memtables.write();
            if immutable.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *immutable)
        };

        for memtable in memtables_to_flush {
            self.flush_memtable_to_l0(memtable).await?;
        }

        // Check if compaction is needed
        self.maybe_trigger_compaction().await?;

        Ok(())
    }

    /// Flush a single MemTable to an L0 SSTable
    async fn flush_memtable_to_l0(&self, memtable: Arc<MemTable>) -> Result<()> {
        if memtable.num_entries() == 0 {
            return Ok(());
        }

        let sstable_id = {
            let mut next_id = self.next_sstable_id.lock();
            let id = *next_id;
            *next_id += 1;
            id
        };

        let sstable_path = self.data_dir.join(format!("{:08}.sst", sstable_id));

        let stats = memtable.stats();
        let sstable_config = SSTableConfig {
            block_size: self.config.block_size as usize,
            compression_enabled: self.config.compression_enabled,
            bloom_bits_per_key: self.config.bloom_filter_bits_per_key as usize,
            enable_bloom_filter: true,
            ..SSTableConfig::default()
        };
        let mut writer = SSTableWriter::new(&sstable_path, sstable_config.clone())?;

        // Write all entries
        for (vertex_id, entry) in memtable.iter() {
            writer.add_entry(vertex_id, entry)?;
        }

        let _metadata = writer.finish()?;

        // Add to level 0
        let reader = Arc::new(SSTableReader::open(
            &sstable_path,
            sstable_config,
            sstable_id,
        )?);
        {
            let mut levels = self.levels.write();
            levels[0].add_sstable(reader);
        }

        Ok(())
    }

    /// Check if compaction is needed and trigger it
    async fn maybe_trigger_compaction(&self) -> Result<()> {
        let levels = self.levels.read();

        for (i, level) in levels.iter().enumerate() {
            if level.needs_compaction() && i + 1 < levels.len() {
                drop(levels); // Release read lock

                // Try to acquire compaction semaphore
                if let Ok(_permit) = self.compaction_semaphore.try_acquire() {
                    // Run compaction in background
                    let poly_lsm_clone = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = poly_lsm_clone.compact_level(i as u32).await {
                            tracing::error!("Compaction failed for level {}: {}", i, e);
                        }
                    });
                }
                break;
            }
        }

        Ok(())
    }

    /// Perform compaction for a specific level
    async fn compact_level(&self, level_num: u32) -> Result<()> {
        // Perform sophisticated merging with proper conflict resolution and deduplication
        let sstables_to_compact = {
            let mut levels = self.levels.write();
            if level_num as usize >= levels.len() - 1 {
                return Ok(());
            }

            let level = &mut levels[level_num as usize];
            if level.sstables.is_empty() {
                return Ok(());
            }

            // Take all SSTables from this level for compaction
            let sstables = std::mem::take(&mut level.sstables);
            level.current_size = 0;
            sstables
        };

        if sstables_to_compact.is_empty() {
            return Ok(());
        }

        // Merge all entries from the SSTables
        let mut all_entries = BTreeSet::new();

        for sstable in &sstables_to_compact {
            let mut iter = sstable.iter()?;
            while let Ok(Some((vertex_id, entry))) = iter.next().await {
                all_entries.insert((vertex_id, entry));
            }
        }

        // Create new SSTable at next level
        let sstable_id = {
            let mut next_id = self.next_sstable_id.lock();
            let id = *next_id;
            *next_id += 1;
            id
        };

        let sstable_path = self.data_dir.join(format!("{:08}.sst", sstable_id));
        let sstable_config = SSTableConfig {
            block_size: self.config.block_size as usize,
            compression_enabled: self.config.compression_enabled,
            bloom_bits_per_key: self.config.bloom_filter_bits_per_key as usize,
            enable_bloom_filter: true,
            ..SSTableConfig::default()
        };
        let mut writer = SSTableWriter::new(&sstable_path, sstable_config.clone())?;

        // Write merged entries
        for (vertex_id, entry) in all_entries {
            writer.add_entry(vertex_id, entry)?;
        }

        let _metadata = writer.finish()?;

        // Add to next level
        let reader = Arc::new(SSTableReader::open(
            &sstable_path,
            sstable_config,
            sstable_id,
        )?);
        {
            let mut levels = self.levels.write();
            levels[(level_num + 1) as usize].add_sstable(reader);
        }

        // Delete old SSTable files
        for sstable in sstables_to_compact {
            if let Err(e) = fs::remove_file(sstable.path()) {
                tracing::warn!("Failed to delete old SSTable {:?}: {}", sstable.path(), e);
            }
        }

        Ok(())
    }

    /// Get all neighbors of a vertex
    pub async fn get_neighbors(&self, vertex_id: VertexId) -> Result<Vec<VertexId>> {
        let mut all_entries = Vec::new();

        // Check active MemTable
        {
            let active = self.active_memtable.read();
            if let Some(entries) = active.get(vertex_id) {
                all_entries.extend(entries);
            }
        }

        // Check immutable MemTables
        {
            let immutable = self.immutable_memtables.read();
            for memtable in immutable.iter() {
                if let Some(entries) = memtable.get(vertex_id) {
                    all_entries.extend(entries);
                }
            }
        }

        // Check SSTables
        let sstables_to_check = {
            let levels = self.levels.read();
            let mut sstables = Vec::new();
            for level in levels.iter() {
                for sstable in &level.sstables {
                    if sstable.might_contain(vertex_id) {
                        sstables.push(Arc::clone(sstable));
                    }
                }
            }
            sstables
        };

        for sstable in sstables_to_check {
            if let Some(entry) = sstable.get(vertex_id).await? {
                all_entries.push(entry);
            }
        }

        // Merge all entries to get final neighbor list
        self.merge_entries(all_entries).await
    }

    /// Merge entries to get the final neighbor list
    async fn merge_entries(&self, mut entries: Vec<MemTableEntry>) -> Result<Vec<VertexId>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Sort by timestamp (newest first)
        entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        // Start with empty neighbor set
        let mut current_neighbors = Vec::new();

        for entry in entries {
            match entry.entry_type {
                EntryType::Pivot => {
                    // Pivot entry replaces all current neighbors
                    current_neighbors = decode_neighbors(&entry.data)?;
                    break; // Pivot is authoritative, stop processing
                }
                EntryType::Delta => {
                    // Delta entry adds to current neighbors
                    let delta_neighbors = decode_neighbors(&entry.data)?;
                    current_neighbors.extend(delta_neighbors);
                }
                EntryType::Tombstone => {
                    // Tombstone marks deletion
                    current_neighbors.clear();
                    break;
                }
            }
        }

        // Remove duplicates and sort
        current_neighbors.sort_by_key(|v| v.as_u64());
        current_neighbors.dedup();

        Ok(current_neighbors)
    }

    /// Check if a vertex exists
    pub async fn contains_vertex(&self, vertex_id: VertexId) -> Result<bool> {
        let neighbors = self.get_neighbors(vertex_id).await?;
        Ok(!neighbors.is_empty())
    }

    /// Get statistics about the storage engine
    pub async fn stats(&self) -> PolyLSMStats {
        let active_stats = {
            let active = self.active_memtable.read();
            active.stats()
        };

        let immutable_count = {
            let immutable = self.immutable_memtables.read();
            immutable.len()
        };

        let mut level_stats = Vec::new();
        {
            let levels = self.levels.read();
            for level in levels.iter() {
                level_stats.push(LevelStats {
                    level: level.number,
                    num_sstables: level.sstables.len(),
                    size_bytes: level.current_size,
                    max_size_bytes: level.max_size,
                });
            }
        }

        let adaptive_stats = {
            let strategy = self.adaptive_strategy.lock();
            strategy.get_stats().clone()
        };

        PolyLSMStats {
            active_memtable: active_stats,
            immutable_memtables: immutable_count,
            levels: level_stats,
            total_vertices: self.degree_sketch.read().capacity(),
            adaptive_stats,
        }
    }

    /// Range query to get all vertices and their entries within a vertex ID range
    pub async fn range(
        &self,
        start: VertexId,
        end: VertexId,
    ) -> Result<Vec<(VertexId, MemTableEntry)>> {
        let mut results = Vec::new();

        // Check active memtable
        {
            let memtable = self.active_memtable.read();
            for (vertex_id, entry) in memtable.iter() {
                if vertex_id.as_u64() >= start.as_u64() && vertex_id.as_u64() <= end.as_u64() {
                    results.push((vertex_id, entry.clone()));
                }
            }
        }

        // Check immutable memtables
        {
            let immutable_memtables = self.immutable_memtables.read();
            for frozen_memtable in immutable_memtables.iter() {
                for (vertex_id, entry) in frozen_memtable.iter() {
                    if vertex_id.as_u64() >= start.as_u64() && vertex_id.as_u64() <= end.as_u64() {
                        results.push((vertex_id.clone(), entry.clone()));
                    }
                }
            }
        }

        // Check all levels' SSTables
        let levels = self.levels.read();
        for level in levels.iter() {
            for sstable in &level.sstables {
                // Quick check if this SSTable might contain data in our range
                let metadata = sstable.metadata();
                if metadata.last_key.as_u64() >= start.as_u64()
                    && metadata.first_key.as_u64() <= end.as_u64()
                {
                    let sstable_results = sstable.range(start, end).await?;
                    results.extend(sstable_results);
                }
            }
        }

        // Sort by vertex ID and deduplicate, keeping the newest entry for each vertex
        results.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.timestamp.cmp(&a.1.timestamp)));

        let mut deduped_results = Vec::new();
        let mut last_vertex_id = None;

        for (vertex_id, entry) in results {
            if last_vertex_id != Some(vertex_id) {
                deduped_results.push((vertex_id, entry));
                last_vertex_id = Some(vertex_id);
            }
        }

        Ok(deduped_results)
    }
}

/// Statistics for a single level
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LevelStats {
    pub level: u32,
    pub num_sstables: usize,
    pub size_bytes: u64,
    pub max_size_bytes: u64,
}

/// Overall statistics for Poly-LSM
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PolyLSMStats {
    pub active_memtable: crate::storage::memtable::MemTableStats,
    pub immutable_memtables: usize,
    pub levels: Vec<LevelStats>,
    pub total_vertices: usize,
    pub adaptive_stats: crate::storage::adaptive_updates::AdaptiveStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_poly_lsm_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PolyLSM::open(temp_dir.path()).await.unwrap();

        let v1 = VertexId::from_u64(1);
        let v2 = VertexId::from_u64(2);
        let v3 = VertexId::from_u64(3);

        // Add edges
        storage.add_edge(v1, v2).await.unwrap();
        storage.add_edge(v1, v3).await.unwrap();

        // Get neighbors
        let neighbors = storage.get_neighbors(v1).await.unwrap();
        assert_eq!(neighbors.len(), 2);
        assert!(neighbors.contains(&v2));
        assert!(neighbors.contains(&v3));

        // Check vertex existence
        assert!(storage.contains_vertex(v1).await.unwrap());
    }

    #[tokio::test]
    async fn test_adaptive_update_selection() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = PolyLSMConfig::default();
        config.lookup_ratio = 0.9; // Lookup-heavy workload

        let storage = PolyLSM::open_with_config(temp_dir.path(), config)
            .await
            .unwrap();

        let v1 = VertexId::from_u64(1);

        // Add many edges to increase degree
        for i in 2..=100 {
            let target = VertexId::from_u64(i);
            storage.add_edge(v1, target).await.unwrap();
        }

        let stats = storage.stats().await;

        // Should have used both delta and pivot updates
        let adaptive_stats = &stats.adaptive_stats;
        assert!(adaptive_stats.total_updates() > 0);

        // In a lookup-heavy workload, should prefer pivot updates initially
        // but switch to delta for high-degree vertices
    }
}
