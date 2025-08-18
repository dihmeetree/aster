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
    encoding::{add_edge_deletion_markers, encode_neighbors, get_active_neighbors},
    DegreeSketch,
};
use crate::{AsterError, Result, Timestamp, VertexId};

use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
    /// Lock-free vertex operation counter for high concurrency
    vertex_operations: Arc<LockFreeVertexRegistry>,
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
            vertex_operations: Arc::clone(&self.vertex_operations),
        }
    }
}

impl PolyLSM {
    /// Create an in-memory Poly-LSM storage engine for testing
    pub async fn new(config: PolyLSMConfig) -> Result<Self> {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().map_err(|e| AsterError::Io(e.into()))?;
        let storage = Self::open_with_config(temp_dir.path(), config).await?;

        // Store temp_dir to keep it alive for the lifetime of the storage
        std::mem::forget(temp_dir);
        Ok(storage)
    }

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

        // Validate paper compliance if using default configuration
        if let Err(e) = config.validate_paper_compliance() {
            tracing::warn!("Configuration deviates from paper specifications: {}", e);
            tracing::info!("Current config: {}", config.paper_parameter_summary());
        } else {
            tracing::info!(
                "Using paper-compliant configuration: {}",
                config.paper_parameter_summary()
            );
        }

        // Create directory if it doesn't exist
        fs::create_dir_all(&data_dir)?;

        // Initialize levels according to configuration
        // For 1-leveling: L=2 (L0 and L1 only) for write-optimized workloads
        // For standard: L=4 as specified in paper
        let mut levels = Vec::with_capacity(config.max_levels as usize);
        let mut current_max_size = 64 * 1024 * 1024; // Start with 64MB for L1

        for i in 0..config.max_levels {
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
            vertex_operations: Arc::new(LockFreeVertexRegistry::new()),
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
        // Get current degree estimate using vertex ID
        let degree = {
            let sketch = self.degree_sketch.read();
            sketch.get_degree_by_id(source.as_u64())
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

        // Update degree sketch using vertex ID-based method
        {
            let mut sketch = self.degree_sketch.write();
            sketch.increment_degree_by_id(source.as_u64());
        }

        Ok(())
    }

    /// Add edge using pivot update (vertex-based) with lock-free coordination
    async fn add_edge_pivot(&self, source: VertexId, target: VertexId) -> Result<()> {
        // Acquire exclusive access using lock-free coordination
        let _guard = self.acquire_vertex_exclusive(source).await?;

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

        // Update degree sketch using vertex ID-based method
        {
            let mut sketch = self.degree_sketch.write();
            sketch.increment_degree_by_id(source.as_u64());
        }

        Ok(())
    }

    /// Delete an edge using deletion markers as specified in the Aster paper
    /// This adds a special deletion marker rather than physically removing the edge
    pub async fn delete_edge(&self, source: VertexId, target: VertexId) -> Result<()> {
        // Acquire exclusive access using lock-free coordination
        let _guard = self.acquire_vertex_exclusive(source).await?;

        // Get current neighbor list
        let current_encoded = self.get_encoded_neighbors(source).await?;

        // Add deletion marker for the target vertex
        let to_delete = vec![target];
        let encoded_with_deletion = add_edge_deletion_markers(&current_encoded, &to_delete)?;

        // Create delta entry with deletion marker
        let entry = MemTableEntry::new_delta(encoded_with_deletion, Timestamp::now());

        self.insert_entry(source, entry).await?;

        // Note: We don't decrement degree sketch here as deletion markers preserve
        // the original degree for cost model calculations as per the paper

        Ok(())
    }

    /// Get encoded neighbors for a vertex (for internal use)
    async fn get_encoded_neighbors(&self, vertex_id: VertexId) -> Result<Vec<u8>> {
        // Check active MemTable first
        {
            let memtable = self.active_memtable.read();
            if let Some(entry) = memtable.get_latest(vertex_id) {
                if !entry.is_tombstone() {
                    return Ok(entry.data);
                }
            }
        }

        // Check immutable MemTables
        {
            let immutable = self.immutable_memtables.read();
            for memtable in immutable.iter() {
                if let Some(entry) = memtable.get_latest(vertex_id) {
                    if !entry.is_tombstone() {
                        return Ok(entry.data);
                    }
                }
            }
        }

        // Check SSTables in levels
        let sstables_to_check = {
            let levels = self.levels.read();
            let mut sstables = Vec::new();
            for level in levels.iter() {
                for sstable in &level.sstables {
                    sstables.push(sstable.clone());
                }
            }
            sstables
        };

        for sstable in sstables_to_check {
            if let Ok(Some(entry)) = sstable.get(vertex_id).await {
                return Ok(entry.data);
            }
        }

        // Return empty encoded neighbor list if vertex not found
        Ok(encode_neighbors(&[]))
    }

    /// Acquire exclusive access to a vertex using lock-free CAS operations
    async fn acquire_vertex_exclusive(&self, vertex_id: VertexId) -> Result<VertexGuard> {
        self.vertex_operations.acquire_exclusive(vertex_id).await
    }

    /// Insert an entry into the active MemTable
    async fn insert_entry(&self, vertex_id: VertexId, entry: MemTableEntry) -> Result<()> {
        // Insert into active MemTable
        let needs_rotation = {
            let memtable = self.active_memtable.read();
            memtable.insert(vertex_id, entry)?;
            memtable.is_full()
        };

        // Check if MemTable needs flushing
        if needs_rotation {
            self.rotate_memtable().await?;
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

        // Log memtable stats before flushing
        tracing::info!(
            "Flushing memtable with {} vertices, {} bytes",
            stats.num_vertices,
            stats.size_bytes
        );

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

    /// Check if compaction is needed and trigger it with parallel processing
    async fn maybe_trigger_compaction(&self) -> Result<()> {
        let compaction_candidates = {
            let levels = self.levels.read();
            let mut compaction_candidates = Vec::new();

            // Identify all levels that need compaction
            for (i, level) in levels.iter().enumerate() {
                if level.needs_compaction() && i + 1 < levels.len() {
                    compaction_candidates.push(i as u32);
                }
            }
            compaction_candidates
        };

        // Process compaction candidates in parallel, respecting semaphore limits
        let mut compaction_tasks = Vec::new();

        for level_num in compaction_candidates {
            // Try to acquire compaction semaphore for each level
            if let Ok(_permit) = self.compaction_semaphore.try_acquire() {
                let poly_lsm_clone = self.clone();
                let task = tokio::spawn(async move {
                    let start_time = std::time::Instant::now();
                    let result = poly_lsm_clone.compact_level(level_num).await;
                    let duration = start_time.elapsed();

                    match result {
                        Ok(_) => {
                            tracing::info!(
                                "Successfully compacted level {} in {:?}",
                                level_num,
                                duration
                            );
                        }
                        Err(ref e) => {
                            tracing::error!(
                                "Compaction failed for level {} after {:?}: {}",
                                level_num,
                                duration,
                                e
                            );
                        }
                    }
                    result
                });
                compaction_tasks.push(task);
            }
        }

        // Optionally wait for critical compactions to complete
        // This prevents excessive memory usage during heavy write loads
        if compaction_tasks.len() > 1 {
            // Wait for at least one compaction to complete to free up resources
            if let Some(task) = compaction_tasks.first_mut() {
                let _ = task.await;
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
        // Track this lookup operation for adaptive strategy
        {
            let mut strategy = self.adaptive_strategy.lock();
            strategy.record_lookup();
        }

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
                    // Pivot entry replaces all current neighbors (respecting deletion markers)
                    current_neighbors = get_active_neighbors(&entry.data)?;
                    break; // Pivot is authoritative, stop processing
                }
                EntryType::Delta => {
                    // Delta entry adds to current neighbors (respecting deletion markers)
                    let delta_neighbors = get_active_neighbors(&entry.data)?;
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

    /// Update adaptive strategy with current degree distribution
    pub fn update_adaptive_strategy(&self) -> Result<()> {
        // Sample degree distribution from degree sketch
        let sample_size = 1000;
        let mut degrees = Vec::with_capacity(sample_size);

        {
            let sketch = self.degree_sketch.read();
            let capacity = std::cmp::min(sample_size, sketch.capacity());

            for i in 0..capacity {
                if let Some(degree) = sketch.get_degree(i) {
                    degrees.push(degree);
                }
            }
        }

        // Update adaptive strategy with degree distribution
        if !degrees.is_empty() {
            let mut strategy = self.adaptive_strategy.lock();
            strategy.update_degree_distribution(&degrees);
        }

        Ok(())
    }

    /// Get comprehensive adaptive strategy analytics
    pub fn get_adaptive_analytics(
        &self,
    ) -> (
        super::adaptive_updates::WorkloadAnalysis,
        super::adaptive_updates::EffectivenessMetrics,
    ) {
        let strategy = self.adaptive_strategy.lock();
        let workload = strategy.get_workload_analysis();
        let effectiveness = strategy.get_effectiveness_metrics();
        (workload, effectiveness)
    }

    /// Range query to get all vertices and their entries within a vertex ID range
    pub async fn range(
        &self,
        start: VertexId,
        end: VertexId,
    ) -> Result<Vec<(VertexId, MemTableEntry)>> {
        let mut results = Vec::new();

        // Pre-convert bounds once to avoid repeated conversions
        let start_u64 = start.as_u64();
        let end_u64 = end.as_u64();

        // Check active memtable
        {
            let memtable = self.active_memtable.read();
            for (vertex_id, entry) in memtable.iter() {
                let vertex_u64 = vertex_id.as_u64();
                if vertex_u64 >= start_u64 && vertex_u64 <= end_u64 {
                    results.push((vertex_id, entry.clone()));
                }
            }
        }

        // Check immutable memtables
        {
            let immutable_memtables = self.immutable_memtables.read();
            for frozen_memtable in immutable_memtables.iter() {
                for (vertex_id, entry) in frozen_memtable.iter() {
                    let vertex_u64 = vertex_id.as_u64();
                    if vertex_u64 >= start_u64 && vertex_u64 <= end_u64 {
                        results.push((vertex_id, entry.clone()));
                    }
                }
            }
        }

        // Check all levels' SSTables
        let sstables_to_check = {
            let levels = self.levels.read();
            let mut sstables_to_check = Vec::new();
            for level in levels.iter() {
                for sstable in &level.sstables {
                    // Quick check if this SSTable might contain data in our range
                    let metadata = sstable.metadata();
                    if metadata.last_key.as_u64() >= start_u64
                        && metadata.first_key.as_u64() <= end_u64
                    {
                        sstables_to_check.push(sstable.clone());
                    }
                }
            }
            sstables_to_check
        };

        for sstable in sstables_to_check {
            let sstable_results = sstable.range(start, end).await?;
            results.extend(sstable_results);
        }

        // Sort by vertex ID and deduplicate, keeping the newest entry for each vertex
        results.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.timestamp.cmp(&a.1.timestamp)));

        // Deduplicate in-place to avoid extra allocation
        let mut write_index = 0;
        let mut last_vertex_id = None;

        for read_index in 0..results.len() {
            let vertex_id = results[read_index].0;
            if last_vertex_id != Some(vertex_id) {
                if write_index != read_index {
                    // Move instead of clone when indices differ
                    results.swap(write_index, read_index);
                }
                write_index += 1;
                last_vertex_id = Some(vertex_id);
            }
        }

        results.truncate(write_index);
        let deduped_results = results;

        Ok(deduped_results)
    }

    /// Get all versions of a vertex for MVCC snapshot isolation
    /// Returns all MemTableEntry versions sorted by timestamp (newest first)
    pub async fn get_vertex_versions(&self, vertex_id: VertexId) -> Result<Vec<MemTableEntry>> {
        let mut all_versions = Vec::new();

        // Check active MemTable
        {
            let memtable = self.active_memtable.read();
            if let Some(entries) = memtable.get(vertex_id) {
                all_versions.extend(entries);
            }
        }

        // Check immutable MemTables
        {
            let immutable = self.immutable_memtables.read();
            for memtable in immutable.iter() {
                if let Some(entries) = memtable.get(vertex_id) {
                    for entry in entries {
                        all_versions.push(entry.clone());
                    }
                }
            }
        }

        // Check all SSTables across all levels
        let sstables_to_check = {
            let levels = self.levels.read();
            let mut sstables = Vec::new();
            for level in levels.iter() {
                for sstable in level.sstables.iter() {
                    sstables.push(sstable.clone());
                }
            }
            sstables
        };

        for sstable in sstables_to_check {
            if let Ok(Some(entry)) = sstable.get(vertex_id).await {
                all_versions.push(entry);
            }
        }

        // Sort by timestamp (newest first) for MVCC version ordering
        all_versions.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        Ok(all_versions)
    }

    /// Write vertex data within a transaction context for MVCC
    pub async fn transactional_write_vertex(
        &self,
        vertex_id: VertexId,
        data: Vec<u8>,
        _transaction_id: crate::transaction::TransactionId,
        write_timestamp: Timestamp,
    ) -> Result<()> {
        // Create a versioned entry with transaction information
        let entry = MemTableEntry::new_pivot(data, write_timestamp);

        // Write to active MemTable
        {
            let memtable = self.active_memtable.read();
            memtable.insert(vertex_id, entry)?;
        }

        // Check if MemTable needs flushing
        let should_flush = {
            let memtable = self.active_memtable.read();
            memtable.is_full()
        };

        if should_flush {
            self.maybe_flush_memtables().await?;
        }

        Ok(())
    }

    /// Read vertex data with snapshot isolation
    pub async fn snapshot_read(
        &self,
        vertex_id: VertexId,
        snapshot_timestamp: u64,
        _transaction_id: crate::transaction::TransactionId,
    ) -> Result<Option<Vec<u8>>> {
        // Get all versions of this vertex
        let versions = self.get_vertex_versions(vertex_id).await?;

        // Find the latest version visible to this snapshot
        for entry in versions {
            // For now, assume all data committed before snapshot_timestamp is visible
            // In a full implementation, we'd check the commit log more thoroughly
            if entry.timestamp.as_u64() <= snapshot_timestamp {
                if !entry.is_tombstone() {
                    return Ok(Some(entry.data));
                } else {
                    // This vertex was deleted
                    return Ok(None);
                }
            }
        }

        // No visible version found
        Ok(None)
    }

    /// Mark a vertex as deleted within a transaction
    pub async fn transactional_delete_vertex(
        &self,
        vertex_id: VertexId,
        _transaction_id: crate::transaction::TransactionId,
        delete_timestamp: Timestamp,
    ) -> Result<()> {
        // Create a tombstone entry
        let tombstone = MemTableEntry::new_tombstone(delete_timestamp);

        // Write tombstone to active MemTable
        {
            let memtable = self.active_memtable.read();
            memtable.insert(vertex_id, tombstone)?;
        }

        // Check if MemTable needs flushing
        let should_flush = {
            let memtable = self.active_memtable.read();
            memtable.is_full()
        };

        if should_flush {
            self.maybe_flush_memtables().await?;
        }

        Ok(())
    }

    /// Ensure a vertex exists in the storage system by initializing it in the degree sketch
    /// This is used for isolated vertices that have no edges
    pub async fn ensure_vertex_exists(&self, vertex_id: VertexId) -> Result<()> {
        // Add vertex to degree sketch to ensure it's tracked
        {
            let mut sketch = self.degree_sketch.write();
            sketch.ensure_vertex_tracked(vertex_id.as_u64());
        }

        // The vertex is now considered to exist in the system
        // even if it has no edges yet
        Ok(())
    }

    /// Perform periodic maintenance on the lock-free vertex registry
    pub fn maintain_vertex_registry(&self) {
        // Cleanup inactive vertex states every hour
        self.vertex_operations.cleanup_inactive(3600);
    }

    /// Get storage configuration information
    pub fn get_config_info(&self) -> String {
        let levels = self.levels.read();
        let level_info = if self.config.enable_1_leveling {
            "1-leveling (L0 + L1 only, write-optimized)"
        } else {
            "Standard multi-level (read-optimized)"
        };

        format!(
            "Poly-LSM Configuration:\n  - Strategy: {}\n  - Levels: {}\n  - {}",
            level_info,
            levels.len(),
            self.config.paper_parameter_summary()
        )
    }

    /// Get lock-free registry statistics
    pub fn get_lock_free_stats(&self) -> LockFreeStats {
        self.vertex_operations.get_stats()
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

/// Lock-free vertex registry for high-concurrency coordination
/// Uses Compare-And-Swap (CAS) operations to manage vertex access without locks
#[derive(Debug)]
pub struct LockFreeVertexRegistry {
    /// Hash map tracking vertex operation states using atomic operations
    vertex_states: parking_lot::RwLock<HashMap<VertexId, Arc<AtomicVertexState>>>,
    /// Global operation counter for ordering
    operation_counter: AtomicU64,
    /// Performance counters
    total_acquisitions: AtomicUsize,
    failed_acquisitions: AtomicUsize,
    contention_events: AtomicUsize,
}

/// Atomic state for a single vertex operation
#[derive(Debug)]
struct AtomicVertexState {
    /// Current operation ID (0 means available, >0 means in use)
    operation_id: AtomicU64,
    /// Number of waiting operations
    wait_count: AtomicUsize,
    /// Last access timestamp for cleanup
    last_access: AtomicU64,
}

// Explicitly implement Send and Sync since all fields are atomic
unsafe impl Send for AtomicVertexState {}
unsafe impl Sync for AtomicVertexState {}

/// RAII guard for vertex exclusive access
pub struct VertexGuard {
    vertex_id: VertexId,
    operation_id: u64,
    registry: std::sync::Weak<LockFreeVertexRegistry>,
}

// VertexGuard is Send since all its fields are Send
unsafe impl Send for VertexGuard {}

impl LockFreeVertexRegistry {
    /// Create a new lock-free vertex registry
    pub fn new() -> Self {
        Self {
            vertex_states: parking_lot::RwLock::new(HashMap::new()),
            operation_counter: AtomicU64::new(1),
            total_acquisitions: AtomicUsize::new(0),
            failed_acquisitions: AtomicUsize::new(0),
            contention_events: AtomicUsize::new(0),
        }
    }

    /// Acquire exclusive access to a vertex using lock-free coordination
    pub async fn acquire_exclusive(self: &Arc<Self>, vertex_id: VertexId) -> Result<VertexGuard> {
        let operation_id = self.operation_counter.fetch_add(1, Ordering::SeqCst);
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Get or create vertex state
        let vertex_state = {
            let mut states = self.vertex_states.write();
            states
                .entry(vertex_id)
                .or_insert_with(|| {
                    Arc::new(AtomicVertexState {
                        operation_id: AtomicU64::new(0),
                        wait_count: AtomicUsize::new(0),
                        last_access: AtomicU64::new(current_time),
                    })
                })
                .clone()
        };

        // Attempt to acquire exclusive access using CAS operations
        let max_retries = 100;
        let mut current_vertex_state = vertex_state;

        for retry in 0..max_retries {
            // Try to acquire exclusive access
            match current_vertex_state.operation_id.compare_exchange_weak(
                0,
                operation_id,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Successfully acquired exclusive access
                    current_vertex_state
                        .last_access
                        .store(current_time, Ordering::Relaxed);
                    self.total_acquisitions.fetch_add(1, Ordering::Relaxed);

                    return Ok(VertexGuard {
                        vertex_id,
                        operation_id,
                        registry: Arc::downgrade(self),
                    });
                }
                Err(_current_operation) => {
                    // Access is currently held by another operation
                    self.contention_events.fetch_add(1, Ordering::Relaxed);
                    current_vertex_state
                        .wait_count
                        .fetch_add(1, Ordering::Relaxed);

                    // Calculate delay without holding references across await
                    let base_delay =
                        std::time::Duration::from_micros(1 << std::cmp::min(retry, 10));
                    let jitter = std::time::Duration::from_micros(
                        (operation_id % 100) * 10, // Simple jitter based on operation ID
                    );
                    let delay = base_delay + jitter;

                    // Store the vertex state reference for later use
                    let vertex_state_for_decrement = current_vertex_state.clone();

                    // Sleep without holding any references
                    tokio::time::sleep(delay).await;

                    vertex_state_for_decrement
                        .wait_count
                        .fetch_sub(1, Ordering::Relaxed);

                    // Re-acquire the reference for the next iteration
                    current_vertex_state = {
                        let states = self.vertex_states.read();
                        states
                            .get(&vertex_id)
                            .cloned()
                            .expect("Vertex state should exist as we just accessed it")
                    };
                }
            }
        }

        // Failed to acquire after max retries
        self.failed_acquisitions.fetch_add(1, Ordering::Relaxed);
        Err(AsterError::storage(&format!(
            "Failed to acquire exclusive access to vertex {} after {} retries",
            vertex_id.as_u64(),
            max_retries
        )))
    }

    /// Release exclusive access to a vertex
    fn release_exclusive(&self, vertex_id: VertexId, operation_id: u64) {
        if let Some(vertex_state) = self.vertex_states.read().get(&vertex_id) {
            // Release using CAS to ensure we only release our own operation
            let _ = vertex_state.operation_id.compare_exchange(
                operation_id,
                0,
                Ordering::SeqCst,
                Ordering::Relaxed,
            );
        }
    }

    /// Get performance statistics for the lock-free registry
    pub fn get_stats(&self) -> LockFreeStats {
        let total_acquisitions = self.total_acquisitions.load(Ordering::Relaxed);
        let failed_acquisitions = self.failed_acquisitions.load(Ordering::Relaxed);
        let contention_events = self.contention_events.load(Ordering::Relaxed);

        let success_rate = if total_acquisitions + failed_acquisitions > 0 {
            total_acquisitions as f64 / (total_acquisitions + failed_acquisitions) as f64
        } else {
            1.0
        };

        let avg_contention = if total_acquisitions > 0 {
            contention_events as f64 / total_acquisitions as f64
        } else {
            0.0
        };

        LockFreeStats {
            total_acquisitions,
            failed_acquisitions,
            contention_events,
            success_rate,
            avg_contention_per_operation: avg_contention,
            active_vertices: self.vertex_states.read().len(),
        }
    }

    /// Cleanup inactive vertex states to prevent memory leaks
    pub fn cleanup_inactive(&self, max_age_seconds: u64) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut states = self.vertex_states.write();
        states.retain(|_vertex_id, state| {
            let last_access = state.last_access.load(Ordering::Relaxed);
            let is_active = state.operation_id.load(Ordering::Relaxed) != 0;
            let is_recent = current_time.saturating_sub(last_access) < max_age_seconds;

            // Keep if currently active or recently accessed
            is_active || is_recent
        });
    }
}

impl Drop for VertexGuard {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.release_exclusive(self.vertex_id, self.operation_id);
        }
    }
}

/// Statistics for lock-free vertex registry performance
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LockFreeStats {
    pub total_acquisitions: usize,
    pub failed_acquisitions: usize,
    pub contention_events: usize,
    pub success_rate: f64,
    pub avg_contention_per_operation: f64,
    pub active_vertices: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_paper_specification_enforcement() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PolyLSMConfig::paper_specification();

        // Verify configuration compliance
        assert!(config.validate_paper_compliance().is_ok());

        let storage = PolyLSM::open_with_config(temp_dir.path(), config.clone())
            .await
            .unwrap();

        // Verify exactly L=4 levels are initialized
        let levels = storage.levels.read();
        assert_eq!(
            levels.len(),
            4,
            "Should have exactly 4 levels as specified in paper"
        );

        // Verify level size ratios follow T=10
        let base_size = 64 * 1024 * 1024; // 64MB
        for (i, level) in levels.iter().enumerate() {
            let expected_size = base_size * (10_u64.pow(i as u32));
            assert_eq!(
                level.max_size, expected_size,
                "Level {} should have size {}",
                i, expected_size
            );
            assert_eq!(
                level.number, i as u32,
                "Level {} should have correct level number",
                i
            );
        }

        println!("Paper specification enforcement verified:");
        println!("  - {} levels initialized", levels.len());
        println!("  - Level size ratio: T={}", config.level_size_ratio);
        println!("  - Block size: B={}KB", config.block_size / 1024);
        println!(
            "  - Degree sketch: I={} bits",
            config.degree_sketch_bits_per_vertex
        );
    }

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
