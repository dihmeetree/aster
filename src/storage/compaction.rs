//! Advanced compaction strategies for LSM-tree levels
//!
//! Implements sophisticated compaction algorithms including:
//! - Level-based compaction for optimal read performance
//! - Size-tiered compaction for write-heavy workloads
//! - Hybrid compaction strategies
//! - Entry merging with proper semantics for pivot/delta entries

use crate::storage::{EntryType, MemTableEntry, SSTableConfig, SSTableReader, SSTableWriter};
use crate::utils::encoding::{
    decode_neighbors, decode_neighbors_adaptive, encode_neighbors, encode_neighbors_adaptive,
    get_encoding_stats,
};
use crate::{AsterError, Result, Timestamp, VertexId};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Compaction strategy selection
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompactionStrategy {
    /// Level-based compaction (optimizes for read performance)
    Leveled,
    /// Size-tiered compaction (optimizes for write performance)
    SizeTiered,
    /// Hybrid strategy that adapts to workload
    Adaptive,
}

/// Configuration for compaction operations
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Compaction strategy to use
    pub strategy: CompactionStrategy,
    /// Maximum number of SSTables to compact at once
    pub max_sstables_per_compaction: usize,
    /// Size ratio threshold for triggering compaction
    pub size_ratio_threshold: f64,
    /// Maximum compaction parallelism
    pub max_concurrent_compactions: usize,
    /// Block size for new SSTables
    pub block_size: u32,
    /// Enable compression in compacted SSTables
    pub compression_enabled: bool,
    /// Bloom filter bits per key
    pub bloom_bits_per_key: u32,
    /// Enable graph-aware merging strategies
    pub graph_aware_merging: bool,
    /// Minimum neighbor count to prefer pivot entries
    pub pivot_threshold: usize,
    /// Maximum time window for delta aggregation (ms)
    pub delta_aggregation_window_ms: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            strategy: CompactionStrategy::Leveled,
            max_sstables_per_compaction: 10,
            size_ratio_threshold: 2.0,
            max_concurrent_compactions: 2,
            block_size: 4096,
            compression_enabled: true,
            bloom_bits_per_key: 10,
            graph_aware_merging: true,
            pivot_threshold: 16, // Create pivot entries for vertices with 16+ neighbors
            delta_aggregation_window_ms: 300000, // 5 minutes
        }
    }
}

/// Represents an entry with its merge state during compaction
#[derive(Debug, Clone)]
struct CompactionEntry {
    vertex_id: VertexId,
    entry: MemTableEntry,
    source_level: u32,
    source_sstable_id: u64,
}

/// Result of a compaction operation
#[derive(Debug)]
pub struct CompactionResult {
    /// New SSTables created
    pub new_sstables: Vec<PathBuf>,
    /// Old SSTables that can be deleted
    pub old_sstables: Vec<PathBuf>,
    /// Statistics about the compaction
    pub stats: CompactionStats,
}

/// Statistics collected during compaction
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionStats {
    pub entries_processed: usize,
    pub entries_merged: usize,
    pub pivot_entries_created: usize,
    pub delta_entries_eliminated: usize,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub compression_ratio: f64,
    pub duration_ms: u64,
    pub neighbor_compression_ratio: f64,
    pub graph_locality_score: f64,
}

/// Version history tracking for a single neighbor during compaction
#[derive(Debug, Clone)]
struct NeighborVersionHistory {
    /// Pivot entries where this neighbor was present: (timestamp, source_level)
    pivot_presences: Vec<(Timestamp, u32)>,
    /// Delta additions for this neighbor: (timestamp, source_level)
    delta_additions: Vec<(Timestamp, u32)>,
    /// First time this neighbor was seen
    first_seen: Option<Timestamp>,
    /// Last time this neighbor was seen
    last_seen: Option<Timestamp>,
}

impl NeighborVersionHistory {
    fn new() -> Self {
        Self {
            pivot_presences: Vec::new(),
            delta_additions: Vec::new(),
            first_seen: None,
            last_seen: None,
        }
    }

    fn add_pivot_presence(&mut self, timestamp: Timestamp, source_level: u32) {
        self.pivot_presences.push((timestamp, source_level));
        self.update_time_bounds(timestamp);

        // Keep sorted by timestamp (newest first)
        self.pivot_presences.sort_by(|a, b| b.0.cmp(&a.0));
    }

    fn add_delta_addition(&mut self, timestamp: Timestamp, source_level: u32) {
        self.delta_additions.push((timestamp, source_level));
        self.update_time_bounds(timestamp);

        // Keep sorted by timestamp (newest first)
        self.delta_additions.sort_by(|a, b| b.0.cmp(&a.0));
    }

    fn update_time_bounds(&mut self, timestamp: Timestamp) {
        if self.first_seen.is_none() || timestamp < self.first_seen.unwrap() {
            self.first_seen = Some(timestamp);
        }
        if self.last_seen.is_none() || timestamp > self.last_seen.unwrap() {
            self.last_seen = Some(timestamp);
        }
    }

    /// Check if there are valid delta additions after a given pivot time
    fn has_valid_delta_after(&self, pivot_time: Timestamp, window_ms: u64) -> bool {
        self.delta_additions.iter().any(|(delta_time, _)| {
            *delta_time > pivot_time
                && pivot_time.as_u64().saturating_sub(delta_time.as_u64()) <= window_ms
        })
    }

    /// Check if the addition is confirmed by frequency or recency
    fn is_addition_confirmed(&self, current_time: Timestamp, window_ms: u64) -> bool {
        if self.delta_additions.is_empty() {
            return false;
        }

        // Check recency - any addition within the window
        let has_recent_addition = self.delta_additions.iter().any(|(timestamp, _)| {
            current_time.as_u64().saturating_sub(timestamp.as_u64()) <= window_ms
        });

        // Check frequency - multiple additions suggest permanence
        let addition_count = self.delta_additions.len();

        has_recent_addition || addition_count >= 2
    }

    /// Check if the neighbor has been recently active
    fn is_recently_active(&self, current_time: Timestamp, window_ms: u64) -> bool {
        if let Some(last_seen) = self.last_seen {
            current_time.as_u64().saturating_sub(last_seen.as_u64()) <= window_ms
        } else {
            false
        }
    }

    /// Check if there has been recent delta activity
    fn has_recent_delta_activity(&self, current_time: Timestamp, window_ms: u64) -> bool {
        self.delta_additions.iter().any(|(timestamp, _)| {
            current_time.as_u64().saturating_sub(timestamp.as_u64()) <= window_ms
        })
    }

    /// Get the activity frequency (number of operations per time unit)
    fn get_activity_frequency(&self) -> f64 {
        let total_operations = self.pivot_presences.len() + self.delta_additions.len();
        if total_operations == 0 {
            return 0.0;
        }

        if let (Some(first), Some(last)) = (self.first_seen, self.last_seen) {
            let time_span = last.as_u64().saturating_sub(first.as_u64()).max(1);
            total_operations as f64 / (time_span as f64 / 1000.0) // Operations per second
        } else {
            total_operations as f64
        }
    }

    /// Get the most authoritative presence (most recent from highest level)
    fn get_most_authoritative_presence(&self) -> Option<(Timestamp, u32)> {
        let mut all_presences = Vec::new();
        all_presences.extend(self.pivot_presences.iter().cloned());
        all_presences.extend(self.delta_additions.iter().cloned());

        all_presences
            .iter()
            .max_by_key(|(timestamp, level)| (*timestamp, *level))
            .copied()
    }
}

/// Main compaction engine
pub struct CompactionEngine {
    config: CompactionConfig,
    data_dir: PathBuf,
}

impl CompactionEngine {
    /// Create a new compaction engine
    pub fn new(config: CompactionConfig, data_dir: PathBuf) -> Self {
        Self { config, data_dir }
    }

    /// Perform level-based compaction
    pub async fn compact_level(
        &self,
        level: u32,
        input_sstables: Vec<Arc<SSTableReader>>,
        next_sstable_id: &mut u64,
    ) -> Result<CompactionResult> {
        let start_time = std::time::Instant::now();
        info!(
            "Starting compaction for level {} with {} SSTables",
            level,
            input_sstables.len()
        );

        if input_sstables.is_empty() {
            return Ok(CompactionResult {
                new_sstables: Vec::new(),
                old_sstables: Vec::new(),
                stats: CompactionStats::default(),
            });
        }

        // Collect all entries from input SSTables
        let entries = self.collect_entries(&input_sstables).await?;
        let num_entries = entries.len();
        info!("Collected {} entries for compaction", num_entries);

        // Merge entries according to Poly-LSM semantics
        let merged_entries = self.merge_entries(entries).await?;
        info!("After merging: {} entries", merged_entries.len());

        // Write merged entries to new SSTables
        let new_sstables = self
            .write_compacted_sstables(&merged_entries, level + 1, next_sstable_id)
            .await?;

        // Collect old SSTable paths for cleanup
        let old_sstables: Vec<PathBuf> = input_sstables
            .iter()
            .map(|sstable| sstable.path().to_path_buf())
            .collect();

        let duration = start_time.elapsed();

        // Calculate compression statistics
        let mut total_original_bytes = 0u64;
        let mut total_compressed_bytes = 0u64;
        let mut pivot_count = 0;
        let mut delta_count = 0;

        for (_, entry) in &merged_entries {
            match entry.entry_type {
                EntryType::Pivot => pivot_count += 1,
                EntryType::Delta => delta_count += 1,
                _ => {}
            }

            // Calculate compression ratio if we have neighbor data
            if !entry.data.is_empty() {
                if let Ok(neighbors) = decode_neighbors_adaptive(&entry.data) {
                    let stats = get_encoding_stats(&neighbors, &entry.data);
                    total_original_bytes += stats.original_size_bytes as u64;
                    total_compressed_bytes += stats.compressed_size_bytes as u64;
                }
            }
        }

        let neighbor_compression_ratio = if total_original_bytes > 0 {
            total_compressed_bytes as f64 / total_original_bytes as f64
        } else {
            1.0
        };

        // Calculate graph locality score (how well neighbors are clustered)
        let graph_locality_score = self.calculate_graph_locality_score(&merged_entries);

        let stats = CompactionStats {
            entries_processed: num_entries,
            entries_merged: num_entries - merged_entries.len(),
            pivot_entries_created: pivot_count,
            delta_entries_eliminated: num_entries.saturating_sub(pivot_count + delta_count),
            bytes_read: input_sstables.iter().map(|s| s.metadata().data_size).sum(),
            bytes_written: total_compressed_bytes,
            compression_ratio: if num_entries > 0 {
                (num_entries - merged_entries.len()) as f64 / num_entries as f64
            } else {
                0.0
            },
            duration_ms: duration.as_millis() as u64,
            neighbor_compression_ratio,
            graph_locality_score,
        };

        info!(
            "Compaction completed in {}ms, created {} new SSTables",
            stats.duration_ms,
            new_sstables.len()
        );

        Ok(CompactionResult {
            new_sstables,
            old_sstables,
            stats,
        })
    }

    /// Collect all entries from input SSTables, sorted by vertex ID and timestamp
    async fn collect_entries(
        &self,
        sstables: &[Arc<SSTableReader>],
    ) -> Result<Vec<CompactionEntry>> {
        let mut all_entries = Vec::new();

        for (sstable_idx, sstable) in sstables.iter().enumerate() {
            let mut sstable_iter = sstable.iter()?;

            while let Ok(Some((vertex_id, entry))) = sstable_iter.next().await {
                all_entries.push(CompactionEntry {
                    vertex_id,
                    entry,
                    source_level: sstable.metadata().level,
                    source_sstable_id: sstable_idx as u64, // Use index as identifier
                });
            }
        }

        // Sort by vertex ID first, then by timestamp (newest first)
        all_entries.sort_by(|a, b| {
            a.vertex_id
                .cmp(&b.vertex_id)
                .then_with(|| b.entry.timestamp.cmp(&a.entry.timestamp))
        });

        Ok(all_entries)
    }

    /// Merge entries according to Poly-LSM semantics
    async fn merge_entries(
        &self,
        entries: Vec<CompactionEntry>,
    ) -> Result<Vec<(VertexId, MemTableEntry)>> {
        let mut merged_entries = Vec::new();
        let mut vertex_entries: BTreeMap<VertexId, Vec<CompactionEntry>> = BTreeMap::new();

        // Group entries by vertex ID
        for entry in entries {
            vertex_entries
                .entry(entry.vertex_id)
                .or_default()
                .push(entry);
        }

        // Process each vertex's entries
        for (vertex_id, mut entries) in vertex_entries {
            // Sort by timestamp (newest first)
            entries.sort_by(|a, b| b.entry.timestamp.cmp(&a.entry.timestamp));

            let merged_entry = self.merge_vertex_entries(&entries).await?;
            if let Some(entry) = merged_entry {
                merged_entries.push((vertex_id, entry));
            }
        }

        Ok(merged_entries)
    }

    /// Merge all entries for a single vertex according to Poly-LSM semantics with graph-aware optimizations
    async fn merge_vertex_entries(
        &self,
        entries: &[CompactionEntry],
    ) -> Result<Option<MemTableEntry>> {
        if entries.is_empty() {
            return Ok(None);
        }

        // Check for tombstone first
        for entry in entries {
            if matches!(entry.entry.entry_type, EntryType::Tombstone) {
                return Ok(None);
            }
        }

        if self.config.graph_aware_merging {
            self.merge_vertex_entries_graph_aware(entries).await
        } else {
            self.merge_vertex_entries_basic(entries).await
        }
    }

    /// Basic merging strategy (original implementation)
    async fn merge_vertex_entries_basic(
        &self,
        entries: &[CompactionEntry],
    ) -> Result<Option<MemTableEntry>> {
        let mut current_neighbors = BTreeSet::new();
        let latest_timestamp = entries[0].entry.timestamp;

        for entry in entries {
            match entry.entry.entry_type {
                EntryType::Pivot => {
                    // Pivot entry is authoritative - replace all current neighbors
                    let pivot_neighbors = decode_neighbors_adaptive(&entry.entry.data)?;
                    current_neighbors = pivot_neighbors.into_iter().map(|v| v.as_u64()).collect();
                    break; // Stop processing older entries
                }
                EntryType::Delta => {
                    // Delta entry adds to current neighbors
                    let delta_neighbors = decode_neighbors_adaptive(&entry.entry.data)?;
                    for neighbor in delta_neighbors {
                        current_neighbors.insert(neighbor.as_u64());
                    }
                }
                EntryType::Tombstone => {
                    return Ok(None);
                }
            }
        }

        if current_neighbors.is_empty() {
            return Ok(None);
        }

        let final_neighbors: Vec<VertexId> = current_neighbors
            .into_iter()
            .map(VertexId::from_u64)
            .collect();

        let encoded_data = encode_neighbors_adaptive(&final_neighbors)?;
        let merged_entry = MemTableEntry::new_pivot(encoded_data, latest_timestamp);

        Ok(Some(merged_entry))
    }

    /// Graph-aware merging strategy that considers neighbor patterns and temporal locality
    async fn merge_vertex_entries_graph_aware(
        &self,
        entries: &[CompactionEntry],
    ) -> Result<Option<MemTableEntry>> {
        // Enhanced data structures for sophisticated merging
        let mut neighbor_state = BTreeMap::new(); // neighbor_id -> NeighborVersionHistory
        let mut pivot_snapshots = Vec::new(); // (timestamp, neighbor_set, source_level)
        let latest_timestamp = entries[0].entry.timestamp;

        // First pass: Build comprehensive version history for each neighbor
        for entry in entries {
            match entry.entry.entry_type {
                EntryType::Pivot => {
                    let neighbors = decode_neighbors_adaptive(&entry.entry.data)?;
                    let neighbor_set: BTreeSet<u64> =
                        neighbors.into_iter().map(|v| v.as_u64()).collect();

                    // Record this pivot snapshot
                    pivot_snapshots.push((
                        entry.entry.timestamp,
                        neighbor_set.clone(),
                        entry.source_level,
                    ));

                    // Mark all neighbors as present in this pivot
                    for neighbor_id in neighbor_set {
                        neighbor_state
                            .entry(neighbor_id)
                            .or_insert_with(|| NeighborVersionHistory::new())
                            .add_pivot_presence(entry.entry.timestamp, entry.source_level);
                    }
                }
                EntryType::Delta => {
                    let delta_neighbors = decode_neighbors_adaptive(&entry.entry.data)?;
                    for neighbor in delta_neighbors {
                        let neighbor_id = neighbor.as_u64();
                        neighbor_state
                            .entry(neighbor_id)
                            .or_insert_with(|| NeighborVersionHistory::new())
                            .add_delta_addition(entry.entry.timestamp, entry.source_level);
                    }
                }
                EntryType::Tombstone => {
                    return Ok(None);
                }
            }
        }

        // Sort pivot snapshots by timestamp (newest first)
        pivot_snapshots.sort_by(|a, b| b.0.cmp(&a.0));

        // Enhanced merging logic: resolve conflicts and determine final neighbor set
        let final_neighbors =
            self.resolve_neighbor_conflicts(&neighbor_state, &pivot_snapshots, latest_timestamp)?;

        if final_neighbors.is_empty() {
            return Ok(None);
        }

        // Convert to sorted vector for optimal encoding
        let neighbors_vec: Vec<VertexId> = final_neighbors
            .into_iter()
            .map(VertexId::from_u64)
            .collect();

        // Use adaptive encoding for better compression
        let encoded_data = encode_neighbors_adaptive(&neighbors_vec)?;

        // Enhanced decision logic for entry type
        let entry = self.decide_entry_type(&neighbors_vec, &neighbor_state, latest_timestamp);

        Ok(Some(entry))
    }

    /// Resolve conflicts between different versions of neighbor information
    fn resolve_neighbor_conflicts(
        &self,
        neighbor_state: &BTreeMap<u64, NeighborVersionHistory>,
        pivot_snapshots: &[(Timestamp, BTreeSet<u64>, u32)],
        latest_timestamp: Timestamp,
    ) -> Result<BTreeSet<u64>> {
        let mut final_neighbors = BTreeSet::new();

        // Find the most authoritative pivot (most recent from highest level)
        let authoritative_pivot = pivot_snapshots
            .iter()
            .max_by_key(|(timestamp, _, level)| (*timestamp, *level));

        if let Some((pivot_time, pivot_neighbors, _)) = authoritative_pivot {
            // Start with authoritative pivot neighbors
            final_neighbors.extend(pivot_neighbors.iter().cloned());

            // Add delta neighbors that are newer than the authoritative pivot
            for (neighbor_id, history) in neighbor_state {
                if !pivot_neighbors.contains(neighbor_id) {
                    // This neighbor is not in the pivot, check if delta additions are valid
                    if history
                        .has_valid_delta_after(*pivot_time, self.config.delta_aggregation_window_ms)
                    {
                        final_neighbors.insert(*neighbor_id);
                    }
                }
            }
        } else {
            // No pivot entries, aggregate all delta entries
            for (neighbor_id, history) in neighbor_state {
                if history.is_addition_confirmed(
                    latest_timestamp,
                    self.config.delta_aggregation_window_ms,
                ) {
                    final_neighbors.insert(*neighbor_id);
                }
            }
        }

        // Apply temporal filtering for recency
        final_neighbors.retain(|neighbor_id| {
            if let Some(history) = neighbor_state.get(neighbor_id) {
                history
                    .is_recently_active(latest_timestamp, self.config.delta_aggregation_window_ms)
            } else {
                false
            }
        });

        Ok(final_neighbors)
    }

    /// Enhanced decision logic for determining entry type
    fn decide_entry_type(
        &self,
        neighbors: &[VertexId],
        neighbor_state: &BTreeMap<u64, NeighborVersionHistory>,
        latest_timestamp: Timestamp,
    ) -> MemTableEntry {
        let encoded_data = encode_neighbors_adaptive(neighbors).unwrap_or_default();

        // Calculate decision factors
        let neighbor_count = neighbors.len();
        let has_recent_deltas = neighbor_state.values().any(|h| {
            h.has_recent_delta_activity(latest_timestamp, self.config.delta_aggregation_window_ms)
        });
        let avg_neighbor_frequency = neighbor_state
            .values()
            .map(|h| h.get_activity_frequency())
            .sum::<f64>()
            / neighbor_count.max(1) as f64;

        // Enhanced decision criteria
        let should_create_pivot = neighbor_count >= self.config.pivot_threshold
            || (!has_recent_deltas && neighbor_count > 4)
            || (avg_neighbor_frequency > 2.0 && neighbor_count > 2);

        if should_create_pivot {
            MemTableEntry::new_pivot(encoded_data, latest_timestamp)
        } else {
            MemTableEntry::new_delta(encoded_data, latest_timestamp)
        }
    }

    /// Write merged entries to new SSTables
    async fn write_compacted_sstables(
        &self,
        entries: &[(VertexId, MemTableEntry)],
        target_level: u32,
        next_sstable_id: &mut u64,
    ) -> Result<Vec<PathBuf>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut new_sstables = Vec::new();
        let mut current_writer: Option<SSTableWriter> = None;
        let mut entries_in_current_sstable = 0;
        const MAX_ENTRIES_PER_SSTABLE: usize = 10000; // Configurable

        for (vertex_id, entry) in entries {
            // Create new SSTable if needed
            if current_writer.is_none() || entries_in_current_sstable >= MAX_ENTRIES_PER_SSTABLE {
                // Finish current writer if exists
                if let Some(writer) = current_writer.take() {
                    writer.finish()?;
                }

                // Create new writer
                let sstable_id = *next_sstable_id;
                *next_sstable_id += 1;

                let sstable_path = self
                    .data_dir
                    .join(format!("L{:02}_{:08}.sst", target_level, sstable_id));
                new_sstables.push(sstable_path.clone());

                let sstable_config = SSTableConfig {
                    block_size: self.config.block_size as usize,
                    compression_enabled: self.config.compression_enabled,
                    bloom_bits_per_key: self.config.bloom_bits_per_key as usize,
                    enable_bloom_filter: true,
                    ..SSTableConfig::default()
                };
                let writer = SSTableWriter::new(&sstable_path, sstable_config)?;

                current_writer = Some(writer);
                entries_in_current_sstable = 0;
            }

            // Add entry to current writer
            if let Some(ref mut writer) = current_writer {
                writer.add_entry(*vertex_id, entry.clone())?;
                entries_in_current_sstable += 1;
            }
        }

        // Finish final writer
        if let Some(writer) = current_writer.take() {
            writer.finish()?;
        }

        Ok(new_sstables)
    }

    /// Calculate graph locality score based on neighbor ID clustering
    fn calculate_graph_locality_score(&self, entries: &[(VertexId, MemTableEntry)]) -> f64 {
        if entries.len() < 2 {
            return 1.0; // Perfect locality for single entry
        }

        let mut total_neighbor_distance = 0u64;
        let mut total_neighbors = 0usize;

        for (vertex_id, entry) in entries {
            if let Ok(neighbors) = decode_neighbors_adaptive(&entry.data) {
                if neighbors.len() > 1 {
                    // Calculate average distance between consecutive neighbors
                    let mut neighbor_ids: Vec<u64> = neighbors.iter().map(|v| v.as_u64()).collect();
                    neighbor_ids.sort_unstable();

                    for window in neighbor_ids.windows(2) {
                        total_neighbor_distance += window[1] - window[0];
                        total_neighbors += 1;
                    }

                    // Also consider distance from vertex to its neighbors
                    let vertex_u64 = vertex_id.as_u64();
                    for &neighbor_id in &neighbor_ids {
                        total_neighbor_distance += vertex_u64.abs_diff(neighbor_id);
                        total_neighbors += 1;
                    }
                }
            }
        }

        if total_neighbors == 0 {
            return 1.0;
        }

        // Calculate average distance and normalize (lower distance = higher locality score)
        let avg_distance = total_neighbor_distance as f64 / total_neighbors as f64;

        // Use exponential decay to convert distance to score (0-1 range)
        // Score approaches 1 for very local graphs, approaches 0 for sparse graphs
        (-avg_distance / 1000000.0).exp() // Adjust divisor based on typical vertex ID ranges
    }

    /// Size-tiered compaction strategy
    pub async fn size_tiered_compaction(
        &self,
        sstables: Vec<Arc<SSTableReader>>,
        next_sstable_id: &mut u64,
    ) -> Result<CompactionResult> {
        // Group SSTables by similar sizes
        let mut size_groups: Vec<Vec<Arc<SSTableReader>>> = Vec::new();

        for sstable in sstables {
            let size = sstable.metadata().data_size;
            let mut added_to_group = false;

            // Find a group with similar size
            for group in &mut size_groups {
                if let Some(first) = group.first() {
                    let first_size = first.metadata().data_size;
                    let ratio = size as f64 / first_size as f64;

                    if ratio >= 0.5 && ratio <= 2.0 {
                        // Within 2x size range
                        group.push(sstable.clone());
                        added_to_group = true;
                        break;
                    }
                }
            }

            if !added_to_group {
                size_groups.push(vec![sstable]);
            }
        }

        // Compact groups that have enough SSTables
        let mut all_new_sstables = Vec::new();
        let mut all_old_sstables = Vec::new();
        let mut combined_stats = CompactionStats::default();

        for group in size_groups {
            if group.len() >= 4 {
                // Minimum group size for compaction
                let result = self.compact_level(0, group, next_sstable_id).await?;
                all_new_sstables.extend(result.new_sstables);
                all_old_sstables.extend(result.old_sstables);

                // Combine stats
                combined_stats.entries_processed += result.stats.entries_processed;
                combined_stats.entries_merged += result.stats.entries_merged;
                combined_stats.bytes_read += result.stats.bytes_read;
                combined_stats.bytes_written += result.stats.bytes_written;
            }
        }

        Ok(CompactionResult {
            new_sstables: all_new_sstables,
            old_sstables: all_old_sstables,
            stats: combined_stats,
        })
    }

    /// Determine if compaction is needed for a set of SSTables
    pub fn needs_compaction(&self, sstables: &[Arc<SSTableReader>], max_size: u64) -> bool {
        if sstables.is_empty() {
            return false;
        }

        let total_size: u64 = sstables.iter().map(|s| s.metadata().data_size).sum();

        match self.config.strategy {
            CompactionStrategy::Leveled => total_size > max_size,
            CompactionStrategy::SizeTiered => {
                // Check if we have enough SSTables of similar size
                sstables.len() >= 4
            }
            CompactionStrategy::Adaptive => {
                // Use a combination of both strategies
                total_size > max_size || sstables.len() >= 6
            }
        }
    }

    /// Get compaction priority for a level (higher means more urgent)
    pub fn compaction_priority(&self, sstables: &[Arc<SSTableReader>], max_size: u64) -> f64 {
        if sstables.is_empty() {
            return 0.0;
        }

        let total_size: u64 = sstables.iter().map(|s| s.metadata().data_size).sum();

        let size_ratio = total_size as f64 / max_size as f64;
        let count_factor = sstables.len() as f64 / 10.0; // Normalize by expected count

        size_ratio + count_factor
    }
}

/// Compaction scheduler that manages multiple concurrent compactions
pub struct CompactionScheduler {
    engine: CompactionEngine,
    max_concurrent: usize,
    active_compactions: std::sync::Arc<std::sync::Mutex<usize>>,
}

impl CompactionScheduler {
    /// Create a new compaction scheduler
    pub fn new(engine: CompactionEngine, max_concurrent: usize) -> Self {
        Self {
            engine,
            max_concurrent,
            active_compactions: std::sync::Arc::new(std::sync::Mutex::new(0)),
        }
    }

    /// Schedule a compaction if resources are available
    pub async fn schedule_compaction(
        &self,
        level: u32,
        sstables: Vec<Arc<SSTableReader>>,
        next_sstable_id: std::sync::Arc<std::sync::Mutex<u64>>,
    ) -> Result<Option<CompactionResult>> {
        // Check if we can start a new compaction
        {
            let mut active = self.active_compactions.lock().unwrap();
            if *active >= self.max_concurrent {
                debug!("Compaction deferred - {} active compactions", *active);
                return Ok(None);
            }
            *active += 1;
        }

        let result = {
            let mut next_id = next_sstable_id.lock().unwrap();
            self.engine
                .compact_level(level, sstables, &mut *next_id)
                .await
        };

        // Decrement active compaction count
        {
            let mut active = self.active_compactions.lock().unwrap();
            *active -= 1;
        }

        result.map(Some)
    }

    /// Get current compaction load
    pub fn compaction_load(&self) -> (usize, usize) {
        let active = *self.active_compactions.lock().unwrap();
        (active, self.max_concurrent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::encoding::encode_neighbors;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_entry_merging() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = CompactionConfig::default();
        config.pivot_threshold = 2; // Lower threshold for testing
        let engine = CompactionEngine::new(config, temp_dir.path().to_path_buf());

        // Create test entries for the same vertex
        let vertex_id = VertexId::from_u64(1);
        let entries = vec![
            CompactionEntry {
                vertex_id,
                entry: MemTableEntry::new_delta(
                    encode_neighbors(&[VertexId::from_u64(2)]),
                    Timestamp::from_u64(100),
                ),
                source_level: 0,
                source_sstable_id: 0,
            },
            CompactionEntry {
                vertex_id,
                entry: MemTableEntry::new_delta(
                    encode_neighbors(&[VertexId::from_u64(3)]),
                    Timestamp::from_u64(200),
                ),
                source_level: 0,
                source_sstable_id: 1,
            },
        ];

        let merged = engine.merge_vertex_entries(&entries).await.unwrap();
        assert!(merged.is_some());

        let entry = merged.unwrap();
        assert!(matches!(entry.entry_type, EntryType::Pivot));

        let neighbors = decode_neighbors_adaptive(&entry.data).unwrap();
        assert_eq!(neighbors.len(), 2);
        assert!(neighbors.contains(&VertexId::from_u64(2)));
        assert!(neighbors.contains(&VertexId::from_u64(3)));
    }

    #[tokio::test]
    async fn test_pivot_entry_precedence() {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig::default();
        let engine = CompactionEngine::new(config, temp_dir.path().to_path_buf());

        let vertex_id = VertexId::from_u64(1);
        let entries = vec![
            CompactionEntry {
                vertex_id,
                entry: MemTableEntry::new_pivot(
                    encode_neighbors(&[VertexId::from_u64(5), VertexId::from_u64(6)]),
                    Timestamp::from_u64(300), // Newest
                ),
                source_level: 1,
                source_sstable_id: 0,
            },
            CompactionEntry {
                vertex_id,
                entry: MemTableEntry::new_delta(
                    encode_neighbors(&[VertexId::from_u64(2)]),
                    Timestamp::from_u64(100), // Older, should be ignored
                ),
                source_level: 0,
                source_sstable_id: 1,
            },
        ];

        let merged = engine.merge_vertex_entries(&entries).await.unwrap();
        assert!(merged.is_some());

        let entry = merged.unwrap();
        let neighbors = decode_neighbors_adaptive(&entry.data).unwrap();

        // Should only contain neighbors from pivot entry
        assert_eq!(neighbors.len(), 2);
        assert!(neighbors.contains(&VertexId::from_u64(5)));
        assert!(neighbors.contains(&VertexId::from_u64(6)));
        assert!(!neighbors.contains(&VertexId::from_u64(2)));
    }

    #[test]
    fn test_compaction_priority() {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig::default();
        let engine = CompactionEngine::new(config, temp_dir.path().to_path_buf());

        let sstables = Vec::new(); // Empty for this test
        let priority = engine.compaction_priority(&sstables, 1024 * 1024);
        assert_eq!(priority, 0.0);
    }
}
