//! In-memory table (MemTable) implementation for Poly-LSM
//!
//! The MemTable buffers incoming writes in memory before flushing to disk as SSTables.

use crate::{AsterError, Result, Timestamp, VertexId};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Entry type in the LSM-tree
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EntryType {
    /// Pivot entry: contains most neighbors for a vertex (vertex-based)
    Pivot,
    /// Delta entry: contains incremental updates (edge-based)
    Delta,
    /// Deletion marker
    Tombstone,
}

/// A single entry in the MemTable
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MemTableEntry {
    /// Type of entry (pivot, delta, or tombstone)
    pub entry_type: EntryType,
    /// Encoded neighbor data
    pub data: Vec<u8>,
    /// Timestamp for MVCC
    pub timestamp: Timestamp,
}

impl MemTableEntry {
    /// Create a new pivot entry
    pub fn new_pivot(data: Vec<u8>, timestamp: Timestamp) -> Self {
        Self {
            entry_type: EntryType::Pivot,
            data,
            timestamp,
        }
    }

    /// Create a new delta entry
    pub fn new_delta(data: Vec<u8>, timestamp: Timestamp) -> Self {
        Self {
            entry_type: EntryType::Delta,
            data,
            timestamp,
        }
    }

    /// Create a tombstone (deletion marker)
    pub fn new_tombstone(timestamp: Timestamp) -> Self {
        Self {
            entry_type: EntryType::Tombstone,
            data: Vec::new(),
            timestamp,
        }
    }

    /// Check if this entry is a tombstone
    pub fn is_tombstone(&self) -> bool {
        matches!(self.entry_type, EntryType::Tombstone)
    }

    /// Get the size of this entry in bytes
    pub fn size_bytes(&self) -> usize {
        std::mem::size_of::<EntryType>() + self.data.len() + std::mem::size_of::<Timestamp>()
    }
}

/// In-memory table that buffers writes before flushing to disk
#[derive(Debug)]
pub struct MemTable {
    /// The actual data, keyed by vertex ID
    data: Arc<RwLock<BTreeMap<VertexId, Vec<MemTableEntry>>>>,
    /// Current size in bytes (approximate)
    size_bytes: Arc<RwLock<usize>>,
    /// Maximum size before flush is triggered
    max_size_bytes: usize,
    /// Creation timestamp
    created_at: Timestamp,
}

impl MemTable {
    /// Create a new MemTable with the specified maximum size
    pub fn new(max_size_bytes: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size_bytes: Arc::new(RwLock::new(0)),
            max_size_bytes,
            created_at: Timestamp::now(),
        }
    }

    /// Insert an entry into the MemTable
    pub fn insert(&self, vertex_id: VertexId, entry: MemTableEntry) -> Result<()> {
        let entry_size = entry.size_bytes();

        {
            let mut data = self.data.write();
            let entries = data.entry(vertex_id).or_insert_with(Vec::new);
            entries.push(entry);
        }

        {
            let mut size = self.size_bytes.write();
            *size += entry_size;
        }

        Ok(())
    }

    /// Get all entries for a vertex ID, sorted by timestamp (newest first)
    pub fn get(&self, vertex_id: VertexId) -> Option<Vec<MemTableEntry>> {
        let data = self.data.read();
        data.get(&vertex_id).map(|entries| {
            let mut sorted_entries = entries.clone();
            // Sort by timestamp, newest first
            sorted_entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            sorted_entries
        })
    }

    /// Get the latest entry for a vertex ID
    pub fn get_latest(&self, vertex_id: VertexId) -> Option<MemTableEntry> {
        self.get(vertex_id)
            .and_then(|entries| entries.into_iter().next())
    }

    /// Check if MemTable contains any entry for a vertex ID
    pub fn contains(&self, vertex_id: VertexId) -> bool {
        let data = self.data.read();
        data.contains_key(&vertex_id)
    }

    /// Get current size in bytes
    pub fn size_bytes(&self) -> usize {
        *self.size_bytes.read()
    }

    /// Check if MemTable is full and needs flushing
    pub fn is_full(&self) -> bool {
        self.size_bytes() >= self.max_size_bytes
    }

    /// Get number of unique vertex IDs
    pub fn num_vertices(&self) -> usize {
        let data = self.data.read();
        data.len()
    }

    /// Get total number of entries
    pub fn num_entries(&self) -> usize {
        let data = self.data.read();
        data.values().map(|entries| entries.len()).sum()
    }

    /// Get creation timestamp
    pub fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Get an iterator over all entries for flushing to disk
    pub fn iter(&self) -> MemTableIterator {
        let data = self.data.read();
        let mut all_entries = Vec::new();

        for (&vertex_id, entries) in data.iter() {
            for entry in entries {
                all_entries.push((vertex_id, entry.clone()));
            }
        }

        // Sort by vertex ID for efficient storage
        all_entries.sort_by_key(|(vertex_id, _)| *vertex_id);

        MemTableIterator::new(all_entries)
    }

    /// Get statistics about this MemTable
    pub fn stats(&self) -> MemTableStats {
        let data = self.data.read();
        let mut pivot_count = 0;
        let mut delta_count = 0;
        let mut tombstone_count = 0;

        for entries in data.values() {
            for entry in entries {
                match entry.entry_type {
                    EntryType::Pivot => pivot_count += 1,
                    EntryType::Delta => delta_count += 1,
                    EntryType::Tombstone => tombstone_count += 1,
                }
            }
        }

        MemTableStats {
            num_vertices: data.len(),
            pivot_entries: pivot_count,
            delta_entries: delta_count,
            tombstone_entries: tombstone_count,
            size_bytes: self.size_bytes(),
            created_at: self.created_at,
        }
    }

    /// Clear the MemTable (used after flushing)
    pub fn clear(&self) {
        {
            let mut data = self.data.write();
            data.clear();
        }
        {
            let mut size = self.size_bytes.write();
            *size = 0;
        }
    }
}

/// Statistics about a MemTable
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemTableStats {
    pub num_vertices: usize,
    pub pivot_entries: usize,
    pub delta_entries: usize,
    pub tombstone_entries: usize,
    pub size_bytes: usize,
    pub created_at: Timestamp,
}

impl MemTableStats {
    pub fn total_entries(&self) -> usize {
        self.pivot_entries + self.delta_entries + self.tombstone_entries
    }

    pub fn pivot_ratio(&self) -> f64 {
        let total = self.total_entries();
        if total > 0 {
            self.pivot_entries as f64 / total as f64
        } else {
            0.0
        }
    }
}

/// Iterator over MemTable entries for flushing
pub struct MemTableIterator {
    entries: Vec<(VertexId, MemTableEntry)>,
    index: usize,
}

impl MemTableIterator {
    fn new(entries: Vec<(VertexId, MemTableEntry)>) -> Self {
        Self { entries, index: 0 }
    }
}

impl Iterator for MemTableIterator {
    type Item = (VertexId, MemTableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.entries.len() {
            let item = self.entries[self.index].clone();
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl ExactSizeIterator for MemTableIterator {
    fn len(&self) -> usize {
        self.entries.len() - self.index
    }
}

/// Immutable MemTable for read operations during compaction
#[derive(Debug, Clone)]
pub struct FrozenMemTable {
    entries: Vec<(VertexId, MemTableEntry)>,
    stats: MemTableStats,
}

impl FrozenMemTable {
    /// Create a frozen snapshot of a MemTable
    pub fn from_memtable(memtable: &MemTable) -> Self {
        let entries: Vec<_> = memtable.iter().collect();
        let stats = memtable.stats();

        Self { entries, stats }
    }

    /// Get an iterator over the frozen entries
    pub fn iter(&self) -> impl Iterator<Item = &(VertexId, MemTableEntry)> {
        self.entries.iter()
    }

    /// Get statistics
    pub fn stats(&self) -> &MemTableStats {
        &self.stats
    }

    /// Find entries for a specific vertex
    pub fn get(&self, vertex_id: VertexId) -> Vec<&MemTableEntry> {
        self.entries
            .iter()
            .filter(|(id, _)| *id == vertex_id)
            .map(|(_, entry)| entry)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::encoding::encode_neighbors;

    #[test]
    fn test_memtable_basic_operations() {
        let memtable = MemTable::new(1024 * 1024); // 1MB
        let vertex_id = VertexId::from_u64(1);
        let timestamp = Timestamp::now();

        // Test insertion
        let neighbors = vec![VertexId::from_u64(2), VertexId::from_u64(3)];
        let encoded_data = encode_neighbors(&neighbors);
        let entry = MemTableEntry::new_pivot(encoded_data, timestamp);

        memtable.insert(vertex_id, entry.clone()).unwrap();

        // Test retrieval
        let retrieved = memtable.get_latest(vertex_id).unwrap();
        assert_eq!(retrieved.entry_type, EntryType::Pivot);
        assert_eq!(retrieved.timestamp, timestamp);
        assert!(memtable.contains(vertex_id));

        // Test stats
        let stats = memtable.stats();
        assert_eq!(stats.num_vertices, 1);
        assert_eq!(stats.pivot_entries, 1);
        assert_eq!(stats.delta_entries, 0);
    }

    #[test]
    fn test_memtable_multiple_entries() {
        let memtable = MemTable::new(1024 * 1024);
        let vertex_id = VertexId::from_u64(1);

        // Insert multiple entries for the same vertex
        let ts1 = Timestamp::from_u64(100);
        let ts2 = Timestamp::from_u64(200);
        let ts3 = Timestamp::from_u64(150);

        let entry1 = MemTableEntry::new_delta(vec![1, 2, 3], ts1);
        let entry2 = MemTableEntry::new_pivot(vec![4, 5, 6], ts2);
        let entry3 = MemTableEntry::new_delta(vec![7, 8, 9], ts3);

        memtable.insert(vertex_id, entry1).unwrap();
        memtable.insert(vertex_id, entry2.clone()).unwrap();
        memtable.insert(vertex_id, entry3).unwrap();

        // Should get the latest entry (highest timestamp)
        let latest = memtable.get_latest(vertex_id).unwrap();
        assert_eq!(latest.timestamp, ts2);
        assert_eq!(latest.entry_type, EntryType::Pivot);

        // Should get all entries, sorted by timestamp (newest first)
        let all_entries = memtable.get(vertex_id).unwrap();
        assert_eq!(all_entries.len(), 3);
        assert!(all_entries[0].timestamp >= all_entries[1].timestamp);
        assert!(all_entries[1].timestamp >= all_entries[2].timestamp);
    }

    #[test]
    fn test_memtable_tombstone() {
        let memtable = MemTable::new(1024 * 1024);
        let vertex_id = VertexId::from_u64(1);
        let timestamp = Timestamp::now();

        let tombstone = MemTableEntry::new_tombstone(timestamp);
        assert!(tombstone.is_tombstone());

        memtable.insert(vertex_id, tombstone.clone()).unwrap();

        let retrieved = memtable.get_latest(vertex_id).unwrap();
        assert!(retrieved.is_tombstone());
    }

    #[test]
    fn test_memtable_size_tracking() {
        let memtable = MemTable::new(100); // Small size for testing
        let vertex_id = VertexId::from_u64(1);

        assert_eq!(memtable.size_bytes(), 0);
        assert!(!memtable.is_full());

        // Insert a large entry
        let large_data = vec![0u8; 200];
        let entry = MemTableEntry::new_pivot(large_data, Timestamp::now());
        memtable.insert(vertex_id, entry).unwrap();

        assert!(memtable.size_bytes() > 0);
        assert!(memtable.is_full());
    }

    #[test]
    fn test_memtable_iterator() {
        let memtable = MemTable::new(1024 * 1024);

        // Insert entries for multiple vertices
        for i in 1..=5 {
            let vertex_id = VertexId::from_u64(i);
            let entry = MemTableEntry::new_pivot(vec![i as u8], Timestamp::now());
            memtable.insert(vertex_id, entry).unwrap();
        }

        let entries: Vec<_> = memtable.iter().collect();
        assert_eq!(entries.len(), 5);

        // Should be sorted by vertex ID
        for i in 0..entries.len() - 1 {
            assert!(entries[i].0.as_u64() <= entries[i + 1].0.as_u64());
        }
    }

    #[test]
    fn test_frozen_memtable() {
        let memtable = MemTable::new(1024 * 1024);
        let vertex_id = VertexId::from_u64(1);
        let entry = MemTableEntry::new_pivot(vec![1, 2, 3], Timestamp::now());

        memtable.insert(vertex_id, entry.clone()).unwrap();

        let frozen = FrozenMemTable::from_memtable(&memtable);
        let frozen_entries = frozen.get(vertex_id);

        assert_eq!(frozen_entries.len(), 1);
        assert_eq!(frozen_entries[0].data, entry.data);

        let stats = frozen.stats();
        assert_eq!(stats.num_vertices, 1);
        assert_eq!(stats.pivot_entries, 1);
    }
}
