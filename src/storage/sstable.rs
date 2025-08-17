//! Enhanced SSTable implementation with block-based I/O and caching
//!
//! This module provides an improved SSTable implementation that:
//! - Uses block-based I/O for efficient data access
//! - Integrates with the block cache for improved performance
//! - Supports bloom filters and compression
//! - Provides iterators and range queries
//! - Handles concurrent access safely

use super::block_cache::{BlockCache, BlockCacheConfig, BlockId, BlockManager};
use super::memtable::{EntryType, MemTableEntry};
use crate::utils::bloom_filter::BloomFilter;
use crate::utils::encoding::{
    decode_neighbors_adaptive, encode_neighbors_adaptive, get_encoding_stats,
};
use crate::{AsterError, Result, Timestamp, VertexId};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Configuration for enhanced SSTable
#[derive(Debug, Clone)]
pub struct SSTableConfig {
    pub block_size: usize,
    pub cache_config: BlockCacheConfig,
    pub enable_bloom_filter: bool,
    pub bloom_bits_per_key: usize,
    pub compression_enabled: bool,
    pub index_block_restart_interval: usize,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            block_size: 64 * 1024, // 64KB blocks
            cache_config: BlockCacheConfig::default(),
            enable_bloom_filter: true,
            bloom_bits_per_key: 10,
            compression_enabled: true,
            index_block_restart_interval: 16,
        }
    }
}

/// Block type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockType {
    Data,
    Index,
    MetaIndex,
    Footer,
}

/// Header for each block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    pub block_type: u8,
    pub compression_type: u8,
    pub checksum: u32,
    pub uncompressed_size: u32,
}

impl BlockHeader {
    pub const SIZE: usize = 13; // 1 + 1 + 4 + 4 + 3 (padding)

    pub fn new(
        block_type: BlockType,
        compression_type: u8,
        checksum: u32,
        uncompressed_size: u32,
    ) -> Self {
        Self {
            block_type: block_type as u8,
            compression_type,
            checksum,
            uncompressed_size,
        }
    }

    pub fn block_type(&self) -> BlockType {
        match self.block_type {
            0 => BlockType::Data,
            1 => BlockType::Index,
            2 => BlockType::MetaIndex,
            3 => BlockType::Footer,
            _ => BlockType::Data,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SIZE);
        bytes.push(self.block_type);
        bytes.push(self.compression_type);
        bytes.extend_from_slice(&self.checksum.to_le_bytes());
        bytes.extend_from_slice(&self.uncompressed_size.to_le_bytes());
        bytes.resize(Self::SIZE, 0); // Padding
        bytes
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            return Err(AsterError::storage("Invalid block header size"));
        }

        Ok(Self {
            block_type: data[0],
            compression_type: data[1],
            checksum: u32::from_le_bytes([data[2], data[3], data[4], data[5]]),
            uncompressed_size: u32::from_le_bytes([data[6], data[7], data[8], data[9]]),
        })
    }
}

/// Data block containing key-value entries
#[derive(Debug, Clone)]
pub struct DataBlock {
    pub entries: Vec<(VertexId, MemTableEntry)>,
    pub restart_points: Vec<u32>,
}

impl DataBlock {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            restart_points: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, vertex_id: VertexId, entry: MemTableEntry) {
        self.entries.push((vertex_id, entry));
    }

    pub fn serialize(&self, compress: bool) -> Result<(Vec<u8>, bool)> {
        let serialized = bincode::serialize(&self.entries)?;

        if compress && serialized.len() > 1024 {
            Ok((self.compress_data(&serialized)?, true))
        } else {
            Ok((serialized, false))
        }
    }

    pub fn deserialize(data: &[u8], compressed: bool) -> Result<Self> {
        let decompressed = if compressed {
            Self::decompress_data(data)?
        } else {
            data.to_vec()
        };

        let entries: Vec<(VertexId, MemTableEntry)> = bincode::deserialize(&decompressed)?;

        Ok(Self {
            entries,
            restart_points: Vec::new(),
        })
    }

    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        use lz4::EncoderBuilder;
        use std::io::Write;

        let mut encoder = EncoderBuilder::new().level(4).build(Vec::new())?;
        encoder.write_all(data)?;
        let (compressed, result) = encoder.finish();
        result?;
        Ok(compressed)
    }

    fn decompress_data(data: &[u8]) -> Result<Vec<u8>> {
        use lz4::Decoder;
        use std::io::Read;

        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    /// Find entry by vertex ID
    pub fn find_entry(&self, vertex_id: VertexId) -> Option<&MemTableEntry> {
        // Binary search for efficiency
        match self.entries.binary_search_by_key(&vertex_id, |(id, _)| *id) {
            Ok(index) => Some(&self.entries[index].1),
            Err(_) => None,
        }
    }

    /// Get all entries in a range
    pub fn range_entries(&self, start: VertexId, end: VertexId) -> Vec<&(VertexId, MemTableEntry)> {
        self.entries
            .iter()
            .filter(|(id, _)| *id >= start && *id <= end)
            .collect()
    }
}

/// Index entry pointing to data blocks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub first_key: VertexId,
    pub last_key: VertexId,
    pub offset: u64,
    pub size: u64,
}

/// Enhanced SSTable writer with block-based storage
pub struct SSTableWriter {
    file: BufWriter<File>,
    path: PathBuf,
    config: SSTableConfig,

    // Writing state
    current_block: DataBlock,
    current_block_size: usize,
    blocks_written: Vec<IndexEntry>,
    bloom_filter: BloomFilter,
    first_key: Option<VertexId>,
    last_key: Option<VertexId>,
    num_entries: u64,
    current_offset: u64,
}

impl SSTableWriter {
    /// Create a new SSTable writer
    pub fn new<P: AsRef<Path>>(path: P, config: SSTableConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let bloom_filter = if config.enable_bloom_filter {
            // Use vertex-optimized Bloom filter with target 1% false positive rate
            BloomFilter::for_vertices(10000, 0.01)
        } else {
            BloomFilter::new(1, 1) // Minimal bloom filter
        };

        Ok(Self {
            file: BufWriter::new(file),
            path,
            config,
            current_block: DataBlock::new(),
            current_block_size: 0,
            blocks_written: Vec::new(),
            bloom_filter,
            first_key: None,
            last_key: None,
            num_entries: 0,
            current_offset: 0,
        })
    }

    /// Add an entry to the SSTable
    pub fn add_entry(&mut self, vertex_id: VertexId, entry: MemTableEntry) -> Result<()> {
        // Update first/last keys
        if self.first_key.is_none() || vertex_id < self.first_key.unwrap() {
            self.first_key = Some(vertex_id);
        }
        if self.last_key.is_none() || vertex_id > self.last_key.unwrap() {
            self.last_key = Some(vertex_id);
        }

        // Add to bloom filter
        if self.config.enable_bloom_filter {
            self.bloom_filter.insert_vertex_id(vertex_id.as_u64());
        }

        // For pivot entries with neighbor data, try to compress using adaptive encoding
        let optimized_entry =
            if matches!(entry.entry_type, EntryType::Pivot) && !entry.data.is_empty() {
                // Try to decode as neighbor list and re-encode with adaptive compression
                if let Ok(neighbors) = decode_neighbors_adaptive(&entry.data) {
                    if let Ok(compressed_data) = encode_neighbors_adaptive(&neighbors) {
                        let stats = get_encoding_stats(&neighbors, &compressed_data);
                        // Only use compressed version if it's meaningfully smaller
                        if stats.compression_ratio < 0.9 {
                            MemTableEntry {
                                entry_type: entry.entry_type.clone(),
                                data: compressed_data,
                                timestamp: entry.timestamp,
                            }
                        } else {
                            entry
                        }
                    } else {
                        entry
                    }
                } else {
                    entry
                }
            } else {
                entry
            };

        // Estimate entry size
        let entry_size = std::mem::size_of::<VertexId>() + optimized_entry.size_bytes();

        // Check if we need to flush the current block
        if self.current_block_size + entry_size >= self.config.block_size {
            self.flush_current_block()?;
        }

        // Add to current block
        self.current_block.add_entry(vertex_id, optimized_entry);
        self.current_block_size += entry_size;
        self.num_entries += 1;

        Ok(())
    }

    /// Flush the current data block
    fn flush_current_block(&mut self) -> Result<()> {
        if self.current_block.entries.is_empty() {
            return Ok(());
        }

        // Sort entries by vertex ID
        self.current_block.entries.sort_by_key(|(id, _)| *id);

        let first_key = self.current_block.entries.first().unwrap().0;
        let last_key = self.current_block.entries.last().unwrap().0;

        // Serialize block and track if actually compressed
        let uncompressed_data = bincode::serialize(&self.current_block.entries)?;
        let (block_data, was_compressed) =
            if self.config.compression_enabled && uncompressed_data.len() > 1024 {
                (self.current_block.compress_data(&uncompressed_data)?, true)
            } else {
                (uncompressed_data, false)
            };

        // Create block header with actual compression status
        let checksum = crc32fast::hash(&block_data);
        let header = BlockHeader::new(
            BlockType::Data,
            if was_compressed { 1 } else { 0 },
            checksum,
            self.current_block.entries.len() as u32 * 64, // Better estimate
        );

        // Write header and data
        let header_bytes = header.serialize();
        self.file.write_all(&header_bytes)?;
        self.file.write_all(&block_data)?;

        // Create index entry
        let index_entry = IndexEntry {
            first_key,
            last_key,
            offset: self.current_offset,
            size: (header_bytes.len() + block_data.len()) as u64,
        };

        self.blocks_written.push(index_entry);
        self.current_offset += (header_bytes.len() + block_data.len()) as u64;

        // Reset current block
        self.current_block = DataBlock::new();
        self.current_block_size = 0;

        Ok(())
    }

    /// Finish writing the SSTable
    pub fn finish(mut self) -> Result<SSTableMetadata> {
        // Flush any remaining data
        self.flush_current_block()?;

        if self.num_entries == 0 {
            return Err(AsterError::invalid_operation("Cannot create empty SSTable"));
        }

        // Write index block
        let index_offset = self.current_offset;
        let index_data = bincode::serialize(&self.blocks_written)?;
        let index_header = BlockHeader::new(
            BlockType::Index,
            0,
            crc32fast::hash(&index_data),
            index_data.len() as u32,
        );

        self.file.write_all(&index_header.serialize())?;
        self.file.write_all(&index_data)?;
        let index_size = index_header.serialize().len() + index_data.len();
        self.current_offset += index_size as u64;

        // Write bloom filter block (if enabled)
        let bloom_offset = self.current_offset;
        let bloom_size = if self.config.enable_bloom_filter {
            let bloom_data = self.bloom_filter.to_bytes();
            let bloom_header = BlockHeader::new(
                BlockType::MetaIndex,
                0,
                crc32fast::hash(&bloom_data),
                bloom_data.len() as u32,
            );

            self.file.write_all(&bloom_header.serialize())?;
            self.file.write_all(&bloom_data)?;
            let size = bloom_header.serialize().len() + bloom_data.len();
            self.current_offset += size as u64;
            size as u64
        } else {
            0
        };

        // Create metadata
        let data_size = self.current_offset; // Total size of data blocks
        let metadata = SSTableMetadata {
            version: 2, // Version 2 for block-based format
            num_entries: self.num_entries,
            num_blocks: self.blocks_written.len() as u64,
            first_key: self.first_key.unwrap(),
            last_key: self.last_key.unwrap(),
            created_at: Timestamp::now(),
            index_offset,
            index_size: index_size as u64,
            bloom_offset,
            bloom_size,
            block_size: self.config.block_size as u64,
            compression_enabled: self.config.compression_enabled,
            checksum: 0, // Will be calculated
            data_size,
            level: 0, // Default level, should be set by caller
        };

        // Write metadata
        let metadata_data = bincode::serialize(&metadata)?;
        let metadata_size = metadata_data.len() as u32;

        // Write metadata and size at the end
        self.file.write_all(&metadata_data)?;
        self.file.write_all(&metadata_size.to_le_bytes())?;

        self.file.flush()?;

        Ok(metadata)
    }
}

/// Enhanced SSTable metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableMetadata {
    pub version: u32,
    pub num_entries: u64,
    pub num_blocks: u64,
    pub first_key: VertexId,
    pub last_key: VertexId,
    pub created_at: Timestamp,
    pub index_offset: u64,
    pub index_size: u64,
    pub bloom_offset: u64,
    pub bloom_size: u64,
    pub block_size: u64,
    pub compression_enabled: bool,
    pub checksum: u64,
    pub data_size: u64,
    pub level: u32,
}

/// Enhanced SSTable reader with block caching
#[derive(Debug, Clone)]
pub struct SSTableReader {
    path: PathBuf,
    file: Arc<RwLock<File>>,
    metadata: SSTableMetadata,
    index: Vec<IndexEntry>,
    bloom_filter: Option<BloomFilter>,
    block_manager: Arc<BlockManager>,
    sstable_id: u64,
}

impl SSTableReader {
    /// Open an existing SSTable
    pub fn open<P: AsRef<Path>>(path: P, config: SSTableConfig, sstable_id: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        // Read metadata size from the end of file
        file.seek(SeekFrom::End(-4))?;
        let mut size_bytes = [0u8; 4];
        file.read_exact(&mut size_bytes)?;
        let metadata_size = u32::from_le_bytes(size_bytes) as usize;

        // Read metadata
        file.seek(SeekFrom::End(-4 - metadata_size as i64))?;
        let mut metadata_bytes = vec![0u8; metadata_size];
        file.read_exact(&mut metadata_bytes)?;
        let metadata: SSTableMetadata = bincode::deserialize(&metadata_bytes)?;

        // Read index
        file.seek(SeekFrom::Start(metadata.index_offset))?;
        let mut index_header_bytes = vec![0u8; BlockHeader::SIZE];
        file.read_exact(&mut index_header_bytes)?;
        let index_header = BlockHeader::deserialize(&index_header_bytes)?;

        // Validate index header
        if index_header.checksum == 0 {
            return Err(AsterError::storage(
                "Invalid index header: checksum is zero",
            ));
        }

        let mut index_data = vec![0u8; metadata.index_size as usize - BlockHeader::SIZE];
        file.read_exact(&mut index_data)?;
        let index: Vec<IndexEntry> = bincode::deserialize(&index_data)?;

        // Read bloom filter (if present)
        let bloom_filter = if metadata.bloom_size > 0 {
            file.seek(SeekFrom::Start(metadata.bloom_offset))?;
            let mut bloom_header_bytes = vec![0u8; BlockHeader::SIZE];
            file.read_exact(&mut bloom_header_bytes)?;
            let bloom_header = BlockHeader::deserialize(&bloom_header_bytes)?;

            // Validate bloom filter header
            if bloom_header.checksum == 0 {
                return Err(AsterError::storage(
                    "Invalid bloom filter header: checksum is zero",
                ));
            }

            let mut bloom_data = vec![0u8; metadata.bloom_size as usize - BlockHeader::SIZE];
            file.read_exact(&mut bloom_data)?;
            BloomFilter::from_bytes(&bloom_data)
        } else {
            None
        };

        // Create block manager
        let cache = Arc::new(BlockCache::new(config.cache_config));
        let block_manager = Arc::new(BlockManager::new(cache, config.block_size));

        Ok(Self {
            path,
            file: Arc::new(RwLock::new(file)),
            metadata,
            index,
            bloom_filter,
            block_manager,
            sstable_id,
        })
    }

    /// Check if a vertex might exist (using bloom filter)
    pub fn might_contain(&self, vertex_id: VertexId) -> bool {
        if let Some(ref bloom) = self.bloom_filter {
            bloom.contains_vertex_id(vertex_id.as_u64())
        } else {
            true // If no bloom filter, assume it might contain
        }
    }

    /// Check if any of the given vertices might exist (useful for batch queries)
    pub fn might_contain_any(&self, vertex_ids: &[VertexId]) -> bool {
        if let Some(ref bloom) = self.bloom_filter {
            let ids: Vec<u64> = vertex_ids.iter().map(|v| v.as_u64()).collect();
            bloom.contains_any_vertex(&ids)
        } else {
            true // If no bloom filter, assume at least one might be present
        }
    }

    /// Get Bloom filter statistics
    pub fn bloom_filter_stats(&self) -> Option<(f64, usize)> {
        self.bloom_filter.as_ref().map(|bloom| {
            let fill_ratio = bloom.fill_ratio();
            let num_hash_functions = bloom.num_hash_functions();
            (fill_ratio, num_hash_functions)
        })
    }

    /// Get an entry by vertex ID
    pub async fn get(&self, vertex_id: VertexId) -> Result<Option<MemTableEntry>> {
        // Quick bloom filter check
        if !self.might_contain(vertex_id) {
            return Ok(None);
        }

        // Find the block that might contain this key
        let block_index = self.find_block_index(vertex_id);
        if let Some(index) = block_index {
            let block = self.load_data_block(index).await?;
            Ok(block.find_entry(vertex_id).cloned())
        } else {
            Ok(None)
        }
    }

    /// Get all entries in a range
    pub async fn range(
        &self,
        start: VertexId,
        end: VertexId,
    ) -> Result<Vec<(VertexId, MemTableEntry)>> {
        let mut results = Vec::new();

        // Find all blocks that might overlap with the range
        for (i, index_entry) in self.index.iter().enumerate() {
            if index_entry.last_key >= start && index_entry.first_key <= end {
                let block = self.load_data_block(i).await?;
                let block_results = block.range_entries(start, end);
                for (id, entry) in block_results {
                    results.push((*id, entry.clone()));
                }
            }
        }

        results.sort_by_key(|(id, _)| *id);
        Ok(results)
    }

    /// Find which block might contain the given vertex ID
    fn find_block_index(&self, vertex_id: VertexId) -> Option<usize> {
        for (i, index_entry) in self.index.iter().enumerate() {
            if vertex_id >= index_entry.first_key && vertex_id <= index_entry.last_key {
                return Some(i);
            }
        }
        None
    }

    /// Load a data block (with caching)
    async fn load_data_block(&self, block_index: usize) -> Result<DataBlock> {
        if block_index >= self.index.len() {
            return Err(AsterError::storage("Invalid block index"));
        }

        let index_entry = &self.index[block_index];

        let loader = |_sstable_id: u64, block_offset: u64| -> Result<Vec<u8>> {
            let mut file = self.file.write();
            file.seek(SeekFrom::Start(block_offset))?;

            // Read block header
            let mut header_bytes = vec![0u8; BlockHeader::SIZE];
            file.read_exact(&mut header_bytes)?;
            let header = BlockHeader::deserialize(&header_bytes)?;

            // Read block data
            let data_size = index_entry.size - BlockHeader::SIZE as u64;
            let mut block_data = vec![0u8; data_size as usize];
            file.read_exact(&mut block_data)?;

            // Verify checksum
            let computed_checksum = crc32fast::hash(&block_data);
            if computed_checksum != header.checksum {
                return Err(AsterError::storage("Block checksum mismatch"));
            }

            Ok(block_data)
        };

        let block_data = self
            .block_manager
            .load_block(self.sstable_id, index_entry.offset, loader)
            .await?;

        // Determine compression from the stored header (need to re-read it)
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(index_entry.offset))?;
        let mut header_bytes = vec![0u8; BlockHeader::SIZE];
        file.read_exact(&mut header_bytes)?;
        let header = BlockHeader::deserialize(&header_bytes)?;
        let is_compressed = header.compression_type != 0;

        DataBlock::deserialize(&block_data, is_compressed)
    }

    /// Get SSTable metadata
    pub fn metadata(&self) -> &SSTableMetadata {
        &self.metadata
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> super::block_cache::CacheStats {
        self.block_manager.cache().get_stats()
    }

    /// Invalidate cache for this SSTable
    pub fn invalidate_cache(&self) {
        self.block_manager.invalidate_sstable(self.sstable_id);
    }

    /// Get the path to this SSTable
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Create an iterator over all entries in this SSTable
    pub fn iter(&self) -> Result<SSTableIterator> {
        Ok(SSTableIterator::new(Arc::new(self.clone())))
    }
}

/// Iterator over SSTable entries
pub struct SSTableIterator {
    reader: Arc<SSTableReader>,
    current_block: usize,
    current_entry: usize,
    current_block_data: Option<DataBlock>,
}

impl SSTableIterator {
    pub fn new(reader: Arc<SSTableReader>) -> Self {
        Self {
            reader,
            current_block: 0,
            current_entry: 0,
            current_block_data: None,
        }
    }

    pub async fn next(&mut self) -> Result<Option<(VertexId, MemTableEntry)>> {
        loop {
            // Load current block if needed
            if self.current_block_data.is_none() {
                if self.current_block >= self.reader.index.len() {
                    return Ok(None); // End of iteration
                }

                let block = self.reader.load_data_block(self.current_block).await?;
                self.current_block_data = Some(block);
                self.current_entry = 0;
            }

            // Get entry from current block
            if let Some(ref block) = self.current_block_data {
                if self.current_entry < block.entries.len() {
                    let entry = block.entries[self.current_entry].clone();
                    self.current_entry += 1;
                    return Ok(Some(entry));
                } else {
                    // Move to next block
                    self.current_block += 1;
                    self.current_block_data = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memtable::MemTableEntry;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sstable_v2_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let sstable_path = temp_dir.path().join("test.sst");

        let config = SSTableConfig::default();

        // Write SSTable
        {
            let mut writer = SSTableWriter::new(&sstable_path, config.clone()).unwrap();

            // Add test entries
            for i in 1..=10 {
                let vertex_id = VertexId::from_u64(i);
                let data = format!("data_{}", i).into_bytes();
                let entry = MemTableEntry::new_pivot(data, Timestamp::now());
                writer.add_entry(vertex_id, entry).unwrap();
            }

            let metadata = writer.finish().unwrap();
            assert_eq!(metadata.num_entries, 10);
            assert!(metadata.num_blocks > 0);
        }

        // Read SSTable
        {
            let reader = SSTableReader::open(&sstable_path, config, 1).unwrap();
            assert_eq!(reader.metadata().num_entries, 10);

            // Test point lookups
            let entry = reader.get(VertexId::from_u64(5)).await.unwrap();
            assert!(entry.is_some());

            let entry = reader.get(VertexId::from_u64(100)).await.unwrap();
            assert!(entry.is_none());

            // Test range query
            let range_results = reader
                .range(VertexId::from_u64(3), VertexId::from_u64(7))
                .await
                .unwrap();
            assert_eq!(range_results.len(), 5);
        }
    }

    #[tokio::test]
    async fn test_bloom_filter_effectiveness() {
        let temp_dir = TempDir::new().unwrap();
        let sstable_path = temp_dir.path().join("test_bloom.sst");

        let config = SSTableConfig {
            enable_bloom_filter: true,
            bloom_bits_per_key: 10,
            ..Default::default()
        };

        // Write SSTable with specific keys
        {
            let mut writer = SSTableWriter::new(&sstable_path, config.clone()).unwrap();

            for i in (1..=100).step_by(2) {
                // Only odd numbers
                let vertex_id = VertexId::from_u64(i);
                let data = format!("data_{}", i).into_bytes();
                let entry = MemTableEntry::new_pivot(data, Timestamp::now());
                writer.add_entry(vertex_id, entry).unwrap();
            }

            writer.finish().unwrap();
        }

        // Read and test bloom filter
        {
            let reader = SSTableReader::open(&sstable_path, config, 1).unwrap();

            // Test existing keys (should all return true)
            for i in (1..=100).step_by(2) {
                assert!(reader.might_contain(VertexId::from_u64(i)));
            }

            // Test non-existing keys (most should return false due to bloom filter)
            let mut false_positives = 0;
            for i in (2..=100).step_by(2) {
                // Even numbers (not in SSTable)
                if reader.might_contain(VertexId::from_u64(i)) {
                    false_positives += 1;
                }
            }

            // False positive rate should be reasonable
            let false_positive_rate = false_positives as f64 / 50.0;
            assert!(false_positive_rate < 0.2); // Less than 20% false positives
        }
    }

    #[test]
    fn test_block_cache_integration() {
        let config = BlockCacheConfig {
            max_size_bytes: 1024 * 1024, // 1MB
            max_entries: 100,
            ..Default::default()
        };

        let cache = Arc::new(BlockCache::new(config));
        let manager = BlockManager::new(cache.clone(), 4096);

        let block_id = BlockId::new(1, 0);
        let test_data = vec![1, 2, 3, 4, 5];

        // Test cache miss and put
        cache.put(block_id, test_data.clone()).unwrap();

        // Test cache hit
        let retrieved = cache.get(block_id).unwrap();
        assert_eq!(test_data, retrieved);

        let stats = cache.get_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.entry_count, 1);
    }

    #[tokio::test]
    async fn test_sstable_iterator() {
        let temp_dir = TempDir::new().unwrap();
        let sstable_path = temp_dir.path().join("test_iter.sst");

        let config = SSTableConfig::default();

        // Write test data
        {
            let mut writer = SSTableWriter::new(&sstable_path, config.clone()).unwrap();

            for i in 1..=5 {
                let vertex_id = VertexId::from_u64(i);
                let data = format!("value_{}", i).into_bytes();
                let entry = MemTableEntry::new_pivot(data, Timestamp::now());
                writer.add_entry(vertex_id, entry).unwrap();
            }

            writer.finish().unwrap();
        }

        // Test iteration
        {
            let reader = Arc::new(SSTableReader::open(&sstable_path, config, 1).unwrap());
            let mut iterator = SSTableIterator::new(reader);

            let mut count = 0;
            while let Some((vertex_id, _entry)) = iterator.next().await.unwrap() {
                count += 1;
                assert_eq!(vertex_id, VertexId::from_u64(count));
            }

            assert_eq!(count, 5);
        }
    }
}
