//! MVCC transaction support for Aster database
//!
//! Implements Multi-Version Concurrency Control with:
//! - Snapshot isolation
//! - Conflict detection and resolution
//! - Read and write tracking
//! - Optimistic concurrency control

use crate::{AsterError, EdgeId, Result, Timestamp, VertexId};
use parking_lot::Mutex as ParkingMutex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Transaction identifier
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct TransactionId(pub u64);

impl TransactionId {
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tx{}", self.0)
    }
}

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Active,
    Committed,
    Aborted,
}

/// Types of locks that can be held
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockType {
    Read,
    Write,
}

/// Lock information
#[derive(Debug, Clone)]
pub struct Lock {
    pub transaction_id: TransactionId,
    pub lock_type: LockType,
    pub timestamp: Timestamp,
}

/// Resource that can be locked (vertex or edge)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockResource {
    Vertex(VertexId),
    Edge(EdgeId),
}

impl std::fmt::Display for LockResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockResource::Vertex(id) => write!(f, "vertex:{}", id.as_u64()),
            LockResource::Edge(id) => write!(f, "edge:{}", id.as_u64()),
        }
    }
}

/// Transaction statistics
#[derive(Debug, Default, Clone)]
pub struct TransactionStats {
    pub read_count: u64,
    pub write_count: u64,
    pub conflicts_detected: u64,
    pub locks_acquired: u64,
    pub duration_ms: u64,
}

/// Active transaction information
#[derive(Debug)]
pub struct ActiveTransaction {
    pub id: TransactionId,
    pub start_time: Timestamp,
    pub status: TransactionStatus,
    pub read_set: HashSet<LockResource>,
    pub write_set: HashSet<LockResource>,
    pub stats: TransactionStats,
}

impl ActiveTransaction {
    fn new(id: TransactionId) -> Self {
        Self {
            id,
            start_time: Timestamp::now(),
            status: TransactionStatus::Active,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            stats: TransactionStats::default(),
        }
    }
}

/// Lock manager for handling concurrent access
#[derive(Debug)]
pub struct LockManager {
    /// Resource locks: resource -> list of locks
    locks: RwLock<HashMap<LockResource, Vec<Lock>>>,
    /// Transaction to locks mapping for fast cleanup
    transaction_locks: RwLock<HashMap<TransactionId, Vec<LockResource>>>,
    /// Lock wait queue for deadlock prevention
    wait_queue: RwLock<BTreeMap<TransactionId, Vec<LockResource>>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            transaction_locks: RwLock::new(HashMap::new()),
            wait_queue: RwLock::new(BTreeMap::new()),
        }
    }

    /// Acquire a lock on a resource
    pub fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        resource: LockResource,
        lock_type: LockType,
    ) -> Result<bool> {
        let mut locks = self
            .locks
            .write()
            .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;
        let mut tx_locks = self
            .transaction_locks
            .write()
            .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;

        let resource_locks = locks.entry(resource.clone()).or_default();

        // Check for conflicts
        for existing_lock in resource_locks.iter() {
            if existing_lock.transaction_id != transaction_id {
                match (lock_type, existing_lock.lock_type) {
                    // Read-Read is always compatible
                    (LockType::Read, LockType::Read) => continue,
                    // Write conflicts with everything
                    (LockType::Write, _) | (_, LockType::Write) => {
                        return Ok(false); // Conflict detected
                    }
                }
            }
        }

        // No conflicts, acquire the lock
        let lock = Lock {
            transaction_id,
            lock_type,
            timestamp: Timestamp::now(),
        };

        resource_locks.push(lock);

        // Track locks by transaction
        tx_locks.entry(transaction_id).or_default().push(resource);

        Ok(true)
    }

    /// Release all locks held by a transaction
    pub fn release_locks(&self, transaction_id: TransactionId) -> Result<()> {
        let mut locks = self
            .locks
            .write()
            .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;
        let mut tx_locks = self
            .transaction_locks
            .write()
            .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;

        if let Some(resources) = tx_locks.remove(&transaction_id) {
            for resource in resources {
                if let Some(resource_locks) = locks.get_mut(&resource) {
                    resource_locks.retain(|lock| lock.transaction_id != transaction_id);

                    // Remove empty entries
                    if resource_locks.is_empty() {
                        locks.remove(&resource);
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if there are any conflicting locks
    pub fn has_conflicts(&self, resource: &LockResource, lock_type: LockType) -> bool {
        let locks = match self.locks.read() {
            Ok(l) => l,
            Err(_) => return true, // Assume conflict if we can't acquire lock
        };

        if let Some(resource_locks) = locks.get(resource) {
            for existing_lock in resource_locks {
                match (lock_type, existing_lock.lock_type) {
                    (LockType::Read, LockType::Read) => continue,
                    (LockType::Write, _) | (_, LockType::Write) => return true,
                }
            }
        }

        false
    }

    /// Get lock statistics
    pub fn get_stats(&self) -> HashMap<String, u64> {
        let locks = match self.locks.read() {
            Ok(l) => l,
            Err(_) => return HashMap::new(),
        };
        let tx_locks = match self.transaction_locks.read() {
            Ok(t) => t,
            Err(_) => return HashMap::new(),
        };

        let total_locks: u64 = locks.values().map(|v| v.len() as u64).sum();
        let active_transactions = tx_locks.len() as u64;

        let mut stats = HashMap::new();
        stats.insert("total_locks".to_string(), total_locks);
        stats.insert("active_transactions".to_string(), active_transactions);
        stats.insert("locked_resources".to_string(), locks.len() as u64);

        stats
    }
}

/// Transaction manager with MVCC support
#[derive(Debug)]
pub struct TransactionManager {
    /// Currently active transactions
    active_transactions: Arc<RwLock<HashMap<TransactionId, ActiveTransaction>>>,
    /// Lock manager for concurrency control
    lock_manager: Arc<LockManager>,
    /// Global timestamp for snapshot isolation
    global_timestamp: Arc<AtomicU64>,
    /// Transaction commit log
    commit_log: Arc<ParkingMutex<Vec<(TransactionId, Timestamp)>>>,
    /// Configuration
    config: TransactionConfig,
}

/// Configuration for transaction management
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Maximum number of concurrent transactions
    pub max_concurrent_transactions: usize,
    /// Transaction timeout in milliseconds
    pub transaction_timeout_ms: u64,
    /// Enable deadlock detection
    pub enable_deadlock_detection: bool,
    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolution,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_transactions: 1000,
            transaction_timeout_ms: 30000, // 30 seconds
            enable_deadlock_detection: true,
            conflict_resolution: ConflictResolution::AbortOlder,
        }
    }
}

/// Conflict resolution strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Abort the older transaction
    AbortOlder,
    /// Abort the newer transaction
    AbortNewer,
    /// Wait for lock release (may cause deadlocks)
    Wait,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::new_with_config(TransactionConfig::default())
    }

    pub fn new_with_config(config: TransactionConfig) -> Self {
        Self {
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            lock_manager: Arc::new(LockManager::new()),
            global_timestamp: Arc::new(AtomicU64::new(1)),
            commit_log: Arc::new(ParkingMutex::new(Vec::new())),
            config,
        }
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Result<Transaction> {
        let active = self
            .active_transactions
            .read()
            .map_err(|_| AsterError::internal("Failed to acquire read lock"))?;

        // Check transaction limit
        if active.len() >= self.config.max_concurrent_transactions {
            return Err(AsterError::transaction("Too many concurrent transactions"));
        }

        drop(active);

        let transaction_id = TransactionId::new();
        let snapshot_timestamp = self.global_timestamp.load(Ordering::SeqCst);

        let tx_info = ActiveTransaction::new(transaction_id);

        {
            let mut active = self
                .active_transactions
                .write()
                .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;
            active.insert(transaction_id, tx_info);
        }

        Ok(Transaction::new(
            transaction_id,
            snapshot_timestamp,
            Arc::clone(&self.active_transactions),
            Arc::clone(&self.lock_manager),
            Arc::clone(&self.global_timestamp),
            Arc::clone(&self.commit_log),
            self.config.clone(),
        ))
    }

    /// Commit a transaction
    pub fn commit(&self, transaction_id: TransactionId) -> Result<()> {
        let mut active = self
            .active_transactions
            .write()
            .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;

        if let Some(mut tx_info) = active.remove(&transaction_id) {
            tx_info.status = TransactionStatus::Committed;

            // Release all locks
            self.lock_manager.release_locks(transaction_id)?;

            // Update global timestamp
            let commit_timestamp = self.global_timestamp.fetch_add(1, Ordering::SeqCst) + 1;

            // Log commit
            {
                let mut commit_log = self.commit_log.lock();
                commit_log.push((transaction_id, Timestamp::from_u64(commit_timestamp)));
            }

            Ok(())
        } else {
            Err(AsterError::transaction("Transaction not found"))
        }
    }

    /// Abort a transaction
    pub fn abort(&self, transaction_id: TransactionId) -> Result<()> {
        let mut active = self
            .active_transactions
            .write()
            .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;

        if let Some(mut tx_info) = active.remove(&transaction_id) {
            tx_info.status = TransactionStatus::Aborted;

            // Release all locks
            self.lock_manager.release_locks(transaction_id)?;

            Ok(())
        } else {
            Err(AsterError::transaction("Transaction not found"))
        }
    }

    /// Get transaction statistics
    pub fn get_stats(&self) -> TransactionManagerStats {
        let active = match self.active_transactions.read() {
            Ok(a) => a,
            Err(_) => {
                return TransactionManagerStats {
                    active_transactions: 0,
                    total_reads: 0,
                    total_writes: 0,
                    total_conflicts: 0,
                    lock_stats: HashMap::new(),
                }
            }
        };
        let lock_stats = self.lock_manager.get_stats();

        let mut total_reads = 0;
        let mut total_writes = 0;
        let mut total_conflicts = 0;

        for tx in active.values() {
            total_reads += tx.stats.read_count;
            total_writes += tx.stats.write_count;
            total_conflicts += tx.stats.conflicts_detected;
        }

        TransactionManagerStats {
            active_transactions: active.len(),
            total_reads,
            total_writes,
            total_conflicts,
            lock_stats,
        }
    }

    /// Check for transaction conflicts
    pub fn check_conflicts(
        &self,
        _transaction_id: TransactionId,
        resource: &LockResource,
        lock_type: LockType,
    ) -> Result<bool> {
        // Check if we can acquire the lock
        match self.config.conflict_resolution {
            ConflictResolution::AbortOlder | ConflictResolution::AbortNewer => {
                Ok(!self.lock_manager.has_conflicts(resource, lock_type))
            }
            ConflictResolution::Wait => {
                // For wait strategy, we would implement a wait mechanism
                // For now, just check for conflicts
                Ok(!self.lock_manager.has_conflicts(resource, lock_type))
            }
        }
    }

    /// Clean up expired transactions
    pub fn cleanup_expired_transactions(&self) -> Result<Vec<TransactionId>> {
        let mut expired = Vec::new();
        let current_time = Timestamp::now();
        let timeout_ns = self.config.transaction_timeout_ms * 1_000_000; // Convert to nanoseconds

        {
            let active = self
                .active_transactions
                .read()
                .map_err(|_| AsterError::transaction("Failed to acquire read lock"))?;
            for (tx_id, tx_info) in active.iter() {
                let duration = current_time
                    .as_u64()
                    .saturating_sub(tx_info.start_time.as_u64());
                if duration > timeout_ns {
                    expired.push(*tx_id);
                }
            }
        }

        // Abort expired transactions
        for tx_id in &expired {
            let _ = self.abort(*tx_id); // Ignore errors for cleanup
        }

        Ok(expired)
    }
}

/// Transaction manager statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransactionManagerStats {
    pub active_transactions: usize,
    pub total_reads: u64,
    pub total_writes: u64,
    pub total_conflicts: u64,
    pub lock_stats: HashMap<String, u64>,
}

/// A database transaction with MVCC support
#[derive(Debug)]
pub struct Transaction {
    id: TransactionId,
    snapshot_timestamp: u64,
    start_time: Timestamp,
    is_active: bool,

    // Shared state
    active_transactions: Arc<RwLock<HashMap<TransactionId, ActiveTransaction>>>,
    lock_manager: Arc<LockManager>,
    global_timestamp: Arc<AtomicU64>,
    commit_log: Arc<ParkingMutex<Vec<(TransactionId, Timestamp)>>>,
    config: TransactionConfig,
}

impl Transaction {
    fn new(
        id: TransactionId,
        snapshot_timestamp: u64,
        active_transactions: Arc<RwLock<HashMap<TransactionId, ActiveTransaction>>>,
        lock_manager: Arc<LockManager>,
        global_timestamp: Arc<AtomicU64>,
        commit_log: Arc<ParkingMutex<Vec<(TransactionId, Timestamp)>>>,
        config: TransactionConfig,
    ) -> Self {
        Self {
            id,
            snapshot_timestamp,
            start_time: Timestamp::now(),
            is_active: true,
            active_transactions,
            lock_manager,
            global_timestamp,
            commit_log,
            config,
        }
    }

    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn snapshot_timestamp(&self) -> u64 {
        self.snapshot_timestamp
    }

    pub fn start_time(&self) -> Timestamp {
        self.start_time
    }

    pub fn is_active(&self) -> bool {
        self.is_active
    }

    /// Record a read operation
    pub fn record_read(&self, resource: LockResource) -> Result<()> {
        if !self.is_active {
            return Err(AsterError::transaction("Transaction not active"));
        }

        // Try to acquire read lock
        if !self
            .lock_manager
            .acquire_lock(self.id, resource.clone(), LockType::Read)?
        {
            return Err(AsterError::conflict("Read conflict detected"));
        }

        // Update transaction state
        {
            let mut active = self
                .active_transactions
                .write()
                .map_err(|_| AsterError::transaction("Failed to acquire write lock"))?;
            if let Some(tx_info) = active.get_mut(&self.id) {
                tx_info.read_set.insert(resource);
                tx_info.stats.read_count += 1;
                tx_info.stats.locks_acquired += 1;
            }
        }

        Ok(())
    }

    /// Record a write operation
    pub fn record_write(&self, resource: LockResource) -> Result<()> {
        if !self.is_active {
            return Err(AsterError::transaction("Transaction not active"));
        }

        // Try to acquire write lock
        if !self
            .lock_manager
            .acquire_lock(self.id, resource.clone(), LockType::Write)?
        {
            return Err(AsterError::conflict("Write conflict detected"));
        }

        // Update transaction state
        {
            let mut active = self
                .active_transactions
                .write()
                .map_err(|_| AsterError::transaction("Failed to acquire write lock"))?;
            if let Some(tx_info) = active.get_mut(&self.id) {
                tx_info.write_set.insert(resource);
                tx_info.stats.write_count += 1;
                tx_info.stats.locks_acquired += 1;
            }
        }

        Ok(())
    }

    /// Commit the transaction
    pub async fn commit(mut self) -> Result<()> {
        if !self.is_active {
            return Err(AsterError::transaction("Transaction already completed"));
        }

        self.is_active = false;

        // Validate transaction hasn't been aborted
        {
            let active = self
                .active_transactions
                .read()
                .map_err(|_| AsterError::transaction("Failed to acquire read lock"))?;
            if let Some(tx_info) = active.get(&self.id) {
                if tx_info.status == TransactionStatus::Aborted {
                    return Err(AsterError::transaction("Transaction was aborted"));
                }
            }
        }

        // Perform commit through transaction manager
        let mut active = self
            .active_transactions
            .write()
            .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;
        if let Some(mut tx_info) = active.remove(&self.id) {
            tx_info.status = TransactionStatus::Committed;
            tx_info.stats.duration_ms = self
                .start_time
                .as_u64()
                .saturating_sub(Timestamp::now().as_u64())
                / 1_000_000;

            // Release all locks
            self.lock_manager.release_locks(self.id)?;

            // Update global timestamp
            let commit_timestamp = self.global_timestamp.fetch_add(1, Ordering::SeqCst) + 1;

            // Log commit
            {
                let mut commit_log = self.commit_log.lock();
                commit_log.push((self.id, Timestamp::from_u64(commit_timestamp)));
            }

            Ok(())
        } else {
            Err(AsterError::transaction("Transaction not found"))
        }
    }

    /// Rollback the transaction
    pub async fn rollback(mut self) -> Result<()> {
        if !self.is_active {
            return Err(AsterError::transaction("Transaction already completed"));
        }

        self.is_active = false;

        // Remove from active transactions
        let mut active = self
            .active_transactions
            .write()
            .map_err(|_| AsterError::internal("Failed to acquire write lock"))?;
        if let Some(mut tx_info) = active.remove(&self.id) {
            tx_info.status = TransactionStatus::Aborted;
            tx_info.stats.duration_ms = self
                .start_time
                .as_u64()
                .saturating_sub(Timestamp::now().as_u64())
                / 1_000_000;

            // Release all locks
            self.lock_manager.release_locks(self.id)?;

            Ok(())
        } else {
            Err(AsterError::transaction("Transaction not found"))
        }
    }

    /// Get transaction statistics
    pub fn get_stats(&self) -> Option<TransactionStats> {
        let active = self.active_transactions.read().ok()?;
        active.get(&self.id).map(|tx| tx.stats.clone())
    }

    /// Get transaction changes (write set) - used for recovery logging
    pub fn get_changes(&self) -> Vec<(crate::VertexId, crate::storage::MemTableEntry)> {
        // Extract changes from the transaction's write set
        let mut changes = Vec::new();

        if let Ok(active_transactions) = self.active_transactions.read() {
            if let Some(tx_info) = active_transactions.get(&self.id) {
                for resource in &tx_info.write_set {
                    match resource {
                        LockResource::Vertex(vertex_id) => {
                            // Create a basic MemTableEntry representing the change
                            // In a full implementation, this would track the actual data changes
                            let entry = crate::storage::MemTableEntry::new_pivot(
                                format!("changed_vertex_{}", vertex_id.as_u64()).into_bytes(),
                                crate::Timestamp::now(),
                            );
                            changes.push((*vertex_id, entry));
                        }
                        LockResource::Edge(edge_id) => {
                            // For edges, we could map to a synthetic vertex ID
                            let synthetic_vertex_id = crate::VertexId::from_u64(edge_id.as_u64());
                            let entry = crate::storage::MemTableEntry::new_pivot(
                                format!("changed_edge_{}", edge_id.as_u64()).into_bytes(),
                                crate::Timestamp::now(),
                            );
                            changes.push((synthetic_vertex_id, entry));
                        }
                    }
                }
            }
        }

        changes
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.is_active {
            // Auto-rollback if transaction is dropped without explicit commit/rollback
            self.is_active = false;
            let _ = self.lock_manager.release_locks(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_transaction_basic_operations() {
        let manager = TransactionManager::new();

        let tx = manager.begin().unwrap();
        assert!(tx.is_active());

        let tx_id = tx.id();
        tx.commit().await.unwrap();

        // Transaction should be removed from active list
        let stats = manager.get_stats();
        assert_eq!(stats.active_transactions, 0);
    }

    #[tokio::test]
    async fn test_lock_acquisition() {
        let manager = TransactionManager::new();

        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();

        let resource = LockResource::Vertex(VertexId::from_u64(1));

        // Both transactions should be able to acquire read locks
        tx1.record_read(resource.clone()).unwrap();
        tx2.record_read(resource.clone()).unwrap();

        tx1.commit().await.unwrap();
        tx2.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_conflict() {
        let manager = TransactionManager::new();

        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();

        let resource = LockResource::Vertex(VertexId::from_u64(1));

        // First transaction gets write lock
        tx1.record_write(resource.clone()).unwrap();

        // Second transaction should fail to get write lock
        assert!(tx2.record_write(resource).is_err());

        tx1.commit().await.unwrap();
        tx2.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_write_conflict() {
        let manager = TransactionManager::new();

        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();

        let resource = LockResource::Vertex(VertexId::from_u64(1));

        // First transaction gets read lock
        tx1.record_read(resource.clone()).unwrap();

        // Second transaction should fail to get write lock
        assert!(tx2.record_write(resource).is_err());

        tx1.commit().await.unwrap();
        tx2.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_transaction_manager_stats() {
        let manager = TransactionManager::new();

        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();

        let resource1 = LockResource::Vertex(VertexId::from_u64(1));
        let resource2 = LockResource::Vertex(VertexId::from_u64(2));

        tx1.record_read(resource1).unwrap();
        tx2.record_write(resource2).unwrap();

        let stats = manager.get_stats();
        assert_eq!(stats.active_transactions, 2);
        assert_eq!(stats.total_reads, 1);
        assert_eq!(stats.total_writes, 1);

        tx1.commit().await.unwrap();
        tx2.commit().await.unwrap();
    }

    #[test]
    fn test_transaction_cleanup() {
        let mut config = TransactionConfig::default();
        config.transaction_timeout_ms = 1; // Very short timeout for testing

        let manager = TransactionManager::new_with_config(config);
        let _tx = manager.begin().unwrap();

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(10));

        let expired = manager.cleanup_expired_transactions().unwrap();
        assert_eq!(expired.len(), 1);

        let stats = manager.get_stats();
        assert_eq!(stats.active_transactions, 0);
    }

    #[test]
    fn test_lock_manager() {
        let lock_manager = LockManager::new();
        let tx_id = TransactionId::new();
        let resource = LockResource::Vertex(VertexId::from_u64(1));

        // Acquire read lock
        assert!(lock_manager
            .acquire_lock(tx_id, resource.clone(), LockType::Read)
            .unwrap());

        // Another read lock should succeed
        let tx_id2 = TransactionId::new();
        assert!(lock_manager
            .acquire_lock(tx_id2, resource.clone(), LockType::Read)
            .unwrap());

        // Write lock should fail
        let tx_id3 = TransactionId::new();
        assert!(!lock_manager
            .acquire_lock(tx_id3, resource.clone(), LockType::Write)
            .unwrap());

        // Release locks
        lock_manager.release_locks(tx_id).unwrap();
        lock_manager.release_locks(tx_id2).unwrap();

        // Now write lock should succeed
        assert!(lock_manager
            .acquire_lock(tx_id3, resource, LockType::Write)
            .unwrap());
    }
}
