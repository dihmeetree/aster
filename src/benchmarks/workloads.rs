use std::fmt;

/// Different types of workloads for benchmarking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadType {
    /// Write-heavy workload (80% writes, 20% reads)
    WriteHeavy,
    /// Read-heavy workload (20% writes, 80% reads)
    ReadHeavy,
    /// Mixed workload (50% writes, 50% reads)
    Mixed,
    /// High contention workload (many operations on few vertices)
    HighContention,
    /// Traversal-heavy workload (graph traversals and path queries)
    Traversal,
    /// Bulk loading workload (large batch operations)
    BulkLoad,
}

impl fmt::Display for WorkloadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkloadType::WriteHeavy => write!(f, "Write-Heavy"),
            WorkloadType::ReadHeavy => write!(f, "Read-Heavy"),
            WorkloadType::Mixed => write!(f, "Mixed"),
            WorkloadType::HighContention => write!(f, "High-Contention"),
            WorkloadType::Traversal => write!(f, "Traversal"),
            WorkloadType::BulkLoad => write!(f, "Bulk-Load"),
        }
    }
}

impl WorkloadType {
    /// Get description of the workload
    pub fn description(&self) -> &'static str {
        match self {
            WorkloadType::WriteHeavy => "80% edge additions/updates, 20% neighbor lookups",
            WorkloadType::ReadHeavy => "20% edge additions/updates, 80% neighbor lookups",
            WorkloadType::Mixed => "50% edge additions/updates, 50% neighbor lookups",
            WorkloadType::HighContention => "Concurrent operations on small vertex set",
            WorkloadType::Traversal => "Multi-hop graph traversals and path queries",
            WorkloadType::BulkLoad => "Large batch operations and bulk data loading",
        }
    }

    /// Get the expected write ratio for this workload
    pub fn write_ratio(&self) -> f64 {
        match self {
            WorkloadType::WriteHeavy => 0.8,
            WorkloadType::ReadHeavy => 0.2,
            WorkloadType::Mixed => 0.5,
            WorkloadType::HighContention => 0.7, // Slightly write-heavy for contention
            WorkloadType::Traversal => 0.1,      // Mostly reads for traversals
            WorkloadType::BulkLoad => 0.95,      // Almost all writes
        }
    }

    /// Get the expected read ratio for this workload
    pub fn read_ratio(&self) -> f64 {
        1.0 - self.write_ratio()
    }

    /// Get all available workload types
    pub fn all() -> Vec<WorkloadType> {
        vec![
            WorkloadType::WriteHeavy,
            WorkloadType::ReadHeavy,
            WorkloadType::Mixed,
            WorkloadType::HighContention,
            WorkloadType::Traversal,
            WorkloadType::BulkLoad,
        ]
    }

    /// Get workloads suitable for performance testing
    pub fn performance_workloads() -> Vec<WorkloadType> {
        vec![
            WorkloadType::WriteHeavy,
            WorkloadType::ReadHeavy,
            WorkloadType::Mixed,
            WorkloadType::HighContention,
        ]
    }

    /// Get workloads suitable for functionality testing
    pub fn functionality_workloads() -> Vec<WorkloadType> {
        vec![
            WorkloadType::Mixed,
            WorkloadType::Traversal,
            WorkloadType::BulkLoad,
        ]
    }
}
