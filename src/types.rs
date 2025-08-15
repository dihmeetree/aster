//! Core types used throughout the Aster database

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

/// Unique identifier for vertices
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VertexId(pub u64);

impl VertexId {
    /// Create a new random vertex ID
    pub fn new() -> Self {
        Self(rand::random())
    }

    /// Create a vertex ID from a u64
    pub fn from_u64(id: u64) -> Self {
        Self(id)
    }

    /// Get the underlying u64 value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for VertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl Default for VertexId {
    fn default() -> Self {
        Self::new()
    }
}

/// Unique identifier for edges
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EdgeId(pub u64);

impl EdgeId {
    /// Create a new random edge ID
    pub fn new() -> Self {
        Self(rand::random())
    }

    /// Create an edge ID from a u64
    pub fn from_u64(id: u64) -> Self {
        Self(id)
    }

    /// Get the underlying u64 value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e{}", self.0)
    }
}

impl Default for EdgeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Timestamp for versioning and MVCC
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// Create a new timestamp with current time
    pub fn now() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Self(duration.as_nanos() as u64)
    }

    /// Create a timestamp from a u64
    pub fn from_u64(ts: u64) -> Self {
        Self(ts)
    }

    /// Get the underlying u64 value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::now()
    }
}

/// Property values that can be stored on vertices and edges
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<PropertyValue>),
    Map(HashMap<String, PropertyValue>),
}

impl PropertyValue {
    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, PropertyValue::Null)
    }

    /// Try to convert to boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            PropertyValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to convert to integer
    pub fn as_int(&self) -> Option<i64> {
        match self {
            PropertyValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to convert to float
    pub fn as_float(&self) -> Option<f64> {
        match self {
            PropertyValue::Float(f) => Some(*f),
            PropertyValue::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to convert to string
    pub fn as_string(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to convert to bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            PropertyValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Get the type name as string
    pub fn type_name(&self) -> &'static str {
        match self {
            PropertyValue::Null => "null",
            PropertyValue::Bool(_) => "bool",
            PropertyValue::Int(_) => "int",
            PropertyValue::Float(_) => "float",
            PropertyValue::String(_) => "string",
            PropertyValue::Bytes(_) => "bytes",
            PropertyValue::List(_) => "list",
            PropertyValue::Map(_) => "map",
        }
    }
}

impl fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyValue::Null => write!(f, "null"),
            PropertyValue::Bool(b) => write!(f, "{}", b),
            PropertyValue::Int(i) => write!(f, "{}", i),
            PropertyValue::Float(fl) => write!(f, "{}", fl),
            PropertyValue::String(s) => write!(f, "\"{}\"", s),
            PropertyValue::Bytes(b) => write!(f, "bytes[{}]", b.len()),
            PropertyValue::List(l) => write!(f, "list[{}]", l.len()),
            PropertyValue::Map(m) => write!(f, "map[{}]", m.len()),
        }
    }
}

// Convenient conversions
impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self {
        PropertyValue::Bool(b)
    }
}

impl From<i64> for PropertyValue {
    fn from(i: i64) -> Self {
        PropertyValue::Int(i)
    }
}

impl From<i32> for PropertyValue {
    fn from(i: i32) -> Self {
        PropertyValue::Int(i as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(f: f64) -> Self {
        PropertyValue::Float(f)
    }
}

impl From<f32> for PropertyValue {
    fn from(f: f32) -> Self {
        PropertyValue::Float(f as f64)
    }
}

impl From<String> for PropertyValue {
    fn from(s: String) -> Self {
        PropertyValue::String(s)
    }
}

impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self {
        PropertyValue::String(s.to_string())
    }
}

impl From<Vec<u8>> for PropertyValue {
    fn from(b: Vec<u8>) -> Self {
        PropertyValue::Bytes(b)
    }
}

impl PartialOrd for PropertyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PropertyValue {}

impl Ord for PropertyValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        use PropertyValue::*;

        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,

            (Bool(a), Bool(b)) => a.cmp(b),
            (Bool(_), _) => Ordering::Less,
            (_, Bool(_)) => Ordering::Greater,

            (Int(a), Int(b)) => a.cmp(b),
            (Int(a), Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
            (Float(a), Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
            (Float(a), Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Int(_), _) => Ordering::Less,
            (Float(_), _) => Ordering::Less,
            (_, Int(_)) => Ordering::Greater,
            (_, Float(_)) => Ordering::Greater,

            (String(a), String(b)) => a.cmp(b),
            (String(_), _) => Ordering::Less,
            (_, String(_)) => Ordering::Greater,

            (Bytes(a), Bytes(b)) => a.cmp(b),
            (Bytes(_), _) => Ordering::Less,
            (_, Bytes(_)) => Ordering::Greater,

            (List(a), List(b)) => a.cmp(b),
            (List(_), _) => Ordering::Less,
            (_, List(_)) => Ordering::Greater,

            (Map(a), Map(b)) => {
                // Compare maps by converting to sorted vectors
                let mut a_vec: Vec<_> = a.iter().collect();
                let mut b_vec: Vec<_> = b.iter().collect();
                a_vec.sort_by_key(|(k, _)| *k);
                b_vec.sort_by_key(|(k, _)| *k);
                a_vec.cmp(&b_vec)
            }
        }
    }
}

/// Properties map for vertices and edges
pub type Properties = HashMap<String, PropertyValue>;

/// Configuration for the Poly-LSM storage engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolyLSMConfig {
    /// Size ratio between adjacent levels (T in the paper)
    pub level_size_ratio: u32,
    /// Size of a disk block in bytes
    pub block_size: u32,
    /// Bits per key for Bloom filters
    pub bloom_filter_bits_per_key: u32,
    /// Bits per vertex for degree sketch
    pub degree_sketch_bits_per_vertex: u32,
    /// Memory buffer size in bytes
    pub memtable_size: usize,
    /// Enable compression
    pub compression_enabled: bool,
    /// Lookup ratio for adaptive updates (between 0.0 and 1.0)
    pub lookup_ratio: f64,
    /// Average degree of the graph
    pub average_degree: f64,
}

impl Default for PolyLSMConfig {
    fn default() -> Self {
        Self {
            level_size_ratio: 10,
            block_size: 4096,
            bloom_filter_bits_per_key: 10,
            degree_sketch_bits_per_vertex: 8,
            memtable_size: 64 * 1024 * 1024, // 64MB
            compression_enabled: true,
            lookup_ratio: 0.5,
            average_degree: 32.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vertex_id() {
        let id1 = VertexId::new();
        let id2 = VertexId::from_u64(123);

        assert_eq!(id2.as_u64(), 123);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_property_value_conversions() {
        let pv: PropertyValue = "hello".into();
        assert_eq!(pv.as_string(), Some("hello"));

        let pv: PropertyValue = 42i32.into();
        assert_eq!(pv.as_int(), Some(42));

        let pv: PropertyValue = 3.14f64.into();
        assert_eq!(pv.as_float(), Some(3.14));
    }

    #[test]
    fn test_timestamp_ordering() {
        let ts1 = Timestamp::now();
        std::thread::sleep(std::time::Duration::from_nanos(1));
        let ts2 = Timestamp::now();

        assert!(ts2 > ts1);
    }
}
