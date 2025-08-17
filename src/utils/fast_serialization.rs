//! High-performance serialization utilities
//!
//! This module provides optimized serialization for hot paths in the database,
//! replacing bincode with faster custom formats for critical data structures.

use crate::storage::memtable::MemTableEntry;
use crate::types::Properties;
use crate::{AsterError, EdgeId, PropertyValue, Result, Timestamp, VertexId};
use std::collections::HashMap;
use std::io::{Read, Write};

/// Fast binary serialization trait
pub trait FastSerialize {
    fn serialize_fast(&self, writer: &mut dyn Write) -> Result<()>;
    fn deserialize_fast(reader: &mut dyn Read) -> Result<Self>
    where
        Self: Sized;

    /// Get the serialized size (if known in advance)
    fn serialized_size(&self) -> Option<usize> {
        None
    }
}

/// Fast binary serialization for VertexId
impl FastSerialize for VertexId {
    fn serialize_fast(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_all(&self.as_u64().to_le_bytes())?;
        Ok(())
    }

    fn deserialize_fast(reader: &mut dyn Read) -> Result<Self> {
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        Ok(VertexId::from_u64(u64::from_le_bytes(bytes)))
    }

    fn serialized_size(&self) -> Option<usize> {
        Some(8)
    }
}

/// Fast binary serialization for EdgeId
impl FastSerialize for EdgeId {
    fn serialize_fast(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_all(&self.as_u64().to_le_bytes())?;
        Ok(())
    }

    fn deserialize_fast(reader: &mut dyn Read) -> Result<Self> {
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        Ok(EdgeId::from_u64(u64::from_le_bytes(bytes)))
    }

    fn serialized_size(&self) -> Option<usize> {
        Some(8)
    }
}

/// Fast binary serialization for Timestamp
impl FastSerialize for Timestamp {
    fn serialize_fast(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_all(&self.as_u64().to_le_bytes())?;
        Ok(())
    }

    fn deserialize_fast(reader: &mut dyn Read) -> Result<Self> {
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        Ok(Timestamp::from_u64(u64::from_le_bytes(bytes)))
    }

    fn serialized_size(&self) -> Option<usize> {
        Some(8)
    }
}

/// Fast binary serialization for PropertyValue
impl FastSerialize for PropertyValue {
    fn serialize_fast(&self, writer: &mut dyn Write) -> Result<()> {
        match self {
            PropertyValue::Null => {
                writer.write_all(&[0u8])?;
            }
            PropertyValue::Bool(b) => {
                writer.write_all(&[1u8])?;
                writer.write_all(&[if *b { 1u8 } else { 0u8 }])?;
            }
            PropertyValue::Int(i) => {
                writer.write_all(&[2u8])?;
                writer.write_all(&i.to_le_bytes())?;
            }
            PropertyValue::Float(f) => {
                writer.write_all(&[3u8])?;
                writer.write_all(&f.to_le_bytes())?;
            }
            PropertyValue::String(s) => {
                writer.write_all(&[4u8])?;
                let bytes = s.as_bytes();
                writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
                writer.write_all(bytes)?;
            }
            PropertyValue::Bytes(b) => {
                writer.write_all(&[5u8])?;
                writer.write_all(&(b.len() as u32).to_le_bytes())?;
                writer.write_all(b)?;
            }
            PropertyValue::List(list) => {
                writer.write_all(&[6u8])?;
                writer.write_all(&(list.len() as u32).to_le_bytes())?;
                for item in list {
                    item.serialize_fast(writer)?;
                }
            }
            PropertyValue::Map(map) => {
                writer.write_all(&[7u8])?;
                writer.write_all(&(map.len() as u32).to_le_bytes())?;
                for (key, value) in map {
                    let key_bytes = key.as_bytes();
                    writer.write_all(&(key_bytes.len() as u32).to_le_bytes())?;
                    writer.write_all(key_bytes)?;
                    value.serialize_fast(writer)?;
                }
            }
        }
        Ok(())
    }

    fn deserialize_fast(reader: &mut dyn Read) -> Result<Self> {
        let mut type_byte = [0u8; 1];
        reader.read_exact(&mut type_byte)?;

        match type_byte[0] {
            0 => Ok(PropertyValue::Null),
            1 => {
                let mut bool_byte = [0u8; 1];
                reader.read_exact(&mut bool_byte)?;
                Ok(PropertyValue::Bool(bool_byte[0] != 0))
            }
            2 => {
                let mut bytes = [0u8; 8];
                reader.read_exact(&mut bytes)?;
                Ok(PropertyValue::Int(i64::from_le_bytes(bytes)))
            }
            3 => {
                let mut bytes = [0u8; 8];
                reader.read_exact(&mut bytes)?;
                Ok(PropertyValue::Float(f64::from_le_bytes(bytes)))
            }
            4 => {
                let mut len_bytes = [0u8; 4];
                reader.read_exact(&mut len_bytes)?;
                let len = u32::from_le_bytes(len_bytes) as usize;
                let mut string_bytes = vec![0u8; len];
                reader.read_exact(&mut string_bytes)?;
                Ok(PropertyValue::String(
                    String::from_utf8(string_bytes)
                        .map_err(|e| AsterError::storage(e.to_string()))?,
                ))
            }
            5 => {
                let mut len_bytes = [0u8; 4];
                reader.read_exact(&mut len_bytes)?;
                let len = u32::from_le_bytes(len_bytes) as usize;
                let mut bytes = vec![0u8; len];
                reader.read_exact(&mut bytes)?;
                Ok(PropertyValue::Bytes(bytes))
            }
            6 => {
                let mut len_bytes = [0u8; 4];
                reader.read_exact(&mut len_bytes)?;
                let len = u32::from_le_bytes(len_bytes) as usize;
                let mut list = Vec::with_capacity(len);
                for _ in 0..len {
                    list.push(PropertyValue::deserialize_fast(reader)?);
                }
                Ok(PropertyValue::List(list))
            }
            7 => {
                let mut len_bytes = [0u8; 4];
                reader.read_exact(&mut len_bytes)?;
                let len = u32::from_le_bytes(len_bytes) as usize;
                let mut map = std::collections::HashMap::with_capacity(len);
                for _ in 0..len {
                    reader.read_exact(&mut len_bytes)?;
                    let key_len = u32::from_le_bytes(len_bytes) as usize;
                    let mut key_bytes = vec![0u8; key_len];
                    reader.read_exact(&mut key_bytes)?;
                    let key = String::from_utf8(key_bytes)
                        .map_err(|e| AsterError::storage(e.to_string()))?;
                    let value = PropertyValue::deserialize_fast(reader)?;
                    map.insert(key, value);
                }
                Ok(PropertyValue::Map(map))
            }
            _ => Err(AsterError::storage("Invalid PropertyValue type tag")),
        }
    }
}

/// Fast binary serialization for Properties (HashMap<String, PropertyValue>)
impl FastSerialize for Properties {
    fn serialize_fast(&self, writer: &mut dyn Write) -> Result<()> {
        // Write number of properties
        writer.write_all(&(self.len() as u32).to_le_bytes())?;

        for (key, value) in self {
            // Write key
            let key_bytes = key.as_bytes();
            writer.write_all(&(key_bytes.len() as u32).to_le_bytes())?;
            writer.write_all(key_bytes)?;

            // Write value
            value.serialize_fast(writer)?;
        }

        Ok(())
    }

    fn deserialize_fast(reader: &mut dyn Read) -> Result<Self> {
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let num_properties = u32::from_le_bytes(len_bytes) as usize;

        let mut properties = HashMap::with_capacity(num_properties);

        for _ in 0..num_properties {
            // Read key
            reader.read_exact(&mut len_bytes)?;
            let key_len = u32::from_le_bytes(len_bytes) as usize;
            let mut key_bytes = vec![0u8; key_len];
            reader.read_exact(&mut key_bytes)?;
            let key =
                String::from_utf8(key_bytes).map_err(|e| AsterError::storage(e.to_string()))?;

            // Read value
            let value = PropertyValue::deserialize_fast(reader)?;

            properties.insert(key, value);
        }

        Ok(properties)
    }
}

/// Fast serialization buffer for zero-allocation serialization
pub struct FastSerializeBuffer {
    buffer: Vec<u8>,
    position: usize,
}

impl FastSerializeBuffer {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(4096), // Start with 4KB
            position: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            position: 0,
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.position = 0;
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buffer
    }

    /// Serialize an object to the buffer
    pub fn serialize<T: FastSerialize>(&mut self, obj: &T) -> Result<()> {
        obj.serialize_fast(self)
    }

    /// Deserialize from a slice
    pub fn deserialize<T: FastSerialize>(data: &[u8]) -> Result<T> {
        let mut cursor = std::io::Cursor::new(data);
        T::deserialize_fast(&mut cursor)
    }
}

impl Write for FastSerializeBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Batch serialization for multiple objects of the same type
pub fn serialize_batch<T: FastSerialize>(objects: &[T]) -> Result<Vec<u8>> {
    let estimated_size = objects
        .get(0)
        .and_then(|obj| obj.serialized_size())
        .map(|size| size * objects.len())
        .unwrap_or(4096);

    let mut buffer = FastSerializeBuffer::with_capacity(estimated_size);

    // Write count
    buffer.write_all(&(objects.len() as u32).to_le_bytes())?;

    // Write objects
    for obj in objects {
        obj.serialize_fast(&mut buffer)?;
    }

    Ok(buffer.into_vec())
}

/// Batch deserialization for multiple objects of the same type
pub fn deserialize_batch<T: FastSerialize>(data: &[u8]) -> Result<Vec<T>> {
    let mut cursor = std::io::Cursor::new(data);

    // Read count
    let mut count_bytes = [0u8; 4];
    cursor.read_exact(&mut count_bytes)?;
    let count = u32::from_le_bytes(count_bytes) as usize;

    // Read objects
    let mut objects = Vec::with_capacity(count);
    for _ in 0..count {
        objects.push(T::deserialize_fast(&mut cursor)?);
    }

    Ok(objects)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vertex_id_serialization() {
        let vertex_id = VertexId::from_u64(12345);
        let mut buffer = FastSerializeBuffer::new();
        vertex_id.serialize_fast(&mut buffer).unwrap();

        let deserialized = FastSerializeBuffer::deserialize::<VertexId>(buffer.as_slice()).unwrap();
        assert_eq!(vertex_id, deserialized);
    }

    #[test]
    fn test_property_value_serialization() {
        let values = vec![
            PropertyValue::Null,
            PropertyValue::Bool(true),
            PropertyValue::Int(42),
            PropertyValue::Float(3.14),
            PropertyValue::String("hello".to_string()),
            PropertyValue::Bytes(vec![1, 2, 3, 4]),
        ];

        for value in values {
            let mut buffer = FastSerializeBuffer::new();
            value.serialize_fast(&mut buffer).unwrap();

            let deserialized =
                FastSerializeBuffer::deserialize::<PropertyValue>(buffer.as_slice()).unwrap();
            assert_eq!(value, deserialized);
        }
    }

    #[test]
    fn test_properties_serialization() {
        let mut properties = Properties::new();
        properties.insert(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );
        properties.insert("age".to_string(), PropertyValue::Int(30));
        properties.insert("active".to_string(), PropertyValue::Bool(true));

        let mut buffer = FastSerializeBuffer::new();
        properties.serialize_fast(&mut buffer).unwrap();

        let deserialized =
            FastSerializeBuffer::deserialize::<Properties>(buffer.as_slice()).unwrap();
        assert_eq!(properties, deserialized);
    }

    #[test]
    fn test_batch_serialization() {
        let vertex_ids = vec![
            VertexId::from_u64(1),
            VertexId::from_u64(2),
            VertexId::from_u64(3),
        ];

        let data = serialize_batch(&vertex_ids).unwrap();
        let deserialized = deserialize_batch::<VertexId>(&data).unwrap();

        assert_eq!(vertex_ids, deserialized);
    }
}
