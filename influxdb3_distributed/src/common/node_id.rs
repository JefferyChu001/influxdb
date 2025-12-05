//! Node identifier type.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

/// Unique identifier for a node in the cluster.
///
/// Each node (Frontend, Datanode, or MetaServer) has a unique NodeId
/// that is used for identification and routing.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    /// Create a new NodeId from a u64 value.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the underlying u64 value.
    pub const fn get(&self) -> u64 {
        self.0
    }

    /// Create a NodeId from bytes (big-endian).
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    /// Convert to bytes (big-endian).
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self::new(id)
    }
}

impl From<NodeId> for u64 {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for NodeId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(NodeId::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_creation() {
        let id = NodeId::new(42);
        assert_eq!(id.get(), 42);
    }

    #[test]
    fn test_node_id_from_u64() {
        let id: NodeId = 123u64.into();
        assert_eq!(id.get(), 123);
    }

    #[test]
    fn test_node_id_to_u64() {
        let id = NodeId::new(456);
        let val: u64 = id.into();
        assert_eq!(val, 456);
    }

    #[test]
    fn test_node_id_bytes_roundtrip() {
        let id = NodeId::new(0x0102030405060708);
        let bytes = id.to_bytes();
        let id2 = NodeId::from_bytes(bytes);
        assert_eq!(id, id2);
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId::new(789);
        assert_eq!(format!("{}", id), "789");
    }

    #[test]
    fn test_node_id_parse() {
        let id: NodeId = "999".parse().unwrap();
        assert_eq!(id.get(), 999);
    }

    #[test]
    fn test_node_id_ordering() {
        let id1 = NodeId::new(1);
        let id2 = NodeId::new(2);
        let id3 = NodeId::new(1);

        assert!(id1 < id2);
        assert_eq!(id1, id3);
    }
}
