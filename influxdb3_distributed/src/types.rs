//! Core types for distributed query execution
//!
//! This module defines the fundamental types used throughout the distributed
//! query framework, heavily inspired by GreptimeDB's architecture.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Region identifier
///
/// In InfluxDB's distributed architecture, a Region is a partition of data
/// that contains a subset of the table's data. Similar to GreptimeDB's Region concept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct RegionId(u64);

impl RegionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn from_u32(table_id: u32, region_number: u32) -> Self {
        Self(((table_id as u64) << 32) | (region_number as u64))
    }

    pub fn table_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub fn region_number(&self) -> u32 {
        (self.0 & 0xFFFFFFFF) as u32
    }
}

impl From<u64> for RegionId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for RegionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "region-{}", self.0)
    }
}

/// Node identifier
///
/// Represents a physical node in the distributed cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct NodeId(u64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

/// Partition identifier
///
/// Partitions are logical divisions of data used for distribution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(u64);

impl PartitionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for PartitionId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "partition-{}", self.0)
    }
}

/// Table identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(u32);

impl TableId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl From<u32> for TableId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "table-{}", self.0)
    }
}

/// Schema reference
pub type SchemaRef = Arc<arrow::datatypes::Schema>;

/// Region status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionStatus {
    /// Region is active and serving requests
    Active,
    /// Region is being created
    Creating,
    /// Region is being migrated
    Migrating,
    /// Region is offline
    Offline,
}

/// Node status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is active and serving requests
    Active,
    /// Node is starting up
    Starting,
    /// Node is shutting down
    ShuttingDown,
    /// Node is offline
    Offline,
}

