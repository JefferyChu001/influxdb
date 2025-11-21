//! Core types for the cluster module

use serde::{Deserialize, Serialize};
use std::fmt;

/// Node identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Shard identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(u64);

impl ShardId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().as_u128() as u64)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for ShardId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shard-{}", self.0)
    }
}

impl From<u32> for ShardId {
    fn from(id: u32) -> Self {
        Self(id as u64)
    }
}

impl From<u64> for ShardId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Database identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DbId(u64);

impl DbId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for DbId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Table identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(u64);

impl TableId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for TableId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Coordinator node - handles routing and coordination
    Coordinator,
    /// Data node - stores data and executes queries
    DataNode,
    /// Mixed mode - both coordinator and data node
    Mixed,
}

/// Node status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is active and healthy
    Active,
    /// Node is inactive
    Inactive,
    /// Node is draining data before shutdown
    Draining,
    /// Node has failed
    Failed,
}

/// Node capacity information
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub cpu_cores: usize,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub current_shards: usize,
    pub max_shards: usize,
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: String,
    pub grpc_port: u16,
    pub http_port: u16,
    pub role: NodeRole,
    pub status: NodeStatus,
    pub capacity: NodeCapacity,
    /// Last heartbeat timestamp in nanoseconds since Unix epoch
    pub last_heartbeat_nanos: i64,
}

/// Shard range definition
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ShardRange {
    /// Hash-based sharding
    Hash {
        start: u64,
        end: u64,
    },
    /// Time-based sharding (timestamps in nanoseconds since Unix epoch)
    Time {
        start_nanos: i64,
        end_nanos: i64,
    },
    /// Hybrid sharding (hash + time)
    Hybrid {
        hash_start: u64,
        hash_end: u64,
        time_start_nanos: i64,
        time_end_nanos: i64,
    },
}

/// Shard status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardStatus {
    /// Shard is active and serving requests
    Active,
    /// Shard is being created
    Creating,
    /// Shard is being migrated
    Migrating,
    /// Shard is offline
    Offline,
}

/// Replica role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaRole {
    /// Leader replica (handles writes)
    Leader,
    /// Follower replica (replicates from leader)
    Follower,
}

/// Replica status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaStatus {
    /// Replica is active and in sync
    Active,
    /// Replica is syncing data
    Syncing,
    /// Replica has failed
    Failed,
}

/// Replica information
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub node_id: NodeId,
    pub role: ReplicaRole,
    pub status: ReplicaStatus,
    pub lag: Option<std::time::Duration>,
}

/// Shard information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub database_id: DbId,
    pub shard_range: ShardRange,
    pub replicas: Vec<ReplicaInfo>,
    pub status: ShardStatus,
}

/// Consistency level for writes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// At least one replica must acknowledge
    One,
    /// Majority of replicas must acknowledge
    Quorum,
    /// All replicas must acknowledge
    All,
}

