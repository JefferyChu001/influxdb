//! Common types and utilities for the distributed framework.

mod node_id;
mod region;

pub use node_id::NodeId;
pub use region::{
    PartitionRange, RegionId, RegionInfo, RegionStatus, compute_partition_hash_from_str,
};

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

/// Information about a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique identifier for this node
    pub node_id: NodeId,

    /// Network address for gRPC communication
    pub grpc_addr: SocketAddr,

    /// Network address for HTTP API (optional, mainly for Frontend)
    pub http_addr: Option<SocketAddr>,

    /// The mode this node is running in
    pub node_mode: NodeRole,

    /// Current status of the node
    pub status: NodeStatus,

    /// Regions managed by this node (only applicable for Datanodes)
    pub regions: Vec<RegionId>,

    /// Last heartbeat timestamp (Unix timestamp in milliseconds)
    pub last_heartbeat_ms: i64,
}

impl NodeInfo {
    /// Create a new NodeInfo
    pub fn new(node_id: NodeId, grpc_addr: SocketAddr, node_mode: NodeRole) -> Self {
        Self {
            node_id,
            grpc_addr,
            http_addr: None,
            node_mode,
            status: NodeStatus::Starting,
            regions: Vec::new(),
            last_heartbeat_ms: 0,
        }
    }

    /// Set the HTTP address
    pub fn with_http_addr(mut self, addr: SocketAddr) -> Self {
        self.http_addr = Some(addr);
        self
    }

    /// Add regions to this node
    pub fn with_regions(mut self, regions: Vec<RegionId>) -> Self {
        self.regions = regions;
        self
    }

    /// Check if this node is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self.status, NodeStatus::Online)
    }

    /// Update the last heartbeat timestamp
    pub fn update_heartbeat(&mut self, timestamp_ms: i64) {
        self.last_heartbeat_ms = timestamp_ms;
    }

    /// Check if the node is considered stale based on the given timeout
    pub fn is_stale(&self, current_time_ms: i64, timeout: Duration) -> bool {
        let timeout_ms = timeout.as_millis() as i64;
        current_time_ms - self.last_heartbeat_ms > timeout_ms
    }
}

/// The role/mode of a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeRole {
    /// Frontend node that handles client requests
    Frontend,

    /// Datanode that stores and processes data
    Datanode,

    /// MetaServer that manages cluster metadata
    MetaServer,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Frontend => write!(f, "Frontend"),
            NodeRole::Datanode => write!(f, "Datanode"),
            NodeRole::MetaServer => write!(f, "MetaServer"),
        }
    }
}

/// Status of a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is starting up
    Starting,

    /// Node is online and healthy
    Online,

    /// Node is offline or unreachable
    Offline,

    /// Node is in maintenance mode
    Maintenance,

    /// Node is being drained before shutdown
    Draining,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStatus::Starting => write!(f, "Starting"),
            NodeStatus::Online => write!(f, "Online"),
            NodeStatus::Offline => write!(f, "Offline"),
            NodeStatus::Maintenance => write!(f, "Maintenance"),
            NodeStatus::Draining => write!(f, "Draining"),
        }
    }
}

/// Heartbeat request from a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// The node sending the heartbeat
    pub node_id: NodeId,

    /// Current status of the node
    pub status: NodeStatus,

    /// Regions currently managed by this node
    pub regions: Vec<RegionId>,

    /// CPU usage percentage (0-100)
    pub cpu_usage: f32,

    /// Memory usage percentage (0-100)
    pub memory_usage: f32,

    /// Disk usage percentage (0-100)
    pub disk_usage: f32,
}

/// Heartbeat response from the MetaServer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Whether the heartbeat was accepted
    pub success: bool,

    /// Any regions that should be added to this node
    pub regions_to_add: Vec<RegionInfo>,

    /// Any regions that should be removed from this node
    pub regions_to_remove: Vec<RegionId>,

    /// Optional message from the MetaServer
    pub message: Option<String>,
}

impl Default for HeartbeatResponse {
    fn default() -> Self {
        Self {
            success: true,
            regions_to_add: Vec::new(),
            regions_to_remove: Vec::new(),
            message: None,
        }
    }
}

/// Table location information used for query routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableLocation {
    /// Database name
    pub database: String,

    /// Table name
    pub table: String,

    /// Regions that contain data for this table
    pub regions: Vec<RegionInfo>,
}

impl TableLocation {
    /// Create a new TableLocation
    pub fn new(database: String, table: String) -> Self {
        Self {
            database,
            table,
            regions: Vec::new(),
        }
    }

    /// Add a region to this table location
    pub fn add_region(&mut self, region: RegionInfo) {
        self.regions.push(region);
    }

    /// Get all unique node IDs that have data for this table
    pub fn node_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<_> = self.regions.iter().map(|r| r.node_id).collect();
        ids.sort();
        ids.dedup();
        ids
    }
}
