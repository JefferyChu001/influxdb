//! Protocol buffer definitions and generated code for distributed services.
//!
//! This module contains the gRPC service definitions and message types
//! for communication between distributed components.

// Include the generated protobuf code if it exists
// The proto file is compiled by build.rs
#[cfg(feature = "proto-gen")]
include!("distributed.rs");

// For now, define the types manually until we set up proto generation
// This allows the crate to compile without proto generation

use crate::common::{NodeId, NodeInfo, NodeRole, NodeStatus, RegionId, RegionInfo, RegionStatus};
use crate::error::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Request to get table regions from MetaServer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTableRegionsRequest {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
}

/// Response containing table region information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTableRegionsResponse {
    /// Regions for the requested table
    pub regions: Vec<RegionInfo>,
}

/// Request to register a datanode with MetaServer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterDatanodeRequest {
    /// Node information
    pub node_info: NodeInfo,
}

/// Response for datanode registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterDatanodeResponse {
    /// Whether registration was successful
    pub success: bool,
    /// Assigned node ID (may differ from requested if auto-assigned)
    pub node_id: NodeId,
    /// Any regions assigned to this node
    pub assigned_regions: Vec<RegionInfo>,
    /// Error message if registration failed
    pub error_message: Option<String>,
}

/// Request for node heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// The node sending the heartbeat
    pub node_id: NodeId,
    /// Current node status
    pub status: NodeStatus,
    /// Regions currently managed
    pub regions: Vec<RegionId>,
    /// Resource usage metrics
    pub metrics: Option<NodeMetrics>,
}

/// Node resource usage metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// CPU usage percentage (0-100)
    pub cpu_usage: f32,
    /// Memory usage percentage (0-100)
    pub memory_usage: f32,
    /// Disk usage percentage (0-100)
    pub disk_usage: f32,
    /// Number of active connections
    pub active_connections: u32,
    /// Queries per second
    pub queries_per_second: f64,
}

/// Response for heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Whether the heartbeat was accepted
    pub success: bool,
    /// Regions to add to this node
    pub regions_to_add: Vec<RegionInfo>,
    /// Regions to remove from this node
    pub regions_to_remove: Vec<RegionId>,
    /// Optional message
    pub message: Option<String>,
}

/// Request to get cluster information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterInfoRequest {
    /// Whether to include detailed node information
    pub include_nodes: bool,
    /// Whether to include region information
    pub include_regions: bool,
}

/// Response containing cluster information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterInfoResponse {
    /// Cluster name
    pub cluster_name: String,
    /// All nodes in the cluster
    pub nodes: Vec<NodeInfo>,
    /// Total number of regions
    pub total_regions: u64,
    /// Cluster health status
    pub health: ClusterHealth,
}

/// Cluster health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterHealth {
    /// All nodes healthy
    Healthy,
    /// Some nodes degraded
    Degraded,
    /// Critical issues
    Critical,
    /// Unknown status
    Unknown,
}

/// Request to execute a query plan on a Datanode.
#[derive(Debug, Clone)]
pub struct ExecutePlanRequest {
    /// Serialized physical plan (using DataFusion's serialization)
    pub plan_bytes: Bytes,
    /// Database context
    pub database: String,
    /// Region ID to query
    pub region_id: RegionId,
    /// Query ID for tracing
    pub query_id: String,
}

/// Request to write data to a Datanode.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    /// Database name
    pub database: String,
    /// Region ID to write to
    pub region_id: RegionId,
    /// Serialized write batch
    pub data: Bytes,
    /// Write precision
    pub precision: WritePrecision,
}

/// Write timestamp precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WritePrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl From<influxdb3_types::write::Precision> for WritePrecision {
    fn from(p: influxdb3_types::write::Precision) -> Self {
        match p {
            influxdb3_types::write::Precision::Auto => WritePrecision::Nanoseconds,
            influxdb3_types::write::Precision::Nanosecond => WritePrecision::Nanoseconds,
            influxdb3_types::write::Precision::Microsecond => WritePrecision::Microseconds,
            influxdb3_types::write::Precision::Millisecond => WritePrecision::Milliseconds,
            influxdb3_types::write::Precision::Second => WritePrecision::Seconds,
        }
    }
}

/// Response for write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResponse {
    /// Whether the write was successful
    pub success: bool,
    /// Number of points written
    pub points_written: u64,
    /// Error message if write failed
    pub error_message: Option<String>,
}

/// Request to get region status from a Datanode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRegionStatusRequest {
    /// Region ID to query
    pub region_id: RegionId,
}

/// Response containing region status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRegionStatusResponse {
    /// Region ID
    pub region_id: RegionId,
    /// Current status
    pub status: RegionStatus,
    /// Number of rows in the region
    pub row_count: u64,
    /// Size in bytes
    pub size_bytes: u64,
    /// Last write timestamp
    pub last_write_time: Option<i64>,
}

/// Request to create a new table (to MetaServer).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableRequest {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Number of regions to create
    pub num_regions: u32,
    /// Partition key columns
    pub partition_keys: Vec<String>,
}

/// Response for table creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableResponse {
    /// Whether creation was successful
    pub success: bool,
    /// Created regions
    pub regions: Vec<RegionInfo>,
    /// Error message if creation failed
    pub error_message: Option<String>,
}

/// Arrow batch for streaming query results.
#[derive(Debug, Clone)]
pub struct ArrowBatch {
    /// Serialized Arrow IPC batch
    pub data: Bytes,
    /// Number of rows in this batch
    pub num_rows: u64,
}

impl ArrowBatch {
    /// Create a new ArrowBatch.
    pub fn new(data: Bytes, num_rows: u64) -> Self {
        Self { data, num_rows }
    }

    /// Check if this batch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }
}
