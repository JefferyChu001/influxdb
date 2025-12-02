//! InfluxDB 3 Cluster Module
//!
//! This module provides distributed clustering capabilities for InfluxDB 3,
//! including:
//! - Node registration and discovery
//! - Shard management and routing
//! - Raft-based replication
//! - Distributed query execution
//! - Multi-table JOIN support

pub mod types;
pub mod error;
pub mod node_registry;
pub mod shard_manager;
pub mod meta_store;
pub mod consensus;
pub mod replication;
pub mod query;
pub mod rpc;
pub mod clustered_write_buffer;

// Re-export commonly used types
pub use types::{
    NodeId, NodeInfo, NodeRole, NodeStatus, NodeCapacity,
    ShardId, ShardInfo, ShardRange, ShardStatus,
    ReplicaInfo, ReplicaRole, ReplicaStatus,
    TableId,
};

pub use error::{Error, Result};

// Include generated protobuf code
pub mod proto {
    tonic::include_proto!("influxdb3.cluster");
}
