//! MetaServer module for cluster metadata management.
//!
//! The MetaServer is responsible for:
//! - Managing cluster topology and node registry
//! - Storing table region distribution information
//! - Handling node registration and heartbeats
//! - Coordinating region assignments

mod client;
mod region_manager;
mod server;
mod service;

pub use client::MetaClient;
pub use region_manager::RegionManager;
pub use server::MetaServer;
pub use service::MetaService;

use crate::common::{NodeId, NodeInfo, RegionId, RegionInfo, TableLocation};
use crate::error::Result;
use crate::proto::{
    CreateTableRequest, CreateTableResponse, GetClusterInfoRequest, GetClusterInfoResponse,
    GetTableRegionsRequest, GetTableRegionsResponse, HeartbeatRequest, HeartbeatResponse,
    RegisterDatanodeRequest, RegisterDatanodeResponse,
};
use async_trait::async_trait;

/// Trait for MetaServer operations.
///
/// This trait defines the interface for interacting with the MetaServer,
/// whether it's a local implementation or a remote client.
#[async_trait]
pub trait MetaServiceApi: Send + Sync + std::fmt::Debug {
    /// Get regions for a specific table.
    async fn get_table_regions(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Vec<RegionInfo>>;

    /// Get all regions for a database.
    async fn get_database_regions(&self, database: &str) -> Result<Vec<RegionInfo>>;

    /// Get information about a specific node.
    async fn get_node(&self, node_id: NodeId) -> Result<NodeInfo>;

    /// Get all nodes in the cluster.
    async fn get_all_nodes(&self) -> Result<Vec<NodeInfo>>;

    /// Get all online datanodes.
    async fn get_online_datanodes(&self) -> Result<Vec<NodeInfo>>;

    /// Register a new datanode.
    async fn register_datanode(&self, node_info: NodeInfo) -> Result<RegisterDatanodeResponse>;

    /// Handle a heartbeat from a node.
    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse>;

    /// Get cluster information.
    async fn get_cluster_info(&self) -> Result<GetClusterInfoResponse>;

    /// Create a new table with the specified number of regions.
    async fn create_table(&self, request: CreateTableRequest) -> Result<CreateTableResponse>;

    /// Get table location (for query routing).
    async fn get_table_location(&self, database: &str, table: &str) -> Result<TableLocation>;

    /// Update node status.
    async fn update_node_status(
        &self,
        node_id: NodeId,
        status: crate::common::NodeStatus,
    ) -> Result<()>;

    /// Assign a region to a node.
    async fn assign_region(&self, region_id: RegionId, node_id: NodeId) -> Result<()>;

    /// Get the region info for a specific region ID.
    async fn get_region(&self, region_id: RegionId) -> Result<RegionInfo>;
}
