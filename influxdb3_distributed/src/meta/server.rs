//! MetaServer implementation.

use crate::common::{
    HeartbeatRequest as CommonHeartbeatRequest, HeartbeatResponse as CommonHeartbeatResponse,
    NodeId, NodeInfo, NodeRole, NodeStatus, RegionId, RegionInfo, TableLocation,
};
use crate::config::MetaServerConfig;
use crate::error::{DistributedError, Result};
use crate::meta::region_manager::{RegionManager, RegionManagerConfig};
use crate::meta::MetaServiceApi;
use crate::proto::{
    ClusterHealth, CreateTableRequest, CreateTableResponse, GetClusterInfoResponse,
    HeartbeatRequest, HeartbeatResponse, RegisterDatanodeRequest, RegisterDatanodeResponse,
};
use async_trait::async_trait;
use dashmap::DashMap;
use observability_deps::tracing::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// MetaServer manages cluster metadata.
///
/// This is an in-memory implementation suitable for development and testing.
/// For production use with HA, a Raft-based implementation would be needed.
#[derive(Debug)]
pub struct MetaServer {
    /// Server configuration
    config: MetaServerConfig,

    /// Registered nodes
    nodes: DashMap<NodeId, NodeInfo>,

    /// Region manager
    region_manager: Arc<RegionManager>,

    /// Next node ID to assign
    next_node_id: AtomicU64,

    /// Cluster name
    cluster_name: String,

    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,

    /// Server start time
    start_time: Instant,
}

impl MetaServer {
    /// Create a new MetaServer.
    pub fn new(config: MetaServerConfig) -> Self {
        let cluster_name = config.cluster_name.clone();
        let region_config = RegionManagerConfig {
            default_num_regions: config.default_num_regions,
            max_regions_per_node: 100,
        };

        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            nodes: DashMap::new(),
            region_manager: Arc::new(RegionManager::new(region_config)),
            next_node_id: AtomicU64::new(1),
            cluster_name,
            shutdown_tx,
            start_time: Instant::now(),
        }
    }

    /// Create a MetaServer with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(MetaServerConfig::default())
    }

    /// Generate the next node ID.
    fn next_node_id(&self) -> NodeId {
        NodeId::new(self.next_node_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Get the current timestamp in milliseconds.
    fn current_time_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Get all online datanode IDs.
    fn get_online_datanode_ids(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|entry| {
                entry.value().node_mode == NodeRole::Datanode
                    && entry.value().status == NodeStatus::Online
            })
            .map(|entry| *entry.key())
            .collect()
    }

    /// Check cluster health based on node status.
    fn check_cluster_health(&self) -> ClusterHealth {
        let total_datanodes = self
            .nodes
            .iter()
            .filter(|e| e.value().node_mode == NodeRole::Datanode)
            .count();

        if total_datanodes == 0 {
            return ClusterHealth::Critical;
        }

        let online_datanodes = self
            .nodes
            .iter()
            .filter(|e| {
                e.value().node_mode == NodeRole::Datanode
                    && e.value().status == NodeStatus::Online
            })
            .count();

        if online_datanodes == total_datanodes {
            ClusterHealth::Healthy
        } else if online_datanodes > 0 {
            ClusterHealth::Degraded
        } else {
            ClusterHealth::Critical
        }
    }

    /// Get the region manager.
    pub fn region_manager(&self) -> Arc<RegionManager> {
        Arc::clone(&self.region_manager)
    }

    /// Subscribe to shutdown signal.
    pub fn subscribe_shutdown(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Trigger shutdown.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Get server uptime.
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

#[async_trait]
impl MetaServiceApi for MetaServer {
    async fn get_table_regions(&self, database: &str, table: &str) -> Result<Vec<RegionInfo>> {
        self.region_manager.get_table_regions(database, table)
    }

    async fn get_database_regions(&self, database: &str) -> Result<Vec<RegionInfo>> {
        Ok(self.region_manager.get_database_regions(database))
    }

    async fn get_node(&self, node_id: NodeId) -> Result<NodeInfo> {
        self.nodes
            .get(&node_id)
            .map(|n| n.clone())
            .ok_or_else(|| DistributedError::NodeNotFound {
                node_id: node_id.get(),
            })
    }

    async fn get_all_nodes(&self) -> Result<Vec<NodeInfo>> {
        Ok(self.nodes.iter().map(|e| e.value().clone()).collect())
    }

    async fn get_online_datanodes(&self) -> Result<Vec<NodeInfo>> {
        Ok(self
            .nodes
            .iter()
            .filter(|e| {
                e.value().node_mode == NodeRole::Datanode
                    && e.value().status == NodeStatus::Online
            })
            .map(|e| e.value().clone())
            .collect())
    }

    async fn register_datanode(&self, mut node_info: NodeInfo) -> Result<RegisterDatanodeResponse> {
        // Assign a new node ID if not specified or 0
        if node_info.node_id.get() == 0 {
            node_info.node_id = self.next_node_id();
        }

        let node_id = node_info.node_id;

        // Check if already registered
        if self.nodes.contains_key(&node_id) {
            return Err(DistributedError::NodeAlreadyRegistered {
                node_id: node_id.get(),
            });
        }

        // Set initial status and heartbeat
        node_info.status = NodeStatus::Online;
        node_info.last_heartbeat_ms = Self::current_time_ms();

        info!(
            node_id = %node_id,
            addr = %node_info.grpc_addr,
            "Registering new datanode"
        );

        self.nodes.insert(node_id, node_info);

        // For now, no regions are assigned on registration
        // Regions are assigned when tables are created
        Ok(RegisterDatanodeResponse {
            success: true,
            node_id,
            assigned_regions: Vec::new(),
            error_message: None,
        })
    }

    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse> {
        let node_id = request.node_id;

        let mut node = self.nodes.get_mut(&node_id).ok_or_else(|| {
            DistributedError::NodeNotFound {
                node_id: node_id.get(),
            }
        })?;

        // Update heartbeat timestamp and status
        node.last_heartbeat_ms = Self::current_time_ms();
        node.status = request.status;
        node.regions = request.regions.clone();

        debug!(node_id = %node_id, "Received heartbeat");

        Ok(HeartbeatResponse {
            success: true,
            regions_to_add: Vec::new(),
            regions_to_remove: Vec::new(),
            message: None,
        })
    }

    async fn get_cluster_info(&self) -> Result<GetClusterInfoResponse> {
        let nodes: Vec<NodeInfo> = self.nodes.iter().map(|e| e.value().clone()).collect();
        let total_regions = self.region_manager.total_regions() as u64;
        let health = self.check_cluster_health();

        Ok(GetClusterInfoResponse {
            cluster_name: self.cluster_name.clone(),
            nodes,
            total_regions,
            health,
        })
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<CreateTableResponse> {
        let available_nodes = self.get_online_datanode_ids();

        if available_nodes.is_empty() {
            return Ok(CreateTableResponse {
                success: false,
                regions: Vec::new(),
                error_message: Some("No online datanodes available".to_string()),
            });
        }

        let num_regions = if request.num_regions == 0 {
            self.config.default_num_regions as u32
        } else {
            request.num_regions
        };

        match self.region_manager.create_table_regions(
            &request.database,
            &request.table,
            num_regions as usize,
            &available_nodes,
        ) {
            Ok(regions) => {
                info!(
                    database = %request.database,
                    table = %request.table,
                    num_regions = regions.len(),
                    "Created table regions"
                );

                Ok(CreateTableResponse {
                    success: true,
                    regions,
                    error_message: None,
                })
            }
            Err(e) => Ok(CreateTableResponse {
                success: false,
                regions: Vec::new(),
                error_message: Some(e.to_string()),
            }),
        }
    }

    async fn get_table_location(&self, database: &str, table: &str) -> Result<TableLocation> {
        let regions = self.region_manager.get_table_regions(database, table)?;

        Ok(TableLocation {
            database: database.to_string(),
            table: table.to_string(),
            regions,
        })
    }

    async fn update_node_status(&self, node_id: NodeId, status: NodeStatus) -> Result<()> {
        let mut node = self.nodes.get_mut(&node_id).ok_or_else(|| {
            DistributedError::NodeNotFound {
                node_id: node_id.get(),
            }
        })?;

        node.status = status;
        info!(node_id = %node_id, status = %status, "Updated node status");

        Ok(())
    }

    async fn assign_region(&self, region_id: RegionId, node_id: NodeId) -> Result<()> {
        // Verify node exists
        if !self.nodes.contains_key(&node_id) {
            return Err(DistributedError::NodeNotFound {
                node_id: node_id.get(),
            });
        }

        self.region_manager.assign_region(region_id, node_id)?;

        info!(
            region_id = %region_id,
            node_id = %node_id,
            "Assigned region to node"
        );

        Ok(())
    }

    async fn get_region(&self, region_id: RegionId) -> Result<RegionInfo> {
        self.region_manager.get_region(region_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn create_test_node_info(id: u64) -> NodeInfo {
        NodeInfo::new(
            NodeId::new(id),
            format!("127.0.0.1:{}", 8180 + id).parse().unwrap(),
            NodeRole::Datanode,
        )
    }

    #[tokio::test]
    async fn test_register_datanode() {
        let server = MetaServer::with_defaults();

        let node_info = create_test_node_info(0); // 0 means auto-assign
        let response = server.register_datanode(node_info).await.unwrap();

        assert!(response.success);
        assert!(response.node_id.get() > 0);

        // Verify node is registered
        let node = server.get_node(response.node_id).await.unwrap();
        assert_eq!(node.status, NodeStatus::Online);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let server = MetaServer::with_defaults();

        // Register a node first
        let node_info = create_test_node_info(1);
        server.register_datanode(node_info).await.unwrap();

        // Send heartbeat
        let request = HeartbeatRequest {
            node_id: NodeId::new(1),
            status: NodeStatus::Online,
            regions: vec![],
            metrics: None,
        };

        let response = server.heartbeat(request).await.unwrap();
        assert!(response.success);
    }

    #[tokio::test]
    async fn test_create_table() {
        let server = MetaServer::with_defaults();

        // Register datanodes
        for id in 1..=3 {
            let node_info = create_test_node_info(id);
            server.register_datanode(node_info).await.unwrap();
        }

        // Create table
        let request = CreateTableRequest {
            database: "testdb".to_string(),
            table: "cpu".to_string(),
            num_regions: 6,
            partition_keys: vec!["host".to_string()],
        };

        let response = server.create_table(request).await.unwrap();
        assert!(response.success);
        assert_eq!(response.regions.len(), 6);

        // Verify regions are distributed
        let regions = server.get_table_regions("testdb", "cpu").await.unwrap();
        assert_eq!(regions.len(), 6);
    }

    #[tokio::test]
    async fn test_get_table_location() {
        let server = MetaServer::with_defaults();

        // Register a datanode
        let node_info = create_test_node_info(1);
        server.register_datanode(node_info).await.unwrap();

        // Create table
        let request = CreateTableRequest {
            database: "testdb".to_string(),
            table: "cpu".to_string(),
            num_regions: 4,
            partition_keys: vec![],
        };
        server.create_table(request).await.unwrap();

        // Get table location
        let location = server.get_table_location("testdb", "cpu").await.unwrap();
        assert_eq!(location.database, "testdb");
        assert_eq!(location.table, "cpu");
        assert_eq!(location.regions.len(), 4);
    }

    #[tokio::test]
    async fn test_cluster_health() {
        let server = MetaServer::with_defaults();

        // No nodes - critical
        let info = server.get_cluster_info().await.unwrap();
        assert_eq!(info.health, ClusterHealth::Critical);

        // Register a node
        let node_info = create_test_node_info(1);
        server.register_datanode(node_info).await.unwrap();

        // One online node - healthy
        let info = server.get_cluster_info().await.unwrap();
        assert_eq!(info.health, ClusterHealth::Healthy);

        // Mark node offline
        server
            .update_node_status(NodeId::new(1), NodeStatus::Offline)
            .await
            .unwrap();

        // All nodes offline - critical
        let info = server.get_cluster_info().await.unwrap();
        assert_eq!(info.health, ClusterHealth::Critical);
    }
}
