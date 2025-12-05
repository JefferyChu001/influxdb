//! MetaServer gRPC service implementation.

use crate::common::{NodeId, NodeInfo, NodeStatus, RegionId, RegionInfo};
use crate::error::{DistributedError, Result};
use crate::meta::MetaServiceApi;
use crate::meta::server::MetaServer;
use crate::proto::{
    CreateTableRequest, CreateTableResponse, GetClusterInfoResponse, HeartbeatRequest,
    HeartbeatResponse, RegisterDatanodeRequest, RegisterDatanodeResponse,
};
use async_trait::async_trait;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// gRPC service wrapper for MetaServer.
///
/// This wraps the MetaServer to provide a gRPC interface for remote access.
#[derive(Debug, Clone)]
pub struct MetaService {
    inner: Arc<MetaServer>,
}

impl MetaService {
    /// Create a new MetaService wrapping the given MetaServer.
    pub fn new(server: Arc<MetaServer>) -> Self {
        Self { inner: server }
    }

    /// Get a reference to the inner MetaServer.
    pub fn inner(&self) -> &MetaServer {
        &self.inner
    }
}

// Note: The From<DistributedError> for Status impl is defined in error.rs

// Note: In a full implementation, we would generate the gRPC service traits from proto
// and implement them here. For now, we provide the core implementation that can be
// exposed via gRPC once proto generation is set up.

impl MetaService {
    /// Handle get_table_regions RPC.
    pub async fn handle_get_table_regions(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Vec<RegionInfo>> {
        self.inner.get_table_regions(database, table).await
    }

    /// Handle register_datanode RPC.
    pub async fn handle_register_datanode(
        &self,
        request: RegisterDatanodeRequest,
    ) -> Result<RegisterDatanodeResponse> {
        self.inner.register_datanode(request.node_info).await
    }

    /// Handle heartbeat RPC.
    pub async fn handle_heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse> {
        self.inner.heartbeat(request).await
    }

    /// Handle get_cluster_info RPC.
    pub async fn handle_get_cluster_info(&self) -> Result<GetClusterInfoResponse> {
        self.inner.get_cluster_info().await
    }

    /// Handle create_table RPC.
    pub async fn handle_create_table(
        &self,
        request: CreateTableRequest,
    ) -> Result<CreateTableResponse> {
        self.inner.create_table(request).await
    }

    /// Handle get_node RPC.
    pub async fn handle_get_node(&self, node_id: NodeId) -> Result<NodeInfo> {
        self.inner.get_node(node_id).await
    }

    /// Handle get_all_nodes RPC.
    pub async fn handle_get_all_nodes(&self) -> Result<Vec<NodeInfo>> {
        self.inner.get_all_nodes().await
    }

    /// Handle update_node_status RPC.
    pub async fn handle_update_node_status(
        &self,
        node_id: NodeId,
        status: NodeStatus,
    ) -> Result<()> {
        self.inner.update_node_status(node_id, status).await
    }

    /// Handle assign_region RPC.
    pub async fn handle_assign_region(&self, region_id: RegionId, node_id: NodeId) -> Result<()> {
        self.inner.assign_region(region_id, node_id).await
    }

    /// Handle get_region RPC.
    pub async fn handle_get_region(&self, region_id: RegionId) -> Result<RegionInfo> {
        self.inner.get_region(region_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NodeRole;
    use std::net::SocketAddr;

    fn create_test_service() -> MetaService {
        let server = Arc::new(MetaServer::with_defaults());
        MetaService::new(server)
    }

    fn create_test_node_info(id: u64) -> NodeInfo {
        NodeInfo::new(
            NodeId::new(id),
            format!("127.0.0.1:{}", 8180 + id).parse().unwrap(),
            NodeRole::Datanode,
        )
    }

    #[tokio::test]
    async fn test_service_register_and_query() {
        let service = create_test_service();

        // Register a datanode
        let request = RegisterDatanodeRequest {
            node_info: create_test_node_info(1),
        };
        let response = service.handle_register_datanode(request).await.unwrap();
        assert!(response.success);

        // Create a table
        let request = CreateTableRequest {
            database: "testdb".to_string(),
            table: "cpu".to_string(),
            num_regions: 2,
            partition_keys: vec![],
        };
        let response = service.handle_create_table(request).await.unwrap();
        assert!(response.success);

        // Get table regions
        let regions = service
            .handle_get_table_regions("testdb", "cpu")
            .await
            .unwrap();
        assert_eq!(regions.len(), 2);
    }

    #[tokio::test]
    async fn test_service_cluster_info() {
        let service = create_test_service();

        // Register some nodes
        for id in 1..=3 {
            let request = RegisterDatanodeRequest {
                node_info: create_test_node_info(id),
            };
            service.handle_register_datanode(request).await.unwrap();
        }

        // Get cluster info
        let info = service.handle_get_cluster_info().await.unwrap();
        assert_eq!(info.nodes.len(), 3);
    }
}
