//! gRPC server implementation for MetaServer.
//!
//! This module provides the gRPC service implementation for the MetaServer,
//! handling node registration, heartbeats, and region management.

use crate::common::{NodeId, NodeInfo, NodeRole, NodeStatus, RegionId, RegionInfo};
use crate::error::{DistributedError, Result};
use crate::meta::MetaServiceApi;
use crate::meta::server::MetaServer;
use crate::proto::{
    CreateTableRequest, CreateTableResponse, GetClusterInfoResponse, HeartbeatRequest,
    HeartbeatResponse, NodeMetrics, RegisterDatanodeRequest, RegisterDatanodeResponse,
};
use observability_deps::tracing::{debug, error, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, transport::Server};

/// gRPC server for MetaServer.
pub struct MetaGrpcServer {
    /// Inner MetaServer
    inner: Arc<MetaServer>,
    /// Server address
    addr: SocketAddr,
}

impl MetaGrpcServer {
    /// Create a new MetaGrpcServer.
    pub fn new(inner: Arc<MetaServer>, addr: SocketAddr) -> Self {
        Self { inner, addr }
    }

    /// Start the gRPC server.
    ///
    /// This runs until the shutdown signal is received.
    pub async fn run(self, shutdown: oneshot::Receiver<()>) -> Result<()> {
        info!(addr = %self.addr, "Starting MetaServer gRPC server");

        let service = MetaGrpcService::new(Arc::clone(&self.inner));

        // Build the service - for now without proto generation, we provide a basic implementation
        // In a full implementation, we'd use the generated proto service
        Server::builder()
            .add_service(service.into_service())
            .serve_with_shutdown(self.addr, async {
                let _ = shutdown.await;
                info!("MetaServer gRPC server shutting down");
            })
            .await
            .map_err(|e| DistributedError::NetworkError(e.to_string()))?;

        Ok(())
    }
}

/// gRPC service implementation for MetaServer.
pub struct MetaGrpcService {
    inner: Arc<MetaServer>,
}

impl MetaGrpcService {
    /// Create a new MetaGrpcService.
    pub fn new(inner: Arc<MetaServer>) -> Self {
        Self { inner }
    }

    /// Convert to a tonic service (placeholder until proto generation).
    pub fn into_service(self) -> tonic::transport::server::Router {
        // This is a placeholder - in a real implementation, we'd return the generated service
        // For now, we create an empty router
        Server::builder()
    }

    /// Handle register node request.
    pub async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> std::result::Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        debug!(node_id = ?req.node_info.node_id, "Registering node");

        let node_info = self.convert_node_info(&req.node_info)?;

        match self.inner.register_datanode(node_info).await {
            Ok(response) => {
                let proto_response = RegisterNodeResponse {
                    success: response.success,
                    assigned_node_id: response.node_id.get(),
                    assigned_regions: response
                        .assigned_regions
                        .into_iter()
                        .map(|r| self.convert_region_info_to_proto(&r))
                        .collect(),
                    error_message: response.error_message,
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(self.to_status(e)),
        }
    }

    /// Handle heartbeat request.
    pub async fn heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> std::result::Result<Response<HeartbeatResponseProto>, Status> {
        let req = request.into_inner();
        debug!(node_id = req.node_id, "Processing heartbeat");

        let heartbeat_req = HeartbeatRequest {
            node_id: NodeId::new(req.node_id),
            status: self.convert_node_status(req.status),
            regions: req.regions.into_iter().map(RegionId::new).collect(),
            metrics: req.metrics.map(|m| NodeMetrics {
                cpu_usage: m.cpu_usage,
                memory_usage: m.memory_usage,
                disk_usage: m.disk_usage,
                active_connections: m.active_connections,
                queries_per_second: m.queries_per_second,
            }),
        };

        match self.inner.heartbeat(heartbeat_req).await {
            Ok(response) => {
                let proto_response = HeartbeatResponseProto {
                    success: response.success,
                    regions_to_add: response
                        .regions_to_add
                        .into_iter()
                        .map(|r| self.convert_region_info_to_proto(&r))
                        .collect(),
                    regions_to_remove: response
                        .regions_to_remove
                        .into_iter()
                        .map(|r| r.get())
                        .collect(),
                    message: response.message,
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(self.to_status(e)),
        }
    }

    /// Handle get table regions request.
    pub async fn get_table_regions(
        &self,
        request: Request<GetTableRegionsRequest>,
    ) -> std::result::Result<Response<GetTableRegionsResponse>, Status> {
        let req = request.into_inner();
        debug!(database = %req.database, table = %req.table, "Getting table regions");

        match self
            .inner
            .get_table_regions(&req.database, &req.table)
            .await
        {
            Ok(regions) => {
                let proto_response = GetTableRegionsResponse {
                    regions: regions
                        .into_iter()
                        .map(|r| self.convert_region_info_to_proto(&r))
                        .collect(),
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(self.to_status(e)),
        }
    }

    /// Handle create table request.
    pub async fn create_table(
        &self,
        request: Request<CreateTableRequestProto>,
    ) -> std::result::Result<Response<CreateTableResponseProto>, Status> {
        let req = request.into_inner();
        debug!(database = %req.database, table = %req.table, "Creating table");

        let create_req = CreateTableRequest {
            database: req.database,
            table: req.table,
            num_regions: req.num_regions,
            partition_keys: req.partition_keys,
        };

        match self.inner.create_table(create_req).await {
            Ok(response) => {
                let proto_response = CreateTableResponseProto {
                    success: response.success,
                    created_regions: response
                        .regions
                        .into_iter()
                        .map(|r| self.convert_region_info_to_proto(&r))
                        .collect(),
                    error_message: response.error_message,
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(self.to_status(e)),
        }
    }

    /// Handle get cluster info request.
    pub async fn get_cluster_info(
        &self,
        _request: Request<GetClusterInfoRequest>,
    ) -> std::result::Result<Response<GetClusterInfoResponseProto>, Status> {
        debug!("Getting cluster info");

        match self.inner.get_cluster_info().await {
            Ok(info) => {
                let proto_response = GetClusterInfoResponseProto {
                    cluster_name: info.cluster_name,
                    nodes: info
                        .nodes
                        .into_iter()
                        .map(|n| self.convert_node_info_to_proto(&n))
                        .collect(),
                    total_regions: info.total_regions,
                    health: match info.health {
                        crate::proto::ClusterHealth::Healthy => 1,
                        crate::proto::ClusterHealth::Degraded => 2,
                        crate::proto::ClusterHealth::Critical => 3,
                        crate::proto::ClusterHealth::Unknown => 0,
                    },
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(self.to_status(e)),
        }
    }

    /// Handle get node request.
    pub async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> std::result::Result<Response<GetNodeResponse>, Status> {
        let req = request.into_inner();
        debug!(node_id = req.node_id, "Getting node info");

        match self.inner.get_node(NodeId::new(req.node_id)).await {
            Ok(node_info) => {
                let proto_response = GetNodeResponse {
                    node_info: Some(self.convert_node_info_to_proto(&node_info)),
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(self.to_status(e)),
        }
    }

    /// Handle get all nodes request.
    pub async fn get_all_nodes(
        &self,
        _request: Request<GetAllNodesRequest>,
    ) -> std::result::Result<Response<GetAllNodesResponse>, Status> {
        debug!("Getting all nodes");

        match self.inner.get_all_nodes().await {
            Ok(nodes) => {
                let proto_response = GetAllNodesResponse {
                    nodes: nodes
                        .into_iter()
                        .map(|n| self.convert_node_info_to_proto(&n))
                        .collect(),
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(self.to_status(e)),
        }
    }

    // Conversion helpers

    fn convert_node_info(&self, proto: &NodeInfoProto) -> std::result::Result<NodeInfo, Status> {
        Ok(NodeInfo {
            node_id: NodeId::new(proto.node_id),
            grpc_addr: proto
                .grpc_addr
                .parse()
                .map_err(|e: std::net::AddrParseError| Status::invalid_argument(e.to_string()))?,
            http_addr: proto
                .http_addr
                .as_ref()
                .map(|s| s.parse())
                .transpose()
                .map_err(|e: std::net::AddrParseError| Status::invalid_argument(e.to_string()))?,
            node_mode: self.convert_node_role(proto.node_role),
            status: self.convert_node_status(proto.status),
            regions: proto.regions.iter().map(|r| RegionId::new(*r)).collect(),
            last_heartbeat_ms: proto.last_heartbeat_ms,
        })
    }

    fn convert_node_info_to_proto(&self, info: &NodeInfo) -> NodeInfoProto {
        NodeInfoProto {
            node_id: info.node_id.get(),
            grpc_addr: info.grpc_addr.to_string(),
            http_addr: info.http_addr.map(|a| a.to_string()),
            node_role: match info.node_mode {
                NodeRole::Frontend => 1,
                NodeRole::Datanode => 2,
                NodeRole::MetaServer => 3,
            },
            status: match info.status {
                NodeStatus::Starting => 1,
                NodeStatus::Online => 2,
                NodeStatus::Offline => 3,
                NodeStatus::Maintenance => 4,
                NodeStatus::Draining => 5,
            },
            regions: info.regions.iter().map(|r| r.get()).collect(),
            last_heartbeat_ms: info.last_heartbeat_ms,
        }
    }

    fn convert_region_info_to_proto(&self, info: &RegionInfo) -> RegionInfoProto {
        RegionInfoProto {
            region_id: info.region_id.get(),
            database: info.database.clone(),
            table: info.table.clone(),
            node_id: info.node_id.get(),
            partition_range: Some(PartitionRangeProto {
                time_start_ns: info.partition_range.time_start_ns,
                time_end_ns: info.partition_range.time_end_ns,
                hash_start: info.partition_range.hash_start as u32,
                hash_end: info.partition_range.hash_end as u32,
            }),
            status: match info.status {
                crate::common::RegionStatus::Active => 1,
                crate::common::RegionStatus::ReadOnly => 2,
                crate::common::RegionStatus::Migrating => 3,
                crate::common::RegionStatus::Offline => 4,
                crate::common::RegionStatus::Creating => 5,
                crate::common::RegionStatus::Deleting => 6,
            },
            epoch: info.epoch,
        }
    }

    fn convert_node_role(&self, role: i32) -> NodeRole {
        match role {
            1 => NodeRole::Frontend,
            2 => NodeRole::Datanode,
            3 => NodeRole::MetaServer,
            _ => NodeRole::Datanode,
        }
    }

    fn convert_node_status(&self, status: i32) -> NodeStatus {
        match status {
            1 => NodeStatus::Starting,
            2 => NodeStatus::Online,
            3 => NodeStatus::Offline,
            4 => NodeStatus::Maintenance,
            5 => NodeStatus::Draining,
            _ => NodeStatus::Offline,
        }
    }

    fn to_status(&self, err: DistributedError) -> Status {
        match &err {
            DistributedError::NodeNotFound { .. }
            | DistributedError::RegionNotFound { .. }
            | DistributedError::DatabaseNotFound { .. }
            | DistributedError::TableNotFound { .. } => Status::not_found(err.to_string()),
            DistributedError::InvalidRequest(_) => Status::invalid_argument(err.to_string()),
            DistributedError::ClusterNotInitialized | DistributedError::NodeNotReady { .. } => {
                Status::unavailable(err.to_string())
            }
            DistributedError::Timeout(_) => Status::deadline_exceeded(err.to_string()),
            DistributedError::NodeAlreadyRegistered { .. }
            | DistributedError::RegionAlreadyAssigned { .. } => {
                Status::already_exists(err.to_string())
            }
            _ => Status::internal(err.to_string()),
        }
    }
}

// Placeholder proto message types until we generate them from the .proto file

#[derive(Debug, Clone)]
pub struct RegisterNodeRequest {
    pub node_info: NodeInfoProto,
}

#[derive(Debug, Clone)]
pub struct RegisterNodeResponse {
    pub success: bool,
    pub assigned_node_id: u64,
    pub assigned_regions: Vec<RegionInfoProto>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HeartbeatRequestProto {
    pub node_id: u64,
    pub status: i32,
    pub regions: Vec<u64>,
    pub metrics: Option<NodeMetricsProto>,
}

#[derive(Debug, Clone)]
pub struct HeartbeatResponseProto {
    pub success: bool,
    pub regions_to_add: Vec<RegionInfoProto>,
    pub regions_to_remove: Vec<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetTableRegionsRequest {
    pub database: String,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct GetTableRegionsResponse {
    pub regions: Vec<RegionInfoProto>,
}

#[derive(Debug, Clone)]
pub struct CreateTableRequestProto {
    pub database: String,
    pub table: String,
    pub num_regions: u32,
    pub partition_keys: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CreateTableResponseProto {
    pub success: bool,
    pub created_regions: Vec<RegionInfoProto>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetClusterInfoRequest {
    pub include_nodes: bool,
    pub include_regions: bool,
}

#[derive(Debug, Clone)]
pub struct GetClusterInfoResponseProto {
    pub cluster_name: String,
    pub nodes: Vec<NodeInfoProto>,
    pub total_regions: u64,
    pub health: i32,
}

#[derive(Debug, Clone)]
pub struct GetNodeRequest {
    pub node_id: u64,
}

#[derive(Debug, Clone)]
pub struct GetNodeResponse {
    pub node_info: Option<NodeInfoProto>,
}

#[derive(Debug, Clone)]
pub struct GetAllNodesRequest {
    pub filter_role: Option<i32>,
    pub filter_status: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct GetAllNodesResponse {
    pub nodes: Vec<NodeInfoProto>,
}

#[derive(Debug, Clone)]
pub struct NodeInfoProto {
    pub node_id: u64,
    pub grpc_addr: String,
    pub http_addr: Option<String>,
    pub node_role: i32,
    pub status: i32,
    pub regions: Vec<u64>,
    pub last_heartbeat_ms: i64,
}

#[derive(Debug, Clone)]
pub struct RegionInfoProto {
    pub region_id: u64,
    pub database: String,
    pub table: String,
    pub node_id: u64,
    pub partition_range: Option<PartitionRangeProto>,
    pub status: i32,
    pub epoch: u64,
}

#[derive(Debug, Clone)]
pub struct PartitionRangeProto {
    pub time_start_ns: Option<i64>,
    pub time_end_ns: Option<i64>,
    pub hash_start: u32,
    pub hash_end: u32,
}

#[derive(Debug, Clone)]
pub struct NodeMetricsProto {
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub disk_usage: f32,
    pub active_connections: u32,
    pub queries_per_second: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_conversion() {
        let service = MetaGrpcService::new(Arc::new(MetaServer::with_defaults()));

        assert_eq!(service.convert_node_status(1), NodeStatus::Starting);
        assert_eq!(service.convert_node_status(2), NodeStatus::Online);
        assert_eq!(service.convert_node_status(999), NodeStatus::Offline);
    }

    #[test]
    fn test_role_conversion() {
        let service = MetaGrpcService::new(Arc::new(MetaServer::with_defaults()));

        assert_eq!(service.convert_node_role(1), NodeRole::Frontend);
        assert_eq!(service.convert_node_role(2), NodeRole::Datanode);
        assert_eq!(service.convert_node_role(3), NodeRole::MetaServer);
    }
}
