//! gRPC server for inter-node communication

use crate::error::{Error, Result};
use crate::node_registry::NodeRegistry;
use crate::proto::cluster_service_server::{ClusterService, ClusterServiceServer};
use crate::proto::{HeartbeatRequest, HeartbeatResponse, RegisterNodeRequest, RegisterNodeResponse, WriteRequest, WriteResponse, QueryRequest, QueryResponse, RaftMessageRequest, RaftMessageResponse, BroadcastDataRequest, BroadcastDataResponse, LocalJoinRequest, LocalJoinResponse, PartitionJoinRequest, PartitionJoinResponse};
use crate::types::{NodeCapacity, NodeId, NodeInfo, NodeRole, NodeStatus};
use influxdb3_write::{Precision, WriteBuffer};
use iox_time::Time;
use observability_deps::tracing::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct ClusterServiceImpl {
    write_buffer: Arc<dyn WriteBuffer>,
    node_registry: Arc<NodeRegistry>,
}

impl ClusterServiceImpl {
    pub fn new(write_buffer: Arc<dyn WriteBuffer>, node_registry: Arc<NodeRegistry>) -> Self {
        Self { write_buffer, node_registry }
    }
}

#[tonic::async_trait]
impl ClusterService for ClusterServiceImpl {
    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;
    type ExecuteLocalJoinStream = ReceiverStream<Result<LocalJoinResponse, Status>>;
    type ExecutePartitionJoinStream = ReceiverStream<Result<PartitionJoinResponse, Status>>;

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        let role = match req.role { 0 => NodeRole::Coordinator, 1 => NodeRole::DataNode, _ => NodeRole::Mixed };
        let node = NodeInfo {
            node_id: NodeId::new(req.node_id.parse::<u64>().unwrap_or(0)),
            address: req.address,
            grpc_port: req.grpc_port as u16,
            http_port: req.http_port as u16,
            role,
            status: NodeStatus::Active,
            capacity: NodeCapacity { cpu_cores: 0, memory_bytes: 0, disk_bytes: 0, current_shards: 0, max_shards: 0 },
            last_heartbeat_nanos: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64,
        };
        self.node_registry.register_node(node).await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RegisterNodeResponse { success: true, message: "ok".into() }))
    }

    async fn heartbeat(&self, request: Request<HeartbeatRequest>) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId::new(req.node_id.parse::<u64>().unwrap_or(0));
        self.node_registry.heartbeat(node_id).await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(HeartbeatResponse { success: true, timestamp_nanos: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64 }))
    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let db = data_types::NamespaceName::new(req.database).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let lp = String::from_utf8(req.data).map_err(|e| Status::invalid_argument(e.to_string()))?;
        // Use current time as ingest_time; precision is ns by default
        let ingest_time = Time::from_timestamp_nanos(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64);
        self.write_buffer
            .write_lp(db, &lp, ingest_time, true, Precision::Nanosecond, false)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(WriteResponse { success: true, error: String::new(), sequence_number: 0 }))
    }

    async fn query(&self, _request: Request<QueryRequest>) -> Result<Response<Self::QueryStream>, Status> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn raft_message(&self, _request: Request<RaftMessageRequest>) -> Result<Response<RaftMessageResponse>, Status> {
        Ok(Response::new(RaftMessageResponse { success: false }))
    }

    async fn broadcast_data(&self, _request: Request<BroadcastDataRequest>) -> Result<Response<BroadcastDataResponse>, Status> {
        Ok(Response::new(BroadcastDataResponse { success: false, error: String::new() }))
    }

    async fn execute_local_join(&self, _request: Request<LocalJoinRequest>) -> Result<Response<Self::ExecuteLocalJoinStream>, Status> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn execute_partition_join(&self, _request: Request<PartitionJoinRequest>) -> Result<Response<Self::ExecutePartitionJoinStream>, Status> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Start the cluster gRPC server on bind_addr
pub async fn start_cluster_grpc(
    bind_addr: SocketAddr,
    write_buffer: Arc<dyn WriteBuffer>,
    node_registry: Arc<NodeRegistry>,
    shutdown: influxdb3_shutdown::ShutdownToken,
) -> Result<()> {
    let svc = ClusterServiceServer::new(ClusterServiceImpl::new(write_buffer, node_registry));
    println!("Cluster gRPC listening on {}", bind_addr);
    tokio::select! {
        _ = shutdown.wait_for_shutdown() => {
            info!("cluster gRPC service shutting down");
            Ok(())
        },
        res = Server::builder().add_service(svc).serve(bind_addr) => {
            res.map_err(|e| Error::InternalError{ message: e.to_string() })
        }
    }
}

