//! Datanode server implementation.

use crate::common::{NodeId, NodeInfo, NodeRole, NodeStatus, RegionId, RegionInfo};
use crate::config::DatanodeConfig;
use crate::datanode::executor::LocalExecutor;
use crate::datanode::DatanodeApi;
use crate::error::{DistributedError, Result};
use crate::meta::MetaServiceApi;
use crate::proto::{
    ExecutePlanRequest, GetRegionStatusResponse, HeartbeatRequest, NodeMetrics, WriteRequest,
    WriteResponse,
};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_write::WriteBuffer;
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Datanode server that stores regions and executes queries.
#[derive(Debug)]
pub struct DatanodeServer {
    /// Server configuration
    config: DatanodeConfig,

    /// Node ID
    node_id: NodeId,

    /// Local query executor
    executor: Arc<LocalExecutor>,

    /// Write buffer for handling writes
    write_buffer: Arc<dyn WriteBuffer>,

    /// Region information cache
    regions: RwLock<HashMap<RegionId, RegionInfo>>,

    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,

    /// Server start time
    start_time: Instant,
}

impl DatanodeServer {
    /// Create a new DatanodeServer.
    pub fn new(
        config: DatanodeConfig,
        query_executor: Arc<dyn QueryExecutor>,
        write_buffer: Arc<dyn WriteBuffer>,
    ) -> Self {
        let node_id = NodeId::new(config.node_id);
        let executor = Arc::new(LocalExecutor::new(query_executor, node_id));
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            node_id,
            executor,
            write_buffer,
            regions: RwLock::new(HashMap::new()),
            shutdown_tx,
            start_time: Instant::now(),
        }
    }

    /// Get the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the gRPC address.
    pub fn grpc_addr(&self) -> SocketAddr {
        self.config.grpc_addr
    }

    /// Get node information.
    pub fn node_info(&self) -> NodeInfo {
        let regions: Vec<RegionId> = self.regions.read().keys().copied().collect();

        NodeInfo::new(self.node_id, self.config.grpc_addr, NodeRole::Datanode)
            .with_regions(regions)
    }

    /// Add a region to this datanode.
    pub fn add_region(&self, region: RegionInfo) {
        let region_id = region.region_id;
        self.regions.write().insert(region_id, region);
        self.executor.add_region(region_id);
        info!(node_id = %self.node_id, region_id = %region_id, "Added region to datanode");
    }

    /// Remove a region from this datanode.
    pub fn remove_region(&self, region_id: RegionId) {
        self.regions.write().remove(&region_id);
        self.executor.remove_region(region_id);
        info!(node_id = %self.node_id, region_id = %region_id, "Removed region from datanode");
    }

    /// Check if this datanode has a region.
    pub fn has_region(&self, region_id: RegionId) -> bool {
        self.regions.read().contains_key(&region_id)
    }

    /// Get all managed regions.
    pub fn get_managed_regions(&self) -> Vec<RegionInfo> {
        self.regions.read().values().cloned().collect()
    }

    /// Get region count.
    pub fn region_count(&self) -> usize {
        self.regions.read().len()
    }

    /// Get the local executor.
    pub fn executor(&self) -> Arc<LocalExecutor> {
        Arc::clone(&self.executor)
    }

    /// Get the write buffer.
    pub fn write_buffer(&self) -> Arc<dyn WriteBuffer> {
        Arc::clone(&self.write_buffer)
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

    /// Start the heartbeat background task.
    ///
    /// This periodically sends heartbeats to the MetaServer to maintain
    /// the node's registration.
    pub fn start_heartbeat<M: MetaServiceApi + 'static>(
        &self,
        meta_client: Arc<M>,
        interval: Duration,
    ) -> JoinHandle<()> {
        let node_id = self.node_id;
        let regions_ref = self.regions.read().keys().copied().collect::<Vec<_>>();
        let mut shutdown_rx = self.subscribe_shutdown();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        let request = HeartbeatRequest {
                            node_id,
                            status: NodeStatus::Online,
                            regions: regions_ref.clone(),
                            metrics: Some(NodeMetrics {
                                cpu_usage: 0.0,  // TODO: Get real metrics
                                memory_usage: 0.0,
                                disk_usage: 0.0,
                                active_connections: 0,
                                queries_per_second: 0.0,
                            }),
                        };

                        match meta_client.heartbeat(request).await {
                            Ok(response) => {
                                if !response.success {
                                    warn!(
                                        node_id = %node_id,
                                        message = ?response.message,
                                        "Heartbeat was not accepted"
                                    );
                                }
                                // TODO: Handle regions_to_add and regions_to_remove
                            }
                            Err(e) => {
                                error!(
                                    node_id = %node_id,
                                    error = %e,
                                    "Failed to send heartbeat"
                                );
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!(node_id = %node_id, "Stopping heartbeat task");
                        break;
                    }
                }
            }
        })
    }

    /// Create a HeartbeatRequest for this datanode.
    pub fn create_heartbeat_request(&self) -> HeartbeatRequest {
        let regions: Vec<RegionId> = self.regions.read().keys().copied().collect();

        HeartbeatRequest {
            node_id: self.node_id,
            status: NodeStatus::Online,
            regions,
            metrics: None, // TODO: Collect real metrics
        }
    }
}

#[async_trait]
impl DatanodeApi for DatanodeServer {
    async fn execute_plan(&self, request: ExecutePlanRequest) -> Result<SendableRecordBatchStream> {
        // Verify we have the requested region
        if !self.has_region(request.region_id) {
            return Err(DistributedError::RegionNotFound {
                region_id: request.region_id.get(),
            });
        }

        // TODO: Deserialize and execute the physical plan
        // For now, return an error indicating this is not yet implemented
        Err(DistributedError::UnsupportedOperation(
            "Physical plan execution not yet implemented. Use SQL query instead.".to_string(),
        ))
    }

    async fn write(&self, request: WriteRequest) -> Result<WriteResponse> {
        // Verify we have the requested region
        if !self.has_region(request.region_id) {
            return Err(DistributedError::RegionNotFound {
                region_id: request.region_id.get(),
            });
        }

        // TODO: Deserialize and write the data batch
        // For now, return an error indicating this is not yet implemented
        Err(DistributedError::UnsupportedOperation(
            "Direct write to region not yet implemented. Use Line Protocol API.".to_string(),
        ))
    }

    async fn get_region_status(&self, region_id: RegionId) -> Result<GetRegionStatusResponse> {
        let region = self.regions.read().get(&region_id).cloned().ok_or_else(|| {
            DistributedError::RegionNotFound {
                region_id: region_id.get(),
            }
        })?;

        Ok(GetRegionStatusResponse {
            region_id,
            status: region.status,
            row_count: 0,     // TODO: Get actual row count
            size_bytes: 0,    // TODO: Get actual size
            last_write_time: None,
        })
    }

    async fn get_managed_regions(&self) -> Result<Vec<RegionInfo>> {
        Ok(self.get_managed_regions())
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn has_region(&self, region_id: RegionId) -> bool {
        self.regions.read().contains_key(&region_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test configuration creation
    #[test]
    fn test_datanode_config() {
        let config = DatanodeConfig::new(1)
            .with_grpc_addr("0.0.0.0:8183".parse().unwrap());

        assert_eq!(config.node_id, 1);
        assert_eq!(config.grpc_addr.port(), 8183);
    }
}
