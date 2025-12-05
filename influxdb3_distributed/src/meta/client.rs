//! MetaServer client for remote access.

use crate::common::{NodeId, NodeInfo, NodeStatus, RegionId, RegionInfo, TableLocation};
use crate::config::ClusterConfig;
use crate::error::{DistributedError, Result};
use crate::meta::MetaServiceApi;
use crate::proto::{
    CreateTableRequest, CreateTableResponse, GetClusterInfoResponse, HeartbeatRequest,
    HeartbeatResponse, RegisterDatanodeRequest, RegisterDatanodeResponse,
};
use async_trait::async_trait;
use observability_deps::tracing::{debug, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Client for connecting to MetaServer.
///
/// This client provides a high-level interface for communicating with the MetaServer.
/// It handles connection management, retries, and failover between multiple MetaServer
/// instances.
#[derive(Debug)]
pub struct MetaClient {
    /// Configuration
    config: ClusterConfig,

    /// List of MetaServer addresses
    meta_addrs: Vec<SocketAddr>,

    /// Current primary MetaServer index
    current_primary: RwLock<usize>,

    /// Connection timeout
    connect_timeout: Duration,

    /// Request timeout
    request_timeout: Duration,
}

impl MetaClient {
    /// Create a new MetaClient with the given configuration.
    pub fn new(config: ClusterConfig) -> Self {
        let meta_addrs = config.meta_server_addrs.clone();
        let connect_timeout = config.connect_timeout;
        let request_timeout = config.request_timeout;

        Self {
            config,
            meta_addrs,
            current_primary: RwLock::new(0),
            connect_timeout,
            request_timeout,
        }
    }

    /// Create a MetaClient from a list of addresses.
    pub fn from_addrs(addrs: Vec<SocketAddr>) -> Self {
        let config = ClusterConfig::default().with_meta_servers(addrs);
        Self::new(config)
    }

    /// Get the current primary MetaServer address.
    pub async fn current_addr(&self) -> Option<SocketAddr> {
        let idx = *self.current_primary.read().await;
        self.meta_addrs.get(idx).copied()
    }

    /// Try the next MetaServer address.
    async fn try_next_addr(&self) {
        let mut idx = self.current_primary.write().await;
        *idx = (*idx + 1) % self.meta_addrs.len();
        debug!("Switching to MetaServer at index {}", *idx);
    }

    /// Execute a request with retry and failover.
    async fn execute_with_retry<T, F, Fut>(&self, operation_name: &str, f: F) -> Result<T>
    where
        F: Fn(SocketAddr) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let max_retries = self.config.max_retries;
        let mut last_error = None;

        for attempt in 0..max_retries {
            let addr = self
                .current_addr()
                .await
                .ok_or_else(|| DistributedError::ClusterNotInitialized)?;

            match f(addr).await {
                Ok(result) => return Ok(result),
                Err(e) if e.is_retriable() => {
                    warn!(
                        operation = %operation_name,
                        attempt = attempt + 1,
                        max_retries = max_retries,
                        error = %e,
                        "Request failed, retrying"
                    );
                    last_error = Some(e);
                    self.try_next_addr().await;

                    // Exponential backoff
                    let backoff = self.config.retry_backoff_base * 2u32.pow(attempt as u32);
                    tokio::time::sleep(backoff).await;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            DistributedError::Internal(format!(
                "{} failed after {} retries",
                operation_name, max_retries
            ))
        }))
    }
}

// Note: In a full implementation, this would use gRPC to communicate with remote MetaServer.
// For now, we provide a placeholder implementation that can be connected to the actual
// gRPC client once proto generation is set up.

#[async_trait]
impl MetaServiceApi for MetaClient {
    async fn get_table_regions(&self, database: &str, table: &str) -> Result<Vec<RegionInfo>> {
        let db = database.to_string();
        let tbl = table.to_string();

        self.execute_with_retry("get_table_regions", |_addr| {
            let db = db.clone();
            let tbl = tbl.clone();
            async move {
                // TODO: Implement actual gRPC call
                // For now, return an error indicating remote call is not implemented
                Err(DistributedError::UnsupportedOperation(
                    "Remote MetaServer call not yet implemented. Use local MetaServer for now."
                        .to_string(),
                ))
            }
        })
        .await
    }

    async fn get_database_regions(&self, database: &str) -> Result<Vec<RegionInfo>> {
        let db = database.to_string();

        self.execute_with_retry("get_database_regions", |_addr| {
            let db = db.clone();
            async move {
                Err(DistributedError::UnsupportedOperation(
                    "Remote MetaServer call not yet implemented".to_string(),
                ))
            }
        })
        .await
    }

    async fn get_node(&self, node_id: NodeId) -> Result<NodeInfo> {
        self.execute_with_retry("get_node", |_addr| async move {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn get_all_nodes(&self) -> Result<Vec<NodeInfo>> {
        self.execute_with_retry("get_all_nodes", |_addr| async move {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn get_online_datanodes(&self) -> Result<Vec<NodeInfo>> {
        self.execute_with_retry("get_online_datanodes", |_addr| async move {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn register_datanode(&self, node_info: NodeInfo) -> Result<RegisterDatanodeResponse> {
        self.execute_with_retry("register_datanode", |_addr| async {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse> {
        self.execute_with_retry("heartbeat", |_addr| async {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn get_cluster_info(&self) -> Result<GetClusterInfoResponse> {
        self.execute_with_retry("get_cluster_info", |_addr| async {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<CreateTableResponse> {
        self.execute_with_retry("create_table", |_addr| async {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn get_table_location(&self, database: &str, table: &str) -> Result<TableLocation> {
        let db = database.to_string();
        let tbl = table.to_string();

        self.execute_with_retry("get_table_location", |_addr| {
            let db = db.clone();
            let tbl = tbl.clone();
            async move {
                Err(DistributedError::UnsupportedOperation(
                    "Remote MetaServer call not yet implemented".to_string(),
                ))
            }
        })
        .await
    }

    async fn update_node_status(&self, node_id: NodeId, status: NodeStatus) -> Result<()> {
        self.execute_with_retry("update_node_status", |_addr| async {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn assign_region(&self, region_id: RegionId, node_id: NodeId) -> Result<()> {
        self.execute_with_retry("assign_region", |_addr| async {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }

    async fn get_region(&self, region_id: RegionId) -> Result<RegionInfo> {
        self.execute_with_retry("get_region", |_addr| async {
            Err(DistributedError::UnsupportedOperation(
                "Remote MetaServer call not yet implemented".to_string(),
            ))
        })
        .await
    }
}

/// Builder for creating MetaClient instances.
#[derive(Debug, Default)]
pub struct MetaClientBuilder {
    addrs: Vec<SocketAddr>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
}

impl MetaClientBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a MetaServer address.
    pub fn add_addr(mut self, addr: SocketAddr) -> Self {
        self.addrs.push(addr);
        self
    }

    /// Add multiple MetaServer addresses.
    pub fn with_addrs(mut self, addrs: Vec<SocketAddr>) -> Self {
        self.addrs.extend(addrs);
        self
    }

    /// Set the connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Build the MetaClient.
    pub fn build(self) -> Result<MetaClient> {
        if self.addrs.is_empty() {
            return Err(DistributedError::ConfigError(
                "At least one MetaServer address is required".to_string(),
            ));
        }

        let mut config = ClusterConfig::default().with_meta_servers(self.addrs);

        if let Some(timeout) = self.connect_timeout {
            config = config.with_connect_timeout(timeout);
        }

        if let Some(timeout) = self.request_timeout {
            config = config.with_request_timeout(timeout);
        }

        Ok(MetaClient::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let client = MetaClientBuilder::new()
            .add_addr("127.0.0.1:9000".parse().unwrap())
            .add_addr("127.0.0.1:9001".parse().unwrap())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        assert_eq!(client.meta_addrs.len(), 2);
    }

    #[test]
    fn test_builder_requires_addrs() {
        let result = MetaClientBuilder::new().build();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_current_addr() {
        let client = MetaClientBuilder::new()
            .add_addr("127.0.0.1:9000".parse().unwrap())
            .build()
            .unwrap();

        let addr = client.current_addr().await;
        assert_eq!(addr, Some("127.0.0.1:9000".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_try_next_addr() {
        let client = MetaClientBuilder::new()
            .add_addr("127.0.0.1:9000".parse().unwrap())
            .add_addr("127.0.0.1:9001".parse().unwrap())
            .build()
            .unwrap();

        // Initial address
        assert_eq!(
            client.current_addr().await,
            Some("127.0.0.1:9000".parse().unwrap())
        );

        // Try next
        client.try_next_addr().await;
        assert_eq!(
            client.current_addr().await,
            Some("127.0.0.1:9001".parse().unwrap())
        );

        // Wrap around
        client.try_next_addr().await;
        assert_eq!(
            client.current_addr().await,
            Some("127.0.0.1:9000".parse().unwrap())
        );
    }
}
