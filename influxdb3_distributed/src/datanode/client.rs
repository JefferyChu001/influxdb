//! Datanode client for remote query execution.

use crate::common::{NodeId, NodeInfo, RegionId, RegionInfo};
use crate::error::{DistributedError, Result};
use crate::proto::{ArrowBatch, ExecutePlanRequest, GetRegionStatusResponse, WriteRequest, WriteResponse};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightDescriptor, Ticket};
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::{self, StreamExt, TryStreamExt};
use observability_deps::tracing::{debug, error, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::Channel;

/// Connection pool for Datanode connections.
#[derive(Debug)]
pub struct DatanodeConnectionPool {
    /// Connections indexed by node ID
    connections: DashMap<NodeId, Arc<DatanodeConnection>>,

    /// Connection timeout
    connect_timeout: Duration,

    /// Request timeout
    request_timeout: Duration,
}

impl DatanodeConnectionPool {
    /// Create a new connection pool.
    pub fn new(connect_timeout: Duration, request_timeout: Duration) -> Self {
        Self {
            connections: DashMap::new(),
            connect_timeout,
            request_timeout,
        }
    }

    /// Get or create a connection to a datanode.
    pub async fn get_or_create(
        &self,
        node_info: &NodeInfo,
    ) -> Result<Arc<DatanodeConnection>> {
        // Check if we have an existing connection
        if let Some(conn) = self.connections.get(&node_info.node_id) {
            if conn.is_connected() {
                return Ok(Arc::clone(&conn));
            }
            // Connection is stale, remove it
            drop(conn);
            self.connections.remove(&node_info.node_id);
        }

        // Create new connection
        let conn = DatanodeConnection::connect(
            node_info.clone(),
            self.connect_timeout,
            self.request_timeout,
        )
        .await?;

        let conn = Arc::new(conn);
        self.connections.insert(node_info.node_id, Arc::clone(&conn));

        Ok(conn)
    }

    /// Remove a connection from the pool.
    pub fn remove(&self, node_id: NodeId) {
        self.connections.remove(&node_id);
    }

    /// Get the number of active connections.
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

impl Default for DatanodeConnectionPool {
    fn default() -> Self {
        Self::new(Duration::from_secs(5), Duration::from_secs(30))
    }
}

/// A connection to a single Datanode.
#[derive(Debug)]
pub struct DatanodeConnection {
    /// Node information
    node_info: NodeInfo,

    /// gRPC channel
    channel: Channel,

    /// Whether the connection is established
    connected: RwLock<bool>,
}

impl DatanodeConnection {
    /// Connect to a datanode.
    pub async fn connect(
        node_info: NodeInfo,
        connect_timeout: Duration,
        _request_timeout: Duration,
    ) -> Result<Self> {
        let addr = format!("http://{}", node_info.grpc_addr);

        debug!(
            node_id = %node_info.node_id,
            addr = %addr,
            "Connecting to datanode"
        );

        let channel = Channel::from_shared(addr)
            .map_err(|e| DistributedError::ConfigError(e.to_string()))?
            .connect_timeout(connect_timeout)
            .connect()
            .await
            .map_err(|e| DistributedError::GrpcTransport(e))?;

        info!(
            node_id = %node_info.node_id,
            "Connected to datanode"
        );

        Ok(Self {
            node_info,
            channel,
            connected: RwLock::new(true),
        })
    }

    /// Check if the connection is established.
    pub fn is_connected(&self) -> bool {
        *self.connected.blocking_read()
    }

    /// Get the node info.
    pub fn node_info(&self) -> &NodeInfo {
        &self.node_info
    }

    /// Get the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_info.node_id
    }

    /// Mark the connection as disconnected.
    pub async fn mark_disconnected(&self) {
        *self.connected.write().await = false;
    }
}

/// Client for communicating with a Datanode.
#[derive(Debug, Clone)]
pub struct DatanodeClient {
    /// Connection pool
    pool: Arc<DatanodeConnectionPool>,
}

impl DatanodeClient {
    /// Create a new DatanodeClient.
    pub fn new(pool: Arc<DatanodeConnectionPool>) -> Self {
        Self { pool }
    }

    /// Create a DatanodeClient with default settings.
    pub fn with_defaults() -> Self {
        Self::new(Arc::new(DatanodeConnectionPool::default()))
    }

    /// Execute a query plan on a remote datanode.
    ///
    /// This sends the serialized physical plan to the datanode and returns
    /// a stream of Arrow record batches.
    pub async fn execute_plan(
        &self,
        node_info: &NodeInfo,
        request: ExecutePlanRequest,
    ) -> Result<Vec<RecordBatch>> {
        let conn = self.pool.get_or_create(node_info).await?;

        // Create Flight client
        let mut flight_client = FlightClient::new(conn.channel.clone());

        // Create a Flight ticket with the serialized plan
        let ticket = Ticket::new(request.plan_bytes.to_vec());

        // Execute the query using do_get
        let stream = flight_client
            .do_get(ticket)
            .await
            .map_err(|e| DistributedError::FlightError(e.to_string()))?;

        // Collect results
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| DistributedError::FlightError(e.to_string()))?;

        Ok(batches)
    }

    /// Execute a SQL query on a remote datanode.
    ///
    /// This is a higher-level method that sends a SQL query string instead
    /// of a serialized plan.
    pub async fn execute_sql(
        &self,
        node_info: &NodeInfo,
        database: &str,
        query: &str,
        region_id: RegionId,
    ) -> Result<Vec<RecordBatch>> {
        let conn = self.pool.get_or_create(node_info).await?;

        // Create Flight client
        let mut flight_client = FlightClient::new(conn.channel.clone());

        // Create a ticket with the query information as JSON
        let query_info = serde_json::json!({
            "database": database,
            "sql_query": query,
            "region_id": region_id.get(),
            "query_type": "sql"
        });

        let ticket = Ticket::new(query_info.to_string().into_bytes());

        // Execute the query
        let stream = flight_client
            .do_get(ticket)
            .await
            .map_err(|e| DistributedError::FlightError(e.to_string()))?;

        // Collect results
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| DistributedError::FlightError(e.to_string()))?;

        Ok(batches)
    }

    /// Write data to a region on a remote datanode.
    pub async fn write(
        &self,
        node_info: &NodeInfo,
        request: WriteRequest,
    ) -> Result<WriteResponse> {
        let _conn = self.pool.get_or_create(node_info).await?;

        // TODO: Implement actual write over gRPC
        // For now, return an error
        Err(DistributedError::UnsupportedOperation(
            "Remote write not yet implemented".to_string(),
        ))
    }

    /// Get region status from a remote datanode.
    pub async fn get_region_status(
        &self,
        node_info: &NodeInfo,
        region_id: RegionId,
    ) -> Result<GetRegionStatusResponse> {
        let _conn = self.pool.get_or_create(node_info).await?;

        // TODO: Implement actual status query over gRPC
        // For now, return an error
        Err(DistributedError::UnsupportedOperation(
            "Remote region status not yet implemented".to_string(),
        ))
    }

    /// Check connectivity to a datanode.
    pub async fn check_connectivity(&self, node_info: &NodeInfo) -> Result<bool> {
        match self.pool.get_or_create(node_info).await {
            Ok(_) => Ok(true),
            Err(e) => {
                warn!(
                    node_id = %node_info.node_id,
                    error = %e,
                    "Failed to connect to datanode"
                );
                Ok(false)
            }
        }
    }
}

/// Builder for creating DatanodeClient instances.
#[derive(Debug, Default)]
pub struct DatanodeClientBuilder {
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
}

impl DatanodeClientBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
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

    /// Build the DatanodeClient.
    pub fn build(self) -> DatanodeClient {
        let pool = DatanodeConnectionPool::new(
            self.connect_timeout.unwrap_or(Duration::from_secs(5)),
            self.request_timeout.unwrap_or(Duration::from_secs(30)),
        );
        DatanodeClient::new(Arc::new(pool))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NodeRole;

    #[test]
    fn test_connection_pool_creation() {
        let pool = DatanodeConnectionPool::default();
        assert!(pool.is_empty());
    }

    #[test]
    fn test_client_builder() {
        let client = DatanodeClientBuilder::new()
            .connect_timeout(Duration::from_secs(10))
            .request_timeout(Duration::from_secs(60))
            .build();

        // Just verify it builds without panic
        assert!(client.pool.is_empty());
    }
}
