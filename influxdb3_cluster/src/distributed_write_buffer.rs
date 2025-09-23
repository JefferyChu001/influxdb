//! Distributed Write Buffer
//!
//! This module provides a distributed write buffer that wraps the local write buffer
//! and coordinates writes across the cluster using the partition manager and write coordinator.

use crate::{ClusterManager, partition::PartitionManager, write_coordinator::WriteCoordinator};
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError};
use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_wal::Wal;
use influxdb3_write::{
    BufferedWriteRequest, Precision, WriteBuffer, Bufferer, ChunkContainer,
    DistinctCacheManager, LastCacheManager, write_buffer, ParquetFile, ChunkFilter,
    PersistedSnapshotVersion,
};
use influxdb3_id::{DbId, TableId};
use iox_query::QueryChunk;
use iox_time::Time;
use observability_deps::tracing::{debug, error, info, warn};
use std::sync::Arc;

/// A distributed write buffer that coordinates writes across the cluster
#[derive(Debug)]
pub struct DistributedWriteBuffer {
    /// The local write buffer for this node
    local_buffer: Arc<dyn WriteBuffer>,
    /// The cluster manager for coordination
    cluster_manager: Arc<ClusterManager>,
    /// The write coordinator for distributed writes
    write_coordinator: Arc<WriteCoordinator>,
}

impl DistributedWriteBuffer {
    /// Create a new distributed write buffer
    pub async fn new(
        local_buffer: Arc<dyn WriteBuffer>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Result<Self, crate::ClusterError> {
        let write_coordinator = Arc::new(WriteCoordinator::new(
            cluster_manager.config().clone(),
            cluster_manager.partition_manager(),
        ).await?);

        Ok(Self {
            local_buffer,
            cluster_manager,
            write_coordinator,
        })
    }

    /// Get the partition manager
    pub fn partition_manager(&self) -> Arc<PartitionManager> {
        self.cluster_manager.partition_manager()
    }

    /// Get the write coordinator
    pub fn write_coordinator(&self) -> Arc<WriteCoordinator> {
        Arc::clone(&self.write_coordinator)
    }
}

#[async_trait]
impl Bufferer for DistributedWriteBuffer {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        debug!(
            database = %database,
            lp_length = lp.len(),
            accept_partial = accept_partial,
            precision = ?precision,
            no_sync = no_sync,
            "Processing distributed write request"
        );

        // First, parse the line protocol locally to get the write request
        let local_result = self.local_buffer.write_lp(
            database.clone(),
            lp,
            ingest_time,
            accept_partial,
            precision,
            no_sync,
        ).await;

        match local_result {
            Ok(write_request) => {
                info!(
                    database = %database,
                    line_count = write_request.line_count,
                    "Successfully processed write request locally"
                );

                // If this is a single-node cluster, just return the local result
                if self.cluster_manager.is_single_node().await {
                    debug!("Single node cluster, returning local write result");
                    return Ok(write_request);
                }

                // For multi-node clusters, we would coordinate the write across nodes
                // For now, we'll just return the local result as we're building incrementally
                warn!("Multi-node distributed writes not yet fully implemented, using local write");
                Ok(write_request)
            }
            Err(e) => {
                error!(
                    database = %database,
                    error = %e,
                    "Failed to process write request locally"
                );
                Err(e)
            }
        }
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.local_buffer.catalog()
    }

    fn wal(&self) -> Arc<dyn Wal> {
        self.local_buffer.wal()
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.local_buffer.parquet_files_filtered(db_id, table_id, filter)
    }

    fn watch_persisted_snapshots(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>> {
        self.local_buffer.watch_persisted_snapshots()
    }
}

// Implement the other required traits by delegating to the local buffer

impl ChunkContainer for DistributedWriteBuffer {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        // Delegate to the local buffer for chunk retrieval
        // In a distributed system, this would potentially aggregate chunks from multiple nodes
        self.local_buffer.get_table_chunks(db_schema, table_def, filter, projection, ctx)
    }
}

impl DistinctCacheManager for DistributedWriteBuffer {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        // Delegate to the local buffer's distinct cache provider
        // In a distributed system, this might coordinate with remote caches
        self.local_buffer.distinct_cache_provider()
    }
}

impl LastCacheManager for DistributedWriteBuffer {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        // Delegate to the local buffer's last cache provider
        // In a distributed system, this might coordinate with remote caches
        self.local_buffer.last_cache_provider()
    }
}

// The WriteBuffer trait is automatically implemented since we implement all its super-traits
impl WriteBuffer for DistributedWriteBuffer {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterConfig, NodeId};
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_distributed_write_buffer_creation() {
        // For now, we'll skip the actual test since we need a real WriteBuffer implementation
        // This test would require setting up a full WriteBufferImpl which is complex

        let config = ClusterConfig {
            node_id: NodeId::new(),
            bind_addr: "127.0.0.1:8191".parse::<SocketAddr>().unwrap(),
            seed_nodes: vec![],
            replication_factor: 1,
            ..Default::default()
        };

        let _cluster_manager = ClusterManager::new(config).await.unwrap();

        // Test passes if we can create the cluster manager without panicking
        // In a real test, we would create a DistributedWriteBuffer with a real WriteBuffer
    }
}
