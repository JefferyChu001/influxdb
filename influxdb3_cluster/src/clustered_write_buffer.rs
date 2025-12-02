//! Clustered write buffer that wraps the existing WriteBuffer with cluster functionality

use crate::error::{Error, Result};
use crate::node_registry::NodeRegistry;
use crate::replication::{WriteBatch, WriteReplicator};
use crate::shard_manager::ShardManager;
use crate::types::{ConsistencyLevel, ShardId};
use async_trait::async_trait;
use data_types::NamespaceName;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::Wal;
use influxdb3_write::{
    write_buffer,
    Bufferer, BufferedWriteRequest, ChunkContainer, ChunkFilter, DistinctCacheManager, LastCacheManager,
    ParquetFile, PersistedSnapshotVersion, Precision, WriteBuffer,
};
use iox_time::Time;
use observability_deps::tracing::{debug, error, info};
use std::sync::Arc;

/// Clustered write buffer adds distributed write capabilities
pub struct ClusteredWriteBuffer {
    /// Local write buffer for this node
    local_buffer: Arc<dyn WriteBuffer>,
    /// Shard manager for routing writes
    shard_manager: Arc<ShardManager>,
    /// Node registry for node information
    #[allow(dead_code)]
    node_registry: Arc<NodeRegistry>,
    /// Write replicator for replication
    replicator: Arc<WriteReplicator>,
    /// Default consistency level
    consistency_level: ConsistencyLevel,
}

impl std::fmt::Debug for ClusteredWriteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusteredWriteBuffer")
            .field("consistency_level", &self.consistency_level)
            .finish()
    }
}

impl ClusteredWriteBuffer {
    pub fn new(
        local_buffer: Arc<dyn WriteBuffer>,
        shard_manager: Arc<ShardManager>,
        node_registry: Arc<NodeRegistry>,
        consistency_level: ConsistencyLevel,
    ) -> Self {
        let replicator = Arc::new(WriteReplicator::new(
            shard_manager.clone(),
            node_registry.clone(),
            Arc::new(crate::rpc::client::ClusterRpcClient::new()),
        ));

        Self {
            local_buffer,
            shard_manager,
            node_registry,
            replicator,
            consistency_level,
        }
    }

    /// Check if a shard is owned by this node
    async fn is_local_shard(&self, shard_id: ShardId) -> Result<bool> {
        let replicas = self.shard_manager.get_shard_replicas(shard_id).await?;
        // In a real implementation, we would check if current node is in the replica list
        // For now, assume all shards are local (single node mode)
        Ok(!replicas.is_empty())
    }

    /// Parse line protocol (simplified version)
    fn parse_line_protocol(&self, lp: &str) -> Result<Vec<ParsedLine>> {
        let mut lines = Vec::new();

        for line in lp.lines() {
            if line.trim().is_empty() || line.trim().starts_with('#') {
                continue;
            }

            // Simple parsing - in production, use influxdb-line-protocol crate
            let parsed = self.parse_single_line(line)?;
            lines.push(parsed);
        }

        Ok(lines)
    }

    /// Parse a single line of line protocol
    fn parse_single_line(&self, line: &str) -> Result<ParsedLine> {
        // Simplified parsing - extract measurement and tags
        // Format: measurement,tag1=value1,tag2=value2 field1=value1 timestamp
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Err(Error::InternalError {
                message: "Empty line".to_string(),
            });
        }

        let measurement_and_tags = parts[0];
        let mut measurement = String::new();
        let mut tags = Vec::new();

        for (i, part) in measurement_and_tags.split(',').enumerate() {
            if i == 0 {
                measurement = part.to_string();
            } else if let Some((key, value)) = part.split_once('=') {
                tags.push((key.to_string(), value.to_string()));
            }
        }

        Ok(ParsedLine {
            measurement,
            tags,
            raw_line: line.to_string(),
        })
    }
}

#[derive(Debug)]
struct ParsedLine {
    measurement: String,
    tags: Vec<(String, String)>,
    raw_line: String,
}

#[async_trait]
impl Bufferer for ClusteredWriteBuffer {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        info!("ClusteredWriteBuffer: Starting write_lp for database: {}, lp length: {}", database, lp.len());
        debug!("ClusteredWriteBuffer: Line protocol data: {}", lp);

        // This is a simplified implementation. A production version would need to handle
        // partial writes and aggregate results from multiple shards/nodes.
        let parsed_lines = self.parse_line_protocol(lp).map_err(|e| {
            error!("ClusteredWriteBuffer: Failed to parse line protocol: {}", e);
            write_buffer::Error::from(anyhow::Error::from(e))
        })?;

        info!("ClusteredWriteBuffer: Parsed {} lines", parsed_lines.len());

        let mut shard_writes: std::collections::HashMap<ShardId, Vec<String>> =
            std::collections::HashMap::new();

        for (i, line) in parsed_lines.iter().enumerate() {
            debug!("ClusteredWriteBuffer: Processing line {}: measurement={}, tags={:?}", i, line.measurement, line.tags);

            let tags_ref: Vec<(&str, &str)> = line
                .tags
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            let shard_id = self.shard_manager.route_write(
                database.as_str(),
                &line.measurement,
                &tags_ref,
            );

            debug!("ClusteredWriteBuffer: Routed to shard_id: {}", shard_id);

            // Ensure shard exists before writing
            if self.shard_manager.get_shard(shard_id).await.is_err() {
                info!("ClusteredWriteBuffer: Creating new shard {}", shard_id);
                let db = self.local_buffer.catalog().db_schema(database.as_str()).ok_or_else(|| {
                    error!("ClusteredWriteBuffer: Database not found: {}", database);
                    write_buffer::Error::DatabaseNotFound { db_name: database.to_string() }
                })?;
                self.shard_manager.create_shard(db.id, crate::types::ShardRange::Hash { start: 0, end: u64::MAX }, &self.node_registry).await.map_err(|e| {
                    error!("ClusteredWriteBuffer: Failed to create shard: {}", e);
                    write_buffer::Error::from(anyhow::Error::from(e))
                })?;
            }

            shard_writes
                .entry(shard_id)
                .or_default()
                .push(line.raw_line.clone());
        }

        let mut local_lp = String::new();
        info!("ClusteredWriteBuffer: Processing {} shard writes", shard_writes.len());

        for (shard_id, lines) in shard_writes {
            let lp_data = lines.join("\n");
            debug!("ClusteredWriteBuffer: Processing shard {} with {} lines", shard_id, lines.len());

            if self.is_local_shard(shard_id).await.unwrap_or(false) {
                info!("ClusteredWriteBuffer: Writing to local shard {}", shard_id);
                local_lp.push_str(&lp_data);
                local_lp.push('\n');
            } else {
                info!("ClusteredWriteBuffer: Replicating to remote shard {}", shard_id);
                let batch = WriteBatch::new(
                    database.to_string(),
                    lp_data.as_bytes().to_vec(),
                    0, // sequence number would be assigned by WAL
                );

                self.replicator
                    .replicate_write(shard_id, batch, self.consistency_level)
                    .await.map_err(|e| {
                        error!("ClusteredWriteBuffer: Failed to replicate write: {}", e);
                        write_buffer::Error::from(anyhow::Error::from(e))
                    })?;
            }
        }

        if !local_lp.is_empty() {
            info!("ClusteredWriteBuffer: Writing {} bytes to local buffer", local_lp.len());
            self.local_buffer.write_lp(database, &local_lp, ingest_time, accept_partial, precision, no_sync).await
        } else {
            info!("ClusteredWriteBuffer: No local writes, returning empty result");
            Ok(BufferedWriteRequest {
                db_name: database,
                invalid_lines: vec![],
                line_count: lp.lines().count(),
                field_count: 0, // Simplified
                index_count: 0, // Simplified
            })
        }
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.local_buffer.catalog()
    }

    fn wal(&self) -> Arc<dyn Wal> {
        self.local_buffer.wal()
    }

    fn parquet_files_filtered(&self, db_id: DbId, table_id: TableId, filter: &ChunkFilter<'_>) -> Vec<ParquetFile> {
        self.local_buffer.parquet_files_filtered(db_id, table_id, filter)
    }

    fn watch_persisted_snapshots(&self) -> tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>> {
        self.local_buffer.watch_persisted_snapshots()
    }
}

impl ChunkContainer for ClusteredWriteBuffer {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn datafusion::catalog::Session,
    ) -> datafusion::error::Result<Vec<Arc<dyn iox_query::QueryChunk>>, datafusion::error::DataFusionError> {
        self.local_buffer.get_table_chunks(db_schema, table_def, filter, projection, ctx)
    }
}

#[async_trait]
impl DistinctCacheManager for ClusteredWriteBuffer {
    fn distinct_cache_provider(&self) -> Arc<influxdb3_cache::distinct_cache::DistinctCacheProvider> {
        self.local_buffer.distinct_cache_provider()
    }
}

#[async_trait]
impl LastCacheManager for ClusteredWriteBuffer {
    fn last_cache_provider(&self) -> Arc<influxdb3_cache::last_cache::LastCacheProvider> {
        self.local_buffer.last_cache_provider()
    }
}

impl WriteBuffer for ClusteredWriteBuffer {}
