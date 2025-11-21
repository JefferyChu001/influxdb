//! Clustered write buffer that wraps the existing WriteBuffer with cluster functionality

use crate::error::{Error, Result};
use crate::node_registry::NodeRegistry;
use crate::replication::{WriteBatch, WriteReplicator};
use crate::shard_manager::ShardManager;
use crate::types::{ConsistencyLevel, ShardId};
use influxdb3_write::{Precision, WriteBuffer};
use iox_time::Time;
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
        ));

        Self {
            local_buffer,
            shard_manager,
            node_registry,
            replicator,
            consistency_level,
        }
    }

    /// Write line protocol data with cluster-aware routing
    pub async fn write_lp(
        &self,
        database: &str,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<()> {
        // Parse line protocol to extract measurement and tags
        let parsed_lines = self.parse_line_protocol(lp)?;

        // Group writes by shard
        let mut shard_writes: std::collections::HashMap<ShardId, Vec<String>> =
            std::collections::HashMap::new();

        for line in parsed_lines {
            // Convert tags to &[(&str, &str)]
            let tags_ref: Vec<(&str, &str)> = line
                .tags
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            let shard_id = self.shard_manager.route_write(
                database,
                &line.measurement,
                &tags_ref,
            );

            shard_writes
                .entry(shard_id)
                .or_insert_with(Vec::new)
                .push(line.raw_line);
        }

        // Write to each shard
        for (shard_id, lines) in shard_writes {
            let lp_data = lines.join("\n");

            // Create write batch
            let batch = WriteBatch::new(
                database.to_string(),
                lp_data.as_bytes().to_vec(),
                0, // sequence number would be assigned by WAL
            );

            // Replicate write based on consistency level
            self.replicator
                .replicate_write(shard_id, batch, self.consistency_level)
                .await?;

            // Also write to local buffer if this node owns the shard
            if self.is_local_shard(shard_id).await? {
                self.write_to_local_buffer(database, &lp_data, ingest_time, accept_partial, precision)
                    .await?;
            }
        }

        Ok(())
    }

    /// Check if a shard is owned by this node
    async fn is_local_shard(&self, shard_id: ShardId) -> Result<bool> {
        let replicas = self.shard_manager.get_shard_replicas(shard_id).await?;
        // In a real implementation, we would check if current node is in the replica list
        // For now, assume all shards are local (single node mode)
        Ok(!replicas.is_empty())
    }

    /// Write to the local write buffer
    async fn write_to_local_buffer(
        &self,
        database: &str,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<()> {
        use data_types::NamespaceName;

        // Convert to owned string to satisfy 'static lifetime requirement
        let db_name = NamespaceName::new(database.to_string())
            .map_err(|e| Error::InternalError {
                message: format!("Invalid database name: {}", e),
            })?;

        self.local_buffer
            .write_lp(db_name, lp, ingest_time, accept_partial, precision, false)
            .await
            .map_err(|e| Error::WriteError {
                message: format!("Local write failed: {}", e),
            })?;

        Ok(())
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

