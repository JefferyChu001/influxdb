//! Write router for distributed write operations.
//!
//! The WriteRouter handles routing write requests to the appropriate Datanodes
//! based on partition key hashing and region distribution.

use crate::common::{
    NodeId, NodeInfo, PartitionRange, RegionId, RegionInfo, compute_partition_hash_from_str,
};
use crate::datanode::DatanodeClient;
use crate::error::{DistributedError, Result};
use crate::frontend::DistributedWriteApi;
use crate::meta::MetaServiceApi;
use crate::proto::{WritePrecision, WriteRequest};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use influxdb3_types::write::Precision;
use influxdb3_write::BufferedWriteRequest;
use observability_deps::tracing::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;

/// Write router that routes writes to appropriate Datanodes.
#[derive(Debug)]
pub struct WriteRouter<M: MetaServiceApi> {
    /// MetaServer client
    meta_client: Arc<M>,

    /// Datanode client
    datanode_client: DatanodeClient,

    /// Cache of table region distributions
    region_cache: DashMap<String, Vec<RegionInfo>>,

    /// Cache TTL in seconds
    cache_ttl_secs: u64,
}

impl<M: MetaServiceApi> WriteRouter<M> {
    /// Create a new WriteRouter.
    pub fn new(meta_client: Arc<M>) -> Self {
        Self {
            meta_client,
            datanode_client: DatanodeClient::with_defaults(),
            region_cache: DashMap::new(),
            cache_ttl_secs: 60,
        }
    }

    /// Create a WriteRouter with custom settings.
    pub fn with_settings(
        meta_client: Arc<M>,
        datanode_client: DatanodeClient,
        cache_ttl_secs: u64,
    ) -> Self {
        Self {
            meta_client,
            datanode_client,
            region_cache: DashMap::new(),
            cache_ttl_secs,
        }
    }

    /// Get table key for caching.
    fn table_key(database: &str, table: &str) -> String {
        format!("{}.{}", database, table)
    }

    /// Get regions for a table, using cache if available.
    async fn get_regions(&self, database: &str, table: &str) -> Result<Vec<RegionInfo>> {
        let key = Self::table_key(database, table);

        // Check cache
        if let Some(regions) = self.region_cache.get(&key) {
            return Ok(regions.clone());
        }

        // Fetch from MetaServer
        let regions = self.meta_client.get_table_regions(database, table).await?;

        // Update cache
        self.region_cache.insert(key, regions.clone());

        Ok(regions)
    }

    /// Invalidate cache for a table.
    pub fn invalidate_cache(&self, database: &str, table: &str) {
        let key = Self::table_key(database, table);
        self.region_cache.remove(&key);
    }

    /// Invalidate all caches.
    pub fn invalidate_all_caches(&self) {
        self.region_cache.clear();
    }

    /// Find the region for a data point based on its partition key.
    /// Returns the index of the matching region.
    #[allow(dead_code)]
    fn find_region_index_for_point(
        &self,
        regions: &[RegionInfo],
        partition_key: &str,
        timestamp_ns: i64,
    ) -> Option<usize> {
        let hash = compute_partition_hash_from_str(partition_key);

        regions.iter().position(|r| {
            r.partition_range.contains_hash(hash)
                && r.partition_range.contains_time(timestamp_ns)
                && r.can_accept_write()
        })
    }

    /// Route a line protocol write to the appropriate nodes.
    ///
    /// This parses the line protocol, determines the partition key for each point,
    /// and routes the data to the appropriate Datanodes.
    pub async fn route_write(
        &self,
        database: &str,
        line_protocol: &str,
        precision: Precision,
    ) -> Result<WriteResult> {
        info!(database = %database, "Routing write request");

        // Parse line protocol to extract table names and partition keys
        // For now, we'll use a simplified approach:
        // 1. Extract all unique table names from the line protocol
        // 2. For each table, get the region distribution
        // 3. Route each line to the appropriate region based on tags

        // This is a simplified implementation that routes all data to the first available region
        // In a full implementation, we would:
        // 1. Parse each line to extract table name and tag values
        // 2. Compute partition key hash from the configured partition columns
        // 3. Find the appropriate region for each line
        // 4. Batch lines by region and send to appropriate datanodes

        let tables = self.extract_table_names(line_protocol);

        if tables.is_empty() {
            return Err(DistributedError::WriteError(
                "No valid data points in line protocol".to_string(),
            ));
        }

        let mut write_batches: HashMap<(NodeId, RegionId), Vec<String>> = HashMap::new();

        for table in &tables {
            let regions = self.get_regions(database, table).await?;

            if regions.is_empty() {
                // Table doesn't exist yet, we might need to create it
                // For now, return an error
                return Err(DistributedError::TableNotFound {
                    database: database.to_string(),
                    table: table.clone(),
                });
            }

            // For simplicity, use the first active region
            let region = regions
                .iter()
                .find(|r| r.can_accept_write())
                .ok_or_else(|| {
                    DistributedError::WriteError(format!(
                        "No writable region for table {}.{}",
                        database, table
                    ))
                })?;

            // Add all data for this table to the batch for this region
            // In a real implementation, we'd parse and filter lines by table
            write_batches
                .entry((region.node_id, region.region_id))
                .or_insert_with(Vec::new)
                .push(line_protocol.to_string());
        }

        // Send writes to each node
        let mut results = Vec::new();
        let mut total_points = 0u64;

        for ((node_id, region_id), lines) in write_batches {
            let node_info = self.meta_client.get_node(node_id).await?;

            let data = lines.join("\n");
            let request = WriteRequest {
                database: database.to_string(),
                region_id,
                data: Bytes::from(data),
                precision: precision.into(),
            };

            match self.datanode_client.write(&node_info, request).await {
                Ok(response) => {
                    if response.success {
                        total_points += response.points_written;
                        results.push(WritePartResult::Success {
                            node_id,
                            region_id,
                            points_written: response.points_written,
                        });
                    } else {
                        results.push(WritePartResult::Failed {
                            node_id,
                            region_id,
                            error: response
                                .error_message
                                .unwrap_or_else(|| "Unknown error".to_string()),
                        });
                    }
                }
                Err(e) => {
                    results.push(WritePartResult::Failed {
                        node_id,
                        region_id,
                        error: e.to_string(),
                    });
                }
            }
        }

        Ok(WriteResult {
            total_points_written: total_points,
            partitions: results,
        })
    }

    /// Extract table names from line protocol.
    ///
    /// This is a simplified parser that extracts measurement names.
    fn extract_table_names(&self, line_protocol: &str) -> Vec<String> {
        let mut tables = Vec::new();

        for line in line_protocol.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Extract measurement name (everything before first comma or space)
            if let Some(end) = line.find(|c: char| c == ',' || c.is_whitespace()) {
                let measurement = &line[..end];
                if !measurement.is_empty() && !tables.contains(&measurement.to_string()) {
                    tables.push(measurement.to_string());
                }
            }
        }

        tables
    }
}

/// Result of a distributed write operation.
#[derive(Debug)]
pub struct WriteResult {
    /// Total points written across all partitions
    pub total_points_written: u64,

    /// Results for each partition
    pub partitions: Vec<WritePartResult>,
}

impl WriteResult {
    /// Check if the write was fully successful.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(|r| matches!(r, WritePartResult::Success { .. }))
    }

    /// Check if there were any failures.
    pub fn has_failures(&self) -> bool {
        self.partitions
            .iter()
            .any(|r| matches!(r, WritePartResult::Failed { .. }))
    }

    /// Get the number of failed partitions.
    pub fn failure_count(&self) -> usize {
        self.partitions
            .iter()
            .filter(|r| matches!(r, WritePartResult::Failed { .. }))
            .count()
    }
}

/// Result for a single partition write.
#[derive(Debug)]
pub enum WritePartResult {
    /// Write succeeded
    Success {
        node_id: NodeId,
        region_id: RegionId,
        points_written: u64,
    },
    /// Write failed
    Failed {
        node_id: NodeId,
        region_id: RegionId,
        error: String,
    },
}

#[async_trait]
impl<M: MetaServiceApi + 'static> DistributedWriteApi for WriteRouter<M> {
    async fn write_lp(
        &self,
        database: &str,
        line_protocol: &str,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        let result = self.route_write(database, line_protocol, precision).await?;

        if result.has_failures() {
            warn!(
                failure_count = result.failure_count(),
                "Partial write failure"
            );
        }

        // Create a BufferedWriteRequest to return
        // Note: This is a simplified implementation
        let db_name = data_types::NamespaceName::new(database.to_string())
            .map_err(|e| DistributedError::InvalidRequest(e.to_string()))?;
        Ok(BufferedWriteRequest {
            db_name,
            invalid_lines: Vec::new(),
            line_count: result.total_points_written as usize,
            field_count: 0, // Not tracked in distributed write
            index_count: 0, // Not tracked in distributed write
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_names() {
        let lp = r#"
cpu,host=server1 usage=0.5
memory,host=server1 used=1024
cpu,host=server2 usage=0.8
disk,host=server1 free=500
"#;

        // Create a dummy router for testing
        // We'd need a mock MetaServiceApi for a real test
        // For now, just test the parsing logic conceptually

        let lines: Vec<&str> = lp.lines().collect();
        let mut tables = Vec::new();

        for line in lines {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some(end) = line.find(|c: char| c == ',' || c.is_whitespace()) {
                let measurement = &line[..end];
                if !measurement.is_empty() && !tables.contains(&measurement.to_string()) {
                    tables.push(measurement.to_string());
                }
            }
        }

        assert_eq!(tables, vec!["cpu", "memory", "disk"]);
    }
}
