//! Distributed write coordination
//! 
//! This module handles coordinating writes across multiple nodes in the cluster,
//! including routing, replication, and consistency guarantees.

use crate::{ClusterConfig, NodeId, Result, partition::PartitionManager};

use influxdb3_wal::{WriteBatch, WalOp};
use std::sync::Arc;
use tokio::sync::RwLock;
use hashbrown::HashMap;
use serde::{Serialize, Deserialize};
use observability_deps::tracing::{info, warn, error};
use reqwest;

/// Coordinates distributed writes across cluster nodes
#[derive(Debug)]
pub struct WriteCoordinator {
    config: ClusterConfig,
    partition_manager: Arc<PartitionManager>,
    running: Arc<RwLock<bool>>,
    node_endpoints: Arc<RwLock<HashMap<NodeId, String>>>,
    http_client: reqwest::Client,
}

impl WriteCoordinator {
    /// Create a new write coordinator
    pub async fn new(
        config: ClusterConfig,
        partition_manager: Arc<PartitionManager>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            partition_manager,
            running: Arc::new(RwLock::new(false)),
            node_endpoints: Arc::new(RwLock::new(HashMap::new())),
            http_client: reqwest::Client::new(),
        })
    }
    
    /// Coordinate a distributed write operation
    pub async fn coordinate_write(&self, write_batch: WriteBatch) -> Result<WriteResult> {
        // Partition the write batch across nodes
        let partitioned_batches = self.partition_manager
            .partition_write_batch(&write_batch)
            .await?;
        
        let mut _write_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut node_results = HashMap::new();
        
        // Send writes to each node
        for (node_id, batch) in partitioned_batches {
            let write_op = WalOp::Write(batch);
            
            // For now, simulate the write operation
            // In a real implementation, this would send the write to the remote node
            let result = self.execute_write_on_node(&node_id, write_op).await?;
            node_results.insert(node_id, result);
        }
        
        // Determine overall write result based on consistency level
        let success_count = node_results.values().filter(|r| r.success).count();
        let required_successes = self.calculate_required_successes(node_results.len());
        
        let overall_success = success_count >= required_successes;
        
        Ok(WriteResult {
            success: overall_success,
            node_results,
            consistency_level: self.config.consistency_level,
        })
    }
    
    /// Execute a write operation on a specific node
    async fn execute_write_on_node(&self, node_id: &NodeId, _write_op: WalOp) -> Result<NodeWriteResult> {
        // TODO: Implement actual network communication to remote nodes
        // For now, simulate a successful write
        
        observability_deps::tracing::debug!(
            node_id = %node_id,
            "Executing write on node"
        );
        
        // Simulate some processing time
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        
        Ok(NodeWriteResult {
            node_id: node_id.clone(),
            success: true,
            error: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        })
    }
    
    /// Calculate the number of successful writes required based on consistency level
    fn calculate_required_successes(&self, total_nodes: usize) -> usize {
        match self.config.consistency_level {
            ConsistencyLevel::One => 1,
            ConsistencyLevel::Quorum => (total_nodes / 2) + 1,
            ConsistencyLevel::All => total_nodes,
        }
    }
    
    /// Start the write coordinator
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        
        *running = true;
        
        observability_deps::tracing::info!("Write coordinator started");
        Ok(())
    }
    
    /// Stop the write coordinator
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        
        *running = false;
        
        observability_deps::tracing::info!("Write coordinator stopped");
        Ok(())
    }

    /// Update node endpoints for HTTP routing
    pub async fn update_node_endpoints(&self, endpoints: std::collections::HashMap<NodeId, String>) {
        let mut node_endpoints = self.node_endpoints.write().await;
        // Convert std::collections::HashMap to hashbrown::HashMap
        *node_endpoints = endpoints.into_iter().collect();
        info!(
            endpoint_count = node_endpoints.len(),
            "Updated write coordinator node endpoints"
        );
    }

    /// Route a Line Protocol write request to the appropriate node(s)
    pub async fn route_line_protocol_write(
        &self,
        database: &str,
        data: &[u8]
    ) -> Result<HttpWriteResponse> {
        let start_time = std::time::Instant::now();

        // Parse line protocol data and partition by series key
        let partitioned_data = self.partition_line_protocol_data(database, data).await?;

        if partitioned_data.is_empty() {
            return Ok(HttpWriteResponse {
                success: false,
                nodes_written: 0,
                nodes_failed: 0,
                error: Some("No valid data to write".to_string()),
                target_nodes: Vec::new(),
                execution_time_ms: start_time.elapsed().as_millis() as u64,
            });
        }

        // Get node endpoints
        let endpoints = self.node_endpoints.read().await;

        // Execute writes to target nodes
        let mut successful_writes = 0;
        let mut failed_writes = 0;
        let mut all_target_nodes = std::collections::HashSet::new();

        info!(
            database = %database,
            total_partitions = partitioned_data.len(),
            "Routing write request to cluster nodes"
        );

        for (node_id, node_data) in partitioned_data {
            all_target_nodes.insert(node_id.clone());

            if let Some(endpoint) = endpoints.get(&node_id) {
                match self.write_to_node_http(&node_id, endpoint, database, &node_data).await {
                    Ok(()) => {
                        successful_writes += 1;
                        info!(
                            node_id = %node_id,
                            endpoint = %endpoint,
                            data_size = node_data.len(),
                            "Successfully wrote to node"
                        );
                    }
                    Err(e) => {
                        failed_writes += 1;
                        warn!(
                            node_id = %node_id,
                            endpoint = %endpoint,
                            error = %e,
                            "Failed to write to node"
                        );
                    }
                }
            } else {
                failed_writes += 1;
                warn!(
                    node_id = %node_id,
                    "No endpoint available for node"
                );
            }
        }

        // For distributed writes, we consider success if at least one node succeeded
        let success = successful_writes > 0;
        let execution_time = start_time.elapsed().as_millis() as u64;

        info!(
            success = success,
            successful_writes = successful_writes,
            failed_writes = failed_writes,
            total_partitions = all_target_nodes.len(),
            execution_time_ms = execution_time,
            "Completed distributed write operation"
        );

        Ok(HttpWriteResponse {
            success,
            nodes_written: successful_writes,
            nodes_failed: failed_writes,
            error: if success { None } else { Some("Failed to write to any nodes".to_string()) },
            target_nodes: all_target_nodes.into_iter().collect(),
            execution_time_ms: execution_time,
        })
    }

    /// Partition Line Protocol data by series key and route to appropriate nodes
    async fn partition_line_protocol_data(
        &self,
        database: &str,
        data: &[u8]
    ) -> Result<std::collections::HashMap<NodeId, Vec<u8>>> {
        let mut partitioned_data: std::collections::HashMap<NodeId, Vec<String>> = std::collections::HashMap::new();

        if let Ok(data_str) = std::str::from_utf8(data) {
            for line in data_str.lines() {
                if line.trim().is_empty() {
                    continue;
                }

                // Extract series key from this line (measurement + tags)
                let series_key = self.extract_series_key_from_line(line);

                // Get the primary node for this series key (first node in the list)
                let target_nodes = self.partition_manager.get_nodes_for_key(&series_key).await;
                if let Some(primary_node) = target_nodes.first() {
                    partitioned_data
                        .entry(primary_node.clone())
                        .or_default()
                        .push(line.to_string());
                }
            }
        }

        // Convert Vec<String> to Vec<u8> for each node
        let mut result = std::collections::HashMap::new();
        for (node_id, lines) in partitioned_data {
            let combined_data = lines.join("\n");
            result.insert(node_id, combined_data.into_bytes());
        }

        Ok(result)
    }

    /// Extract series key from a single line protocol line
    fn extract_series_key_from_line(&self, line: &str) -> String {
        // Parse line protocol: measurement[,tag_set] field_set [timestamp]
        if let Some(space_pos) = line.find(' ') {
            let measurement_and_tags = &line[..space_pos];

            // If there are tags, use measurement + tags as series key
            if measurement_and_tags.contains(',') {
                return measurement_and_tags.to_string();
            } else {
                // No tags, just use measurement
                return measurement_and_tags.to_string();
            }
        }

        // Fallback: use the whole line (shouldn't happen with valid line protocol)
        line.to_string()
    }

    /// Extract partition key from Line Protocol data (legacy method, kept for compatibility)
    fn extract_partition_key_from_lp(&self, database: &str, data: &[u8]) -> String {
        // Try to extract measurement name from line protocol data
        if let Ok(data_str) = std::str::from_utf8(data) {
            // Parse first line to get measurement name
            if let Some(first_line) = data_str.lines().next() {
                if let Some(measurement) = first_line.split(',').next() {
                    if let Some(space_pos) = measurement.find(' ') {
                        return measurement[..space_pos].to_string();
                    } else {
                        return measurement.to_string();
                    }
                }
            }
        }

        // Fallback to database name
        database.to_string()
    }

    /// Execute HTTP write to a specific node
    async fn write_to_node_http(
        &self,
        _node_id: &NodeId,
        endpoint: &str,
        database: &str,
        data: &[u8]
    ) -> Result<()> {
        let url = format!("{}/api/v3/write_lp_internal?db={}", endpoint, database);

        let response = self.http_client
            .post(&url)
            .header("Content-Type", "text/plain")
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| crate::error::ClusterError::Network(crate::error::NetworkError::RequestFailed(e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            Err(crate::error::ClusterError::Generic(format!(
                "HTTP write failed with status {}: {}", status, error_text
            )))
        }
    }
}

/// Consistency levels for distributed writes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// Write succeeds when acknowledged by at least one node
    One,
    /// Write succeeds when acknowledged by a majority of nodes
    Quorum,
    /// Write succeeds when acknowledged by all nodes
    All,
}

impl Default for ConsistencyLevel {
    fn default() -> Self {
        ConsistencyLevel::Quorum
    }
}

/// Result of a distributed write operation
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Whether the overall write operation succeeded
    pub success: bool,
    /// Results from individual nodes
    pub node_results: HashMap<NodeId, NodeWriteResult>,
    /// Consistency level used for the write
    pub consistency_level: ConsistencyLevel,
}

/// HTTP write response for Line Protocol writes
#[derive(Debug, Clone)]
pub struct HttpWriteResponse {
    /// Whether the write operation succeeded
    pub success: bool,
    /// Number of nodes successfully written to
    pub nodes_written: usize,
    /// Number of nodes that failed
    pub nodes_failed: usize,
    /// Error message if any
    pub error: Option<String>,
    /// Target nodes for the write
    pub target_nodes: Vec<NodeId>,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

/// Result of a write operation on a single node
#[derive(Debug, Clone)]
pub struct NodeWriteResult {
    /// The node that processed the write
    pub node_id: NodeId,
    /// Whether the write succeeded on this node
    pub success: bool,
    /// Error message if the write failed
    pub error: Option<String>,
    /// Timestamp when the write completed
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::MembershipManager;

    
    #[tokio::test]
    async fn test_write_coordinator_creation() {
        let config = ClusterConfig::test_config();
        let membership = Arc::new(RwLock::new(MembershipManager::new(config.node_id.clone())));
        let partition_manager = Arc::new(PartitionManager::new(config.clone(), membership).await.unwrap());
        
        let coordinator = WriteCoordinator::new(config, partition_manager).await.unwrap();
        
        coordinator.start().await.unwrap();
        coordinator.stop().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_consistency_level_calculation() {
        let config = ClusterConfig::test_config();
        let membership = Arc::new(RwLock::new(MembershipManager::new(config.node_id.clone())));
        let partition_manager = Arc::new(PartitionManager::new(config.clone(), membership).await.unwrap());
        
        let coordinator = WriteCoordinator::new(config, partition_manager).await.unwrap();
        
        // Test different consistency levels
        assert_eq!(coordinator.calculate_required_successes(3), 2); // Quorum of 3
        assert_eq!(coordinator.calculate_required_successes(5), 3); // Quorum of 5
        assert_eq!(coordinator.calculate_required_successes(1), 1); // Quorum of 1
    }
}
