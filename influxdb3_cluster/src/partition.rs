//! Partition management and consistent hashing

use crate::{ClusterConfig, Component, NodeId, Result, membership::MembershipManager};
use async_trait::async_trait;
use hashbrown::HashMap;
use sha2::{Sha256, Digest};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use influxdb3_id::TableId;
use influxdb3_wal::{WriteBatch, TableChunks, Row};
use serde::{Serialize, Deserialize};

/// Manages data partitioning across cluster nodes using consistent hashing
#[derive(Debug)]
pub struct PartitionManager {
    config: ClusterConfig,
    membership: Arc<RwLock<MembershipManager>>,
    hash_ring: Arc<RwLock<ConsistentHashRing>>,
    running: Arc<RwLock<bool>>,
}

impl PartitionManager {
    /// Create a new partition manager
    pub async fn new(
        config: ClusterConfig,
        membership: Arc<RwLock<MembershipManager>>,
    ) -> Result<Self> {
        let hash_ring = ConsistentHashRing::new(config.partition.virtual_nodes);
        
        Ok(Self {
            config,
            membership,
            hash_ring: Arc::new(RwLock::new(hash_ring)),
            running: Arc::new(RwLock::new(false)),
        })
    }
    
    /// Get the primary node responsible for a partition key
    pub async fn get_node_for_key(&self, key: &str) -> Option<NodeId> {
        let ring = self.hash_ring.read().await;
        ring.get_node(key)
    }
    
    /// Get all nodes responsible for a partition key (including replicas)
    pub async fn get_nodes_for_key(&self, key: &str) -> Vec<NodeId> {
        let ring = self.hash_ring.read().await;
        ring.get_nodes(key, self.config.replication_factor)
    }

    /// Partition a write batch across multiple nodes based on series keys
    pub async fn partition_write_batch(&self, write_batch: &WriteBatch) -> Result<HashMap<NodeId, WriteBatch>> {
        let mut partitioned_batches: HashMap<NodeId, HashMap<TableId, TableChunks>> = HashMap::new();

        // Process each table in the write batch
        for (table_id, table_chunks) in &write_batch.table_chunks {
            // Process each chunk in the table
            for (chunk_time, table_chunk) in &table_chunks.chunk_time_to_chunk {
                // Group rows by their partition key (series key)
                let mut rows_by_partition: HashMap<String, Vec<Row>> = HashMap::new();

                for row in &table_chunk.rows {
                    let partition_key = self.extract_partition_key(row);
                    rows_by_partition.entry(partition_key).or_default().push(row.clone());
                }

                // Distribute rows to appropriate nodes
                for (partition_key, rows) in rows_by_partition {
                    let target_nodes = self.get_nodes_for_key(&partition_key).await;

                    for node_id in target_nodes {
                        let node_batches = partitioned_batches.entry(node_id).or_default();
                        let node_table_chunks = node_batches.entry(*table_id).or_default();
                        let node_chunk = node_table_chunks.chunk_time_to_chunk.entry(*chunk_time).or_default();

                        // Add rows to this node's chunk
                        for row in &rows {
                            node_chunk.rows.push(row.clone());
                            // Update time bounds
                            node_table_chunks.min_time = node_table_chunks.min_time.min(row.time);
                            node_table_chunks.max_time = node_table_chunks.max_time.max(row.time);
                        }
                    }
                }
            }
        }

        // Convert to WriteBatch format
        let mut result = HashMap::new();
        for (node_id, table_chunks_map) in partitioned_batches {
            let batch = WriteBatch::new(
                write_batch.catalog_sequence,
                write_batch.database_id,
                write_batch.database_name.clone(),
                table_chunks_map.into_iter().collect(),
            );
            result.insert(node_id, batch);
        }

        Ok(result)
    }

    /// Extract partition key from a row based on tag fields (series key)
    fn extract_partition_key(&self, row: &Row) -> String {
        // Extract tag fields to form the series key
        let mut tag_values = Vec::new();

        for field in &row.fields {
            match &field.value {
                influxdb3_wal::FieldData::Tag(tag_value) => {
                    tag_values.push(format!("{}={}", field.id, tag_value));
                }
                influxdb3_wal::FieldData::Key(key_value) => {
                    tag_values.push(format!("{}={}", field.id, key_value));
                }
                _ => {} // Skip non-tag fields for partitioning
            }
        }

        // Sort to ensure consistent ordering
        tag_values.sort();
        tag_values.join(",")
    }
    
    /// Add a node to the hash ring
    pub async fn add_node(&self, node_id: NodeId) -> Result<()> {
        let mut ring = self.hash_ring.write().await;
        ring.add_node(node_id);
        Ok(())
    }
    
    /// Remove a node from the hash ring
    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        let mut ring = self.hash_ring.write().await;
        ring.remove_node(node_id);
        Ok(())
    }

    /// Get partition assignments for rebalancing
    pub async fn get_partition_assignments(&self) -> HashMap<String, Vec<NodeId>> {
        let ring = self.hash_ring.read().await;
        let mut assignments = HashMap::new();

        // Sample partition keys to understand current distribution
        let sample_keys = self.generate_sample_partition_keys();

        for key in sample_keys {
            let nodes = ring.get_nodes(&key, self.config.replication_factor);
            assignments.insert(key, nodes);
        }

        assignments
    }

    /// Generate sample partition keys for rebalancing analysis
    fn generate_sample_partition_keys(&self) -> Vec<String> {
        // Generate a set of sample keys to understand partition distribution
        let mut keys = Vec::new();

        // Generate keys based on common patterns
        for i in 0..1000 {
            keys.push(format!("sample_key_{}", i));
        }

        keys
    }

    /// Calculate which partitions need to be moved during rebalancing
    pub async fn calculate_rebalancing_plan(&self, old_assignments: HashMap<String, Vec<NodeId>>) -> RebalancingPlan {
        let new_assignments = self.get_partition_assignments().await;
        let mut moves = Vec::new();

        for (partition_key, new_nodes) in &new_assignments {
            if let Some(old_nodes) = old_assignments.get(partition_key) {
                // Find nodes that no longer should have this partition
                for old_node in old_nodes {
                    if !new_nodes.contains(old_node) {
                        // Find a new node that should have this partition
                        for new_node in new_nodes {
                            if !old_nodes.contains(new_node) {
                                moves.push(PartitionMove {
                                    partition_key: partition_key.clone(),
                                    from_node: old_node.clone(),
                                    to_node: new_node.clone(),
                                });
                                break;
                            }
                        }
                    }
                }
            }
        }

        RebalancingPlan { moves }
    }
    
    /// Rebalance partitions based on current membership
    pub async fn rebalance(&self) -> Result<()> {
        let membership = self.membership.read().await;
        let active_nodes = membership.get_active_nodes();
        
        let mut ring = self.hash_ring.write().await;
        ring.clear();
        
        for node in active_nodes {
            ring.add_node(node.id);
        }
        
        observability_deps::tracing::info!(
            node_count = ring.node_count(),
            "Partition ring rebalanced"
        );
        
        Ok(())
    }

    /// Get routing information for a write operation
    pub async fn get_write_routing(&self, partition_key: &str) -> Option<WriteRouting> {
        let nodes = self.get_nodes_for_key(partition_key).await;
        if nodes.is_empty() {
            return None;
        }

        let primary_node = nodes[0].clone();
        let replica_nodes = nodes[1..].to_vec();

        Some(WriteRouting {
            primary_node,
            replica_nodes,
            partition_key: partition_key.to_string(),
        })
    }

    /// Get partition statistics
    pub async fn get_partition_stats(&self) -> PartitionStats {
        let assignments = self.get_partition_assignments().await;
        let mut partitions_per_node: HashMap<NodeId, usize> = HashMap::new();

        for nodes in assignments.values() {
            for node in nodes {
                *partitions_per_node.entry(node.clone()).or_insert(0) += 1;
            }
        }

        let total_partitions = assignments.len();
        let node_count = partitions_per_node.len();
        let average_partitions_per_node = if node_count > 0 {
            total_partitions as f64 / node_count as f64
        } else {
            0.0
        };

        // Calculate standard deviation
        let variance = if node_count > 0 {
            let sum_squared_diff: f64 = partitions_per_node
                .values()
                .map(|&count| {
                    let diff = count as f64 - average_partitions_per_node;
                    diff * diff
                })
                .sum();
            sum_squared_diff / node_count as f64
        } else {
            0.0
        };
        let partition_distribution_stddev = variance.sqrt();

        PartitionStats {
            partitions_per_node,
            total_partitions,
            average_partitions_per_node,
            partition_distribution_stddev,
        }
    }
}

#[async_trait]
impl Component for PartitionManager {
    async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        
        *running = true;
        
        // Initial rebalance
        self.rebalance().await?;
        
        observability_deps::tracing::info!("Partition manager started");
        Ok(())
    }
    
    async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        
        *running = false;
        
        observability_deps::tracing::info!("Partition manager stopped");
        Ok(())
    }
}

/// Consistent hash ring implementation
#[derive(Debug)]
pub struct ConsistentHashRing {
    virtual_nodes: usize,
    ring: BTreeMap<u64, NodeId>,
    nodes: HashMap<NodeId, Vec<u64>>,
}

impl ConsistentHashRing {
    /// Create a new consistent hash ring
    pub fn new(virtual_nodes: usize) -> Self {
        Self {
            virtual_nodes,
            ring: BTreeMap::new(),
            nodes: HashMap::new(),
        }
    }
    
    /// Add a node to the ring
    pub fn add_node(&mut self, node_id: NodeId) {
        if self.nodes.contains_key(&node_id) {
            return;
        }
        
        let mut virtual_node_hashes = Vec::new();
        
        for i in 0..self.virtual_nodes {
            let virtual_key = format!("{}:{}", node_id, i);
            let hash = self.hash_key(&virtual_key);
            self.ring.insert(hash, node_id.clone());
            virtual_node_hashes.push(hash);
        }
        
        self.nodes.insert(node_id, virtual_node_hashes);
    }
    
    /// Remove a node from the ring
    pub fn remove_node(&mut self, node_id: &NodeId) {
        if let Some(virtual_hashes) = self.nodes.remove(node_id) {
            for hash in virtual_hashes {
                self.ring.remove(&hash);
            }
        }
    }
    
    /// Clear all nodes from the ring
    pub fn clear(&mut self) {
        self.ring.clear();
        self.nodes.clear();
    }
    
    /// Get the primary node for a key
    pub fn get_node(&self, key: &str) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }
        
        let hash = self.hash_key(key);
        
        // Find the first node with hash >= key hash
        if let Some((_, node_id)) = self.ring.range(hash..).next() {
            Some(node_id.clone())
        } else {
            // Wrap around to the first node
            self.ring.values().next().cloned()
        }
    }
    
    /// Get multiple nodes for a key (for replication)
    pub fn get_nodes(&self, key: &str, count: usize) -> Vec<NodeId> {
        if self.ring.is_empty() {
            return Vec::new();
        }
        
        let hash = self.hash_key(key);
        let mut result = Vec::new();
        let mut seen_nodes = std::collections::HashSet::new();
        
        // Start from the first node >= hash
        let iter = self.ring.range(hash..).chain(self.ring.range(..hash));
        
        for (_, node_id) in iter {
            if !seen_nodes.contains(node_id) {
                result.push(node_id.clone());
                seen_nodes.insert(node_id.clone());
                
                if result.len() >= count {
                    break;
                }
            }
        }
        
        result
    }
    
    /// Get the number of nodes in the ring
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
    
    /// Hash a key to a position on the ring
    fn hash_key(&self, key: &str) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();
        
        // Take the first 8 bytes and convert to u64
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&result[..8]);
        u64::from_be_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;
    
    #[test]
    fn test_consistent_hash_ring() {
        let mut ring = ConsistentHashRing::new(3);
        
        let node1 = NodeId::new();
        let node2 = NodeId::new();
        let node3 = NodeId::new();
        
        // Add nodes
        ring.add_node(node1.clone());
        ring.add_node(node2.clone());
        ring.add_node(node3.clone());
        
        assert_eq!(ring.node_count(), 3);
        
        // Test key assignment
        let key = "test_key";
        let assigned_node = ring.get_node(key);
        assert!(assigned_node.is_some());
        
        // Test replication
        let replica_nodes = ring.get_nodes(key, 2);
        assert_eq!(replica_nodes.len(), 2);
        
        // Remove a node
        ring.remove_node(&node1);
        assert_eq!(ring.node_count(), 2);
        
        // Key should still be assigned
        let new_assigned_node = ring.get_node(key);
        assert!(new_assigned_node.is_some());
    }
    
    #[test]
    fn test_empty_ring() {
        let ring = ConsistentHashRing::new(3);
        assert!(ring.get_node("test").is_none());
        assert!(ring.get_nodes("test", 3).is_empty());
    }

    #[tokio::test]
    async fn test_partition_write_batch() {
        use influxdb3_wal::{WriteBatch, TableChunks, TableChunk, Row, Field, FieldData};
        use influxdb3_id::{DbId, TableId, ColumnId};
        use indexmap::IndexMap;
        use std::sync::Arc;

        let config = crate::ClusterConfig::test_config();
        let membership = Arc::new(RwLock::new(crate::membership::MembershipManager::new(config.node_id.clone())));
        let partition_manager = PartitionManager::new(config, membership).await.unwrap();

        // Add some nodes
        let node1 = NodeId::new();
        let node2 = NodeId::new();
        partition_manager.add_node(node1.clone()).await.unwrap();
        partition_manager.add_node(node2.clone()).await.unwrap();

        // Create a test write batch
        let mut table_chunks = IndexMap::new();
        let mut chunk_map = HashMap::new();

        let row1 = Row {
            time: 1000,
            fields: vec![
                Field::new(ColumnId::new(1), FieldData::Tag("host1".to_string())),
                Field::new(ColumnId::new(2), FieldData::Integer(100)),
            ],
        };

        let row2 = Row {
            time: 2000,
            fields: vec![
                Field::new(ColumnId::new(1), FieldData::Tag("host2".to_string())),
                Field::new(ColumnId::new(2), FieldData::Integer(200)),
            ],
        };

        chunk_map.insert(0, TableChunk { rows: vec![row1, row2] });

        let table_chunk = TableChunks {
            min_time: 1000,
            max_time: 2000,
            chunk_time_to_chunk: chunk_map,
        };

        table_chunks.insert(TableId::new(1), table_chunk);

        let write_batch = WriteBatch::new(
            1,
            DbId::new(1),
            Arc::from("test_db"),
            table_chunks,
        );

        // Partition the write batch
        let partitioned = partition_manager.partition_write_batch(&write_batch).await.unwrap();

        // Should have partitioned data across nodes
        assert!(!partitioned.is_empty());
        assert!(partitioned.len() <= 2); // At most 2 nodes since we only have 2
    }
}

/// Plan for rebalancing partitions across nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingPlan {
    pub moves: Vec<PartitionMove>,
}

/// Represents a single partition move during rebalancing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMove {
    pub partition_key: String,
    pub from_node: NodeId,
    pub to_node: NodeId,
}

/// Routing information for a write operation
#[derive(Debug, Clone)]
pub struct WriteRouting {
    /// Primary node for the write
    pub primary_node: NodeId,
    /// Replica nodes for the write
    pub replica_nodes: Vec<NodeId>,
    /// Partition key used for routing
    pub partition_key: String,
}

/// Statistics about partition distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStats {
    /// Number of partitions per node
    pub partitions_per_node: HashMap<NodeId, usize>,
    /// Total number of partitions
    pub total_partitions: usize,
    /// Average partitions per node
    pub average_partitions_per_node: f64,
    /// Standard deviation of partition distribution
    pub partition_distribution_stddev: f64,
}
