//! Partition management and consistent hashing

use crate::{ClusterConfig, Component, NodeId, Result, membership::MembershipManager};
use async_trait::async_trait;
use hashbrown::HashMap;
use sha2::{Sha256, Digest};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{WriteBatch, TableChunks, Row, Field};
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
}
