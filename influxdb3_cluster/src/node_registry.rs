//! Node registry for cluster membership management

use crate::error::{Error, Result};
use crate::types::{NodeId, NodeInfo, NodeRole, NodeStatus};
use crate::meta_store::MetaStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Node registry manages cluster membership
#[derive(Debug)]
pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    meta_store: Arc<dyn MetaStore>,
}

impl NodeRegistry {
    pub fn new(meta_store: Arc<dyn MetaStore>) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            meta_store,
        }
    }

    /// Register a new node in the cluster
    pub async fn register_node(&self, node_info: NodeInfo) -> Result<()> {
        // Validate node information
        self.validate_node(&node_info)?;

        // Write to metadata store (ensures consistency via Raft/etcd)
        self.meta_store.put_node(node_info.clone()).await?;

        // Update local cache
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_info.node_id, node_info);

        Ok(())
    }

    /// Update node heartbeat
    pub async fn heartbeat(&self, node_id: NodeId) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.last_heartbeat_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;
            node.status = NodeStatus::Active;
        }
        Ok(())
    }

    /// Get active nodes, optionally filtered by role
    pub async fn get_active_nodes(&self, role: Option<NodeRole>) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .filter(|n| n.status == NodeStatus::Active)
            .filter(|n| role.map_or(true, |r| n.role == r))
            .cloned()
            .collect()
    }

    /// Get node by ID
    pub async fn get_node(&self, node_id: NodeId) -> Result<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes
            .get(&node_id)
            .cloned()
            .ok_or(Error::NodeNotFound { node_id })
    }

    /// Detect failed nodes based on heartbeat timeout
    pub async fn detect_failures(&self, timeout: Duration) -> Vec<NodeId> {
        let nodes = self.nodes.read().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        nodes
            .values()
            .filter(|n| {
                n.status == NodeStatus::Active
                    && now - n.last_heartbeat_nanos > timeout.as_nanos() as i64
            })
            .map(|n| n.node_id)
            .collect()
    }

    /// Validate node information
    fn validate_node(&self, node: &NodeInfo) -> Result<()> {
        if node.address.is_empty() {
            return Err(Error::InvalidNodeConfig {
                message: "Node address cannot be empty".to_string(),
            });
        }

        if node.grpc_port == 0 {
            return Err(Error::InvalidNodeConfig {
                message: "gRPC port must be specified".to_string(),
            });
        }

        Ok(())
    }

    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: NodeId) -> Result<()> {
        // Remove from metadata store
        self.meta_store.delete_node(node_id).await?;

        // Remove from local cache
        let mut nodes = self.nodes.write().await;
        nodes.remove(&node_id);

        Ok(())
    }

    /// List all nodes
    pub async fn list_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Sync local cache from meta store (load all nodes)
    pub async fn sync_from_meta(&self) -> Result<()> {
        let list = self.meta_store.list_nodes().await.map_err(|e| Error::MetaStoreError { source: e.into() })?;
        let mut nodes = self.nodes.write().await;
        nodes.clear();
        for n in list {
            nodes.insert(n.node_id, n);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;


