//! Cluster membership management

use crate::{Node, NodeId, NodeState, Result, error::MembershipError};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Manages the membership of nodes in the cluster
pub struct MembershipManager {
    /// This node's ID
    local_node_id: NodeId,
    /// Map of all known nodes in the cluster
    nodes: Arc<DashMap<NodeId, Node>>,
    /// Current membership view version
    version: Arc<RwLock<u64>>,
    /// Membership change listeners
    listeners: Arc<RwLock<Vec<Arc<dyn MembershipListener>>>>,
}

impl std::fmt::Debug for MembershipManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MembershipManager")
            .field("local_node_id", &self.local_node_id)
            .field("nodes", &self.nodes.len())
            .field("version", &"<version>")
            .field("listeners", &self.listeners.try_read().map(|l| l.len()).unwrap_or(0))
            .finish()
    }
}

impl MembershipManager {
    /// Create a new membership manager
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            nodes: Arc::new(DashMap::new()),
            version: Arc::new(RwLock::new(0)),
            listeners: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    /// Add a node to the cluster membership
    pub async fn add_node(&self, node: Node) -> Result<()> {
        let node_id = node.id.clone();

        // Check if node already exists
        if self.nodes.contains_key(&node_id) {
            return Err(MembershipError::NodeAlreadyExists(node_id.to_string()).into());
        }

        // Mark the node as active when adding it to the cluster
        let mut active_node = node.clone();
        active_node.mark_active();

        // Add the node
        self.nodes.insert(node_id.clone(), active_node.clone());

        // Increment version
        {
            let mut version = self.version.write().await;
            *version += 1;
        }

        // Notify listeners
        self.notify_listeners(MembershipEvent::NodeJoined(active_node)).await;

        observability_deps::tracing::info!(
            node_id = %node_id,
            "Node added to cluster membership"
        );

        Ok(())
    }
    
    /// Remove a node from the cluster membership
    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        let node = self.nodes.remove(node_id)
            .ok_or_else(|| MembershipError::NodeNotInCluster(node_id.to_string()))?
            .1;
        
        // Increment version
        {
            let mut version = self.version.write().await;
            *version += 1;
        }
        
        // Notify listeners
        self.notify_listeners(MembershipEvent::NodeLeft(node)).await;
        
        observability_deps::tracing::info!(
            node_id = %node_id,
            "Node removed from cluster membership"
        );
        
        Ok(())
    }
    
    /// Update a node's information
    pub async fn update_node(&self, node: Node) -> Result<()> {
        let node_id = node.id.clone();
        
        // Check if node exists
        if !self.nodes.contains_key(&node_id) {
            return Err(MembershipError::NodeNotInCluster(node_id.to_string()).into());
        }
        
        let old_node = self.nodes.get(&node_id).unwrap().clone();
        self.nodes.insert(node_id.clone(), node.clone());
        
        // Increment version if state changed
        if old_node.status.state != node.status.state {
            let mut version = self.version.write().await;
            *version += 1;
            
            // Notify listeners of state change
            self.notify_listeners(MembershipEvent::NodeStateChanged {
                node: node.clone(),
                old_state: old_node.status.state,
                new_state: node.status.state,
            }).await;
        }
        
        Ok(())
    }
    
    /// Get a node by ID
    pub fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        self.nodes.get(node_id).map(|entry| entry.clone())
    }
    
    /// Get all nodes in the cluster
    pub fn get_all_nodes(&self) -> Vec<Node> {
        self.nodes.iter().map(|entry| entry.value().clone()).collect()
    }
    
    /// Get all active nodes
    pub fn get_active_nodes(&self) -> Vec<Node> {
        self.nodes
            .iter()
            .filter(|entry| entry.value().status.is_available())
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    /// Get the number of active nodes
    pub fn active_node_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|entry| entry.value().status.is_available())
            .count()
    }
    
    /// Check if this node is the leader (simple leader election based on node ID)
    pub async fn is_leader(&self, node_id: &NodeId) -> bool {
        let active_nodes = self.get_active_nodes();
        if active_nodes.is_empty() {
            return false;
        }
        
        // Simple leader election: node with smallest ID among active nodes
        let leader_id = active_nodes
            .iter()
            .min_by_key(|node| &node.id)
            .map(|node| &node.id);
        
        leader_id == Some(node_id)
    }
    
    /// Get the current membership view
    pub async fn get_membership(&self) -> ClusterMembership {
        let version = *self.version.read().await;
        let nodes = self.get_all_nodes();
        
        ClusterMembership {
            version,
            nodes: nodes.into_iter().map(|node| (node.id.clone(), node)).collect(),
            local_node_id: self.local_node_id.clone(),
        }
    }
    
    /// Add a membership change listener
    pub async fn add_listener(&self, listener: Arc<dyn MembershipListener>) {
        let mut listeners = self.listeners.write().await;
        listeners.push(listener);
    }

    /// Get node endpoints for HTTP communication
    pub async fn get_node_endpoints(&self) -> std::collections::HashMap<NodeId, String> {
        let mut endpoints = std::collections::HashMap::new();
        for entry in self.nodes.iter() {
            let node = entry.value();
            if node.status.is_available() {
                if let Some(http_endpoint) = &node.http_endpoint {
                    endpoints.insert(node.id.clone(), http_endpoint.clone());
                } else {
                    // Fallback: Convert cluster address to HTTP endpoint
                    // Assume HTTP port is cluster port + 1000 (this is a simplification)
                    let http_port = node.addr.port() + 1000;
                    let endpoint = format!("http://{}:{}", node.addr.ip(), http_port);
                    endpoints.insert(node.id.clone(), endpoint);
                }
            }
        }
        endpoints
    }
    
    /// Remove nodes that have been down for too long
    pub async fn cleanup_failed_nodes(&self, timeout: Duration) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let mut to_remove = Vec::new();
        
        for entry in self.nodes.iter() {
            let node = entry.value();
            if matches!(node.status.state, NodeState::Down | NodeState::Left) {
                let age = Duration::from_secs(now.as_secs() - node.status.last_seen);
                if age > timeout {
                    to_remove.push(node.id.clone());
                }
            }
        }
        
        for node_id in to_remove {
            self.remove_node(&node_id).await?;
        }
        
        Ok(())
    }
    
    /// Notify all listeners of a membership event
    async fn notify_listeners(&self, event: MembershipEvent) {
        let listeners = self.listeners.read().await;
        for listener in listeners.iter() {
            listener.on_membership_change(&event).await;
        }
    }
}

/// A snapshot of the cluster membership at a specific point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMembership {
    /// Version number of this membership view
    pub version: u64,
    /// Map of all nodes in the cluster
    pub nodes: HashMap<NodeId, Node>,
    /// ID of the local node
    pub local_node_id: NodeId,
}

impl ClusterMembership {
    /// Get all active nodes
    pub fn active_nodes(&self) -> Vec<&Node> {
        self.nodes
            .values()
            .filter(|node| node.status.is_available())
            .collect()
    }
    
    /// Get the number of active nodes
    pub fn active_count(&self) -> usize {
        self.active_nodes().len()
    }
    
    /// Check if the cluster has enough nodes for the given replication factor
    pub fn has_quorum(&self, replication_factor: usize) -> bool {
        self.active_count() >= replication_factor
    }
}

/// Events that can occur in cluster membership
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    /// A new node joined the cluster
    NodeJoined(Node),
    /// A node left the cluster
    NodeLeft(Node),
    /// A node's state changed
    NodeStateChanged {
        node: Node,
        old_state: NodeState,
        new_state: NodeState,
    },
}

/// Trait for listening to membership changes
#[async_trait::async_trait]
pub trait MembershipListener: Send + Sync {
    /// Called when a membership change occurs
    async fn on_membership_change(&self, event: &MembershipEvent);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;
    use std::net::SocketAddr;
    
    #[tokio::test]
    async fn test_membership_basic_operations() {
        let local_id = NodeId::new();
        let manager = MembershipManager::new(local_id.clone());
        
        // Test adding a node
        let node_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:8300".parse().unwrap();
        let node = Node::new(node_id.clone(), addr);
        
        manager.add_node(node.clone()).await.unwrap();
        
        // Test getting the node
        let retrieved = manager.get_node(&node_id).unwrap();
        assert_eq!(retrieved.id, node_id);
        
        // Test updating the node
        let mut updated_node = node.clone();
        updated_node.mark_active();
        manager.update_node(updated_node).await.unwrap();
        
        let retrieved = manager.get_node(&node_id).unwrap();
        assert_eq!(retrieved.status.state, NodeState::Active);
        
        // Test removing the node
        manager.remove_node(&node_id).await.unwrap();
        assert!(manager.get_node(&node_id).is_none());
    }
    
    #[tokio::test]
    async fn test_membership_active_nodes() {
        let local_id = NodeId::new();
        let manager = MembershipManager::new(local_id.clone());
        
        // Add multiple nodes with different states
        for i in 0..5 {
            let node_id = NodeId::new();
            let addr: SocketAddr = format!("127.0.0.1:830{}", i).parse().unwrap();
            let mut node = Node::new(node_id, addr);
            
            if i < 3 {
                node.mark_active();
            } else {
                node.mark_down();
            }
            
            manager.add_node(node).await.unwrap();
        }
        
        assert_eq!(manager.active_node_count(), 3);
        assert_eq!(manager.get_active_nodes().len(), 3);
    }
}
