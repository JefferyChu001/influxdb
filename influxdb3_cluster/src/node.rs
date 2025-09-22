//! Node representation and management

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Unique identifier for a cluster node
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(Uuid);

impl NodeId {
    /// Create a new random node ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    
    /// Create a node ID from a UUID
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
    
    /// Get the underlying UUID
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
    
    /// Create a node ID from a string representation
    pub fn from_string(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }
    
    /// Get the string representation
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for NodeId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<NodeId> for Uuid {
    fn from(node_id: NodeId) -> Self {
        node_id.0
    }
}

/// Current state of a node in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is joining the cluster
    Joining,
    /// Node is active and healthy
    Active,
    /// Node is suspected to be down
    Suspected,
    /// Node is confirmed to be down
    Down,
    /// Node is leaving the cluster gracefully
    Leaving,
    /// Node has left the cluster
    Left,
}

impl Default for NodeState {
    fn default() -> Self {
        NodeState::Joining
    }
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Joining => write!(f, "joining"),
            NodeState::Active => write!(f, "active"),
            NodeState::Suspected => write!(f, "suspected"),
            NodeState::Down => write!(f, "down"),
            NodeState::Leaving => write!(f, "leaving"),
            NodeState::Left => write!(f, "left"),
        }
    }
}

/// Health status of a node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    /// Current state of the node
    pub state: NodeState,
    /// Last time the node was seen
    pub last_seen: u64,
    /// Number of consecutive failed health checks
    pub failed_checks: usize,
    /// Additional metadata about the node
    pub metadata: HashMap<String, String>,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self {
            state: NodeState::default(),
            last_seen: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            failed_checks: 0,
            metadata: HashMap::new(),
        }
    }
}

impl NodeStatus {
    /// Create a new node status
    pub fn new(state: NodeState) -> Self {
        Self {
            state,
            ..Default::default()
        }
    }
    
    /// Update the last seen timestamp
    pub fn update_last_seen(&mut self) {
        self.last_seen = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Check if the node is considered healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self.state, NodeState::Active | NodeState::Joining)
    }
    
    /// Check if the node is available for operations
    pub fn is_available(&self) -> bool {
        matches!(self.state, NodeState::Active)
    }
    
    /// Get the age of the last seen timestamp
    pub fn age(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Duration::from_secs(now.saturating_sub(self.last_seen))
    }
}

/// Complete information about a cluster node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier for the node
    pub id: NodeId,
    /// Network address for cluster communication
    pub addr: SocketAddr,
    /// Current status of the node
    pub status: NodeStatus,
    /// Node capabilities and configuration
    pub capabilities: NodeCapabilities,
}

impl Node {
    /// Create a new node
    pub fn new(id: NodeId, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            status: NodeStatus::default(),
            capabilities: NodeCapabilities::default(),
        }
    }
    
    /// Update the node's status
    pub fn update_status(&mut self, status: NodeStatus) {
        self.status = status;
    }
    
    /// Mark the node as active
    pub fn mark_active(&mut self) {
        self.status.state = NodeState::Active;
        self.status.update_last_seen();
        self.status.failed_checks = 0;
    }
    
    /// Mark the node as suspected
    pub fn mark_suspected(&mut self) {
        self.status.state = NodeState::Suspected;
        self.status.failed_checks += 1;
    }
    
    /// Mark the node as down
    pub fn mark_down(&mut self) {
        self.status.state = NodeState::Down;
    }
    
    /// Check if this node can handle writes
    pub fn can_write(&self) -> bool {
        self.status.is_available() && self.capabilities.can_write
    }
    
    /// Check if this node can handle reads
    pub fn can_read(&self) -> bool {
        self.status.is_available() && self.capabilities.can_read
    }
}

/// Node capabilities and configuration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Whether this node can handle write operations
    pub can_write: bool,
    /// Whether this node can handle read operations
    pub can_read: bool,
    /// Whether this node can participate in consensus
    pub can_vote: bool,
    /// Maximum number of partitions this node can handle
    pub max_partitions: Option<usize>,
    /// Node version information
    pub version: String,
    /// Additional node tags
    pub tags: HashMap<String, String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            can_write: true,
            can_read: true,
            can_vote: true,
            max_partitions: None,
            version: env!("CARGO_PKG_VERSION").to_string(),
            tags: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_node_id_creation() {
        let id1 = NodeId::new();
        let id2 = NodeId::new();
        assert_ne!(id1, id2);
        
        let uuid = Uuid::new_v4();
        let id3 = NodeId::from_uuid(uuid);
        assert_eq!(id3.as_uuid(), uuid);
    }
    
    #[test]
    fn test_node_state_display() {
        assert_eq!(NodeState::Active.to_string(), "active");
        assert_eq!(NodeState::Down.to_string(), "down");
    }
    
    #[test]
    fn test_node_status() {
        let mut status = NodeStatus::new(NodeState::Active);
        assert!(status.is_healthy());
        assert!(status.is_available());
        
        status.state = NodeState::Down;
        assert!(!status.is_healthy());
        assert!(!status.is_available());
    }
    
    #[test]
    fn test_node_operations() {
        let id = NodeId::new();
        let addr = "127.0.0.1:8300".parse().unwrap();
        let mut node = Node::new(id.clone(), addr);

        assert_eq!(node.id, id);
        assert_eq!(node.addr, addr);

        // New nodes start in Joining state, so they can't handle operations yet
        assert!(!node.can_read());
        assert!(!node.can_write());

        // Mark as active to enable operations
        node.mark_active();
        assert!(node.can_read());
        assert!(node.can_write());

        node.mark_suspected();
        assert_eq!(node.status.state, NodeState::Suspected);
        assert_eq!(node.status.failed_checks, 1);

        node.mark_down();
        assert_eq!(node.status.state, NodeState::Down);
        assert!(!node.can_read());
        assert!(!node.can_write());
    }
}
