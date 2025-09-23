//! Cluster configuration types and defaults

use crate::{NodeId, Result};
use crate::write_coordinator::ConsistencyLevel;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

/// Configuration for the cluster manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Unique identifier for this node
    pub node_id: NodeId,

    /// Address to bind the cluster communication port
    pub bind_addr: SocketAddr,

    /// HTTP endpoint for client communication
    pub http_endpoint: Option<String>,

    /// List of seed nodes to connect to when joining the cluster
    pub seed_nodes: Vec<SocketAddr>,
    
    /// Gossip protocol configuration
    pub gossip: GossipConfig,
    
    /// Health monitoring configuration
    pub health: HealthConfig,
    
    /// Partition management configuration
    pub partition: PartitionConfig,
    
    /// Raft consensus configuration
    pub raft: RaftConfig,
    
    /// Whether to enable Raft consensus (disable for testing)
    pub enable_raft: bool,
    
    /// Replication factor for data
    pub replication_factor: usize,

    /// Consistency level for writes
    pub consistency_level: ConsistencyLevel,

    /// Timeout for cluster operations
    pub operation_timeout: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::new(),
            bind_addr: "127.0.0.1:8300".parse().unwrap(),
            http_endpoint: None,
            seed_nodes: Vec::new(),
            gossip: GossipConfig::default(),
            health: HealthConfig::default(),
            partition: PartitionConfig::default(),
            raft: RaftConfig::default(),
            enable_raft: true,
            replication_factor: 3,
            consistency_level: ConsistencyLevel::default(),
            operation_timeout: Duration::from_secs(30),
        }
    }
}

/// Configuration for the gossip protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipConfig {
    /// Interval between gossip rounds
    pub gossip_interval: Duration,
    
    /// Number of nodes to gossip with in each round
    pub gossip_fanout: usize,
    
    /// Maximum message size for gossip
    pub max_message_size: usize,
    
    /// Timeout for gossip operations
    pub gossip_timeout: Duration,
    
    /// Maximum number of retries for failed gossip
    pub max_retries: usize,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_secs(1),
            gossip_fanout: 3,
            max_message_size: 64 * 1024, // 64KB
            gossip_timeout: Duration::from_secs(5),
            max_retries: 3,
        }
    }
}

/// Configuration for health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Interval between health checks
    pub health_check_interval: Duration,
    
    /// Timeout for health check requests
    pub health_check_timeout: Duration,
    
    /// Number of failed health checks before marking a node as unhealthy
    pub failure_threshold: usize,
    
    /// Number of successful health checks before marking a node as healthy
    pub recovery_threshold: usize,
    
    /// Maximum time to keep information about failed nodes
    pub failed_node_timeout: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(5),
            health_check_timeout: Duration::from_secs(3),
            failure_threshold: 3,
            recovery_threshold: 2,
            failed_node_timeout: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Configuration for partition management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionConfig {
    /// Number of virtual nodes per physical node for consistent hashing
    pub virtual_nodes: usize,
    
    /// Interval for rebalancing partitions
    pub rebalance_interval: Duration,
    
    /// Minimum time between rebalancing operations
    pub rebalance_cooldown: Duration,
    
    /// Maximum imbalance ratio before triggering rebalancing
    pub max_imbalance_ratio: f64,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            virtual_nodes: 256,
            rebalance_interval: Duration::from_secs(60),
            rebalance_cooldown: Duration::from_secs(300),
            max_imbalance_ratio: 0.1, // 10% imbalance
        }
    }
}

/// Configuration for Raft consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Raft election timeout
    pub election_timeout: Duration,
    
    /// Raft heartbeat interval
    pub heartbeat_interval: Duration,
    
    /// Maximum number of log entries per append
    pub max_append_entries: usize,
    
    /// Snapshot threshold (number of log entries)
    pub snapshot_threshold: usize,
    
    /// Directory to store Raft logs and snapshots
    pub data_dir: Option<std::path::PathBuf>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
            max_append_entries: 100,
            snapshot_threshold: 1000,
            data_dir: None,
        }
    }
}

impl ClusterConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.replication_factor == 0 {
            return Err(crate::ClusterError::InvalidConfig(
                "replication_factor must be greater than 0".to_string()
            ));
        }
        
        if self.gossip.gossip_fanout == 0 {
            return Err(crate::ClusterError::InvalidConfig(
                "gossip_fanout must be greater than 0".to_string()
            ));
        }
        
        if self.health.failure_threshold == 0 {
            return Err(crate::ClusterError::InvalidConfig(
                "failure_threshold must be greater than 0".to_string()
            ));
        }
        
        if self.partition.virtual_nodes == 0 {
            return Err(crate::ClusterError::InvalidConfig(
                "virtual_nodes must be greater than 0".to_string()
            ));
        }
        
        Ok(())
    }
    
    /// Create a test configuration with minimal settings
    pub fn test_config() -> Self {
        Self {
            enable_raft: false,
            gossip: GossipConfig {
                gossip_interval: Duration::from_millis(100),
                gossip_timeout: Duration::from_millis(500),
                ..Default::default()
            },
            health: HealthConfig {
                health_check_interval: Duration::from_millis(500),
                health_check_timeout: Duration::from_millis(200),
                ..Default::default()
            },
            operation_timeout: Duration::from_secs(5),
            ..Default::default()
        }
    }
}
