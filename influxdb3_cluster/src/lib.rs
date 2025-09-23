//! # InfluxDB 3 Cluster Management
//!
//! This crate provides distributed cluster management capabilities for InfluxDB 3,
//! including node discovery, membership management, and cluster coordination.
//!
//! ## Architecture
//!
//! The cluster management system is built around several key components:
//!
//! - **Node Discovery**: Automatic discovery of cluster nodes using gossip protocol
//! - **Membership Management**: Tracking active nodes and handling joins/leaves
//! - **Health Monitoring**: Continuous health checks and failure detection
//! - **Partition Management**: Consistent hashing for data distribution
//! - **Leader Election**: Raft-based consensus for critical operations
//!
//! ## Usage
//!
//! ```rust,no_run
//! use influxdb3_cluster::{ClusterManager, ClusterConfig, NodeId};
//! use std::net::SocketAddr;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = ClusterConfig {
//!     node_id: NodeId::new(),
//!     bind_addr: "127.0.0.1:8300".parse()?,
//!     seed_nodes: vec!["127.0.0.1:8301".parse()?],
//!     ..Default::default()
//! };
//!
//! let cluster = ClusterManager::new(config).await?;
//! cluster.start().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod discovery;
pub mod distributed_write_buffer;
pub mod error;
pub mod gossip;
pub mod health;
pub mod membership;
pub mod node;
pub mod partition;
pub mod raft;
pub mod write_coordinator;

#[cfg(test)]
pub mod integration_tests;

#[cfg(test)]
mod tests;

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use config::ClusterConfig;
pub use distributed_write_buffer::DistributedWriteBuffer;
pub use error::{ClusterError, Result};
pub use node::{Node, NodeId, NodeState, NodeStatus};

/// The main cluster manager that coordinates all distributed operations
#[derive(Debug)]
pub struct ClusterManager {
    config: ClusterConfig,
    membership: Arc<RwLock<membership::MembershipManager>>,
    gossip: Arc<gossip::GossipProtocol>,
    health: Arc<health::HealthMonitor>,
    partition: Arc<partition::PartitionManager>,
    raft: Option<Arc<raft::RaftConsensus>>,
}

impl ClusterManager {
    /// Create a new cluster manager with the given configuration
    pub async fn new(config: ClusterConfig) -> Result<Self> {
        let membership = Arc::new(RwLock::new(
            membership::MembershipManager::new(config.node_id.clone())
        ));
        
        let gossip = Arc::new(
            gossip::GossipProtocol::new(
                config.clone(),
                Arc::clone(&membership),
            ).await?
        );
        
        let health = Arc::new(
            health::HealthMonitor::new(
                config.clone(),
                Arc::clone(&membership),
            ).await?
        );
        
        let partition = Arc::new(
            partition::PartitionManager::new(
                config.clone(),
                Arc::clone(&membership),
            ).await?
        );
        
        let raft = if config.enable_raft {
            Some(Arc::new(
                raft::RaftConsensus::new(
                    config.clone(),
                    Arc::clone(&membership),
                ).await?
            ))
        } else {
            None
        };
        
        Ok(Self {
            config,
            membership,
            gossip,
            health,
            partition,
            raft,
        })
    }
    
    /// Start the cluster manager and all its components
    pub async fn start(&self) -> Result<()> {
        // Start gossip protocol for node discovery
        self.gossip.start().await?;
        
        // Start health monitoring
        self.health.start().await?;
        
        // Start partition management
        self.partition.start().await?;
        
        // Start Raft consensus if enabled
        if let Some(raft) = &self.raft {
            raft.start().await?;
        }
        
        // Join the cluster by connecting to seed nodes
        self.join_cluster().await?;
        
        Ok(())
    }
    
    /// Get the partition manager
    pub fn partition_manager(&self) -> Arc<partition::PartitionManager> {
        Arc::clone(&self.partition)
    }

    /// Get the membership manager
    pub fn membership_manager(&self) -> Arc<RwLock<membership::MembershipManager>> {
        Arc::clone(&self.membership)
    }

    /// Stop the cluster manager and all its components
    pub async fn stop(&self) -> Result<()> {
        // Stop components in reverse order
        if let Some(raft) = &self.raft {
            raft.stop().await?;
        }

        self.partition.stop().await?;
        self.health.stop().await?;
        self.gossip.stop().await?;
        
        Ok(())
    }
    
    /// Get the current cluster membership
    pub async fn get_membership(&self) -> membership::ClusterMembership {
        self.membership.read().await.get_membership().await
    }
    
    /// Get the node responsible for a given partition key
    pub async fn get_node_for_key(&self, key: &str) -> Option<NodeId> {
        self.partition.get_node_for_key(key).await
    }
    
    /// Get all nodes responsible for a given partition key (including replicas)
    pub async fn get_nodes_for_key(&self, key: &str) -> Vec<NodeId> {
        self.partition.get_nodes_for_key(key).await
    }
    
    /// Check if this node is the leader for consensus operations
    pub async fn is_leader(&self) -> bool {
        if let Some(raft) = &self.raft {
            raft.is_leader().await
        } else {
            // If Raft is disabled, use a simple leader election based on node ID
            let membership = self.membership.read().await;
            membership.is_leader(&self.config.node_id).await
        }
    }

    /// Check if this is a single-node cluster
    pub async fn is_single_node(&self) -> bool {
        let membership = self.membership.read().await;
        let active_nodes = membership.get_active_nodes();
        active_nodes.len() <= 1
    }

    /// Get the cluster configuration
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }
    
    /// Join the cluster by connecting to seed nodes
    async fn join_cluster(&self) -> Result<()> {
        for seed_addr in &self.config.seed_nodes {
            if let Err(e) = self.gossip.connect_to_seed(*seed_addr).await {
                observability_deps::tracing::warn!(
                    seed_addr = %seed_addr,
                    error = %e,
                    "Failed to connect to seed node"
                );
            }
        }
        Ok(())
    }
}

/// Trait for components that can be started and stopped
#[async_trait]
pub trait Component: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
}
