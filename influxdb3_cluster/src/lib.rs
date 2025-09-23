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
pub mod distributed_query_coordinator;
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
use observability_deps::tracing::info;

pub use config::ClusterConfig;
pub use distributed_write_buffer::DistributedWriteBuffer;
pub use distributed_query_coordinator::{
    DistributedQueryCoordinator, DistributedQuery, DistributedQueryResult,
    QueryStrategy, ConsistencyLevel, NodeQueryResult, QueryStats
};
pub use error::{ClusterError, Result};
pub use node::{Node, NodeId, NodeState, NodeStatus};

/// Node role in the cluster
#[derive(Debug, Clone, PartialEq)]
pub enum NodeRole {
    /// Master node - handles client requests and coordinates tasks
    Master,
    /// Slave node - processes tasks assigned by master
    Slave,
}

/// The main cluster manager that coordinates all distributed operations
#[derive(Debug)]
pub struct ClusterManager {
    config: ClusterConfig,
    role: NodeRole,
    membership: Arc<RwLock<membership::MembershipManager>>,
    gossip: Arc<gossip::GossipProtocol>,
    health: Arc<health::HealthMonitor>,
    partition: Arc<partition::PartitionManager>,
    raft: Option<Arc<raft::RaftConsensus>>,
    query_coordinator: Arc<distributed_query_coordinator::DistributedQueryCoordinator>,
    write_coordinator: Arc<write_coordinator::WriteCoordinator>,
}

impl ClusterManager {
    /// Create a new cluster manager with the given configuration
    pub async fn new(config: ClusterConfig, role: NodeRole) -> Result<Self> {
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

        let query_coordinator = Arc::new(
            distributed_query_coordinator::DistributedQueryCoordinator::new(None)
        );

        let write_coordinator = Arc::new(
            write_coordinator::WriteCoordinator::new(config.clone(), partition.clone()).await?
        );

        Ok(Self {
            config,
            role,
            membership,
            gossip,
            health,
            partition,
            raft,
            query_coordinator,
            write_coordinator,
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

        // Update all coordinators with initial node list
        self.update_query_coordinator_nodes().await?;

        // Start a background task to periodically update endpoints
        self.start_endpoint_updater().await;

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

    /// Get the distributed query coordinator
    pub fn query_coordinator(&self) -> Arc<distributed_query_coordinator::DistributedQueryCoordinator> {
        Arc::clone(&self.query_coordinator)
    }

    /// Get the write coordinator
    pub fn write_coordinator(&self) -> Arc<write_coordinator::WriteCoordinator> {
        Arc::clone(&self.write_coordinator)
    }

    /// Get the gossip protocol
    pub fn gossip_protocol(&self) -> Arc<gossip::GossipProtocol> {
        Arc::clone(&self.gossip)
    }

    /// Get the node role
    pub fn role(&self) -> &NodeRole {
        &self.role
    }

    /// Check if this node is the master
    pub fn is_master(&self) -> bool {
        self.role == NodeRole::Master
    }

    /// Check if this node is a slave
    pub fn is_slave(&self) -> bool {
        self.role == NodeRole::Slave
    }

    /// Get the node ID
    pub fn node_id(&self) -> &NodeId {
        &self.config.node_id
    }

    /// Get current node endpoints
    pub async fn get_node_endpoints(&self) -> std::collections::HashMap<NodeId, String> {
        let membership = self.membership.read().await;
        membership.get_node_endpoints().await
    }

    /// Update node endpoints for both query and write coordinators
    pub async fn update_node_endpoints(&self, endpoints: std::collections::HashMap<NodeId, String>) {
        // Update query coordinator endpoints (it uses std::collections::HashMap)
        self.query_coordinator.update_nodes(endpoints.clone()).await;

        // Update write coordinator endpoints (it uses std::collections::HashMap)
        self.write_coordinator.update_node_endpoints(endpoints.clone()).await;

        // Update partition manager with the new nodes (exclude master node)
        // First, get current nodes in partition manager
        let current_partition_nodes = self.partition.get_current_nodes().await;

        // Determine which nodes should be in the partition manager (only slaves)
        let target_partition_nodes: std::collections::HashSet<NodeId> = endpoints.keys()
            .filter(|node_id| {
                // Skip the master node - it should not store data, only coordinate
                !(*node_id == &self.config.node_id && self.role == NodeRole::Master)
            })
            .cloned()
            .collect();

        // Remove nodes that are no longer in the cluster
        for node_id in current_partition_nodes.difference(&target_partition_nodes) {
            if let Err(e) = self.partition.remove_node(node_id).await {
                observability_deps::tracing::warn!(
                    node_id = %node_id,
                    error = %e,
                    "Failed to remove node from partition manager"
                );
            }
        }

        // Add new nodes to the partition manager
        for node_id in target_partition_nodes.difference(&current_partition_nodes) {
            if let Err(e) = self.partition.add_node(node_id.clone()).await {
                observability_deps::tracing::warn!(
                    node_id = %node_id,
                    error = %e,
                    "Failed to add node to partition manager"
                );
            }
        }

        observability_deps::tracing::info!(
            endpoint_count = endpoints.len(),
            "Updated cluster manager node endpoints"
        );
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

    /// Update all coordinators with current node endpoints
    async fn update_query_coordinator_nodes(&self) -> Result<()> {
        // Get all node endpoints from membership manager
        let endpoints = self.get_node_endpoints().await;

        info!(
            current_node = %self.config.node_id,
            total_endpoints = endpoints.len(),
            endpoints = ?endpoints,
            "Updating all coordinators with cluster endpoints"
        );

        // Update all coordinators with the latest endpoints
        self.update_node_endpoints(endpoints).await;

        Ok(())
    }

    /// Start a background task to periodically update endpoints
    async fn start_endpoint_updater(&self) {
        let membership = Arc::clone(&self.membership);
        let query_coordinator = Arc::clone(&self.query_coordinator);
        let write_coordinator = Arc::clone(&self.write_coordinator);
        let partition = Arc::clone(&self.partition);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;

                // Get current endpoints
                let membership_guard = membership.read().await;
                let endpoints = membership_guard.get_node_endpoints().await;
                drop(membership_guard);

                if !endpoints.is_empty() {
                    // Update query coordinator
                    query_coordinator.update_nodes(endpoints.clone()).await;

                    // Update write coordinator
                    write_coordinator.update_node_endpoints(endpoints.clone()).await;

                    // Update partition manager
                    for node_id in endpoints.keys() {
                        let _ = partition.add_node(node_id.clone()).await;
                    }
                }
            }
        });
    }
}

/// Trait for components that can be started and stopped
#[async_trait]
pub trait Component: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
}
