//! Error types for cluster operations

use thiserror::Error;

/// Result type for cluster operations
pub type Result<T> = std::result::Result<T, ClusterError>;

/// Errors that can occur during cluster operations
#[derive(Debug, Error)]
pub enum ClusterError {
    /// Configuration validation error
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    
    /// Network communication error
    #[error("network error: {0}")]
    Network(#[from] NetworkError),
    
    /// Node discovery error
    #[error("discovery error: {0}")]
    Discovery(#[from] DiscoveryError),
    
    /// Membership management error
    #[error("membership error: {0}")]
    Membership(#[from] MembershipError),
    
    /// Health monitoring error
    #[error("health error: {0}")]
    Health(#[from] HealthError),
    
    /// Partition management error
    #[error("partition error: {0}")]
    Partition(#[from] PartitionError),
    
    /// Raft consensus error
    #[error("raft error: {0}")]
    Raft(#[from] RaftError),
    
    /// Serialization/deserialization error
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    /// Timeout error
    #[error("operation timed out")]
    Timeout,
    
    /// Node not found error
    #[error("node not found: {0}")]
    NodeNotFound(String),
    
    /// Cluster not ready error
    #[error("cluster not ready")]
    ClusterNotReady,
    
    /// Generic error
    #[error("cluster error: {0}")]
    Generic(String),
}

/// Network-related errors
#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),
    
    #[error("invalid address: {0}")]
    InvalidAddress(#[from] std::net::AddrParseError),
    
    #[error("bind failed: {0}")]
    BindFailed(std::io::Error),
    
    #[error("message too large: {size} bytes")]
    MessageTooLarge { size: usize },
}

/// Node discovery errors
#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("no seed nodes configured")]
    NoSeedNodes,
    
    #[error("all seed nodes unreachable")]
    AllSeedNodesUnreachable,
    
    #[error("invalid node information: {0}")]
    InvalidNodeInfo(String),
    
    #[error("discovery service unavailable")]
    ServiceUnavailable,
}

/// Membership management errors
#[derive(Debug, Error)]
pub enum MembershipError {
    #[error("node already exists: {0}")]
    NodeAlreadyExists(String),
    
    #[error("node not in cluster: {0}")]
    NodeNotInCluster(String),
    
    #[error("insufficient nodes for operation")]
    InsufficientNodes,
    
    #[error("membership update conflict")]
    UpdateConflict,
}

/// Health monitoring errors
#[derive(Debug, Error)]
pub enum HealthError {
    #[error("health check failed: {0}")]
    CheckFailed(String),
    
    #[error("node unhealthy: {0}")]
    NodeUnhealthy(String),
    
    #[error("health monitor not running")]
    MonitorNotRunning,
}

/// Partition management errors
#[derive(Debug, Error)]
pub enum PartitionError {
    #[error("no nodes available for partition")]
    NoNodesAvailable,
    
    #[error("rebalancing in progress")]
    RebalancingInProgress,
    
    #[error("partition not found: {0}")]
    PartitionNotFound(String),
    
    #[error("invalid partition key: {0}")]
    InvalidPartitionKey(String),
}

/// Raft consensus errors
#[derive(Debug, Error)]
pub enum RaftError {
    #[error("not leader")]
    NotLeader,
    
    #[error("election in progress")]
    ElectionInProgress,
    
    #[error("log inconsistency")]
    LogInconsistency,
    
    #[error("snapshot failed: {0}")]
    SnapshotFailed(String),
    
    #[error("raft not initialized")]
    NotInitialized,
}

impl From<anyhow::Error> for ClusterError {
    fn from(err: anyhow::Error) -> Self {
        ClusterError::Generic(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for ClusterError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        ClusterError::Timeout
    }
}
