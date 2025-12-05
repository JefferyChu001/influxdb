//! Configuration types for the distributed framework.

use crate::common::NodeRole;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// The mode that the distributed node is running in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeMode {
    /// Standalone mode - single node, no distribution
    #[default]
    Standalone,

    /// Frontend mode - handles client requests and coordinates queries
    Frontend,

    /// Datanode mode - stores and processes data
    Datanode,

    /// MetaServer mode - manages cluster metadata
    MetaServer,
}

impl NodeMode {
    /// Check if this is a distributed mode
    pub fn is_distributed(&self) -> bool {
        !matches!(self, NodeMode::Standalone)
    }

    /// Convert to NodeRole (for distributed modes only)
    pub fn to_role(&self) -> Option<NodeRole> {
        match self {
            NodeMode::Standalone => None,
            NodeMode::Frontend => Some(NodeRole::Frontend),
            NodeMode::Datanode => Some(NodeRole::Datanode),
            NodeMode::MetaServer => Some(NodeRole::MetaServer),
        }
    }
}

impl std::fmt::Display for NodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeMode::Standalone => write!(f, "standalone"),
            NodeMode::Frontend => write!(f, "frontend"),
            NodeMode::Datanode => write!(f, "datanode"),
            NodeMode::MetaServer => write!(f, "metaserver"),
        }
    }
}

impl std::str::FromStr for NodeMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "standalone" => Ok(NodeMode::Standalone),
            "frontend" => Ok(NodeMode::Frontend),
            "datanode" => Ok(NodeMode::Datanode),
            "metaserver" | "meta" => Ok(NodeMode::MetaServer),
            _ => Err(format!(
                "Unknown node mode: {}. Valid modes are: standalone, frontend, datanode, metaserver",
                s
            )),
        }
    }
}

/// Configuration for the distributed cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Name of this cluster
    pub cluster_name: String,

    /// MetaServer addresses for connection
    pub meta_server_addrs: Vec<SocketAddr>,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Request timeout
    pub request_timeout: Duration,

    /// Heartbeat interval
    pub heartbeat_interval: Duration,

    /// Node considered stale after this duration without heartbeat
    pub node_stale_timeout: Duration,

    /// Maximum number of retries for retriable operations
    pub max_retries: usize,

    /// Retry backoff base duration
    pub retry_backoff_base: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: "influxdb3-cluster".to_string(),
            meta_server_addrs: Vec::new(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
            node_stale_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(100),
        }
    }
}

impl ClusterConfig {
    /// Create a new ClusterConfig with the given cluster name.
    pub fn new(cluster_name: impl Into<String>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            ..Default::default()
        }
    }

    /// Set the MetaServer addresses.
    pub fn with_meta_servers(mut self, addrs: Vec<SocketAddr>) -> Self {
        self.meta_server_addrs = addrs;
        self
    }

    /// Add a MetaServer address.
    pub fn add_meta_server(mut self, addr: SocketAddr) -> Self {
        self.meta_server_addrs.push(addr);
        self
    }

    /// Set the connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the request timeout.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the heartbeat interval.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.cluster_name.is_empty() {
            return Err("Cluster name cannot be empty".to_string());
        }
        if self.meta_server_addrs.is_empty() {
            return Err("At least one MetaServer address is required".to_string());
        }
        if self.heartbeat_interval >= self.node_stale_timeout {
            return Err("Heartbeat interval must be less than node stale timeout".to_string());
        }
        Ok(())
    }
}

/// Configuration for a Frontend node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontendConfig {
    /// Base cluster configuration
    pub cluster: ClusterConfig,

    /// HTTP bind address
    pub http_addr: SocketAddr,

    /// gRPC bind address
    pub grpc_addr: SocketAddr,

    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,

    /// Query timeout
    pub query_timeout: Duration,

    /// Enable query result caching
    pub enable_query_cache: bool,

    /// Query cache size in MB
    pub query_cache_size_mb: usize,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            cluster: ClusterConfig::default(),
            http_addr: "0.0.0.0:8181".parse().unwrap(),
            grpc_addr: "0.0.0.0:8182".parse().unwrap(),
            max_concurrent_queries: 100,
            query_timeout: Duration::from_secs(300),
            enable_query_cache: false,
            query_cache_size_mb: 256,
        }
    }
}

/// Configuration for a Datanode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatanodeConfig {
    /// Base cluster configuration
    pub cluster: ClusterConfig,

    /// Unique ID for this datanode
    pub node_id: u64,

    /// gRPC bind address for internal communication
    pub grpc_addr: SocketAddr,

    /// Data directory
    pub data_dir: PathBuf,

    /// Maximum number of regions this node can host
    pub max_regions: usize,

    /// Write buffer size per region
    pub write_buffer_size_mb: usize,
}

impl Default for DatanodeConfig {
    fn default() -> Self {
        Self {
            cluster: ClusterConfig::default(),
            node_id: 0,
            grpc_addr: "0.0.0.0:8183".parse().unwrap(),
            data_dir: PathBuf::from("/var/lib/influxdb3/data"),
            max_regions: 100,
            write_buffer_size_mb: 256,
        }
    }
}

impl DatanodeConfig {
    /// Create a new DatanodeConfig with the given node ID.
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            ..Default::default()
        }
    }

    /// Set the gRPC address.
    pub fn with_grpc_addr(mut self, addr: SocketAddr) -> Self {
        self.grpc_addr = addr;
        self
    }

    /// Set the data directory.
    pub fn with_data_dir(mut self, path: PathBuf) -> Self {
        self.data_dir = path;
        self
    }
}

/// Configuration for a MetaServer node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaServerConfig {
    /// Cluster name
    pub cluster_name: String,

    /// gRPC bind address
    pub grpc_addr: SocketAddr,

    /// Data directory for metadata storage
    pub data_dir: PathBuf,

    /// Raft peer addresses (for HA mode)
    pub raft_peers: Vec<SocketAddr>,

    /// Heartbeat timeout for nodes
    pub node_heartbeat_timeout: Duration,

    /// How often to check for stale nodes
    pub node_check_interval: Duration,

    /// Default number of regions for new tables
    pub default_num_regions: usize,
}

impl Default for MetaServerConfig {
    fn default() -> Self {
        Self {
            cluster_name: "influxdb3-cluster".to_string(),
            grpc_addr: "0.0.0.0:9000".parse().unwrap(),
            data_dir: PathBuf::from("/var/lib/influxdb3/meta"),
            raft_peers: Vec::new(),
            node_heartbeat_timeout: Duration::from_secs(30),
            node_check_interval: Duration::from_secs(10),
            default_num_regions: 4,
        }
    }
}

impl MetaServerConfig {
    /// Create a new MetaServerConfig.
    pub fn new(cluster_name: impl Into<String>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            ..Default::default()
        }
    }

    /// Set the gRPC address.
    pub fn with_grpc_addr(mut self, addr: SocketAddr) -> Self {
        self.grpc_addr = addr;
        self
    }

    /// Set the Raft peers.
    pub fn with_raft_peers(mut self, peers: Vec<SocketAddr>) -> Self {
        self.raft_peers = peers;
        self
    }

    /// Check if HA mode is enabled (has Raft peers).
    pub fn is_ha_enabled(&self) -> bool {
        !self.raft_peers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_mode_parsing() {
        assert_eq!("standalone".parse::<NodeMode>().unwrap(), NodeMode::Standalone);
        assert_eq!("frontend".parse::<NodeMode>().unwrap(), NodeMode::Frontend);
        assert_eq!("datanode".parse::<NodeMode>().unwrap(), NodeMode::Datanode);
        assert_eq!("metaserver".parse::<NodeMode>().unwrap(), NodeMode::MetaServer);
        assert_eq!("meta".parse::<NodeMode>().unwrap(), NodeMode::MetaServer);
    }

    #[test]
    fn test_node_mode_is_distributed() {
        assert!(!NodeMode::Standalone.is_distributed());
        assert!(NodeMode::Frontend.is_distributed());
        assert!(NodeMode::Datanode.is_distributed());
        assert!(NodeMode::MetaServer.is_distributed());
    }

    #[test]
    fn test_cluster_config_validation() {
        let config = ClusterConfig::new("test");
        assert!(config.validate().is_err()); // No meta servers

        let config = config.add_meta_server("127.0.0.1:9000".parse().unwrap());
        assert!(config.validate().is_ok());
    }
}
