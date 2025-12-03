//! Metadata service for distributed cluster coordination
//!
//! This module manages:
//! - Node registration and health tracking
//! - Table schema and partition information
//! - Region to node mapping
//! - Cluster topology

use crate::error::*;
use crate::types::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Node information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: String,
    pub grpc_port: u16,
    pub http_port: u16,
    pub status: NodeStatus,
    pub regions: Vec<RegionId>,
}

/// Table metadata
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub id: TableId,
    pub name: String,
    pub schema: SchemaRef,
    pub regions: Vec<RegionId>,
}

/// Region metadata
#[derive(Debug, Clone)]
pub struct RegionMeta {
    pub id: RegionId,
    pub table_id: TableId,
    pub node_id: NodeId,
    pub status: RegionStatus,
}

/// Metadata service trait
///
/// This is the core interface for cluster metadata management,
/// similar to GreptimeDB's MetaSrv
#[async_trait]
pub trait MetaService: Send + Sync {
    /// Register a node in the cluster
    async fn register_node(&self, node: NodeInfo) -> Result<()>;

    /// Get node information by ID
    async fn get_node(&self, node_id: NodeId) -> Result<NodeInfo>;

    /// List all active nodes
    async fn list_active_nodes(&self) -> Result<Vec<NodeInfo>>;

    /// Update node status
    async fn update_node_status(&self, node_id: NodeId, status: NodeStatus) -> Result<()>;

    /// Register a table
    async fn register_table(&self, table: TableMeta) -> Result<()>;

    /// Get table metadata by name
    async fn get_table(&self, table_name: &str) -> Result<TableMeta>;

    /// Get region metadata
    async fn get_region(&self, region_id: RegionId) -> Result<RegionMeta>;

    /// List regions for a table
    async fn list_table_regions(&self, table_id: TableId) -> Result<Vec<RegionMeta>>;

    /// Get nodes that hold a specific region
    async fn get_region_nodes(&self, region_id: RegionId) -> Result<Vec<NodeId>>;

    /// Get all regions on a node
    async fn get_node_regions(&self, node_id: NodeId) -> Result<Vec<RegionId>>;

    /// Register a region (for testing)
    async fn register_region(&self, region: RegionMeta) -> Result<()>;
}

pub type MetaServiceRef = Arc<dyn MetaService>;

/// In-memory implementation of MetaService for testing and development
pub struct InMemoryMetaService {
    nodes: Arc<parking_lot::RwLock<HashMap<NodeId, NodeInfo>>>,
    tables: Arc<parking_lot::RwLock<HashMap<String, TableMeta>>>,
    regions: Arc<parking_lot::RwLock<HashMap<RegionId, RegionMeta>>>,
}

impl InMemoryMetaService {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            tables: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            regions: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryMetaService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetaService for InMemoryMetaService {
    async fn register_node(&self, node: NodeInfo) -> Result<()> {
        let mut nodes = self.nodes.write();
        nodes.insert(node.id, node);
        Ok(())
    }

    async fn get_node(&self, node_id: NodeId) -> Result<NodeInfo> {
        let nodes = self.nodes.read();
        nodes
            .get(&node_id)
            .cloned()
            .ok_or_else(|| NodeNotFoundSnafu { node_id }.build())
    }

    async fn list_active_nodes(&self) -> Result<Vec<NodeInfo>> {
        let nodes = self.nodes.read();
        Ok(nodes
            .values()
            .filter(|n| n.status == NodeStatus::Active)
            .cloned()
            .collect())
    }

    async fn update_node_status(&self, node_id: NodeId, status: NodeStatus) -> Result<()> {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(&node_id) {
            node.status = status;
            Ok(())
        } else {
            NodeNotFoundSnafu { node_id }.fail()
        }
    }

    async fn register_table(&self, table: TableMeta) -> Result<()> {
        let mut tables = self.tables.write();
        tables.insert(table.name.clone(), table);
        Ok(())
    }

    async fn get_table(&self, table_name: &str) -> Result<TableMeta> {
        let tables = self.tables.read();
        tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| TableNotFoundSnafu { table_name }.build())
    }

    async fn get_region(&self, region_id: RegionId) -> Result<RegionMeta> {
        let regions = self.regions.read();
        regions
            .get(&region_id)
            .cloned()
            .ok_or_else(|| RegionNotFoundSnafu { region_id }.build())
    }

    async fn list_table_regions(&self, table_id: TableId) -> Result<Vec<RegionMeta>> {
        let regions = self.regions.read();
        Ok(regions
            .values()
            .filter(|r| r.table_id == table_id)
            .cloned()
            .collect())
    }

    async fn get_region_nodes(&self, region_id: RegionId) -> Result<Vec<NodeId>> {
        let region = self.get_region(region_id).await?;
        Ok(vec![region.node_id])
    }

    async fn get_node_regions(&self, node_id: NodeId) -> Result<Vec<RegionId>> {
        let regions = self.regions.read();
        Ok(regions
            .values()
            .filter(|r| r.node_id == node_id)
            .map(|r| r.id)
            .collect())
    }

    async fn register_region(&self, region: RegionMeta) -> Result<()> {
        let mut regions = self.regions.write();
        regions.insert(region.id, region);
        Ok(())
    }
}

