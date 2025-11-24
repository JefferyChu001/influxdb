//! Shard management and routing

use crate::error::{Error, Result};
use crate::types::{
    DbId, NodeId, ReplicaInfo, ReplicaRole, ReplicaStatus, ShardId, ShardInfo, ShardRange,
    ShardStatus,
};
use crate::meta_store::MetaStore;
use crate::node_registry::NodeRegistry;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shard manager handles data partitioning and routing
#[derive(Debug)]
pub struct ShardManager {
    shards: Arc<RwLock<HashMap<ShardId, ShardInfo>>>,
    shard_count: usize,
    replication_factor: usize,
    meta_store: Arc<dyn MetaStore>,
}

impl ShardManager {
    pub fn new(
        shard_count: usize,
        replication_factor: usize,
        meta_store: Arc<dyn MetaStore>,
    ) -> Self {
        Self {
            shards: Arc::new(RwLock::new(HashMap::new())),
            shard_count,
            replication_factor,
            meta_store,
        }
    }

    /// Route a write to the appropriate shard based on hash
    pub fn route_write(
        &self,
        database: &str,
        measurement: &str,
        series_key: &[(&str, &str)],
    ) -> ShardId {
        let mut hasher = DefaultHasher::new();
        database.hash(&mut hasher);
        measurement.hash(&mut hasher);
        for (key, value) in series_key {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }

        let hash = hasher.finish();
        ShardId::from((hash % self.shard_count as u64) as u32)
    }

    /// Get all replicas for a shard
    pub async fn get_shard_replicas(&self, shard_id: ShardId) -> Result<Vec<ReplicaInfo>> {
        let shards = self.shards.read().await;
        shards
            .get(&shard_id)
            .map(|s| s.replicas.clone())
            .ok_or(Error::ShardNotFound { shard_id })
    }

    /// Get the leader node for a shard
    pub async fn get_shard_leader(&self, shard_id: ShardId) -> Result<NodeId> {
        let replicas = self.get_shard_replicas(shard_id).await?;
        replicas
            .iter()
            .find(|r| r.role == ReplicaRole::Leader)
            .map(|r| r.node_id)
            .ok_or(Error::NoLeaderFound { shard_id })
    }

    /// Create a new shard
    pub async fn create_shard(
        &self,
        database_id: DbId,
        shard_range: ShardRange,
        node_registry: &NodeRegistry,
    ) -> Result<ShardId> {
        // Select nodes for replicas
        let nodes = self.select_nodes_for_replicas(node_registry).await?;

        // Create shard info
        let shard_id = ShardId::new();
        let replicas = nodes
            .into_iter()
            .enumerate()
            .map(|(i, node_id)| ReplicaInfo {
                node_id,
                role: if i == 0 {
                    ReplicaRole::Leader
                } else {
                    ReplicaRole::Follower
                },
                status: ReplicaStatus::Active,
                lag: None,
            })
            .collect();

        let shard_info = ShardInfo {
            shard_id,
            database_id,
            shard_range,
            replicas,
            status: ShardStatus::Active,
        };

        // Persist to metadata store
        self.meta_store.put_shard(shard_info.clone()).await?;

        // Update local cache
        let mut shards = self.shards.write().await;
        shards.insert(shard_id, shard_info);

        Ok(shard_id)
    }

    /// Select nodes for replicas using load balancing
    async fn select_nodes_for_replicas(
        &self,
        node_registry: &NodeRegistry,
    ) -> Result<Vec<NodeId>> {
        use crate::types::NodeRole;

        let nodes = node_registry
            .get_active_nodes(Some(NodeRole::DataNode))
            .await;

        if nodes.len() < self.replication_factor {
            return Err(Error::InsufficientNodes {
                required: self.replication_factor,
                available: nodes.len(),
            });
        }

        // Sort by current shard count (load balancing)
        let mut sorted_nodes = nodes;
        sorted_nodes.sort_by_key(|n| n.capacity.current_shards);

        Ok(sorted_nodes
            .iter()
            .take(self.replication_factor)
            .map(|n| n.node_id)
            .collect())
    }

    /// Get shard by ID
    pub async fn get_shard(&self, shard_id: ShardId) -> Result<ShardInfo> {
        let shards = self.shards.read().await;
        shards
            .get(&shard_id)
            .cloned()
            .ok_or(Error::ShardNotFound { shard_id })
    }

    /// Load shards for a database from the meta store into local cache
    pub async fn load_shards_from_meta(&self, database_id: DbId) -> Result<()> {
        let list = self.meta_store.list_shards(database_id).await.map_err(|e| Error::MetaStoreError { source: e.into() })?;
        let mut shards = self.shards.write().await;
        for s in list {
            shards.insert(s.shard_id, s);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;


