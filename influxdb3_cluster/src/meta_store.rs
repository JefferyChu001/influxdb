//! Metadata store abstraction for cluster state

use crate::error::Result;
use crate::types::{DbId, NodeId, NodeInfo, ShardId, ShardInfo};
use async_trait::async_trait;

/// Metadata store trait for persisting cluster state
#[async_trait]
pub trait MetaStore: Send + Sync + std::fmt::Debug + 'static {
    /// Store node information
    async fn put_node(&self, node: NodeInfo) -> Result<()>;

    /// Get node information
    async fn get_node(&self, node_id: NodeId) -> Result<Option<NodeInfo>>;

    /// List all nodes
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>>;

    /// Delete node
    async fn delete_node(&self, node_id: NodeId) -> Result<()>;

    /// Store shard information
    async fn put_shard(&self, shard: ShardInfo) -> Result<()>;

    /// Get shard information
    async fn get_shard(&self, shard_id: ShardId) -> Result<Option<ShardInfo>>;

    /// List shards for a database
    async fn list_shards(&self, database_id: DbId) -> Result<Vec<ShardInfo>>;

    /// Delete shard
    async fn delete_shard(&self, shard_id: ShardId) -> Result<()>;
}

/// In-memory metadata store for testing
#[derive(Debug)]
pub struct InMemoryMetaStore {
    nodes: parking_lot::RwLock<std::collections::HashMap<NodeId, NodeInfo>>,
    shards: parking_lot::RwLock<std::collections::HashMap<ShardId, ShardInfo>>,
}

impl InMemoryMetaStore {
    pub fn new() -> Self {
        Self {
            nodes: parking_lot::RwLock::new(std::collections::HashMap::new()),
            shards: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for InMemoryMetaStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetaStore for InMemoryMetaStore {
    async fn put_node(&self, node: NodeInfo) -> Result<()> {
        let mut nodes = self.nodes.write();
        nodes.insert(node.node_id, node);
        Ok(())
    }

    async fn get_node(&self, node_id: NodeId) -> Result<Option<NodeInfo>> {
        let nodes = self.nodes.read();
        Ok(nodes.get(&node_id).cloned())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let nodes = self.nodes.read();
        Ok(nodes.values().cloned().collect())
    }

    async fn delete_node(&self, node_id: NodeId) -> Result<()> {
        let mut nodes = self.nodes.write();
        nodes.remove(&node_id);
        Ok(())
    }

    async fn put_shard(&self, shard: ShardInfo) -> Result<()> {
        let mut shards = self.shards.write();
        shards.insert(shard.shard_id, shard);
        Ok(())
    }

    async fn get_shard(&self, shard_id: ShardId) -> Result<Option<ShardInfo>> {
        let shards = self.shards.read();
        Ok(shards.get(&shard_id).cloned())
    }

    async fn list_shards(&self, database_id: DbId) -> Result<Vec<ShardInfo>> {
        let shards = self.shards.read();
        Ok(shards
            .values()
            .filter(|s| s.database_id == database_id)
            .cloned()
            .collect())
    }

    async fn delete_shard(&self, shard_id: ShardId) -> Result<()> {
        let mut shards = self.shards.write();
        shards.remove(&shard_id);
        Ok(())
    }
}

/// etcd-based metadata store for production use
pub struct EtcdMetaStore {
    client: etcd_client::Client,
    prefix: String,
}

impl std::fmt::Debug for EtcdMetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdMetaStore")
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl EtcdMetaStore {
    /// Create a new etcd metadata store
    pub async fn new(endpoints: Vec<String>, prefix: String) -> Result<Self> {
        let client = etcd_client::Client::connect(endpoints, None)
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        Ok(Self { client, prefix })
    }

    fn node_key(&self, node_id: NodeId) -> String {
        format!("{}/nodes/{}", self.prefix, node_id.as_u64())
    }

    fn shard_key(&self, shard_id: ShardId) -> String {
        format!("{}/shards/{}", self.prefix, shard_id.as_u64())
    }

    fn database_key(&self, db_id: DbId) -> String {
        format!("{}/databases/{}", self.prefix, db_id.as_u64())
    }
}

#[async_trait]
impl MetaStore for EtcdMetaStore {
    async fn put_node(&self, node: NodeInfo) -> Result<()> {
        let key = self.node_key(node.node_id);
        let value = serde_json::to_vec(&node)?;

        self.client
            .clone()
            .put(key, value, None)
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        Ok(())
    }

    async fn get_node(&self, node_id: NodeId) -> Result<Option<NodeInfo>> {
        let key = self.node_key(node_id);

        let resp = self
            .client
            .clone()
            .get(key, None)
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        if let Some(kv) = resp.kvs().first() {
            let node: NodeInfo = serde_json::from_slice(kv.value())?;
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let prefix = format!("{}/nodes/", self.prefix);

        let resp = self
            .client
            .clone()
            .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        let mut nodes = Vec::new();
        for kv in resp.kvs() {
            let node: NodeInfo = serde_json::from_slice(kv.value())?;
            nodes.push(node);
        }

        Ok(nodes)
    }

    async fn delete_node(&self, node_id: NodeId) -> Result<()> {
        let key = self.node_key(node_id);

        self.client
            .clone()
            .delete(key, None)
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        Ok(())
    }

    async fn put_shard(&self, shard: ShardInfo) -> Result<()> {
        let key = self.shard_key(shard.shard_id);
        let value = serde_json::to_vec(&shard)?;

        self.client
            .clone()
            .put(key, value, None)
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        Ok(())
    }

    async fn get_shard(&self, shard_id: ShardId) -> Result<Option<ShardInfo>> {
        let key = self.shard_key(shard_id);

        let resp = self
            .client
            .clone()
            .get(key, None)
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        if let Some(kv) = resp.kvs().first() {
            let shard: ShardInfo = serde_json::from_slice(kv.value())?;
            Ok(Some(shard))
        } else {
            Ok(None)
        }
    }

    async fn list_shards(&self, database_id: DbId) -> Result<Vec<ShardInfo>> {
        let prefix = format!("{}/shards/", self.prefix);

        let resp = self
            .client
            .clone()
            .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        let mut shards = Vec::new();
        for kv in resp.kvs() {
            let shard: ShardInfo = serde_json::from_slice(kv.value())?;
            if shard.database_id == database_id {
                shards.push(shard);
            }
        }

        Ok(shards)
    }

    async fn delete_shard(&self, shard_id: ShardId) -> Result<()> {
        let key = self.shard_key(shard_id);

        self.client
            .clone()
            .delete(key, None)
            .await
            .map_err(|e| crate::Error::MetaStoreError {
                source: Box::new(e),
            })?;

        Ok(())
    }
}

