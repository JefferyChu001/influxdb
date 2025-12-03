//! 协调节点（Coordinator Node）
//!
//! 负责：
//! 1. 接收客户端的写入请求，通过 ShardManager 计算分片并路由到对应的数据节点
//! 2. 接收客户端的查询请求，下发到各个数据节点，收集并聚合结果
//! 3. 管理集群元数据（通过 MetaStore）

use influxdb3_cluster::shard_manager::ShardManager;
use influxdb3_cluster::node_registry::NodeRegistry;
use influxdb3_cluster::meta_store::MetaStore;
use influxdb3_cluster::rpc::client::ClusterRpcClient;
use influxdb3_cluster::types::NodeId;
use influxdb3_cluster::error::Result;
use influxdb3_id::DbId;
use std::sync::Arc;
use std::collections::HashMap;

/// 协调节点
pub(crate) struct CoordinatorNode {
    /// 节点注册表
    node_registry: Arc<NodeRegistry>,
    /// 分片管理器
    shard_manager: Arc<ShardManager>,
    /// 元数据存储
    meta_store: Arc<dyn MetaStore>,
    /// RPC 客户端（用于与数据节点通信）
    rpc_client: Arc<ClusterRpcClient>,
    /// 数据库名称到 ID 的映射
    database_map: HashMap<String, DbId>,
}

impl CoordinatorNode {
    /// 创建新的协调节点
    pub(crate) fn new(
        node_registry: Arc<NodeRegistry>,
        shard_manager: Arc<ShardManager>,
        meta_store: Arc<dyn MetaStore>,
        rpc_client: Arc<ClusterRpcClient>,
    ) -> Self {
        Self {
            node_registry,
            shard_manager,
            meta_store,
            rpc_client,
            database_map: HashMap::new(),
        }
    }

    /// 注册数据库
    pub(crate) fn register_database(&mut self, name: String, id: DbId) {
        self.database_map.insert(name, id);
    }

    /// 写入数据（Line Protocol）
    ///
    /// 流程：
    /// 1. 解析 Line Protocol 获取 measurement 和 tags
    /// 2. 使用 ShardManager 计算分片
    /// 3. 获取分片的 leader 节点
    /// 4. 通过 HTTP API 将数据写入 leader 节点
    pub(crate) async fn write_lp(&self, database: &str, line: &str) -> Result<()> {
        // 解析 Line Protocol
        let (measurement, series_key) = self.parse_line_protocol(line)?;

        // 计算分片
        let shard_id = self.shard_manager.route_write(database, &measurement, &series_key);

        // 获取分片的 leader 节点
        let leader_node = self.shard_manager.get_shard_leader(shard_id).await?;

        // 通过 HTTP API 写入到 leader 节点
        let node_info = self.node_registry.get_node(leader_node).await?;
        let url = format!("http://{}:{}/api/v3/write_lp?db={}",
                         node_info.address, node_info.http_port, database);

        let client = reqwest::Client::new();
        client.post(&url)
            .body(line.to_string())
            .send()
            .await
            .map_err(|e| influxdb3_cluster::error::Error::InternalError {
                message: format!("HTTP write error: {}", e)
            })?;

        println!("  [Coordinator] Routed write to node {} (shard {}): {}={:?}",
                 leader_node, shard_id, measurement, series_key);

        Ok(())
    }

    /// 批量写入
    pub(crate) async fn write_batch(&self, database: &str, lines: &[&str]) -> Result<()> {
        for line in lines {
            self.write_lp(database, line).await?;
        }
        Ok(())
    }

    /// 获取表所在的所有节点（用于查询）
    ///
    /// 返回存储该表数据的所有节点 ID
    pub(crate) async fn get_table_nodes(&self, _database: &str, _table: &str) -> Result<Vec<NodeId>> {
        // 简化实现：返回所有活跃的数据节点
        use influxdb3_cluster::types::NodeRole;
        let nodes = self.node_registry.get_active_nodes(Some(NodeRole::DataNode)).await;
        Ok(nodes.into_iter().map(|n| n.node_id).collect())
    }

    /// 解析 Line Protocol
    ///
    /// 返回 (measurement, series_key)
    fn parse_line_protocol<'a>(&self, line: &'a str) -> Result<(String, Vec<(&'a str, &'a str)>)> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Err(influxdb3_cluster::error::Error::InvalidNodeConfig {
                message: "Empty line protocol".to_string(),
            });
        }

        let measurement_and_tags = parts[0];
        let mut measurement = String::new();
        let mut series_key = vec![];

        if let Some(comma_pos) = measurement_and_tags.find(',') {
            measurement = measurement_and_tags[..comma_pos].to_string();
            let tags_str = &measurement_and_tags[comma_pos + 1..];
            for tag_pair in tags_str.split(',') {
                if let Some(eq_pos) = tag_pair.find('=') {
                    let key = &tag_pair[..eq_pos];
                    let value = &tag_pair[eq_pos + 1..];
                    series_key.push((key, value));
                }
            }
        } else {
            measurement = measurement_and_tags.to_string();
        }

        Ok((measurement, series_key))
    }

    /// 获取节点注册表
    pub(crate) fn node_registry(&self) -> &Arc<NodeRegistry> {
        &self.node_registry
    }

    /// 获取分片管理器
    pub(crate) fn shard_manager(&self) -> &Arc<ShardManager> {
        &self.shard_manager
    }

    /// 获取 RPC 客户端
    pub(crate) fn rpc_client(&self) -> &Arc<ClusterRpcClient> {
        &self.rpc_client
    }
}

