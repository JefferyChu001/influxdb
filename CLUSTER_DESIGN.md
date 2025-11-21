# InfluxDB 3 Core 集群化改造技术方案

## 目录
1. [项目概述](#项目概述)
2. [当前架构分析](#当前架构分析)
3. [集群化架构设计](#集群化架构设计)
4. [核心组件改造](#核心组件改造)
5. [数据分片与路由](#数据分片与路由)
6. [一致性与复制](#一致性与复制)
7. [查询分布式执行](#查询分布式执行)
8. [元数据管理](#元数据管理)
9. [实施路线图](#实施路线图)
10. [代码实现细节](#代码实现细节)

---

## 项目概述

### 当前状态
InfluxDB 3 Core 是一个**单机版**时序数据库，具有以下特点：
- 基于 Rust 实现
- 使用 Apache Arrow 和 DataFusion 作为查询引擎
- 支持对象存储（S3/GCS/Azure）作为持久化层
- WAL（Write-Ahead Log）机制保证数据持久性
- 内置 Parquet 文件格式存储
- 支持 InfluxQL 和 SQL 查询

### 集群化目标
将单机版改造为分布式集群系统，实现：
1. **水平扩展能力**：支持多节点部署，提升写入和查询吞吐量
2. **高可用性**：数据多副本，节点故障自动恢复
3. **数据分片**：自动数据分区和负载均衡
4. **分布式查询**：跨节点并行查询执行
5. **一致性保证**：强一致性或最终一致性可选

---

## 当前架构分析

### 核心模块结构

```
influxdb3/
├── influxdb3_server/        # HTTP/gRPC 服务器
├── influxdb3_write/         # 写入缓冲和持久化
├── influxdb3_wal/           # Write-Ahead Log
├── influxdb3_catalog/       # 元数据目录管理
├── influxdb3_cache/         # 缓存层（Last/Distinct/Parquet）
├── influxdb3_authz/         # 认证授权
├── influxdb3_processing_engine/  # 数据处理引擎
└── influxdb3_internal_api/  # 内部 API 定义
```

### 关键组件分析

#### 1. **写入路径** (`influxdb3_write/src/write_buffer/mod.rs`)
```rust
// 当前单机写入流程
pub async fn write_lp(
    &self,
    database: NamespaceName<'static>,
    lp: &str,
    ingest_time: Time,
    accept_partial: bool,
    precision: Precision,
    no_sync: bool,
) -> Result<BufferedWriteRequest>
```

**问题**：
- 写入直接到本地 WAL
- 无分片逻辑
- 无副本机制

#### 2. **WAL 实现** (`influxdb3_wal/src/object_store.rs`)
```rust
pub struct WalObjectStore {
    node_identifier_prefix: String,
    object_store: Arc<dyn ObjectStore>,
    flush_buffer: Arc<Mutex<FlushBuffer>>,
    // ...
}
```

**问题**：
- 单节点 WAL
- 无跨节点同步
- 无分布式事务支持

#### 3. **Catalog 元数据** (`influxdb3_catalog/src/catalog.rs`)
```rust
pub struct Catalog {
    metric_registry: Arc<Registry>,
    state: parking_lot::Mutex<CatalogState>,
    store: ObjectStoreCatalog,
    inner: RwLock<InnerCatalog>,
    // ...
}
```

**问题**：
- 本地内存状态
- 对象存储仅用于持久化
- 无分布式一致性保证

#### 4. **查询执行器** (`influxdb3_server/src/query_executor/mod.rs`)
```rust
pub struct QueryExecutorImpl {
    catalog: Arc<Catalog>,
    write_buffer: Arc<dyn WriteBuffer>,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
    // ...
}
```

**问题**：
- 单节点查询执行
- 无分布式查询计划
- 不支持跨节点 JOIN

---

## 集群化架构设计

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Layer                              │
│  (HTTP/gRPC API, InfluxQL/SQL Parser)                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                   Coordinator Layer                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Query Router │  │ Write Router │  │ Metadata Mgr │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└────────────────────────┬────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
┌───────▼──────┐  ┌─────▼──────┐  ┌─────▼──────┐
│  Data Node 1 │  │ Data Node 2│  │ Data Node N│
│              │  │            │  │            │
│ ┌──────────┐ │  │┌──────────┐│  │┌──────────┐│
│ │WAL+Buffer│ │  ││WAL+Buffer││  ││WAL+Buffer││
│ └──────────┘ │  │└──────────┘│  │└──────────┘│
│ ┌──────────┐ │  │┌──────────┐│  │┌──────────┐│
│ │  Cache   │ │  ││  Cache   ││  ││  Cache   ││
│ └──────────┘ │  │└──────────┘│  │└──────────┘│
│ ┌──────────┐ │  │┌──────────┐│  │┌──────────┐│
│ │Query Exec│ │  ││Query Exec││  ││Query Exec││
│ └──────────┘ │  │└──────────┘│  │└──────────┘│
└──────┬───────┘  └──────┬─────┘  └──────┬─────┘
       │                 │                │
       └─────────────────┼────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│              Shared Object Storage Layer                         │
│  (S3/GCS/Azure - Parquet Files, WAL, Metadata)                  │
└─────────────────────────────────────────────────────────────────┘
       ▲                 ▲                ▲
       │                 │                │
┌──────┴───────┐  ┌──────┴──────┐  ┌─────┴───────┐
│ Raft Group 1 │  │Raft Group 2 │  │Raft Group N │
│ (Consensus)  │  │ (Consensus) │  │ (Consensus) │
└──────────────┘  └─────────────┘  └─────────────┘
```

### 节点角色定义

#### 1. **Coordinator Node（协调节点）**
- 接收客户端请求
- 路由写入和查询
- 管理集群元数据
- 执行分布式查询计划
- 可选：独立部署或与 Data Node 混合部署

#### 2. **Data Node（数据节点）**
- 存储数据分片
- 执行本地查询
- 维护 WAL 和缓存
- 参与副本同步

#### 3. **Meta Node（元数据节点）**
- 使用 Raft 协议保证一致性
- 存储集群配置、分片映射、节点状态
- 可选：嵌入到 Coordinator 中

---

## 核心组件改造

### 1. 分布式协调层

#### 新增模块：`influxdb3_cluster`

```rust
// influxdb3_cluster/src/lib.rs
pub mod coordinator;
pub mod node_registry;
pub mod shard_manager;
pub mod replication;
pub mod consensus;
pub mod rpc;
```

#### 节点注册与发现

```rust
// influxdb3_cluster/src/node_registry.rs
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: String,
    pub grpc_port: u16,
    pub http_port: u16,
    pub role: NodeRole,
    pub status: NodeStatus,
    pub capacity: NodeCapacity,
    pub last_heartbeat: Time,
}

#[derive(Debug, Clone, Copy)]
pub enum NodeRole {
    Coordinator,
    DataNode,
    Mixed,  // 混合模式
}

#[derive(Debug, Clone, Copy)]
pub enum NodeStatus {
    Active,
    Inactive,
    Draining,  // 正在排空数据
    Failed,
}

#[derive(Debug, Clone)]
pub struct NodeCapacity {
    pub cpu_cores: usize,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub current_shards: usize,
    pub max_shards: usize,
}

pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    meta_store: Arc<dyn MetaStore>,
}

impl NodeRegistry {
    pub async fn register_node(&self, node_info: NodeInfo) -> Result<()> {
        // 1. 验证节点信息
        self.validate_node(&node_info)?;

        // 2. 写入元数据存储（通过 Raft 保证一致性）
        self.meta_store.put_node(node_info.clone()).await?;

        // 3. 更新本地缓存
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_info.node_id, node_info);

        Ok(())
    }

    pub async fn heartbeat(&self, node_id: NodeId) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.last_heartbeat = Time::now();
            node.status = NodeStatus::Active;
        }
        Ok(())
    }

    pub async fn get_active_nodes(&self, role: Option<NodeRole>) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|n| n.status == NodeStatus::Active)
            .filter(|n| role.map_or(true, |r| n.role == r))
            .cloned()
            .collect()
    }

    // 故障检测
    pub async fn detect_failures(&self, timeout: Duration) -> Vec<NodeId> {
        let nodes = self.nodes.read().await;
        let now = Time::now();
        nodes.values()
            .filter(|n| {
                n.status == NodeStatus::Active &&
                now.timestamp_nanos() - n.last_heartbeat.timestamp_nanos()
                    > timeout.as_nanos() as i64
            })
            .map(|n| n.node_id)
            .collect()
    }
}
```

---

## 数据分片与路由

### 分片策略

#### 1. **Hash 分片（默认）**
基于 measurement + series key 的哈希值进行分片：

```rust
// influxdb3_cluster/src/shard_manager.rs
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub database_id: DbId,
    pub shard_range: ShardRange,
    pub replicas: Vec<ReplicaInfo>,
    pub status: ShardStatus,
}

#[derive(Debug, Clone)]
pub enum ShardRange {
    Hash {
        start: u64,  // 哈希范围起始
        end: u64,    // 哈希范围结束
    },
    Time {
        start: Time,
        end: Time,
    },
    Hybrid {
        hash_start: u64,
        hash_end: u64,
        time_start: Time,
        time_end: Time,
    },
}

#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub node_id: NodeId,
    pub role: ReplicaRole,
    pub status: ReplicaStatus,
    pub lag: Option<Duration>,  // 副本延迟
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplicaRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicaStatus {
    Active,
    Syncing,
    Failed,
}

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

    /// 计算数据应该写入哪个分片
    pub fn route_write(
        &self,
        database: &str,
        measurement: &str,
        series_key: &[(&str, &str)],  // tag key-value pairs
    ) -> ShardId {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

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

    /// 获取分片的所有副本节点
    pub async fn get_shard_replicas(&self, shard_id: ShardId) -> Result<Vec<ReplicaInfo>> {
        let shards = self.shards.read().await;
        shards.get(&shard_id)
            .map(|s| s.replicas.clone())
            .ok_or_else(|| Error::ShardNotFound(shard_id))
    }

    /// 获取分片的 Leader 节点
    pub async fn get_shard_leader(&self, shard_id: ShardId) -> Result<NodeId> {
        let replicas = self.get_shard_replicas(shard_id).await?;
        replicas.iter()
            .find(|r| r.role == ReplicaRole::Leader)
            .map(|r| r.node_id)
            .ok_or_else(|| Error::NoLeaderFound(shard_id))
    }

    /// 创建新分片
    pub async fn create_shard(
        &self,
        database_id: DbId,
        shard_range: ShardRange,
        node_registry: &NodeRegistry,
    ) -> Result<ShardId> {
        // 1. 选择节点放置副本
        let nodes = self.select_nodes_for_replicas(node_registry).await?;

        // 2. 创建分片信息
        let shard_id = ShardId::new();
        let replicas = nodes.into_iter().enumerate().map(|(i, node_id)| {
            ReplicaInfo {
                node_id,
                role: if i == 0 { ReplicaRole::Leader } else { ReplicaRole::Follower },
                status: ReplicaStatus::Active,
                lag: None,
            }
        }).collect();

        let shard_info = ShardInfo {
            shard_id,
            database_id,
            shard_range,
            replicas,
            status: ShardStatus::Active,
        };

        // 3. 持久化到元数据存储
        self.meta_store.put_shard(shard_info.clone()).await?;

        // 4. 更新本地缓存
        let mut shards = self.shards.write().await;
        shards.insert(shard_id, shard_info);

        Ok(shard_id)
    }

    /// 选择节点放置副本（负载均衡）
    async fn select_nodes_for_replicas(
        &self,
        node_registry: &NodeRegistry,
    ) -> Result<Vec<NodeId>> {
        let nodes = node_registry.get_active_nodes(Some(NodeRole::DataNode)).await;

        if nodes.len() < self.replication_factor {
            return Err(Error::InsufficientNodes {
                required: self.replication_factor,
                available: nodes.len(),
            });
        }

        // 按当前分片数排序，选择负载最低的节点
        let mut sorted_nodes = nodes;
        sorted_nodes.sort_by_key(|n| n.capacity.current_shards);

        Ok(sorted_nodes.iter()
            .take(self.replication_factor)
            .map(|n| n.node_id)
            .collect())
    }
}

#### 2. **时间范围分片**
按时间范围分片，适合时序数据：

```rust
impl ShardManager {
    /// 按时间范围路由查询
    pub async fn route_query_by_time(
        &self,
        database_id: DbId,
        time_range: (Time, Time),
    ) -> Vec<ShardId> {
        let shards = self.shards.read().await;
        shards.values()
            .filter(|s| s.database_id == database_id)
            .filter(|s| self.time_range_overlaps(&s.shard_range, time_range))
            .map(|s| s.shard_id)
            .collect()
    }

    fn time_range_overlaps(&self, shard_range: &ShardRange, query_range: (Time, Time)) -> bool {
        match shard_range {
            ShardRange::Time { start, end } => {
                query_range.0 <= *end && query_range.1 >= *start
            }
            ShardRange::Hybrid { time_start, time_end, .. } => {
                query_range.0 <= *time_end && query_range.1 >= *time_start
            }
            _ => true,  // Hash 分片需要查询所有分片
        }
    }
}
```

---

## 一致性与复制

### Raft 共识协议集成

使用 `tikv/raft-rs` 实现分片级别的 Raft 复制：

```rust
// influxdb3_cluster/src/consensus/raft_group.rs
use raft::{Config, RawNode, Storage};
use std::sync::Arc;

pub struct RaftGroup {
    node: RawNode<MemStorage>,
    shard_id: ShardId,
    peers: Vec<NodeId>,
    wal: Arc<dyn Wal>,
}

impl RaftGroup {
    pub fn new(
        node_id: u64,
        shard_id: ShardId,
        peers: Vec<NodeId>,
        wal: Arc<dyn Wal>,
    ) -> Result<Self> {
        let config = Config {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };

        let storage = MemStorage::new();
        let node = RawNode::new(&config, storage, &[])?;

        Ok(Self {
            node,
            shard_id,
            peers: peers.into_iter().map(|p| p.as_u64()).collect(),
            wal,
        })
    }

    /// 提议写入操作
    pub async fn propose_write(&mut self, data: Vec<u8>) -> Result<()> {
        self.node.propose(vec![], data)?;
        Ok(())
    }

    /// 处理 Raft 消息
    pub async fn step(&mut self, msg: raft::prelude::Message) -> Result<()> {
        self.node.step(msg)?;
        Ok(())
    }

    /// 驱动 Raft 状态机
    pub async fn tick(&mut self) {
        self.node.tick();
    }

    /// 应用已提交的日志
    pub async fn apply_committed_entries(&mut self) -> Result<()> {
        if !self.node.has_ready() {
            return Ok(());
        }

        let mut ready = self.node.ready();

        // 1. 发送消息给其他节点
        for msg in ready.take_messages() {
            self.send_message(msg).await?;
        }

        // 2. 持久化日志
        if !ready.entries().is_empty() {
            self.wal.write_ops(
                ready.entries().iter()
                    .map(|e| WalOp::from_raft_entry(e))
                    .collect()
            ).await?;
        }

        // 3. 应用已提交的条目
        for entry in ready.take_committed_entries() {
            if entry.data.is_empty() {
                continue;
            }
            self.apply_entry(&entry).await?;
        }

        // 4. 推进 Raft 状态
        let mut light_rd = self.node.advance(ready);

        // 5. 发送新消息
        for msg in light_rd.take_messages() {
            self.send_message(msg).await?;
        }

        self.node.advance_apply();

        Ok(())
    }

    async fn apply_entry(&self, entry: &raft::prelude::Entry) -> Result<()> {
        // 解析并应用写入操作
        let write_op: WriteOperation = bincode::deserialize(&entry.data)?;
        // 实际写入到存储引擎
        // ...
        Ok(())
    }

    async fn send_message(&self, msg: raft::prelude::Message) -> Result<()> {
        // 通过 gRPC 发送到目标节点
        // ...
        Ok(())
    }
}
```


### 写入复制流程

```rust
// influxdb3_cluster/src/replication/write_replicator.rs
pub struct WriteReplicator {
    shard_manager: Arc<ShardManager>,
    node_registry: Arc<NodeRegistry>,
    rpc_client: Arc<ClusterRpcClient>,
}

impl WriteReplicator {
    /// 复制写入到所有副本
    pub async fn replicate_write(
        &self,
        shard_id: ShardId,
        write_batch: WriteBatch,
        consistency: ConsistencyLevel,
    ) -> Result<()> {
        let replicas = self.shard_manager.get_shard_replicas(shard_id).await?;

        match consistency {
            ConsistencyLevel::One => {
                // 只需要一个副本确认
                self.write_to_leader(shard_id, write_batch).await
            }
            ConsistencyLevel::Quorum => {
                // 需要多数副本确认
                self.write_with_quorum(replicas, write_batch).await
            }
            ConsistencyLevel::All => {
                // 需要所有副本确认
                self.write_to_all(replicas, write_batch).await
            }
        }
    }

    async fn write_to_leader(
        &self,
        shard_id: ShardId,
        write_batch: WriteBatch,
    ) -> Result<()> {
        let leader_id = self.shard_manager.get_shard_leader(shard_id).await?;
        let leader_node = self.node_registry.get_node(leader_id).await?;

        self.rpc_client
            .write_to_node(&leader_node.address, write_batch)
            .await
    }

    async fn write_with_quorum(
        &self,
        replicas: Vec<ReplicaInfo>,
        write_batch: WriteBatch,
    ) -> Result<()> {
        let quorum_size = (replicas.len() / 2) + 1;
        let mut tasks = Vec::new();

        for replica in replicas {
            let node = self.node_registry.get_node(replica.node_id).await?;
            let client = self.rpc_client.clone();
            let batch = write_batch.clone();

            tasks.push(tokio::spawn(async move {
                client.write_to_node(&node.address, batch).await
            }));
        }

        // 等待 quorum 数量的成功响应
        let mut success_count = 0;
        for task in tasks {
            if task.await.is_ok() {
                success_count += 1;
                if success_count >= quorum_size {
                    return Ok(());
                }
            }
        }

        Err(Error::QuorumNotReached {
            required: quorum_size,
            achieved: success_count,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ConsistencyLevel {
    One,     // 至少一个副本
    Quorum,  // 多数副本
    All,     // 所有副本
}
```

---

## 查询分布式执行

### 分布式查询计划器

```rust
// influxdb3_cluster/src/query/distributed_planner.rs
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;

pub struct DistributedQueryPlanner {
    shard_manager: Arc<ShardManager>,
    node_registry: Arc<NodeRegistry>,
    local_planner: Arc<Planner>,
}

impl DistributedQueryPlanner {
    /// 将逻辑计划转换为分布式物理计划
    pub async fn create_distributed_plan(
        &self,
        logical_plan: LogicalPlan,
        database_id: DbId,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. 分析查询涉及的表和时间范围
        let query_info = self.analyze_query(&logical_plan)?;

        // 2. 确定需要查询的分片
        let target_shards = self.determine_target_shards(
            database_id,
            &query_info,
        ).await?;

        // 3. 为每个分片生成子查询计划
        let shard_plans = self.create_shard_plans(
            &logical_plan,
            &target_shards,
        ).await?;

        // 4. 创建分布式执行计划
        let distributed_plan = self.build_distributed_plan(
            shard_plans,
            &query_info,
        ).await?;

        Ok(distributed_plan)
    }

    fn analyze_query(&self, plan: &LogicalPlan) -> Result<QueryInfo> {
        let mut visitor = QueryAnalyzer::new();
        plan.accept(&mut visitor)?;

        Ok(QueryInfo {
            tables: visitor.tables,
            time_range: visitor.time_range,
            filters: visitor.filters,
            has_join: visitor.has_join,
            join_type: visitor.join_type,
        })
    }
}
```


### 分布式 JOIN 实现

#### 1. **Broadcast JOIN（广播 JOIN）**
适用于小表 JOIN 大表的场景：

```rust
// influxdb3_cluster/src/query/join/broadcast_join.rs
pub struct BroadcastJoinExec {
    /// 小表（广播到所有节点）
    small_table: Arc<dyn ExecutionPlan>,
    /// 大表（分布在多个节点）
    large_table: Arc<dyn ExecutionPlan>,
    /// JOIN 条件
    on: Vec<(Column, Column)>,
    /// JOIN 类型
    join_type: JoinType,
    /// 目标节点列表
    target_nodes: Vec<NodeId>,
    /// RPC 客户端
    rpc_client: Arc<ClusterRpcClient>,
}

impl BroadcastJoinExec {
    pub async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // 1. 在协调节点收集小表的所有数据
        let small_table_data = self.collect_small_table().await?;

        // 2. 将小表数据广播到所有目标节点
        self.broadcast_small_table(&small_table_data).await?;

        // 3. 在各个节点上执行本地 JOIN
        let join_results = self.execute_local_joins(partition, &small_table_data).await?;

        // 4. 合并结果
        Ok(Box::pin(MergedStream::new(join_results)))
    }

    async fn collect_small_table(&self) -> Result<Vec<RecordBatch>> {
        let stream = self.small_table.execute(0, Arc::new(TaskContext::default()))?;
        let batches = collect_stream(stream).await?;
        Ok(batches)
    }

    async fn broadcast_small_table(&self, data: &[RecordBatch]) -> Result<()> {
        let mut tasks = Vec::new();

        for node_id in &self.target_nodes {
            let node = self.get_node_info(*node_id).await?;
            let client = self.rpc_client.clone();
            let data_clone = data.to_vec();

            tasks.push(tokio::spawn(async move {
                client.send_broadcast_data(&node.address, data_clone).await
            }));
        }

        // 等待所有广播完成
        for task in tasks {
            task.await??;
        }

        Ok(())
    }

    async fn execute_local_joins(
        &self,
        partition: usize,
        small_table_data: &[RecordBatch],
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let mut streams = Vec::new();

        for node_id in &self.target_nodes {
            let node = self.get_node_info(*node_id).await?;

            // 向每个节点发送 JOIN 请求
            let stream = self.rpc_client
                .execute_local_join(
                    &node.address,
                    partition,
                    &self.on,
                    self.join_type,
                )
                .await?;

            streams.push(stream);
        }

        Ok(streams)
    }
}
```

#### 2. **Shuffle JOIN（重分区 JOIN）**
适用于两个大表 JOIN 的场景：

```rust
// influxdb3_cluster/src/query/join/shuffle_join.rs
pub struct ShuffleJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(Column, Column)>,
    join_type: JoinType,
    partition_count: usize,
    node_registry: Arc<NodeRegistry>,
    rpc_client: Arc<ClusterRpcClient>,
}

impl ShuffleJoinExec {
    pub async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // 1. 对左右两表按 JOIN key 进行重分区
        let left_partitions = self.repartition_table(&self.left, &self.on, true).await?;
        let right_partitions = self.repartition_table(&self.right, &self.on, false).await?;

        // 2. 在各个节点上执行本地 JOIN
        let join_results = self.execute_partitioned_joins(
            partition,
            left_partitions,
            right_partitions,
        ).await?;

        // 3. 返回结果流
        Ok(Box::pin(MergedStream::new(join_results)))
    }

    /// 按 JOIN key 重分区数据
    async fn repartition_table(
        &self,
        table: &Arc<dyn ExecutionPlan>,
        join_keys: &[(Column, Column)],
        is_left: bool,
    ) -> Result<HashMap<usize, Vec<RecordBatch>>> {
        let mut partitions: HashMap<usize, Vec<RecordBatch>> = HashMap::new();

        // 执行表扫描
        let stream = table.execute(0, Arc::new(TaskContext::default()))?;

        // 按 JOIN key 哈希分区
        let mut stream = Box::pin(stream);
        while let Some(batch) = stream.next().await {
            let batch = batch?;

            // 计算每行应该去哪个分区
            let partition_ids = self.compute_partition_ids(&batch, join_keys, is_left)?;

            // 将数据分发到对应分区
            for (row_idx, partition_id) in partition_ids.iter().enumerate() {
                let entry = partitions.entry(*partition_id).or_insert_with(Vec::new);
                // 提取单行数据并添加到对应分区
                let row_batch = batch.slice(row_idx, 1);
                entry.push(row_batch);
            }
        }

        Ok(partitions)
    }

    fn compute_partition_ids(
        &self,
        batch: &RecordBatch,
        join_keys: &[(Column, Column)],
        is_left: bool,
    ) -> Result<Vec<usize>> {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let num_rows = batch.num_rows();
        let mut partition_ids = Vec::with_capacity(num_rows);

        // 获取 JOIN key 列
        let key_columns: Vec<_> = join_keys.iter()
            .map(|(left_col, right_col)| {
                let col = if is_left { left_col } else { right_col };
                batch.column_by_name(&col.name)
                    .ok_or_else(|| Error::ColumnNotFound(col.name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        // 为每行计算分区 ID
        for row_idx in 0..num_rows {
            let mut hasher = DefaultHasher::new();

            // 对所有 JOIN key 列的值进行哈希
            for col in &key_columns {
                hash_array_value(col, row_idx, &mut hasher)?;
            }

            let hash = hasher.finish();
            let partition_id = (hash % self.partition_count as u64) as usize;
            partition_ids.push(partition_id);
        }

        Ok(partition_ids)
    }

    async fn execute_partitioned_joins(
        &self,
        partition: usize,
        left_partitions: HashMap<usize, Vec<RecordBatch>>,
        right_partitions: HashMap<usize, Vec<RecordBatch>>,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let nodes = self.node_registry
            .get_active_nodes(Some(NodeRole::DataNode))
            .await;

        let mut tasks = Vec::new();

        // 为每个分区分配一个节点执行 JOIN
        for partition_id in 0..self.partition_count {
            let node = &nodes[partition_id % nodes.len()];
            let left_data = left_partitions.get(&partition_id).cloned().unwrap_or_default();
            let right_data = right_partitions.get(&partition_id).cloned().unwrap_or_default();

            let client = self.rpc_client.clone();
            let node_addr = node.address.clone();
            let on = self.on.clone();
            let join_type = self.join_type;

            tasks.push(tokio::spawn(async move {
                client.execute_partition_join(
                    &node_addr,
                    partition_id,
                    left_data,
                    right_data,
                    on,
                    join_type,
                ).await
            }));
        }

        // 收集所有分区的结果
        let mut results = Vec::new();
        for task in tasks {
            let stream = task.await??;
            results.push(stream);
        }

        Ok(results)
    }
}

/// 对 Arrow 数组的单个值进行哈希
fn hash_array_value(
    array: &Arc<dyn Array>,
    row_idx: usize,
    hasher: &mut impl Hasher,
) -> Result<()> {
    use arrow::datatypes::DataType;

    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            if arr.is_valid(row_idx) {
                arr.value(row_idx).hash(hasher);
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            if arr.is_valid(row_idx) {
                arr.value(row_idx).hash(hasher);
            }
        }
        DataType::Timestamp(_, _) => {
            let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            if arr.is_valid(row_idx) {
                arr.value(row_idx).hash(hasher);
            }
        }
        // 添加更多数据类型支持...
        _ => return Err(Error::UnsupportedJoinKeyType(array.data_type().clone())),
    }

    Ok(())
}
```


#### 3. **JOIN 策略选择器**
根据表大小和数据分布自动选择最优 JOIN 策略：

```rust
// influxdb3_cluster/src/query/join/join_optimizer.rs
pub struct JoinOptimizer {
    catalog: Arc<Catalog>,
    shard_manager: Arc<ShardManager>,
    statistics_collector: Arc<StatisticsCollector>,
}

impl JoinOptimizer {
    /// 选择最优的 JOIN 策略
    pub async fn optimize_join(
        &self,
        left_table: &str,
        right_table: &str,
        join_type: JoinType,
        on: &[(Column, Column)],
    ) -> Result<JoinStrategy> {
        // 1. 收集表统计信息
        let left_stats = self.statistics_collector.get_table_stats(left_table).await?;
        let right_stats = self.statistics_collector.get_table_stats(right_table).await?;

        // 2. 估算表大小
        let left_size = left_stats.estimated_size_bytes;
        let right_size = right_stats.estimated_size_bytes;

        // 3. 根据表大小选择策略
        const BROADCAST_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB

        let strategy = if left_size < BROADCAST_THRESHOLD {
            JoinStrategy::Broadcast {
                small_table: left_table.to_string(),
                large_table: right_table.to_string(),
                broadcast_side: BroadcastSide::Left,
            }
        } else if right_size < BROADCAST_THRESHOLD {
            JoinStrategy::Broadcast {
                small_table: right_table.to_string(),
                large_table: left_table.to_string(),
                broadcast_side: BroadcastSide::Right,
            }
        } else {
            // 两个表都很大，使用 Shuffle JOIN
            JoinStrategy::Shuffle {
                partition_count: self.calculate_optimal_partitions(left_size + right_size),
            }
        };

        info!(
            left_table = %left_table,
            right_table = %right_table,
            left_size = %left_size,
            right_size = %right_size,
            ?strategy,
            "Selected JOIN strategy"
        );

        Ok(strategy)
    }

    fn calculate_optimal_partitions(&self, total_size: u64) -> usize {
        // 每个分区目标大小：256MB
        const TARGET_PARTITION_SIZE: u64 = 256 * 1024 * 1024;
        let partitions = (total_size / TARGET_PARTITION_SIZE).max(1) as usize;

        // 限制最大分区数
        partitions.min(256)
    }
}

#[derive(Debug, Clone)]
pub enum JoinStrategy {
    Broadcast {
        small_table: String,
        large_table: String,
        broadcast_side: BroadcastSide,
    },
    Shuffle {
        partition_count: usize,
    },
    CoLocated,  // 数据已经按 JOIN key 共同分区
}

#[derive(Debug, Clone, Copy)]
pub enum BroadcastSide {
    Left,
    Right,
}

/// 表统计信息
#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub row_count: u64,
    pub estimated_size_bytes: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub distinct_count: Option<u64>,
    pub null_count: u64,
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
}
```

#### 4. **Co-Located JOIN（协同定位 JOIN）**
当两个表按相同的 key 分片时，可以直接在本地执行 JOIN：

```rust
// influxdb3_cluster/src/query/join/colocated_join.rs
pub struct CoLocatedJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(Column, Column)>,
    join_type: JoinType,
    shard_manager: Arc<ShardManager>,
}

impl CoLocatedJoinExec {
    /// 检查两个表是否按相同的 key 分片
    pub async fn is_colocated(
        &self,
        left_table: &str,
        right_table: &str,
    ) -> Result<bool> {
        let left_sharding = self.shard_manager.get_sharding_scheme(left_table).await?;
        let right_sharding = self.shard_manager.get_sharding_scheme(right_table).await?;

        // 检查分片 key 是否匹配
        Ok(left_sharding.keys == right_sharding.keys &&
           left_sharding.shard_count == right_sharding.shard_count)
    }

    pub async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // 直接在本地执行 JOIN，无需数据移动
        let left_stream = self.left.execute(partition, Arc::new(TaskContext::default()))?;
        let right_stream = self.right.execute(partition, Arc::new(TaskContext::default()))?;

        // 使用 DataFusion 的本地 JOIN 实现
        let join_exec = HashJoinExec::try_new(
            self.left.clone(),
            self.right.clone(),
            self.on.clone(),
            None,  // filter
            &self.join_type,
            PartitionMode::Partitioned,
        )?;

        join_exec.execute(partition, Arc::new(TaskContext::default()))
    }
}
```

---

## 元数据管理

### 分布式元数据存储

使用 etcd 或自建 Raft 集群存储元数据：

```rust
// influxdb3_cluster/src/meta/meta_store.rs
use async_trait::async_trait;

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// 存储节点信息
    async fn put_node(&self, node: NodeInfo) -> Result<()>;
    async fn get_node(&self, node_id: NodeId) -> Result<Option<NodeInfo>>;
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>>;
    async fn delete_node(&self, node_id: NodeId) -> Result<()>;

    /// 存储分片信息
    async fn put_shard(&self, shard: ShardInfo) -> Result<()>;
    async fn get_shard(&self, shard_id: ShardId) -> Result<Option<ShardInfo>>;
    async fn list_shards(&self, database_id: DbId) -> Result<Vec<ShardInfo>>;

    /// 存储数据库元数据
    async fn put_database(&self, db: DatabaseMetadata) -> Result<()>;
    async fn get_database(&self, db_id: DbId) -> Result<Option<DatabaseMetadata>>;
    async fn list_databases(&self) -> Result<Vec<DatabaseMetadata>>;

    /// 监听元数据变更
    async fn watch(&self, prefix: &str) -> Result<MetaWatcher>;
}

/// etcd 实现
pub struct EtcdMetaStore {
    client: etcd_client::Client,
    prefix: String,
}

impl EtcdMetaStore {
    pub async fn new(endpoints: Vec<String>, prefix: String) -> Result<Self> {
        let client = etcd_client::Client::connect(endpoints, None).await?;
        Ok(Self { client, prefix })
    }

    fn node_key(&self, node_id: NodeId) -> String {
        format!("{}/nodes/{}", self.prefix, node_id.as_u64())
    }

    fn shard_key(&self, shard_id: ShardId) -> String {
        format!("{}/shards/{}", self.prefix, shard_id.as_u64())
    }
}

#[async_trait]
impl MetaStore for EtcdMetaStore {
    async fn put_node(&self, node: NodeInfo) -> Result<()> {
        let key = self.node_key(node.node_id);
        let value = serde_json::to_vec(&node)?;
        self.client.put(key, value, None).await?;
        Ok(())
    }

    async fn get_node(&self, node_id: NodeId) -> Result<Option<NodeInfo>> {
        let key = self.node_key(node_id);
        let resp = self.client.get(key, None).await?;

        if let Some(kv) = resp.kvs().first() {
            let node: NodeInfo = serde_json::from_slice(kv.value())?;
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let prefix = format!("{}/nodes/", self.prefix);
        let resp = self.client.get(prefix, Some(GetOptions::new().with_prefix())).await?;

        let mut nodes = Vec::new();
        for kv in resp.kvs() {
            let node: NodeInfo = serde_json::from_slice(kv.value())?;
            nodes.push(node);
        }

        Ok(nodes)
    }

    async fn watch(&self, prefix: &str) -> Result<MetaWatcher> {
        let watch_key = format!("{}/{}", self.prefix, prefix);
        let (watcher, stream) = self.client.watch(
            watch_key,
            Some(WatchOptions::new().with_prefix()),
        ).await?;

        Ok(MetaWatcher {
            _watcher: watcher,
            stream,
        })
    }

    // 实现其他方法...
}
```


---

## 实施路线图

### 阶段 1：基础设施（1-2 个月）

#### 1.1 创建集群模块
```bash
# 创建新的 crate
cargo new --lib influxdb3_cluster
```

**任务清单**：
- [ ] 实现节点注册与发现机制
- [ ] 实现心跳检测
- [ ] 集成 etcd 或实现自建 Raft 元数据存储
- [ ] 实现分片管理器基础框架
- [ ] 定义 gRPC 服务接口

#### 1.2 修改 Cargo.toml
```toml
[workspace]
members = [
    # ... 现有成员
    "influxdb3_cluster",
]

[workspace.dependencies]
# 新增依赖
etcd-client = "0.12"
raft = "0.7"
tonic = { version = "0.11.0", features = ["tls", "tls-roots"] }
prost = "0.12"
```

### 阶段 2：写入路径改造（2-3 个月）

#### 2.1 修改 WriteBuffer
```rust
// influxdb3_write/src/write_buffer/mod.rs
impl WriteBufferImpl {
    pub async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> Result<BufferedWriteRequest> {
        // 1. 解析 line protocol
        let parsed = parse_lines(lp)?;

        // 2. 按分片分组
        let mut shard_batches: HashMap<ShardId, Vec<Row>> = HashMap::new();
        for row in parsed {
            let shard_id = self.cluster_manager
                .route_write(&database, &row.measurement, &row.tags)
                .await?;

            shard_batches.entry(shard_id)
                .or_insert_with(Vec::new)
                .push(row);
        }

        // 3. 并行写入各个分片
        let mut tasks = Vec::new();
        for (shard_id, rows) in shard_batches {
            let replicator = self.replicator.clone();
            let batch = WriteBatch::from_rows(rows);

            tasks.push(tokio::spawn(async move {
                replicator.replicate_write(
                    shard_id,
                    batch,
                    ConsistencyLevel::Quorum,
                ).await
            }));
        }

        // 4. 等待所有写入完成
        for task in tasks {
            task.await??;
        }

        Ok(BufferedWriteRequest { /* ... */ })
    }
}
```

**任务清单**：
- [ ] 实现写入路由逻辑
- [ ] 实现 Raft 复制
- [ ] 修改 WAL 支持分片
- [ ] 实现一致性级别控制
- [ ] 添加写入性能监控

### 阶段 3：查询路径改造（3-4 个月）

#### 3.1 修改 QueryExecutor
```rust
// influxdb3_server/src/query_executor/mod.rs
impl QueryExecutorImpl {
    pub async fn query_sql(
        &self,
        database: &str,
        query: &str,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        // 1. 解析 SQL
        let logical_plan = self.parse_sql(query, params)?;

        // 2. 创建分布式查询计划
        let distributed_plan = self.distributed_planner
            .create_distributed_plan(logical_plan, database)
            .await?;

        // 3. 执行分布式查询
        let stream = distributed_plan.execute(0, Arc::new(TaskContext::default()))?;

        Ok(stream)
    }
}
```

**任务清单**：
- [ ] 实现分布式查询计划器
- [ ] 实现查询路由
- [ ] 实现结果聚合
- [ ] 实现查询下推优化
- [ ] 添加查询性能监控

### 阶段 4：JOIN 支持（2-3 个月）

**任务清单**：
- [ ] 实现 Broadcast JOIN
- [ ] 实现 Shuffle JOIN
- [ ] 实现 Co-Located JOIN
- [ ] 实现 JOIN 优化器
- [ ] 实现统计信息收集
- [ ] 添加 JOIN 性能测试

### 阶段 5：运维工具（1-2 个月）

**任务清单**：
- [ ] 实现集群管理 CLI
- [ ] 实现数据迁移工具
- [ ] 实现负载均衡
- [ ] 实现故障恢复
- [ ] 实现监控和告警

---

## 代码实现细节

### 1. gRPC 服务定义

```protobuf
// influxdb3_cluster/proto/cluster.proto
syntax = "proto3";

package influxdb3.cluster;

// 集群节点服务
service ClusterService {
    // 节点注册
    rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);

    // 心跳
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);

    // 写入数据
    rpc Write(WriteRequest) returns (WriteResponse);

    // 执行查询
    rpc Query(QueryRequest) returns (stream QueryResponse);

    // Raft 消息
    rpc RaftMessage(RaftMessageRequest) returns (RaftMessageResponse);

    // 广播数据（用于 Broadcast JOIN）
    rpc BroadcastData(BroadcastDataRequest) returns (BroadcastDataResponse);

    // 执行本地 JOIN
    rpc ExecuteLocalJoin(LocalJoinRequest) returns (stream LocalJoinResponse);
}

message RegisterNodeRequest {
    string node_id = 1;
    string address = 2;
    uint32 grpc_port = 3;
    uint32 http_port = 4;
    NodeRole role = 5;
    NodeCapacity capacity = 6;
}

message WriteRequest {
    uint64 shard_id = 1;
    bytes data = 2;
    ConsistencyLevel consistency = 3;
}

message QueryRequest {
    string database = 1;
    string query = 2;
    repeated ShardId target_shards = 3;
}

message BroadcastDataRequest {
    string table_name = 1;
    repeated bytes record_batches = 2;
}

message LocalJoinRequest {
    uint32 partition = 1;
    repeated JoinColumn on = 2;
    JoinType join_type = 3;
}

enum NodeRole {
    COORDINATOR = 0;
    DATA_NODE = 1;
    MIXED = 2;
}

enum ConsistencyLevel {
    ONE = 0;
    QUORUM = 1;
    ALL = 2;
}

enum JoinType {
    INNER = 0;
    LEFT = 1;
    RIGHT = 2;
    FULL = 3;
}
```

### 2. 集群配置文件

```toml
# cluster.toml

[cluster]
# 集群名称
name = "influxdb3-cluster"

# 节点 ID（唯一）
node_id = "node-1"

# 节点角色：coordinator, data_node, mixed
role = "mixed"

[cluster.network]
# 监听地址
bind_address = "0.0.0.0:8086"

# gRPC 端口
grpc_port = 8087

# 对外广告地址（其他节点连接用）
advertise_address = "192.168.1.10:8086"

[cluster.meta]
# 元数据存储类型：etcd, raft
type = "etcd"

# etcd 端点
etcd_endpoints = ["http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"]

# 或使用内置 Raft
# type = "raft"
# raft_peers = ["node-1:8087", "node-2:8087", "node-3:8087"]

[cluster.sharding]
# 默认分片数
default_shard_count = 16

# 副本因子
replication_factor = 3

# 分片策略：hash, time, hybrid
strategy = "hash"

[cluster.consistency]
# 默认写入一致性级别
write_consistency = "quorum"

# 默认读取一致性级别
read_consistency = "one"

[cluster.join]
# JOIN 优化器配置
broadcast_threshold_mb = 100

# 最大分区数
max_partitions = 256

# 是否启用 Co-Located JOIN 优化
enable_colocated_join = true

[cluster.performance]
# 查询超时（秒）
query_timeout = 300

# 写入超时（秒）
write_timeout = 30

# 最大并发查询数
max_concurrent_queries = 100

# 最大并发写入数
max_concurrent_writes = 1000
```

### 3. 启动集群示例

```bash
# 启动第一个节点（Coordinator + Data Node）
influxdb3 serve \
    --node-id=node-1 \
    --cluster-role=mixed \
    --bind-address=0.0.0.0:8086 \
    --grpc-port=8087 \
    --advertise-address=192.168.1.10:8086 \
    --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379 \
    --shard-count=16 \
    --replication-factor=3 \
    --object-store=s3 \
    --bucket=influxdb-cluster \
    --aws-region=us-east-1

# 启动第二个节点
influxdb3 serve \
    --node-id=node-2 \
    --cluster-role=data_node \
    --bind-address=0.0.0.0:8086 \
    --grpc-port=8087 \
    --advertise-address=192.168.1.11:8086 \
    --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379 \
    --object-store=s3 \
    --bucket=influxdb-cluster \
    --aws-region=us-east-1

# 启动第三个节点
influxdb3 serve \
    --node-id=node-3 \
    --cluster-role=data_node \
    --bind-address=0.0.0.0:8086 \
    --grpc-port=8087 \
    --advertise-address=192.168.1.12:8086 \
    --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379 \
    --object-store=s3 \
    --bucket=influxdb-cluster \
    --aws-region=us-east-1
```


### 4. 多表 JOIN 查询示例

```sql
-- 示例 1：Broadcast JOIN（小表 JOIN 大表）
SELECT
    m.measurement_name,
    m.description,
    d.value,
    d.time
FROM
    metadata m
INNER JOIN
    sensor_data d ON m.sensor_id = d.sensor_id
WHERE
    d.time >= NOW() - INTERVAL '1 hour'
ORDER BY
    d.time DESC
LIMIT 1000;

-- 示例 2：Shuffle JOIN（大表 JOIN 大表）
SELECT
    t1.host,
    t1.cpu_usage,
    t2.memory_usage,
    t1.time
FROM
    cpu_metrics t1
INNER JOIN
    memory_metrics t2
    ON t1.host = t2.host
    AND t1.time = t2.time
WHERE
    t1.time >= NOW() - INTERVAL '24 hours'
    AND t1.cpu_usage > 80
    AND t2.memory_usage > 90;

-- 示例 3：多表 JOIN
SELECT
    h.hostname,
    h.datacenter,
    c.cpu_usage,
    m.memory_usage,
    d.disk_usage
FROM
    hosts h
INNER JOIN cpu_metrics c ON h.host_id = c.host_id
INNER JOIN memory_metrics m ON h.host_id = m.host_id
INNER JOIN disk_metrics d ON h.host_id = d.host_id
WHERE
    c.time >= NOW() - INTERVAL '1 hour'
    AND m.time >= NOW() - INTERVAL '1 hour'
    AND d.time >= NOW() - INTERVAL '1 hour';
```

---

## 性能优化建议

### 1. 查询优化

#### 谓词下推
```rust
// influxdb3_cluster/src/query/optimizer/predicate_pushdown.rs
pub struct PredicatePushdownOptimizer;

impl PredicatePushdownOptimizer {
    /// 将过滤条件下推到数据节点
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // 1. 识别可下推的过滤条件
        // 2. 将过滤条件添加到远程扫描算子
        // 3. 减少网络传输的数据量

        // 示例：将 WHERE time > xxx 下推到每个分片
        // 这样每个节点只扫描符合条件的数据
    }
}
```

#### 投影下推
```rust
// 只选择需要的列，减少数据传输
pub struct ProjectionPushdownOptimizer;

impl ProjectionPushdownOptimizer {
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // 分析查询需要的列
        // 在远程扫描时只读取这些列
    }
}
```

### 2. 缓存策略

#### 查询结果缓存
```rust
// influxdb3_cluster/src/cache/query_cache.rs
pub struct QueryResultCache {
    cache: Arc<RwLock<LruCache<QueryKey, CachedResult>>>,
    ttl: Duration,
}

impl QueryResultCache {
    pub async fn get_or_execute<F>(
        &self,
        query: &str,
        executor: F,
    ) -> Result<SendableRecordBatchStream>
    where
        F: Future<Output = Result<SendableRecordBatchStream>>,
    {
        let key = QueryKey::from_query(query);

        // 检查缓存
        if let Some(cached) = self.cache.read().await.get(&key) {
            if !cached.is_expired() {
                return Ok(cached.to_stream());
            }
        }

        // 执行查询
        let result = executor.await?;

        // 缓存结果
        self.cache_result(key, result).await
    }
}
```

#### 元数据缓存
```rust
// 缓存分片信息、表统计信息等
pub struct MetadataCache {
    shard_cache: Arc<RwLock<HashMap<DbId, Vec<ShardInfo>>>>,
    stats_cache: Arc<RwLock<HashMap<TableId, TableStatistics>>>,
}
```

### 3. 并行执行

#### 分片并行查询
```rust
pub async fn execute_parallel_shard_query(
    &self,
    shards: Vec<ShardId>,
    query: &str,
) -> Result<SendableRecordBatchStream> {
    // 并行查询所有分片
    let tasks: Vec<_> = shards.iter().map(|shard_id| {
        let query = query.to_string();
        let client = self.rpc_client.clone();

        tokio::spawn(async move {
            client.query_shard(*shard_id, &query).await
        })
    }).collect();

    // 收集结果
    let mut streams = Vec::new();
    for task in tasks {
        streams.push(task.await??);
    }

    // 合并流
    Ok(Box::pin(MergedStream::new(streams)))
}
```

### 4. 网络优化

#### 批量传输
```rust
// 使用批量传输减少网络往返
pub struct BatchedDataTransfer {
    batch_size: usize,
    compression: CompressionType,
}

impl BatchedDataTransfer {
    pub async fn send_batches(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        // 1. 合并小批次
        let merged = self.merge_small_batches(batches)?;

        // 2. 压缩数据
        let compressed = self.compress_batches(&merged)?;

        // 3. 批量发送
        self.send_compressed(compressed).await
    }
}
```

#### Arrow Flight 优化
```rust
// 使用 Arrow Flight 进行高效的列式数据传输
pub struct FlightDataTransfer {
    flight_client: FlightClient,
}

impl FlightDataTransfer {
    pub async fn transfer_data(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        // Arrow Flight 原生支持列式数据传输
        // 比 gRPC + Protobuf 更高效
        let flight_data = FlightData::from_batches(batches)?;
        self.flight_client.do_put(flight_data).await
    }
}
```

---

## 测试用例

### 1. 单元测试

```rust
// influxdb3_cluster/src/shard_manager/tests.rs
#[tokio::test]
async fn test_shard_routing() {
    let shard_manager = ShardManager::new(16, 3, Arc::new(MockMetaStore::new()));

    // 测试相同的 series key 总是路由到同一个分片
    let shard1 = shard_manager.route_write(
        "mydb",
        "cpu",
        &[("host", "server1"), ("region", "us-east")],
    );

    let shard2 = shard_manager.route_write(
        "mydb",
        "cpu",
        &[("host", "server1"), ("region", "us-east")],
    );

    assert_eq!(shard1, shard2);
}

#[tokio::test]
async fn test_replication() {
    let replicator = WriteReplicator::new(/* ... */);

    let batch = WriteBatch::new(/* ... */);
    let result = replicator.replicate_write(
        ShardId::from(1),
        batch,
        ConsistencyLevel::Quorum,
    ).await;

    assert!(result.is_ok());
}
```

### 2. 集成测试

```rust
// influxdb3_cluster/tests/integration_test.rs
#[tokio::test]
async fn test_distributed_query() {
    // 启动 3 节点集群
    let cluster = TestCluster::new(3).await;

    // 写入数据到不同分片
    cluster.write("mydb", "cpu,host=server1 value=80").await?;
    cluster.write("mydb", "cpu,host=server2 value=90").await?;

    // 执行分布式查询
    let result = cluster.query("SELECT * FROM cpu").await?;

    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_broadcast_join() {
    let cluster = TestCluster::new(3).await;

    // 小表
    cluster.write("mydb", "metadata,sensor_id=1 name=\"temp1\"").await?;
    cluster.write("mydb", "metadata,sensor_id=2 name=\"temp2\"").await?;

    // 大表
    for i in 0..10000 {
        cluster.write("mydb", &format!("sensor_data,sensor_id=1 value={}", i)).await?;
    }

    // 执行 Broadcast JOIN
    let result = cluster.query(
        "SELECT m.name, AVG(d.value)
         FROM metadata m
         JOIN sensor_data d ON m.sensor_id = d.sensor_id
         GROUP BY m.name"
    ).await?;

    assert!(result.row_count() > 0);
}
```

### 3. 性能测试

```rust
#[tokio::test]
async fn benchmark_write_throughput() {
    let cluster = TestCluster::new(3).await;
    let start = Instant::now();

    // 并发写入 100万条数据
    let mut tasks = Vec::new();
    for i in 0..1_000_000 {
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            cluster.write("mydb", &format!("cpu,host=server{} value={}", i % 100, i)).await
        }));
    }

    for task in tasks {
        task.await??;
    }

    let duration = start.elapsed();
    let throughput = 1_000_000.0 / duration.as_secs_f64();

    println!("Write throughput: {:.2} writes/sec", throughput);
    assert!(throughput > 10_000.0); // 至少 10k writes/sec
}

#[tokio::test]
async fn benchmark_join_performance() {
    let cluster = TestCluster::new(3).await;

    // 准备测试数据
    // ...

    let start = Instant::now();
    let result = cluster.query(
        "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id"
    ).await?;
    let duration = start.elapsed();

    println!("JOIN query time: {:?}", duration);
    assert!(duration.as_secs() < 10); // 应该在 10 秒内完成
}
```

---

## 监控和运维

### 1. 关键指标

```rust
// influxdb3_cluster/src/metrics.rs
pub struct ClusterMetrics {
    // 节点指标
    pub node_count: Gauge,
    pub node_failures: Counter,

    // 分片指标
    pub shard_count: Gauge,
    pub shard_rebalances: Counter,

    // 写入指标
    pub write_requests: Counter,
    pub write_latency: Histogram,
    pub write_errors: Counter,
    pub replication_lag: Gauge,

    // 查询指标
    pub query_requests: Counter,
    pub query_latency: Histogram,
    pub query_errors: Counter,
    pub distributed_queries: Counter,

    // JOIN 指标
    pub join_operations: Counter,
    pub broadcast_joins: Counter,
    pub shuffle_joins: Counter,
    pub join_latency: Histogram,

    // 网络指标
    pub network_bytes_sent: Counter,
    pub network_bytes_received: Counter,
    pub rpc_calls: Counter,
    pub rpc_errors: Counter,
}
```

### 2. 健康检查

```rust
// influxdb3_cluster/src/health.rs
pub struct HealthChecker {
    node_registry: Arc<NodeRegistry>,
    shard_manager: Arc<ShardManager>,
}

impl HealthChecker {
    pub async fn check_cluster_health(&self) -> ClusterHealth {
        let mut health = ClusterHealth::default();

        // 检查节点健康
        let nodes = self.node_registry.list_nodes().await;
        health.total_nodes = nodes.len();
        health.active_nodes = nodes.iter()
            .filter(|n| n.status == NodeStatus::Active)
            .count();

        // 检查分片健康
        let shards = self.shard_manager.list_all_shards().await;
        health.total_shards = shards.len();
        health.healthy_shards = shards.iter()
            .filter(|s| self.is_shard_healthy(s))
            .count();

        // 检查副本健康
        health.under_replicated_shards = shards.iter()
            .filter(|s| s.replicas.len() < self.shard_manager.replication_factor)
            .count();

        health
    }
}
```

---

## 总结

本方案详细描述了将 InfluxDB 3 Core 从单机版改造为集群版的完整技术方案，包括：

1. **分布式架构设计**：Coordinator + Data Node 架构，支持水平扩展
2. **数据分片与路由**：Hash/Time/Hybrid 分片策略，自动负载均衡
3. **一致性与复制**：基于 Raft 的强一致性复制，支持多种一致性级别
4. **分布式查询**：查询路由、并行执行、结果聚合
5. **多表 JOIN 支持**：
   - Broadcast JOIN（小表 JOIN 大表）
   - Shuffle JOIN（大表 JOIN 大表）
   - Co-Located JOIN（协同定位优化）
   - 智能 JOIN 策略选择
6. **元数据管理**：etcd 或 Raft 集群存储元数据
7. **性能优化**：谓词下推、投影下推、缓存、并行执行
8. **运维工具**：监控、健康检查、故障恢复

预计总开发周期：**9-13 个月**

关键技术栈：
- Rust + Tokio（异步运行时）
- Apache Arrow + DataFusion（查询引擎）
- Raft（共识协议）
- gRPC + Protobuf（RPC 通信）
- etcd（元数据存储）
- S3/GCS/Azure（对象存储）








