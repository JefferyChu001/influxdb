# InfluxDB 3 分布式查询层技术方案分析

## 目标

实现高效的分布式查询层，支持跨节点的联邦查询（Federated Query），具备谓词下推、投影下推等优化功能。

---

## 当前问题分析

### 现有实现的缺陷

我们当前的简化版联邦查询实现存在以下问题：

```rust
// 当前实现：拉取整张表
let cpu_data = query_node("node1", "SELECT * FROM cpu").await;  // 拉取全部数据
let mem_data = query_node("node2", "SELECT * FROM mem").await;  // 拉取全部数据

// 在本地执行 JOIN
local_join(cpu_data, mem_data);
```

**问题**：
1. ❌ 无谓词下推（Predicate Pushdown）：总是 `SELECT *`
2. ❌ 无投影下推（Projection Pushdown）：拉取所有列
3. ❌ 无聚合下推（Aggregation Pushdown）：在本地进行聚合
4. ❌ 网络传输量巨大：对于大表，性能极差
5. ❌ 内存压力：所有数据加载到协调节点内存

### 理想实现目标

```sql
-- 用户查询
SELECT c.host, c.value, m.used 
FROM cpu c JOIN mem m ON c.host = m.host 
WHERE c.host = 'server01'
```

**期望行为**：
```
1. 查询规划器分析 SQL，提取谓词和投影
2. 生成分布式查询计划：
   - 子查询1: SELECT host, value FROM cpu WHERE host = 'server01' (发送到节点1)
   - 子查询2: SELECT host, used FROM mem WHERE host = 'server01' (发送到节点2)
3. 并行执行远程查询
4. 在协调节点执行 JOIN（数据量很小）
5. 返回结果
```

---

## 方案一：参考 GreptimeDB 架构，构建独立查询层

### GreptimeDB 架构概述

GreptimeDB 采用**三层架构**：

```
┌──────────────────────────────────────────────────────────┐
│                     Frontend (查询层)                      │
│  - 接收查询请求                                             │
│  - SQL 解析和查询规划                                       │
│  - 分布式查询协调                                           │
│  - 结果聚合和返回                                           │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                    Meta Server (元数据层)                  │
│  - 存储表 schema 信息                                       │
│  - 记录数据分布（表在哪些节点）                             │
│  - 节点注册和心跳                                           │
│  - 分区信息管理                                             │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                   Datanode (存储层)                        │
│  - 实际存储数据                                             │
│  - 执行本地查询                                             │
│  - 返回查询结果                                             │
└──────────────────────────────────────────────────────────┘
```

### 核心组件设计

#### 1. Frontend (查询协调层)

**职责**：
- 接收 HTTP/gRPC 查询请求
- SQL 解析和逻辑计划生成
- 分布式查询规划
- 协调多个 Datanode 执行
- 结果聚合

**关键代码结构**（参考 GreptimeDB）：

```rust
// src/frontend/src/instance.rs
pub struct Instance {
    query_engine: QueryEngineRef,
    catalog_manager: CatalogManagerRef,
    partition_manager: PartitionManagerRef,
}

impl Instance {
    pub async fn execute_sql(&self, sql: &str) -> Result<RecordBatchStream> {
        // 1. SQL 解析
        let statement = self.parse_sql(sql)?;
        
        // 2. 逻辑计划生成
        let logical_plan = self.create_logical_plan(statement)?;
        
        // 3. 分布式物理计划
        let physical_plan = self.create_distributed_plan(logical_plan)?;
        
        // 4. 执行
        self.execute_plan(physical_plan).await
    }
}
```

#### 2. Distributed Query Planner

**核心功能**：将逻辑计划转换为分布式物理计划

```rust
// src/query/src/dist_plan/planner.rs
pub struct DistributedPlanner {
    meta_client: MetaClientRef,
    partition_manager: PartitionManagerRef,
}

impl DistributedPlanner {
    pub fn plan(&self, logical_plan: LogicalPlan) -> Result<DistributedPlan> {
        match logical_plan {
            LogicalPlan::Join { left, right, on, .. } => {
                // 1. 分析 JOIN 条件
                // 2. 确定左右表的数据位置
                // 3. 决定 JOIN 策略（broadcast / shuffle / local）
                self.plan_distributed_join(left, right, on)
            }
            LogicalPlan::Filter { predicate, input } => {
                // 谓词下推
                self.push_down_filter(predicate, input)
            }
            LogicalPlan::Projection { expr, input } => {
                // 投影下推
                self.push_down_projection(expr, input)
            }
            _ => self.plan_default(logical_plan),
        }
    }
}
```

#### 3. Remote Query Executor

**职责**：执行远程查询并获取结果

```rust
pub struct RemoteQueryExecutor {
    node_manager: NodeManagerRef,
    grpc_client: GrpcClientRef,
}

impl RemoteQueryExecutor {
    pub async fn query_datanode(
        &self,
        node_id: NodeId,
        region_id: RegionId,
        plan: PhysicalPlanRef,
    ) -> Result<SendableRecordBatchStream> {
        // 1. 序列化物理计划
        let encoded_plan = self.encode_plan(plan)?;
        
        // 2. gRPC 调用 Datanode
        let request = QueryRequest {
            region_id,
            plan: encoded_plan,
        };
        
        let stream = self.grpc_client
            .query(node_id, request)
            .await?;
        
        // 3. 反序列化结果流
        Ok(stream)
    }
}
```

### 实现步骤

#### Phase 1: 基础架构搭建（2-3 周）

1. **创建 Frontend 服务**
   ```bash
   influxdb3_cluster/
   ├── src/
   │   ├── frontend/           # 新增：查询协调层
   │   │   ├── instance.rs     # Frontend 主逻辑
   │   │   ├── handler.rs      # HTTP/gRPC 请求处理
   │   │   └── mod.rs
   │   ├── query/
   │   │   ├── dist_planner.rs # 分布式查询规划器
   │   │   ├── optimizer.rs    # 查询优化器（下推等）
   │   │   └── executor.rs     # 远程查询执行器
   ```

2. **元数据管理增强**
   ```rust
   // 在 etcd 中存储表分布信息
   pub struct TableLocationMetadata {
       table_id: TableId,
       database_id: DbId,
       partitions: Vec<PartitionInfo>,
   }

   pub struct PartitionInfo {
       partition_id: u64,
       node_ids: Vec<NodeId>,  // 存储该分区的节点列表
       key_range: Option<(Bound, Bound)>,  // 分区键范围
   }
   ```

3. **实现基础的分布式查询协议**
   ```protobuf
   // proto/distributed_query.proto
   service DistributedQuery {
       rpc ExecuteQuery(QueryRequest) returns (stream QueryResponse);
       rpc GetTableLocations(TableRequest) returns (TableLocations);
   }

   message QueryRequest {
       string database = 1;
       bytes physical_plan = 2;  // 序列化的物理计划
       repeated string table_names = 3;
   }
   ```

#### Phase 2: 查询优化实现（3-4 周）

1. **谓词下推（Predicate Pushdown）**
   ```rust
   impl DistributedPlanner {
       fn push_down_predicates(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
           match plan {
               LogicalPlan::Filter { predicate, input } => {
                   match *input {
                       LogicalPlan::TableScan { .. } => {
                           // 将 WHERE 条件推送到远程节点
                           self.create_remote_scan_with_filter(predicate)
                       }
                       _ => // 递归处理
                   }
               }
           }
       }
   }
   ```

2. **投影下推（Projection Pushdown）**
   ```rust
   fn optimize_projection(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
       // 分析整个查询树，确定每个表实际需要的列
       let required_columns = self.analyze_required_columns(&plan)?;

       // 修改 TableScan，只请求需要的列
       self.apply_column_pruning(plan, required_columns)
   }
   ```

3. **JOIN 策略优化**
   ```rust
   enum JoinStrategy {
       Broadcast {
           // 小表广播到大表所在的所有节点
           small_table: TableId,
           broadcast_to_nodes: Vec<NodeId>,
       },
       Shuffle {
           // 两个大表，按 JOIN key 重分区
           partition_key: String,
           num_partitions: usize,
       },
       ColocatedJoin {
           // 数据已经在同一节点，直接本地 JOIN
           node_id: NodeId,
       },
   }
   ```

#### Phase 3: 并行执行和流式处理（2-3 周）

1. **并行查询执行**
   ```rust
   async fn execute_distributed_query(
       &self,
       sub_queries: Vec<(NodeId, PhysicalPlan)>,
   ) -> Result<Vec<SendableRecordBatchStream>> {
       // 并行查询所有节点
       let futures = sub_queries.into_iter()
           .map(|(node_id, plan)| {
               let executor = self.remote_executor.clone();
               async move {
                   executor.execute_on_node(node_id, plan).await
               }
           });

       futures::future::try_join_all(futures).await
   }
   ```

2. **流式 JOIN**
   ```rust
   // 使用 DataFusion 的流式 JOIN，避免全量加载
   let join_exec = HashJoinExec::try_new(
       left_stream,
       right_stream,
       on,
       JoinType::Inner,
       PartitionMode::Partitioned,  // 流式处理
   )?;
   ```

### 方案一的优缺点

#### ✅ 优点

1. **架构清晰**：前端/元数据/存储三层分离，职责明确
2. **可扩展性强**：可以独立扩展查询层和存储层
3. **性能优化空间大**：支持各种下推优化
4. **成熟案例**：GreptimeDB 已验证此架构可行

#### ❌ 缺点

1. **工程量大**：需要从头实现整个查询层
2. **复杂度高**：需要实现 SQL 解析、查询规划、优化器
3. **时间成本**：预计 2-3 个月完成基础功能
4. **维护成本**：需要维护独立的查询层代码

### 技术挑战

1. **物理计划序列化**：如何在网络上传输 DataFusion 的 PhysicalPlan
2. **结果流合并**：如何高效合并多个节点的流式结果
3. **错误处理**：部分节点失败时如何处理
4. **事务一致性**：跨节点查询的一致性保证

---

## 方案二：基于 iox_query 扩展，实现分布式查询

### iox_query 架构分析

iox_query 是 InfluxDB 3 Core 的查询引擎，基于 DataFusion 构建。

#### 核心接口

```rust
// 1. QueryDatabase: 顶层查询接口
pub trait QueryDatabase: Send + Sync {
    async fn namespace(
        &self,
        name: &str,
        span: Option<Span>,
        include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>>;
}

// 2. QueryNamespace: 数据库命名空间
pub trait QueryNamespace: SchemaProvider + Send + Sync {
    fn new_query_context(&self, ...) -> IOxSessionContext;
    fn record_query(&self, ...) -> QueryCompletedToken;
}

// 3. QueryChunk: 数据块抽象
pub trait QueryChunk: Send + Sync {
    fn data(&self) -> QueryChunkData;
    fn schema(&self) -> &Schema;
    fn stats(&self) -> Arc<Statistics>;
}

// 4. ChunkContainer: 数据容器
pub trait ChunkContainer: Send + Sync {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>>;
}
```

### 扩展点分析

#### 扩展点 1: 自定义 TableProvider

**核心思路**：实现一个 `DistributedTableProvider`，代理远程表

```rust
pub struct DistributedTableProvider {
    table_name: String,
    schema: SchemaRef,
    remote_nodes: Vec<NodeId>,  // 存储该表的节点列表
    node_manager: Arc<NodeRegistry>,
    grpc_client: Arc<GrpcClient>,
}

#[async_trait]
impl TableProvider for DistributedTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 关键：创建一个自定义的 ExecutionPlan
        // 它在执行时会查询远程节点
        Ok(Arc::new(RemoteTableScanExec::new(
            self.remote_nodes.clone(),
            self.table_name.clone(),
            self.schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
            self.grpc_client.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // ✅ 关键：声明支持谓词下推
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}
```

#### 扩展点 2: 自定义 ExecutionPlan

**核心思路**：实现 `RemoteTableScanExec`，在执行时查询远程节点

```rust
pub struct RemoteTableScanExec {
    remote_nodes: Vec<NodeId>,
    table_name: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,  // 谓词会被自动下推到这里
    limit: Option<usize>,
    grpc_client: Arc<GrpcClient>,
}

impl ExecutionPlan for RemoteTableScanExec {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // 1. 选择一个远程节点
        let node_id = self.remote_nodes[partition % self.remote_nodes.len()];

        // 2. 构建远程查询请求
        let query = self.build_remote_query();  // 包含 filters 和 projection

        // 3. 异步查询远程节点
        let stream = self.query_remote_node(node_id, query);

        Ok(stream)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl RemoteTableScanExec {
    fn build_remote_query(&self) -> String {
        // 将 filters 和 projection 转换为 SQL
        let columns = self.projection.as_ref()
            .map(|p| p.iter().map(|i| self.schema.field(*i).name()).join(", "))
            .unwrap_or("*".to_string());

        let where_clause = self.filters.iter()
            .map(|f| format!("{}", f))
            .join(" AND ");

        let mut sql = format!("SELECT {} FROM {}", columns, self.table_name);
        if !where_clause.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        sql
    }
}
```

#### 扩展点 3: 自定义 SchemaProvider

**核心思路**：扩展 `Database` 的 `table()` 方法，返回分布式表

```rust
// 在 influxdb3_query_executor/src/lib.rs 中扩展
impl Database {
    async fn query_table(&self, table_name: &str) -> Option<Arc<QueryTable>> {
        // 1. 检查是否是本地表
        if self.db_schema.table_name_to_id(table_name).is_some() {
            // 本地表，返回原有的 QueryTable
            return self.get_local_table(table_name).await;
        }

        // 2. 查询元数据，检查是否是远程表
        if let Some(remote_info) = self.meta_store
            .get_table_location(&self.db_schema.id, table_name)
            .await
        {
            // 3. 返回分布式表
            return Some(Arc::new(DistributedQueryTable::new(
                table_name,
                remote_info.nodes,
                remote_info.schema,
                self.grpc_client.clone(),
            )));
        }

        None
    }
}

// 包装器：既可以是本地表，也可以是远程表
pub enum QueryTable {
    Local(LocalQueryTable),
    Distributed(DistributedQueryTable),
}

impl TableProvider for QueryTable {
    async fn scan(&self, ...) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            QueryTable::Local(local) => local.scan(...).await,
            QueryTable::Distributed(remote) => remote.scan(...).await,
        }
    }
}
```

#### 扩展点 4: 利用 DataFusion 的优化器

DataFusion 内置了强大的优化器，我们可以利用它：

```rust
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::OptimizerRule;

// 自定义优化规则：识别跨节点的 JOIN
pub struct DistributedJoinOptimizer {
    meta_store: Arc<MetaStore>,
    node_registry: Arc<NodeRegistry>,
}

impl OptimizerRule for DistributedJoinOptimizer {
    fn name(&self) -> &str {
        "distributed_join_optimizer"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(join) => {
                // 1. 分析左右表的位置
                let left_nodes = self.find_table_nodes(&join.left)?;
                let right_nodes = self.find_table_nodes(&join.right)?;

                // 2. 如果表在不同节点，选择 JOIN 策略
                if !self.are_colocated(&left_nodes, &right_nodes) {
                    return Ok(Some(self.optimize_distributed_join(join)?));
                }

                Ok(None)  // 不需要优化
            }
            _ => Ok(None),
        }
    }

    fn supports_rewrite(&self) -> bool {
        true
    }
}

// 注册优化器
let mut optimizer = Optimizer::new();
optimizer.rules.push(Arc::new(DistributedJoinOptimizer::new(
    meta_store,
    node_registry,
)));
```

### 实现步骤

#### Phase 1: 基础扩展（1-2 周）

1. **实现 DistributedTableProvider**
   ```bash
   influxdb3_query_executor/src/
   ├── distributed/
   │   ├── table_provider.rs   # 分布式表提供者
   │   ├── scan_exec.rs         # RemoteTableScanExec
   │   └── mod.rs
   ```

2. **扩展元数据存储**
   ```rust
   // 在 etcd 中存储表位置信息
   impl MetaStore {
       async fn register_table_location(
           &self,
           db_id: DbId,
           table_name: &str,
           nodes: Vec<NodeId>,
           schema: SchemaRef,
       ) -> Result<()>;

       async fn get_table_location(
           &self,
           db_id: DbId,
           table_name: &str,
       ) -> Result<Option<TableLocationInfo>>;
   }
   ```

3. **实现 gRPC 查询接口**
   ```rust
   // 在现有的 gRPC 服务中添加
   service ClusterService {
       rpc ExecuteQuery(QueryRequest) returns (stream RecordBatch);
   }
   ```

#### Phase 2: 优化器集成（1-2 周）

1. **实现谓词和投影下推**
   - ✅ DataFusion 会自动将 filters 和 projection 传递给 `TableProvider::scan()`
   - ✅ 我们只需要在 `RemoteTableScanExec` 中使用这些信息构建远程查询

2. **实现分布式 JOIN 优化规则**
   ```rust
   // 注册到 QueryExecutorImpl
   impl QueryExecutorImpl {
       pub fn new(...) -> Self {
           let mut optimizer = Optimizer::new();
           optimizer.rules.push(Arc::new(DistributedJoinOptimizer::new(...)));

           // ... 其他初始化
       }
   }
   ```

#### Phase 3: 性能优化（1-2 周）

1. **并行查询**：使用 `output_partitioning()` 支持并行
2. **结果缓存**：缓存频繁查询的远程表元数据
3. **连接池**：复用 gRPC 连接

### 方案二的优缺点

#### ✅ 优点

1. **工程量小**：复用 iox_query 和 DataFusion 的大量功能
2. **开发周期短**：预计 3-5 周完成基础功能
3. **代码侵入性低**：主要是扩展，不需要大改现有代码
4. **自动优化**：DataFusion 的优化器会自动处理很多优化
5. **维护成本低**：依赖成熟的 DataFusion，只维护扩展部分

#### ❌ 缺点

1. **受限于 DataFusion**：某些高级优化可能难以实现
2. **灵活性较低**：不能完全控制查询执行流程
3. **调试困难**：DataFusion 内部逻辑复杂
4. **依赖稳定性**：iox_query 的接口变更会影响我们

### 技术挑战

1. **Expr 到 SQL 的转换**：如何将 DataFusion 的 Expr 转换为 SQL WHERE 子句
2. **Schema 一致性**：确保远程表的 schema 与本地 catalog 一致
3. **流式传输**：gRPC stream 的高效实现
4. **错误传播**：远程节点错误如何正确传播到客户端

---

## 方案对比

| 维度 | 方案一（独立查询层） | 方案二（iox_query 扩展） |
|-----|------------------|---------------------|
| **工程量** | 大（2-3个月） | 小（3-5周） |
| **代码行数** | ~5000-8000 行 | ~1000-2000 行 |
| **技术复杂度** | 高 | 中 |
| **维护成本** | 高 | 低 |
| **性能优化空间** | 大 | 中 |
| **灵活性** | 高 | 中 |
| **风险** | 高（新架构） | 低（基于现有） |
| **可扩展性** | 优秀 | 良好 |

### 核心对比：谓词下推实现

#### 方案一（手动实现）

```rust
// 需要自己解析 SQL，提取 WHERE 条件
let statement = parse_sql(query)?;
let predicates = extract_predicates(&statement)?;  // 自己实现

// 需要自己构建远程查询
let remote_query = format!(
    "SELECT {} FROM {} WHERE {}",
    columns,
    table_name,
    predicates_to_sql(&predicates)  // 自己实现
);
```

#### 方案二（自动处理）

```rust
// DataFusion 自动传递 filters
async fn scan(
    &self,
    _state: &dyn Session,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],  // ✅ DataFusion 自动提取的谓词
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // 直接使用 filters 构建远程查询
    let where_clause = filters.iter()
        .map(|f| expr_to_sql(f))  // 简单的转换
        .join(" AND ");
}
```

### 核心对比：JOIN 优化

#### 方案一

```rust
// 需要自己实现完整的 JOIN 策略选择
impl DistributedPlanner {
    fn plan_join(&self, left: TableId, right: TableId) -> JoinStrategy {
        let left_size = self.estimate_table_size(left);
        let right_size = self.estimate_table_size(right);

        if left_size < BROADCAST_THRESHOLD {
            JoinStrategy::Broadcast { small: left }
        } else if self.are_colocated(left, right) {
            JoinStrategy::Local
        } else {
            JoinStrategy::Shuffle { ... }
        }
    }
}
```

#### 方案二

```rust
// DataFusion 的 JOIN 优化器会自动选择策略
// 我们只需要提供统计信息
impl TableProvider for DistributedTableProvider {
    async fn statistics(&self) -> Result<Statistics> {
        // 查询远程节点获取统计信息
        let stats = self.query_remote_statistics().await?;
        Ok(stats)
    }
}

// DataFusion 会根据统计信息自动选择：
// - HashJoin / MergeJoin / NestedLoopJoin
// - Partitioned / CollectLeft
```

---

## 推荐方案：方案二（iox_query 扩展）

### 推荐理由

1. **投入产出比高**
   - 3-5 周即可实现基础功能
   - 方案一需要 2-3 个月

2. **风险低**
   - 基于成熟的 DataFusion 和 iox_query
   - 方案一需要从头验证架构

3. **代码质量高**
   - 复用 DataFusion 经过验证的优化器
   - 方案一需要自己实现和调试

4. **维护成本低**
   - 主要维护扩展代码（~2000 行）
   - 方案一需要维护整个查询层（~8000 行）

5. **渐进式演进**
   - 可以先实现基础功能快速验证
   - 后续如需更多控制，可以逐步增强
   - 方案一是"all or nothing"

### 实现路线图

#### Week 1-2: 基础框架

- [ ] 实现 `DistributedTableProvider`
- [ ] 实现 `RemoteTableScanExec`
- [ ] 扩展 `Database::query_table()` 支持远程表
- [ ] gRPC 查询接口

#### Week 3: 谓词和投影下推

- [ ] 实现 `Expr` 到 SQL 的转换
- [ ] 测试 WHERE 条件下推
- [ ] 测试列裁剪

#### Week 4: JOIN 优化

- [ ] 实现 `DistributedJoinOptimizer`
- [ ] 支持统计信息查询
- [ ] 测试跨节点 JOIN

#### Week 5: 性能优化和测试

- [ ] 并行查询优化
- [ ] 连接池和缓存
- [ ] 性能基准测试
- [ ] 集成测试

### 关键代码示例

```rust
// 最小可用版本（MVP）
pub struct DistributedTableProvider {
    table_name: String,
    schema: SchemaRef,
    remote_nodes: Vec<NodeId>,
    grpc_client: Arc<GrpcClient>,
}

#[async_trait]
impl TableProvider for DistributedTableProvider {
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RemoteTableScanExec::new(
            self.remote_nodes[0],  // 简化：只查询第一个节点
            self.build_query(projection, filters, limit),
            self.schema.clone(),
            self.grpc_client.clone(),
        )))
    }
}

// 3-5 周即可实现核心功能！
```

---

## 下一步行动

### 立即可做

1. **创建基础结构**
   ```bash
   mkdir -p influxdb3_query_executor/src/distributed
   ```

2. **定义 gRPC 接口**
   ```bash
   vim influxdb3_cluster/proto/distributed_query.proto
   ```

3. **实现最小原型**
   - 实现 `DistributedTableProvider`（只支持 SELECT *）
   - 测试跨节点查询

### 验证关键假设

1. **测试 DataFusion 的 filter 下推**
   ```rust
   // 验证 DataFusion 是否会自动将 WHERE 传递给 scan()
   ```

2. **测试 Expr 到 SQL 转换**
   ```rust
   // 验证能否正确转换各种 Expr
   ```

3. **测试 gRPC streaming 性能**
   ```rust
   // 验证流式传输的效率
   ```

---

## 总结

- **推荐方案二**：基于 iox_query 扩展
- **理由**：快速、低风险、高质量
- **时间**：3-5 周完成 MVP
- **关键**：充分利用 DataFusion 和 iox_query 的能力

要开始实施吗？我可以先帮你实现第一个原型！


