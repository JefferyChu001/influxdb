# 联邦查询实现指南

## 目标
实现最简单的联邦查询：**节点1写入 cpu 表，节点2写入 mem 表，从任意节点查询时能 JOIN 这两张表**

## 当前实现状态

### ✅ 已完成的组件

1. **集群基础设施**
   - ✅ NodeRegistry: 节点注册和发现
   - ✅ MetaStore: etcd 元数据存储
   - ✅ ClusteredWriteBuffer: 分布式写入缓冲区
   - ✅ RPC Server/Client: gRPC 通信框架

2. **联邦查询基础**
   - ✅ FederatedQueryExecutor: 联邦查询执行器
     - `query_remote_node_http()`: HTTP 远程查询
     - `find_nodes_with_table()`: 查找表所在节点
     - `fetch_table_from_nodes()`: 从多节点获取表数据

### ❌ 尚未完成的部分

1. **查询规划集成**
   - ❌ 将 FederatedQueryExecutor 集成到 QueryExecutor
   - ❌ SQL 解析器识别 JOIN 查询中的表
   - ❌ 判断哪些表在本地，哪些在远程

2. **远程数据获取**
   - ❌ 将远程查询结果转换为 Arrow RecordBatch
   - ❌ 创建 DataFusion 可用的数据源

3. **JOIN 执行**
   - ❌ 在本地执行跨节点表的 JOIN
   - ❌ 结果合并和返回

## 实现方案

### 方案 1: 在 QueryExecutor 层实现（推荐）

修改 `influxdb3_query_executor/src/lib.rs` 中的 `QueryExecutorImpl`:

```rust
// 1. 在 query_sql 方法中拦截 JOIN 查询
pub async fn query_sql(
    &self,
    database: &str,
    query: &str,
    ...
) -> Result<SendableRecordBatchStream> {
    // 解析 SQL，检查是否有 JOIN
    if let Some(tables) = extract_tables_from_sql(query) {
        if tables.len() > 1 {
            // 多表查询，可能需要联邦查询
            return self.execute_federated_join(database, query, &tables).await;
        }
    }
    
    // 单表查询，走正常流程
    ...
}

// 2. 实现联邦 JOIN 方法
async fn execute_federated_join(
    &self,
    database: &str,
    query: &str,
    tables: &[String],
) -> Result<SendableRecordBatchStream> {
    // 为每个表创建数据源
    let mut table_data = HashMap::new();
    
    for table in tables {
        // 检查本地是否有这个表
        if let Some(local_data) = self.get_local_table_data(database, table).await? {
            table_data.insert(table.clone(), local_data);
        } else {
            // 从远程节点获取
            let remote_data = self.federated_executor
                .fetch_table_from_nodes(database, table)
                .await?;
            table_data.insert(table.clone(), remote_data);
        }
    }
    
    // 使用 DataFusion 执行 JOIN
    self.execute_join_with_datafusion(query, table_data).await
}
```

### 方案 2: 在 ClusteredWriteBuffer 层实现（简化版）

在写入时记录表的位置，查询时自动路由：

```rust
// 在 ClusteredWriteBuffer 中维护表位置映射
struct TableLocationMap {
    // table_name -> Vec<NodeId>
    locations: Arc<RwLock<HashMap<String, Vec<NodeId>>>>,
}

// 写入时更新映射
async fn write_lp(...) {
    // ... 写入逻辑
    
    // 记录表位置
    for table in parsed_tables {
        self.table_locations.insert(table, current_node_id);
    }
}

// 查询时使用映射
async fn query(...) {
    if is_join_query(query) {
        let tables = extract_tables(query);
        for table in tables {
            let nodes = self.table_locations.get(table);
            // 从这些节点获取数据
        }
    }
}
```

## 最简单的实现步骤

### Step 1: 手动指定表位置（最快）

创建配置文件 `table_locations.json`:
```json
{
  "testdb": {
    "cpu": ["node1"],
    "mem": ["node2"],
    "disk": ["node3"]
  }
}
```

### Step 2: 实现简单的联邦查询处理器

```rust
// 在查询前检查表位置
if query.contains("JOIN") {
    // 1. 解析出需要的表: cpu, mem
    // 2. 查配置文件: cpu在node1, mem在node2
    // 3. 分别查询:
    //    - curl http://node1:8181/api/v3/query_sql {"db":"testdb","query":"SELECT * FROM cpu"}
    //    - curl http://node2:8182/api/v3/query_sql {"db":"testdb","query":"SELECT * FROM mem"}
    // 4. 将JSON结果转为RecordBatch
    // 5. 注册为DataFusion临时表
    // 6. 执行JOIN查询
    // 7. 返回结果
}
```

### Step 3: 代码示例

```rust
use datafusion::prelude::*;

async fn execute_cross_node_join() -> Result<()> {
    // 1. 获取远程数据
    let cpu_json = query_node("http://localhost:8181", "SELECT * FROM cpu").await?;
    let mem_json = query_node("http://localhost:8182", "SELECT * FROM mem").await?;
    
    // 2. 转换为 RecordBatch
    let cpu_batch = json_to_record_batch(&cpu_json)?;
    let mem_batch = json_to_record_batch(&mem_json)?;
    
    // 3. 创建 DataFusion context
    let ctx = SessionContext::new();
    ctx.register_batch("cpu", cpu_batch)?;
    ctx.register_batch("mem", mem_batch)?;
    
    // 4. 执行 JOIN
    let df = ctx.sql("
        SELECT c.host, c.value, m.used 
        FROM cpu c 
        JOIN mem m ON c.host = m.host
    ").await?;
    
    // 5. 返回结果
    df.collect().await
}
```

## 测试步骤

1. **启动集群**
```bash
# 终端1: 节点1
./target/debug/influxdb3 serve --node-id node1 --object-store file --data-dir ./node1 \
  --http-bind 127.0.0.1:8181 --without-auth \
  --cluster-enable --cluster-node-id 1 --cluster-grpc-bind 127.0.0.1:8087 \
  --cluster-etcd-endpoints http://127.0.0.1:2379

# 终端2: 节点2
./target/debug/influxdb3 serve --node-id node2 --object-store file --data-dir ./node2 \
  --http-bind 127.0.0.1:8182 --without-auth \
  --cluster-enable --cluster-node-id 2 --cluster-grpc-bind 127.0.0.1:8088 \
  --cluster-etcd-endpoints http://127.0.0.1:2379
```

2. **写入测试数据**
```bash
# 节点1: 写入 cpu 表
curl -X POST "http://localhost:8181/api/v3/write_lp?db=testdb" \
  -d "cpu,host=server01 value=0.64 1700000000000000000"

# 节点2: 写入 mem 表  
curl -X POST "http://localhost:8182/api/v3/write_lp?db=testdb" \
  -d "mem,host=server01 used=8192 1700000000000000000"
```

3. **执行联邦查询**
```bash
# 在任意节点执行 JOIN 查询
curl -X POST "http://localhost:8181/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d '{
    "db": "testdb",
    "query": "SELECT c.host, c.value, m.used FROM cpu c JOIN mem m ON c.host = m.host"
  }'
```

## 下一步行动

1. **立即可做**: 修改 test_federated_query.sh，改为实际可运行的测试
2. **需要实现**: 在 QueryExecutorImpl 中添加 execute_federated_join 方法
3. **需要工具**: 实现 json_to_record_batch 转换函数

要我帮你实现哪一部分？

