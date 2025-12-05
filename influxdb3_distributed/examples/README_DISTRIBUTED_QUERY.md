# 分布式查询示例说明

## 概述

这个示例展示了如何使用分布式查询框架来执行真实的查询。

## 工作原理

### 当前实现方式（HTTP）

```
┌─────────────┐
│  协调器节点  │
│  (运行示例)  │
└──────┬──────┘
       │
       │ 1. 生成分布式计划
       │
       ▼
┌──────────────────┐
│ DistributedPlanner│
│  - 解析 SQL       │
│  - 生成逻辑计划    │
│  - 生成分布式物理  │
│    执行计划       │
└──────┬───────────┘
       │
       │ 2. HttpTableProvider 执行
       │
       ▼
┌──────────────────┐
│  InfluxDB 节点    │
│  (127.0.0.1:8181)│
│                  │
│  HTTP API:       │
│  /api/v3/query_sql│
└──────────────────┘
```

### 执行流程

1. **生成逻辑计划**: SessionContext 将 SQL 转换为 DataFusion 逻辑计划
2. **生成分布式计划**: DistributedPlanner 分析逻辑计划，生成：
   - `remote_plans`: 每个节点需要执行的子计划
   - `coordinator_plan`: 协调器侧的合并/聚合计划
3. **执行查询**: 
   - HttpTableProvider 通过 HTTP POST 请求发送 SQL 到各节点
   - 节点执行 SQL 并返回 Arrow IPC 格式的结果
   - 协调器接收并合并结果

## 运行示例

### 前置条件

1. 启动 InfluxDB 3.0 节点：
```bash
# 假设你已经有一个运行中的 InfluxDB 3.0 实例
# 默认监听 127.0.0.1:8181
```

2. 写入测试数据：
```bash
curl -X POST "http://127.0.0.1:8181/api/v3/write?db=mydb" \
  --data-binary "cpu,host=server1,region=us-east usage=75.0
cpu,host=server2,region=us-west usage=82.5
cpu,host=server3,region=eu-west usage=68.3"
```

### 运行示例

```bash
cd influxdb3_distributed
cargo run --example distributed_query_with_flight
```

### 预期输出

```
🚀 真实可运行的分布式查询示例
================================================

📋 步骤 1: 初始化元数据服务
  ✓ 注册节点 1 (http://127.0.0.1:8181)

📋 步骤 2: 注册表结构
  ✓ 注册表 'cpu' 包含 3 个区域

📋 步骤 3: 创建查询上下文
  ✓ 注册 HttpTableProvider 连接到 http://127.0.0.1:8181

📋 步骤 4: 创建分布式规划器

📋 步骤 5: 执行真实查询

============================================
查询 1: 简单扫描（前10条）
============================================
📝 SQL: SELECT * FROM cpu LIMIT 10
⚙️  生成分布式执行计划...
✓ 分布式计划包含 1 个子计划:
   • 子计划 1: node_id=1, regions=[1, 2, 3]
🚀 开始执行查询...
  📡 查询 http://127.0.0.1:8181: SELECT * FROM cpu LIMIT 10

📊 结果预览（第一批数据）:
+----------------------------+--------+---------+-------+
| time                       | host   | region  | usage |
+----------------------------+--------+---------+-------+
| 2024-01-01T00:00:00.000000 | server1| us-east | 75.0  |
| 2024-01-01T00:00:00.000000 | server2| us-west | 82.5  |
| 2024-01-01T00:00:00.000000 | server3| eu-west | 68.3  |
+----------------------------+--------+---------+-------+

⏱  查询耗时: 45.2ms (45 ms)
📈 统计: 1 批次, 共 3 行
✅ 返回 3 行
```

## 架构说明

### 为什么使用 HTTP 而不是 Arrow Flight？

当前实现使用 HTTP 方式，因为：

1. **简单直接**: InfluxDB 3.0 已经提供了 `/api/v3/query_sql` HTTP 端点
2. **易于调试**: 可以用 curl 测试
3. **无需额外实现**: 不需要在节点侧实现 Arrow Flight 服务

### Arrow Flight 方式（未来优化）

```
协调器                          数据节点
   │                               │
   │  1. 序列化物理执行计划          │
   │ ─────────────────────────────>│
   │     (Protobuf/Substrait)      │
   │                               │
   │  2. 在本地区域执行计划          │
   │                               │
   │  3. 流式返回 Arrow 数据         │
   │<─────────────────────────────│
   │     (Arrow Flight Stream)     │
```

优点：
- 不需要重复解析 SQL
- 保留所有优化信息
- gRPC 流式传输性能更好
- 支持更复杂的执行计划

缺点：
- 需要实现计划序列化（Substrait）
- 需要在节点侧实现 Arrow Flight 服务
- 实现复杂度更高

## 与 QueryExecutor 的集成

### 问题

你问的核心问题是：生成分布式计划后，能否用 `QueryExecutor::query_sql()` 执行节点本地查询？

### 答案

**不能直接使用**，原因：

1. `query_sql()` 接收 **SQL 字符串**
2. `remote_plans` 包含的是 **物理执行计划** (`Arc<dyn ExecutionPlan>`)

### 解决方案

有三种方式：

#### 方案 1: HTTP + SQL（当前示例使用）✅

```rust
// HttpTableProvider 内部会：
let sql = "SELECT * FROM cpu WHERE usage > 50";
let response = client.post("/api/v3/query_sql")
    .json(&{ "db": "mydb", "query": sql })
    .send().await?;
```

这种方式实际上**间接使用了** `QueryExecutor::query_sql()`，因为节点的 HTTP API 内部会调用它。

#### 方案 2: Arrow Flight + 物理计划（推荐但需要实现）

需要在 `QueryExecutor` trait 添加新方法：

```rust
async fn execute_physical_plan(
    &self,
    database: &str,
    plan: Arc<dyn ExecutionPlan>,
    regions: Vec<RegionId>,
    span_ctx: Option<SpanContext>,
) -> Result<SendableRecordBatchStream, QueryExecutorError>;
```

#### 方案 3: 物理计划转 SQL（不推荐）

理论上可以，但会丢失优化信息。

## 总结

当前示例是**真实可运行的**，它：

1. ✅ 连接到真实的 InfluxDB 节点
2. ✅ 通过 HTTP API 发送 SQL
3. ✅ 节点内部会调用 `QueryExecutor::query_sql()` 执行查询
4. ✅ 接收并显示真实结果

只需要：
1. 启动一个 InfluxDB 3.0 节点（127.0.0.1:8181）
2. 写入一些 cpu 表数据
3. 运行示例

