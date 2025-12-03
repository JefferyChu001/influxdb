# InfluxDB 分布式架构实现状态

## 当前状态

### 已完成
1. ✅ 基本的分布式查询测试 (`test_true_distributed.rs`)
   - 演示了如何从多个节点查询数据
   - 实现了简单的数据合并
   - 测试了分组聚合、排序、JOIN

2. ✅ 简单的协调节点架构 (`test_simple_coordinator.rs`)
   - 轮询策略写入数据到多个节点
   - 从所有节点收集查询结果
   - 基本的分布式表提供者

3. ✅ 数据准备脚本
   - 自动启动 etcd 和 3 个 InfluxDB 节点
   - 向所有节点写入测试数据
   - CPU 和 MEM 表，共 300 行数据

4. ✅ DistributedTableProvider
   - 实现了 DataFusion 的 TableProvider trait
   - 支持多节点并行查询
   - 基本的结果合并

### 当前问题

1. **不是真正的分布式计算框架**
   - 当前只是简单的并行查询 + 结果合并
   - 没有真正的分布式查询规划
   - 没有谓词下推、部分聚合等优化
   - 依赖 DataFusion 的本地聚合能力

2. **缺少关键组件**
   - 没有分布式查询规划器 (Dist Planner)
   - 没有查询优化器规则
   - 没有 Region 级查询抽象
   - 没有元数据服务集成

3. **Schema 不匹配问题**
   - JSON 到 Arrow RecordBatch 转换时 time 列类型不匹配
   - 需要在查询时指定预期的 schema

## 下一步计划

### 短期目标：构建完整的分布式查询框架

参考 GreptimeDB 的架构 (`/Users/admin/greptimedb/src/query`)，实现：

#### 1. 创建 `influxdb3_distributed` Crate
```
influxdb3_distributed/
├── src/
│   ├── lib.rs
│   ├── error.rs              # 错误类型
│   ├── types.rs              # 基本类型 (RegionId, PartitionId, etc.)
│   ├── dist_plan/            # 分布式查询计划
│   │   ├── mod.rs
│   │   ├── analyzer.rs       # 分析逻辑计划
│   │   ├── planner.rs        # 生成分布式物理计划
│   │   ├── merge_scan.rs     # 合并扫描
│   │   ├── merge_sort.rs     # 分布式排序
│   │   ├── region_pruner.rs  # Region 剪枝
│   │   └── predicate_extractor.rs  # 谓词提取
│   ├── optimizer/            # 查询优化器
│   │   ├── mod.rs
│   │   ├── predicate_pushdown.rs
│   │   ├── projection_pushdown.rs
│   │   └── limit_pushdown.rs
│   ├── executor/             # 执行器
│   │   ├── mod.rs
│   │   ├── remote_exec.rs    # 远程执行
│   │   └── merge_exec.rs     # 合并执行
│   ├── region_query/         # Region 查询
│   │   ├── mod.rs
│   │   └── local_scan.rs
│   └── meta/                 # 元数据服务
│       ├── mod.rs
│       └── partition_manager.rs
```

#### 2. 核心接口设计

```rust
// 分布式查询规划器
pub trait DistributedPlanner {
    async fn plan(&self, logical_plan: LogicalPlan) 
        -> Result<DistributedPlan>;
}

// 分布式查询计划
pub struct DistributedPlan {
    pub coordinator_plan: Arc<dyn ExecutionPlan>,  // 协调节点执行计划
    pub remote_plans: Vec<RemotePlan>,              // 远程节点执行计划
}

// 远程执行计划
pub struct RemotePlan {
    pub node_id: NodeId,
    pub regions: Vec<RegionId>,
    pub plan: Arc<dyn ExecutionPlan>,
}
```

#### 3. 查询执行流程

```
1. SQL 解析 → LogicalPlan (DataFusion)
2. DistributedPlanner 分析:
   - 提取表引用
   - 识别 Region (通过 MetaService)
   - Region 剪枝 (基于谓词)
   - 生成子查询计划
3. 生成 DistributedPlan:
   - RemoteExec: 在数据节点执行
   - MergeScan: 合并扫描结果
   - PartialAgg → FinalAgg: 两阶段聚合
4. 执行:
   - 并行发送子查询到数据节点
   - 收集 RecordBatch 流
   - 协调节点合并/聚合
   - 返回最终结果
```

## 技术挑战

1. **复杂的查询优化**
   - 需要实现完整的分布式优化规则
   - 处理复杂的 JOIN、子查询
   - 成本估算和计划选择

2. **容错和重试**
   - 节点失败时的处理
   - 部分结果的处理
   - 查询超时和取消

3. **性能优化**
   - 减少网络传输
   - 批量数据传输
   - 流式处理大结果集

## 参考资源

- **GreptimeDB Query**: `/Users/admin/greptimedb/src/query`
- **DataFusion**: Apache Arrow DataFusion 文档
- **当前实现**: 
  - `/Users/admin/Downloads/influxdb/influxdb3_query_executor/src/distributed/`
  - `/Users/admin/Downloads/influxdb/influxdb3_query_executor/examples/`

## 测试环境

- **etcd**: http://127.0.0.1:2379
- **节点1**: http://127.0.0.1:8181
- **节点2**: http://127.0.0.1:8182
- **节点3**: http://127.0.0.1:8183

启动命令:
```bash
./scripts/start_distributed_cluster.sh
```

测试命令:
```bash
cargo run --example test_simple_coordinator
```

