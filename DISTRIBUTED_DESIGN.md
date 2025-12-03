# InfluxDB 3.0 分布式查询框架设计

基于 GreptimeDB 的分布式查询架构，为 InfluxDB 3.0 构建完整的分布式查询能力。

## 核心架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      Frontend (协调节点)                          │
│  ┌────────────┐  ┌──────────────┐  ┌────────────┐              │
│  │ SQL Parser │→ │ Dist Planner │→ │  Executor  │              │
│  └────────────┘  └──────────────┘  └────────────┘              │
└─────────────────────────────┬───────────────────────────────────┘
                              │ gRPC
                ┌─────────────┼─────────────┐
                │             │             │
       ┌────────▼───┐  ┌──────▼──────┐  ┌─▼───────────┐
       │ Datanode 1 │  │ Datanode 2  │  │ Datanode 3  │
       │ ┌────────┐ │  │ ┌────────┐  │  │ ┌────────┐  │
       │ │Region 1│ │  │ │Region 2│  │  │ │Region 3│  │
       │ └────────┘ │  │ └────────┘  │  │ └────────┘  │
       └────────────┘  └─────────────┘  └─────────────┘
                │             │             │
                └─────────────┼─────────────┘
                              │
                    ┌─────────▼──────────┐
                    │   Meta Service     │
                    │ (元数据+分片信息)   │
                    └────────────────────┘
```

## 核心组件 (基于 GreptimeDB)

### 1. dist_plan (分布式查询计划)
- **analyzer.rs**: 分析逻辑计划，识别可下推的操作
- **planner.rs**: 生成分布式物理计划
- **merge_scan.rs**: 合并多个节点的扫描结果
- **merge_sort.rs**: 分布式排序
- **region_pruner.rs**: Region 剪枝优化
- **predicate_extractor.rs**: 谓词提取和下推

### 2. query_engine (查询引擎)
- 查询引擎接口定义
- 支持分布式查询执行
- 管理查询上下文

### 3. optimizer (查询优化器)
- 分布式查询优化规则
- 谓词下推
- 投影下推
- Limit 下推
- 聚合下推

### 4. region_query (Region 查询)
- Region 级别的查询执行
- 本地数据扫描
- 过滤和投影执行

### 5. planner (计划器)
- 逻辑计划到物理计划的转换
- 集成 DataFusion 的优化器

## 查询执行流程

### 写入流程
```
Client → Frontend → ShardManager (计算分片) → 路由到 Datanode → 写入本地存储
```

### 查询流程
```
1. Client 发送 SQL 到 Frontend
2. Frontend 解析 SQL，生成逻辑计划
3. DistPlanner 分析逻辑计划：
   - 识别需要访问的表
   - 根据谓词进行 Region 剪枝
   - 确定需要查询的 Datanodes
4. 生成分布式物理计划：
   - 为每个 Datanode 生成子查询计划
   - 插入 MergeScan/MergeSort 操作符
5. Frontend 并行发送子查询到各个 Datanodes
6. Datanodes 执行本地查询，返回结果
7. Frontend 合并结果：
   - 对于聚合查询：先部分聚合，再全局聚合
   - 对于排序查询：多路归并排序
   - 对于简单查询：Union 合并
8. 返回最终结果给 Client
```

## 关键优化

1. **谓词下推**：WHERE 条件下推到 Datanode
2. **投影下推**：只查询需要的列
3. **Limit 下推**：在每个节点先做 LIMIT
4. **部分聚合**：在 Datanode 做部分聚合，Frontend 做最终聚合
5. **Region 剪枝**：根据时间范围等条件剪枝不相关的 Region

## 实现计划

### Phase 1: 基础框架 (当前任务)
- [ ] 创建 influxdb3_distributed crate
- [ ] 实现基本的错误类型
- [ ] 实现 Region/Partition 数据结构
- [ ] 实现 Meta Service 接口

### Phase 2: 分布式计划器
- [ ] 实现 DistPlanner
- [ ] 实现 Analyzer (分析逻辑计划)
- [ ] 实现 Region Pruner (剪枝优化)
- [ ] 实现 Predicate Extractor (谓词提取)

### Phase 3: 执行器
- [ ] 实现 MergeScan (合并扫描)
- [ ] 实现 MergeSort (分布式排序)
- [ ] 实现 RemoteExec (远程执行)

### Phase 4: 集成测试
- [ ] 实现完整的查询流程
- [ ] 性能测试
- [ ] 容错测试

## 参考
- GreptimeDB: /Users/admin/greptimedb/src/query
- DataFusion: 底层查询引擎

