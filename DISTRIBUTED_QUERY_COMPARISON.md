# 分布式查询层方案快速对比

## 一句话总结

- **方案一（GreptimeDB 架构）**：从头构建独立的分布式查询层，功能完整但工程量大
- **方案二（iox_query 扩展）**：基于现有 DataFusion 和 iox_query 扩展，快速实现且风险低

---

## 核心差异

| 特性 | 方案一 | 方案二 |
|------|--------|--------|
| **开发时间** | 2-3 个月 | 3-5 周 |
| **代码量** | ~8000 行 | ~2000 行 |
| **技术难度** | 高 | 中 |
| **风险** | 高（新架构） | 低（基于现有） |
| **维护成本** | 高 | 低 |
| **灵活性** | 非常高 | 中 |
| **优化能力** | 完全可控 | 依赖 DataFusion |

---

## 关键技术对比

### 谓词下推（Predicate Pushdown）

#### 方案一：手动实现
```rust
// ❌ 需要自己解析 SQL 和提取 WHERE 条件
let statement = parse_sql("SELECT * FROM cpu WHERE host = 'server01'")?;
let predicates = extract_predicates(&statement)?;  // 需要实现
let remote_query = build_query_with_predicates(table, predicates);  // 需要实现
```

#### 方案二：自动处理
```rust
// ✅ DataFusion 自动提取谓词并传递
impl TableProvider for DistributedTableProvider {
    async fn scan(&self, projection, filters, limit) -> ExecutionPlan {
        // filters 已经由 DataFusion 优化器自动提取好了！
        let query = build_query(projection, filters, limit);  // 简单转换即可
    }
}
```

**结论**：方案二省去了大量 SQL 解析和优化工作。

---

### JOIN 优化

#### 方案一：完全自定义
```rust
// 需要实现：
// 1. 表统计信息收集
// 2. JOIN 策略选择（broadcast/shuffle/colocated）
// 3. 数据重分区逻辑
// 4. 结果合并逻辑

impl DistributedPlanner {
    fn plan_join(&self, left, right) -> JoinStrategy {
        // 100+ 行逻辑...
    }
}
```

#### 方案二：利用 DataFusion
```rust
// DataFusion 自动选择最优 JOIN 策略
// 我们只需要提供统计信息
impl TableProvider for DistributedTableProvider {
    async fn statistics(&self) -> Statistics {
        self.query_remote_statistics().await
    }
}
```

**结论**：方案二直接复用 DataFusion 的成熟 JOIN 优化器。

---

### 并行执行

#### 方案一：手动协调
```rust
// 需要自己管理并行度、任务分发、错误处理
let futures: Vec<_> = nodes.iter()
    .map(|node| spawn_remote_query(node, sub_plan))
    .collect();
let results = join_all(futures).await?;
```

#### 方案二：ExecutionPlan 内置
```rust
// DataFusion 的 ExecutionPlan 自动支持并行
impl ExecutionPlan for RemoteTableScanExec {
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.nodes.len())
    }
}
```

**结论**：方案二自动获得并行执行能力。

---

## 优势对比

### 方案一优势

✅ **完全控制**
- 可以实现任意优化策略
- 不受 DataFusion 限制

✅ **架构清晰**
- 前端/元数据/存储三层分离
- 易于理解和扩展

✅ **长期扩展性**
- 可以添加高级功能（如物化视图、预聚合）

### 方案二优势

✅ **快速实现**
- 3-5 周 vs 2-3 个月
- 更快验证想法

✅ **低风险**
- 基于成熟的 DataFusion 和 iox_query
- 已被大量生产环境验证

✅ **高质量**
- 复用经过严格测试的优化器
- 自动获得性能改进

✅ **易维护**
- 只需维护 ~2000 行扩展代码
- DataFusion 升级带来免费优化

---

## 功能支持对比

| 功能 | 方案一 | 方案二 |
|------|--------|--------|
| 谓词下推 | ✅ 需要实现 | ✅ 自动支持 |
| 投影下推 | ✅ 需要实现 | ✅ 自动支持 |
| 聚合下推 | ✅ 需要实现 | ⚠️ 需要扩展 |
| LIMIT 下推 | ✅ 需要实现 | ✅ 自动支持 |
| JOIN 优化 | ✅ 需要实现 | ✅ 自动支持 |
| 并行执行 | ✅ 需要实现 | ✅ 自动支持 |
| 流式处理 | ✅ 需要实现 | ✅ 自动支持 |
| 错误处理 | ✅ 需要实现 | ✅ 继承 DataFusion |

**说明**：方案二中打 ✅ 的功能都是"几乎免费"获得的。

---

## 实际场景模拟

### 场景：查询单个主机的 CPU 和内存数据

```sql
SELECT c.host, c.value as cpu, m.used as mem
FROM cpu c JOIN mem m ON c.host = m.host
WHERE c.host = 'server01'
  AND c.time > '2024-01-01'
LIMIT 100
```

#### 方案一的处理流程

```
1. SQL 解析 (自己实现)           → 100 行代码
2. 逻辑计划生成 (自己实现)        → 200 行代码
3. 谓词下推优化 (自己实现)        → 150 行代码
4. JOIN 策略选择 (自己实现)       → 150 行代码
5. 生成远程查询 (自己实现)        → 100 行代码
6. 并行执行 (自己实现)            → 100 行代码
7. 结果合并 (自己实现)            → 100 行代码
------------------------------------------
总计：~900 行核心代码
```

#### 方案二的处理流程

```
1. SQL 解析                      → DataFusion ✅
2. 逻辑计划生成                  → DataFusion ✅
3. 谓词下推优化                  → DataFusion ✅
4. JOIN 策略选择                 → DataFusion ✅
5. scan() 调用时获取 filters     → 自动传递 ✅
6. 生成远程查询                  → 50 行代码
7. 执行查询                      → 100 行代码
------------------------------------------
总计：~150 行核心代码
```

---

## 建议

### 推荐：方案二（iox_query 扩展）

**核心原因**：
1. **时间效率**：3-5 周 vs 2-3 个月
2. **投入产出比**：150 行 vs 900 行获得相同功能
3. **风险控制**：基于成熟技术栈
4. **可演进**：先快速实现，后续可增强

### 适用方案一的情况

只有以下情况才建议方案一：
1. 需要完全自定义的查询优化策略
2. 有充足的开发时间（3+ 个月）
3. 团队对分布式查询有深入理解
4. 需要实现 DataFusion 不支持的特殊功能

---

## 快速决策树

```
需要分布式查询吗？
  ├─ 是 → 有 2-3 个月时间吗？
  │       ├─ 是 → 需要完全自定义优化吗？
  │       │       ├─ 是 → 方案一
  │       │       └─ 否 → 方案二
  │       └─ 否 → 方案二
  └─ 否 → 不需要本文档
```

**99% 的情况下答案是：方案二**

---

## 实施建议

如果选择方案二，建议按以下顺序实施：

### Week 1-2: MVP
- [ ] 实现 `DistributedTableProvider`（只支持简单查询）
- [ ] 实现 `RemoteTableScanExec`（通过 HTTP 查询）
- [ ] 测试跨节点查询

### Week 3: 优化
- [ ] 添加谓词和投影下推
- [ ] 切换到 gRPC 通信
- [ ] 添加统计信息支持

### Week 4-5: 完善
- [ ] 实现分布式 JOIN
- [ ] 性能调优
- [ ] 集成测试

**3-5 周后，你就有了一个生产级的分布式查询层！**


