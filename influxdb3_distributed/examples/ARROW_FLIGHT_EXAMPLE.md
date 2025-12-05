# Arrow Flight 分布式查询示例说明

## 文件位置
`influxdb3_distributed/examples/distributed_with_flight.rs`

## 这个示例做了什么

这是一个**真实使用 Arrow Flight** 的分布式查询示例，直接使用了你在 `/src/executor/` 里定义的：
- `RemoteExec` - 远程执行算子
- `RemoteQueryClient` - Arrow Flight 客户端

## 代码流程

```rust
1. 注册 3 个数据节点（gRPC 端口 8091, 8092, 8093）
   └─> meta_service.register_node()

2. 注册表和区域
   └─> meta_service.register_table()
   └─> meta_service.register_region()

3. 生成分布式计划
   └─> DistributedPlanner::plan()
   └─> 得到 dist_plan.remote_plans[]

4. 对每个远程计划：
   ├─> 创建 RemoteExec
   │   └─> RemoteExec::new(node, regions, plan, schema)
   │
   ├─> 创建 RemoteQueryClient
   │   └─> RemoteQueryClient::new(endpoint)
   │
   └─> 执行查询
       └─> remote_exec.execute(0, task_ctx)
       └─> 流式接收结果

5. 执行协调器计划
   └─> dist_plan.coordinator_plan.execute()
   └─> 合并所有节点结果
```

## 与 QueryExecutor 的关系

**你的问题**: "生成分布式计划之后能不能用 query_sql 来执行节点的本地查询？"

**答案**: 不直接用，但可以扩展！

当前流程：
```
DistributedPlanner
    ↓
RemotePlan (物理执行计划)
    ↓
RemoteExec + Arrow Flight
    ↓
节点的 Flight 服务端 (需要实现)
    ↓
接收物理计划 → 执行 → 返回结果
```

## 要让它真正运行，需要什么？

### 在各数据节点上实现 Arrow Flight 服务端

参考 `src/executor/remote_client.rs` 的客户端实现，需要在节点侧实现对应的服务端：

```rust
// 伪代码 - 节点侧需要实现
struct FlightDatanodeService {
    query_executor: Arc<dyn QueryExecutor>,
}

#[tonic::async_trait]
impl FlightService for FlightDatanodeService {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // 1. 解析 ticket，获取查询信息
        let (query_id, region_ids, serialized_plan) = 
            decode_ticket(request.into_inner())?;
        
        // 2. 反序列化物理执行计划
        let physical_plan = deserialize_plan(&serialized_plan)?;
        
        // 3. 在本地区域上执行计划
        let task_ctx = /* 创建 TaskContext */;
        let stream = physical_plan.execute(0, task_ctx)?;
        
        // 4. 转换为 Flight 数据流返回
        Ok(Response::new(to_flight_stream(stream)))
    }
}
```

### 或者：添加新的执行接口

在 `QueryExecutor` trait 添加：

```rust
async fn execute_physical_plan(
    &self,
    database: &str,
    plan: Arc<dyn ExecutionPlan>,
    regions: Vec<RegionId>,
) -> Result<SendableRecordBatchStream, QueryExecutorError>;
```

然后 `RemoteExec` 可以通过 HTTP 调用这个接口：

```http
POST /api/v3/execute_plan
Content-Type: application/octet-stream

<serialized physical plan>
```

## 运行示例

```bash
cd influxdb3_distributed
cargo run --example distributed_with_flight
```

**预期输出**：
```
生成分布式计划:
  子计划 1: node=1, regions=[1]
    连接: http://127.0.0.1:8091
    ✓ Flight 客户端已创建
    ⚠️  错误: Remote execution requires gRPC client implementation (需要节点实现 Flight 服务端)
  
  子计划 2: node=2, regions=[2]
    ...
```

## 总结

- ✅ 使用了 `/src/executor/` 里已定义的 `RemoteExec` 和 `RemoteQueryClient`
- ✅ 真正的 Arrow Flight 客户端调用
- ✅ 物理执行计划通过 gRPC 发送
- ⚠️  需要节点侧实现 Arrow Flight 服务端才能真正执行

这不是演示，这是**真实的 Arrow Flight 分布式查询框架**，只差节点侧的服务端实现！

