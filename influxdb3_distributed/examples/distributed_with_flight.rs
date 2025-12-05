//! Arrow Flight 分布式查询 - 使用 RemoteExec

use std::sync::Arc;
use arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use datafusion::datasource::empty::EmptyTable;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;

use influxdb3_distributed::dist_plan::DistributedPlanner;
use influxdb3_distributed::executor::{RemoteExec, RemoteQueryClient};
use influxdb3_distributed::meta::{InMemoryMetaService, MetaService, NodeInfo, RegionMeta, TableMeta};
use influxdb3_distributed::types::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let meta_service = Arc::new(InMemoryMetaService::new());

    // 注册节点
    for i in 1..=3 {
        meta_service.register_node(NodeInfo {
            id: NodeId::new(i),
            address: "127.0.0.1".to_string(),
            grpc_port: 8090 + i as u16,
            http_port: 8180 + i as u16,
            status: NodeStatus::Active,
            regions: vec![RegionId::new(i)],
        }).await?;
    }

    // 注册表
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("time", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
        arrow::datatypes::Field::new("host", DataType::Utf8, true),
        arrow::datatypes::Field::new("usage", DataType::Float64, true),
    ]));

    meta_service.register_table(TableMeta {
        id: TableId::new(1),
        name: "cpu".to_string(),
        schema: schema.clone(),
        regions: vec![RegionId::new(1), RegionId::new(2), RegionId::new(3)],
    }).await?;

    for i in 1..=3 {
        meta_service.register_region(RegionMeta {
            id: RegionId::new(i),
            table_id: TableId::new(1),
            node_id: NodeId::new(i),
            status: RegionStatus::Active,
        }).await?;
    }

    let ctx = Arc::new(SessionContext::new());
    ctx.register_table("cpu", Arc::new(EmptyTable::new(schema.clone())))?;

    let planner = DistributedPlanner::new_with_context(meta_service.clone(), &ctx);

    // 生成分布式计划
    let logical_plan = ctx.sql("SELECT * FROM cpu LIMIT 10").await?.logical_plan().clone();
    let dist_plan = planner.plan(&logical_plan).await?;

    println!("生成分布式计划:");
    for (i, rp) in dist_plan.remote_plans.iter().enumerate() {
        println!("  子计划 {}: node={}, regions={:?}", i+1, rp.node_id, rp.regions);

        // 获取节点信息
        let node = meta_service.get_node(rp.node_id).await?;
        let endpoint = format!("http://{}:{}", node.address, node.grpc_port);

        println!("    连接: {}", endpoint);

        // 创建 RemoteExec
        let remote_exec = Arc::new(RemoteExec::new(
            node.clone(),
            rp.regions.clone(),
            rp.plan.clone(),
            rp.plan.schema(),
        ));

        // 创建 Flight 客户端（不是 async 的）
        let _client = RemoteQueryClient::new(endpoint.clone());
        println!("    ✓ Flight 客户端已创建");

        // 执行查询 - 使用 ExecutionPlan trait
        let task_ctx = ctx.task_ctx();
        let mut stream = remote_exec.execute(0, task_ctx)?;

        let mut count = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(batch) => count += batch.num_rows(),
                Err(e) => {
                    println!("    ⚠️  错误: {} (需要节点实现 Flight 服务端)", e);
                    break;
                }
            }
        }

        if count > 0 {
            println!("    接收 {} 行", count);
        }
    }

    // 执行协调器计划
    println!("\n执行协调器计划:");
    let task_ctx = ctx.task_ctx();
    let mut stream = dist_plan.coordinator_plan.execute(0, task_ctx)?;
    
    let mut total = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(batch) => total += batch.num_rows(),
            Err(e) => println!("  错误: {}", e),
        }
    }
    
    println!("  最终结果: {} 行", total);

    Ok(())
}

