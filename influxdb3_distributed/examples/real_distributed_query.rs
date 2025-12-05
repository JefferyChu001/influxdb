//! 分布式查询演示 - 从真实 InfluxDB 节点查询数据
//!
//! 这个例子演示了如何使用分布式查询框架从真实的 InfluxDB 节点查询数据

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use futures::StreamExt;

use influxdb3_distributed::dist_plan::{DistPlannerAnalyzer, DistributedPlanner};
use influxdb3_distributed::http_table_provider::HttpTableProvider;
use influxdb3_distributed::meta::{InMemoryMetaService, MetaService, NodeInfo, RegionMeta, TableMeta};
use influxdb3_distributed::types::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 InfluxDB 分布式查询测试 (真实节点)");
    println!("======================================\n");

    // 1. 创建元数据服务和集群配置
    println!("📋 步骤 1: 初始化集群配置");
    let meta_service = Arc::new(InMemoryMetaService::new());

    // 注册 3 个真实的 InfluxDB 数据节点
    for i in 1..=3 {
        let node = NodeInfo {
            id: NodeId::new(i),
            address: "127.0.0.1".to_string(),
            grpc_port: 8180 + i as u16,
            http_port: 8180 + i as u16,
            status: NodeStatus::Active,
            regions: vec![RegionId::new(i)],
        };
        meta_service.register_node(node).await?;
        println!("  ✓ 注册节点 {} (http://127.0.0.1:{})", i, 8180 + i);
    }

    // 2. 定义表 schema (基于真实的 InfluxDB 数据)
    println!("\n📋 步骤 2: 定义表 Schema");

    let cpu_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("time", DataType::Utf8, true),
        arrow::datatypes::Field::new("host", DataType::Utf8, true),
        arrow::datatypes::Field::new("region", DataType::Utf8, true),
        arrow::datatypes::Field::new("value", DataType::Float64, true),
        arrow::datatypes::Field::new("cores", DataType::Int64, true),
        arrow::datatypes::Field::new("load", DataType::Float64, true),
    ]));
    println!("  ✓ 定义 CPU 表 schema");

    let memory_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("time", DataType::Utf8, true),
        arrow::datatypes::Field::new("host", DataType::Utf8, true),
        arrow::datatypes::Field::new("region", DataType::Utf8, true),
        arrow::datatypes::Field::new("total", DataType::Float64, true),      // 总内存
        arrow::datatypes::Field::new("used", DataType::Float64, true),       // 已用内存
        arrow::datatypes::Field::new("available", DataType::Float64, true),  // 可用内存
        arrow::datatypes::Field::new("cached", DataType::Float64, true),     // 缓存内存
    ]));
    println!("  ✓ 定义 Memory 表 schema (total, used, available, cached)");

    // 3. 注册表和 Region
    println!("\n📋 步骤 3: 注册表和 Region");

    let cpu_table = TableMeta {
        id: TableId::new(1),
        name: "cpu".to_string(),
        schema: cpu_schema.clone(),
        regions: vec![RegionId::new(1), RegionId::new(2), RegionId::new(3)],
    };
    meta_service.register_table(cpu_table).await?;

    let memory_table = TableMeta {
        id: TableId::new(2),
        name: "mem".to_string(),  // 修正：使用 mem 作为表名
        schema: memory_schema.clone(),
        regions: vec![RegionId::new(101), RegionId::new(102), RegionId::new(103)],
    };
    meta_service.register_table(memory_table).await?;

    for i in 1..=3 {
        let cpu_region = RegionMeta {
            id: RegionId::new(i),
            table_id: TableId::new(1),
            node_id: NodeId::new(i),
            status: RegionStatus::Active,
        };
        meta_service.register_region(cpu_region).await?;

        let mem_region = RegionMeta {
            id: RegionId::new(i + 100),
            table_id: TableId::new(2),
            node_id: NodeId::new(i),
            status: RegionStatus::Active,
        };
        meta_service.register_region(mem_region).await?;
    }

    println!("  ✓ 注册 CPU 表 (3 regions)");
    println!("  ✓ 注册 Memory 表 (3 regions)");

    // 4. 创建 DataFusion SessionContext
    println!("\n📋 步骤 4: 初始化查询引擎");
    let session_ctx = Arc::new(SessionContext::new());

    // 使用 HttpTableProvider 注册表
    let cpu_provider = HttpTableProvider::new(
        "cpu".to_string(),
        cpu_schema.clone(),
        "testdb".to_string(),
        "http://127.0.0.1:8181".to_string(),
    );
    session_ctx.register_table("cpu", Arc::new(cpu_provider))?;

    let memory_provider = HttpTableProvider::new(
        "mem".to_string(),  // 修正: 表名是 mem 不是 memory
        memory_schema.clone(),
        "testdb".to_string(),
        "http://127.0.0.1:8181".to_string(),
    );
    session_ctx.register_table("mem", Arc::new(memory_provider))?;

    println!("  ✓ 注册 CPU 和 Memory 表到 DataFusion");

    // 创建分布式规划器
    let dist_analyzer = DistPlannerAnalyzer::new();
    let dist_planner = DistributedPlanner::new_with_context(
        meta_service.clone(),
        &session_ctx,
    );

    println!("  ✓ 初始化分布式查询组件");
    println!("\n✅ 系统初始化完成！\n");

    // 5. 执行查询测试
    println!("🔍 开始执行查询测试");
    println!("====================\n");

    // 查询 1: 简单查询
    println!("查询 1: 简单查询 (LIMIT)");
    println!("------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT * FROM cpu c JOIN cpu m ON c.host = m.host LIMIT 10").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 2: 排序查询（已修复：排序在 coordinator 端执行）
    println!("查询 2: 排序查询 (ORDER BY)");
    println!("------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT host, usage FROM cpu ORDER BY usage DESC LIMIT 10").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 3: 过滤查询（下推优化生效）
    println!("查询 3: 过滤查询 (WHERE - 测试 Filter Pushdown)");
    println!("------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT host, value, load FROM cpu WHERE value > 80 LIMIT 10").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 4: 聚合查询（COUNT 不需要类型转换）
    println!("查询 4: 聚合查询 (COUNT)");
    println!("------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT COUNT(*) as total FROM cpu").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 5: 跨表查询 - Memory 表
    println!("查询 5: Memory 表查询");
    println!("------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT host, total, used, available FROM mem LIMIT 10").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    println!("✅ 所有查询测试完成！");

    Ok(())
}

async fn execute_and_display_query(
    session_ctx: &Arc<SessionContext>,
    _dist_analyzer: &DistPlannerAnalyzer,
    dist_planner: &DistributedPlanner,
    sql: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    println!("SQL: {}", sql);

    // 解析 SQL 生成逻辑计划
    let logical_plan = session_ctx
        .sql(sql)
        .await?
        .logical_plan()
        .clone();

    // 生成分布式物理计划
    let dist_plan = dist_planner.plan(&logical_plan).await?;

    println!("📡 分布式执行计划:");
    println!("  - 远程计划数: {}", dist_plan.remote_plans.len());
    for (i, remote_plan) in dist_plan.remote_plans.iter().enumerate() {
        println!("    • 计划 {}: node={}, regions={:?}",
            i + 1, remote_plan.node_id, remote_plan.regions);
    }

    // 执行 coordinator 计划
    let task_ctx = session_ctx.task_ctx();
    let mut stream = dist_plan.coordinator_plan.execute(0, task_ctx)?;

    let mut total_rows = 0;
    let mut batch_count = 0;

    while let Some(batch_result) = stream.next().await {
        match batch_result {
            Ok(batch) => {
                batch_count += 1;
                total_rows += batch.num_rows();

                // 打印前 10 行
                if batch_count == 1 && batch.num_rows() > 0 {
                    print_batch(&batch);
                }
            }
            Err(e) => {
                return Err(format!("Error reading batch: {}", e).into());
            }
        }
    }

    let duration = start_time.elapsed();
    println!("⏱️  查询耗时: {:?} ({} ms)", duration, duration.as_millis());

    Ok(total_rows)
}

fn print_batch(batch: &RecordBatch) {
    use arrow::util::pretty::print_batches;
    let _ = print_batches(&[batch.clone()]);
}


