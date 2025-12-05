//! 真实分布式集群查询示例 - 使用 HttpTableProvider

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
    println!("🚀 InfluxDB 真实分布式查询测试");
    println!("================================\n");

    // 1. 配置真实集群的元数据
    println!("📋 步骤 1: 配置集群元数据");
    let meta_service = Arc::new(InMemoryMetaService::new());

    // 注册 3 个真实的数据节点
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

    // 2. 定义 schema
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

    // 3. 注册表
    println!("\n📋 步骤 3: 注册表和 Region");
    let cpu_table = TableMeta {
        id: TableId::new(1),
        name: "cpu".to_string(),
        schema: cpu_schema.clone(),
        regions: vec![RegionId::new(1), RegionId::new(2), RegionId::new(3)],
    };
    meta_service.register_table(cpu_table).await?;
    println!("  ✓ 注册表: cpu");

    // 注册 memory 表用于 JOIN 查询
    let memory_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("time", DataType::Utf8, true),
        arrow::datatypes::Field::new("host", DataType::Utf8, true),
        arrow::datatypes::Field::new("region", DataType::Utf8, true),
        arrow::datatypes::Field::new("used", DataType::Int64, true),
        arrow::datatypes::Field::new("total", DataType::Int64, true),
    ]));

    let memory_table = TableMeta {
        id: TableId::new(2),
        name: "memory".to_string(),
        schema: memory_schema.clone(),
        regions: vec![RegionId::new(101), RegionId::new(102), RegionId::new(103)],
    };
    meta_service.register_table(memory_table).await?;
    println!("  ✓ 注册表: memory");

    for i in 1..=3 {
        let cpu_region = RegionMeta {
            id: RegionId::new(i),  // Region 1-3 for CPU
            table_id: TableId::new(1),
            node_id: NodeId::new(i),
            status: RegionStatus::Active,
        };
        meta_service.register_region(cpu_region).await?;

        let mem_region = RegionMeta {
            id: RegionId::new(i + 100),  // Region 101-103 for Memory (avoid conflicts)
            table_id: TableId::new(2),
            node_id: NodeId::new(i),
            status: RegionStatus::Active,
        };
        meta_service.register_region(mem_region).await?;
    }
    println!("  ✓ 注册 6 个 regions");

    // 4. 初始化查询引擎
    println!("\n📋 步骤 4: 初始化查询引擎");
    let session_ctx = Arc::new(SessionContext::new());

    let cpu_table_provider = HttpTableProvider::new(
        "cpu".to_string(),
        cpu_schema.clone(),
        "testdb".to_string(),
        "http://127.0.0.1:8181".to_string(),
    );
    session_ctx.register_table("cpu", Arc::new(cpu_table_provider))?;
    println!("  ✓ 注册 cpu 表");

    let memory_table_provider = HttpTableProvider::new(
        "memory".to_string(),
        memory_schema.clone(),
        "testdb".to_string(),
        "http://127.0.0.1:8181".to_string(),
    );
    session_ctx.register_table("memory", Arc::new(memory_table_provider))?;
    println!("  ✓ 注册 memory 表");

    let dist_analyzer = DistPlannerAnalyzer::new();
    let dist_planner = DistributedPlanner::new_with_context(meta_service.clone(), &session_ctx);
    println!("  ✓ 初始化分布式组件");

    println!("\n✅ 系统初始化完成!\n");

    // 5. 执行查询
    println!("🔍 开始执行查询测试");
    println!("===================\n");

    // 查询 1: 简单查询
    let query1 = "SELECT * FROM cpu LIMIT 10";
    println!("查询 1: 简单查询 (LIMIT)");
    println!("SQL: {}", query1);
    println!("------------------------------------");
    let start = Instant::now();
    match execute_query(&session_ctx, &dist_analyzer, &dist_planner, query1).await {
        Ok(count) => {
            let elapsed = start.elapsed();
            println!("✓ 返回 {} 行", count);
            println!("⏱️  查询耗时: {:?} ({} ms)\n", elapsed, elapsed.as_millis());
        }
        Err(e) => println!("✗ 失败: {}\n", e),
    }

    // 查询 2: 最大值查询
    let query2 = "SELECT MAX(value) as max_value, MAX(load) as max_load FROM cpu";
    println!("查询 2: 最大值查询 (MAX)");
    println!("SQL: {}", query2);
    println!("------------------------------------");
    let start = Instant::now();
    match execute_query(&session_ctx, &dist_analyzer, &dist_planner, query2).await {
        Ok(count) => {
            let elapsed = start.elapsed();
            println!("✓ 返回 {} 行", count);
            println!("⏱️  查询耗时: {:?} ({} ms)\n", elapsed, elapsed.as_millis());
        }
        Err(e) => println!("✗ 失败: {}\n", e),
    }

    // 查询 3: 排序查询
    let query3 = "SELECT host, value, load FROM cpu ORDER BY value DESC LIMIT 10";
    println!("查询 3: 排序查询 (ORDER BY)");
    println!("SQL: {}", query3);
    println!("------------------------------------");
    let start = Instant::now();
    match execute_query(&session_ctx, &dist_analyzer, &dist_planner, query3).await {
        Ok(count) => {
            let elapsed = start.elapsed();
            println!("✓ 返回 {} 行", count);
            println!("⏱️  查询耗时: {:?} ({} ms)\n", elapsed, elapsed.as_millis());
        }
        Err(e) => println!("✗ 失败: {}\n", e),
    }

    // 查询 4: 聚合查询 - 明确指定列避免 schema 推断错误
    let query4 = "SELECT host, COUNT(*) as count, AVG(CAST(value AS DOUBLE)) as avg_value, MAX(CAST(value AS DOUBLE)) as max_value FROM cpu GROUP BY host";
    println!("查询 4: 聚合查询 (GROUP BY)");
    println!("SQL: {}", query4);
    println!("------------------------------------");
    let start = Instant::now();
    match execute_query(&session_ctx, &dist_analyzer, &dist_planner, query4).await {
        Ok(count) => {
            let elapsed = start.elapsed();
            println!("✓ 返回 {} 行", count);
            println!("⏱️  查询耗时: {:?} ({} ms)\n", elapsed, elapsed.as_millis());
        }
        Err(e) => println!("✗ 失败: {}\n", e),
    }

    // 查询 5: 过滤 + 聚合
    let query5 = "SELECT region, COUNT(*) as count, AVG(CAST(value AS DOUBLE)) as avg_value FROM cpu WHERE CAST(value AS DOUBLE) > 50 GROUP BY region";
    println!("查询 5: 过滤 + 聚合 (WHERE + GROUP BY)");
    println!("SQL: {}", query5);
    println!("------------------------------------");
    let start = Instant::now();
    match execute_query(&session_ctx, &dist_analyzer, &dist_planner, query5).await {
        Ok(count) => {
            let elapsed = start.elapsed();
            println!("✓ 返回 {} 行", count);
            println!("⏱️  查询耗时: {:?} ({} ms)\n", elapsed, elapsed.as_millis());
        }
        Err(e) => println!("✗ 失败: {}\n", e),
    }

    // 查询 6: 全局统计
    let query6 = "SELECT COUNT(*) as total_count, AVG(CAST(value AS DOUBLE)) as avg_value, MIN(CAST(value AS DOUBLE)) as min_value, MAX(CAST(value AS DOUBLE)) as max_value FROM cpu";
    println!("查询 6: 全局统计 (多聚合函数)");
    println!("SQL: {}", query6);
    println!("------------------------------------");
    let start = Instant::now();
    match execute_query(&session_ctx, &dist_analyzer, &dist_planner, query6).await {
        Ok(count) => {
            let elapsed = start.elapsed();
            println!("✓ 返回 {} 行", count);
            println!("⏱️  查询耗时: {:?} ({} ms)\n", elapsed, elapsed.as_millis());
        }
        Err(e) => println!("✗ 失败: {}\n", e),
    }

    // 查询 7: JOIN 查询
    let query7 = "SELECT c.host, c.value as cpu_value, m.used as mem_used FROM cpu c JOIN memory m ON c.host = m.host LIMIT 10";
    println!("查询 7: JOIN 查询 (跨表连接)");
    println!("SQL: {}", query7);
    println!("------------------------------------");
    let start = Instant::now();
    match execute_query(&session_ctx, &dist_analyzer, &dist_planner, query7).await {
        Ok(count) => {
            let elapsed = start.elapsed();
            println!("✓ 返回 {} 行", count);
            println!("⏱️  查询耗时: {:?} ({} ms)\n", elapsed, elapsed.as_millis());
        }
        Err(e) => println!("✗ 失败: {}\n", e),
    }

    println!("\n✅ 查询完成！");
    Ok(())
}

async fn execute_query(
    session_ctx: &Arc<SessionContext>,
    _dist_analyzer: &DistPlannerAnalyzer,
    dist_planner: &DistributedPlanner,
    sql: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    // 1. SQL 解析
    let parse_start = Instant::now();
    let logical_plan = session_ctx.sql(sql).await?.logical_plan().clone();
    let parse_duration = parse_start.elapsed();

    // 2. 分布式规划
    let plan_start = Instant::now();
    let dist_plan = dist_planner.plan(&logical_plan).await?;
    let plan_duration = plan_start.elapsed();

    println!("  📊 分布式执行计划:");
    println!("    - 远程节点数: {}", dist_plan.remote_plans.len());
    for (i, remote_plan) in dist_plan.remote_plans.iter().enumerate() {
        println!("    - 节点 {}: node={}, regions={:?}",
            i + 1, remote_plan.node_id, remote_plan.regions);
    }

    // 3. 执行查询
    let exec_start = Instant::now();
    let task_ctx = session_ctx.task_ctx();
    let mut stream = dist_plan.coordinator_plan.execute(0, task_ctx)?;

    let mut total_rows = 0;
    let mut batch_count = 0;
    let mut displayed_rows = 0;

    while let Some(batch_result) = stream.next().await {
        if let Ok(batch) = batch_result {
            batch_count += 1;
            let batch_rows = batch.num_rows();
            total_rows += batch_rows;

            // 显示前 10 行数据
            if displayed_rows < 10 && batch_rows > 0 {
                print_batch(&batch);
                displayed_rows += batch_rows;
            }
        }
    }
    let exec_duration = exec_start.elapsed();

    // 4. 打印详细统计
    println!("  ⏱️  性能统计:");
    println!("    - SQL 解析: {:?}", parse_duration);
    println!("    - 分布式规划: {:?}", plan_duration);
    println!("    - 查询执行: {:?}", exec_duration);

    Ok(total_rows)
}

fn print_batch(batch: &RecordBatch) {
    use arrow::util::pretty::print_batches;
    let _ = print_batches(&[batch.clone()]);
}
