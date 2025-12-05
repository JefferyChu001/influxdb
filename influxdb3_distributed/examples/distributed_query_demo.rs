//! 分布式查询测试示例
//!
//! 这个示例程序演示如何使用 influxdb3_distributed 进行分布式查询

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion_optimizer::analyzer::AnalyzerRule;
use futures::StreamExt;

use influxdb3_distributed::dist_plan::{DistPlannerAnalyzer, DistributedPlanner};
use influxdb3_distributed::meta::{InMemoryMetaService, MetaService, NodeInfo, RegionMeta, TableMeta};
use influxdb3_distributed::types::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    // tracing_subscriber::fmt::init();

    println!("🚀 InfluxDB 分布式查询测试");
    println!("================================\n");

    // 1. 创建元数据服务和集群配置
    println!("📋 步骤 1: 初始化集群配置");
    let meta_service = Arc::new(InMemoryMetaService::new());

    // 注册 3 个数据节点
    for i in 1..=3 {
        let node = NodeInfo {
            id: NodeId::new(i),
            address: "127.0.0.1".to_string(),
            grpc_port: 8080 + i as u16,
            http_port: 9090 + i as u16,
            status: NodeStatus::Active,
            regions: vec![RegionId::new(i)],
        };
        meta_service.register_node(node).await?;
        println!("  ✓ 注册节点 {} (127.0.0.1:{})", i, 8080 + i);
    }

    // 2. 创建测试数据 (1000 行)
    println!("\n📋 步骤 2: 准备测试数据");

    // CPU 使用率数据 - 生成 1000 行
    let cpu_schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("host", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("usage", DataType::Float64, false),
        Field::new("cores", DataType::Int64, false),
    ]));

    // 生成 1000 行数据
    let num_rows = 10000;
    let mut time_data = Vec::with_capacity(num_rows);
    let mut host_data = Vec::with_capacity(num_rows);
    let mut region_data = Vec::with_capacity(num_rows);
    let mut usage_data = Vec::with_capacity(num_rows);
    let mut cores_data = Vec::with_capacity(num_rows);

    let hosts = ["server1", "server2", "server3"];
    let regions = ["us-east", "us-west", "eu-west"];
    let cores_options = [4, 8, 16];

    for i in 0..num_rows {
        let host_idx = i % 3;
        time_data.push(1609459200 + (i as i64 * 60)); // 每分钟一个数据点
        host_data.push(hosts[host_idx]);
        region_data.push(regions[host_idx]);
        // 生成 50-100 之间的随机 CPU 使用率
        usage_data.push(50.0 + ((i * 7 + 13) % 50) as f64);
        cores_data.push(cores_options[host_idx] as i64);
    }

    let cpu_batch = RecordBatch::try_new(
        cpu_schema.clone(),
        vec![
            Arc::new(Int64Array::from(time_data)),
            Arc::new(StringArray::from(host_data)),
            Arc::new(StringArray::from(region_data)),
            Arc::new(Float64Array::from(usage_data)),
            Arc::new(Int64Array::from(cores_data)),
        ],
    )?;

    println!("  ✓ 创建 CPU 数据表 ({} 行)", num_rows);

    // 内存使用率数据 - 生成 1000 行
    let memory_schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("host", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("used_mb", DataType::Int64, false),
        Field::new("total_mb", DataType::Int64, false),
    ]));

    let mut mem_time_data = Vec::with_capacity(num_rows);
    let mut mem_host_data = Vec::with_capacity(num_rows);
    let mut mem_region_data = Vec::with_capacity(num_rows);
    let mut mem_used_data = Vec::with_capacity(num_rows);
    let mut mem_total_data = Vec::with_capacity(num_rows);

    let memory_total = [4096, 8192, 16384];

    for i in 0..num_rows {
        let host_idx = i % 3;
        mem_time_data.push(1609459200 + (i as i64 * 60));
        mem_host_data.push(hosts[host_idx]);
        mem_region_data.push(regions[host_idx]);
        // 生成内存使用率 40-90%
        let total = memory_total[host_idx];
        let used = (total as f64 * (0.4 + ((i * 11 + 7) % 50) as f64 / 100.0)) as i64;
        mem_used_data.push(used);
        mem_total_data.push(total as i64);
    }

    let memory_batch = RecordBatch::try_new(
        memory_schema.clone(),
        vec![
            Arc::new(Int64Array::from(mem_time_data)),
            Arc::new(StringArray::from(mem_host_data)),
            Arc::new(StringArray::from(mem_region_data)),
            Arc::new(Int64Array::from(mem_used_data)),
            Arc::new(Int64Array::from(mem_total_data)),
        ],
    )?;

    println!("  ✓ 创建 Memory 数据表 ({} 行)", num_rows);

    // 3. 注册表到元数据服务
    println!("\n📋 步骤 3: 注册表和 Region");
    
    let cpu_table = TableMeta {
        id: TableId::new(1),
        name: "cpu".to_string(),
        schema: cpu_schema.clone(),
        regions: vec![RegionId::new(1), RegionId::new(2), RegionId::new(3)],
    };
    meta_service.register_table(cpu_table).await?;
    println!("  ✓ 注册表: cpu");

    let memory_table = TableMeta {
        id: TableId::new(2),
        name: "memory".to_string(),
        schema: memory_schema.clone(),
        regions: vec![RegionId::new(1), RegionId::new(2)],
    };
    meta_service.register_table(memory_table).await?;
    println!("  ✓ 注册表: memory");

    // 注册 regions
    for i in 1..=3 {
        let region = RegionMeta {
            id: RegionId::new(i),
            table_id: TableId::new(1),
            node_id: NodeId::new(i),
            status: RegionStatus::Active,
        };
        meta_service.register_region(region).await?;
    }
    println!("  ✓ 注册 3 个 CPU regions");

    for i in 1..=2 {
        let region = RegionMeta {
            id: RegionId::new(i),
            table_id: TableId::new(2),
            node_id: NodeId::new(i),
            status: RegionStatus::Active,
        };
        meta_service.register_region(region).await?;
    }
    println!("  ✓ 注册 2 个 Memory regions");

    // 4. 创建 DataFusion SessionContext 和分布式组件
    println!("\n📋 步骤 4: 初始化查询引擎");
    let session_ctx = Arc::new(SessionContext::new());

    // 注册表到 DataFusion
    let cpu_table_provider = MemTable::try_new(cpu_schema, vec![vec![cpu_batch]])?;
    session_ctx.register_table("cpu", Arc::new(cpu_table_provider))?;
    println!("  ✓ 注册 cpu 表到 DataFusion");

    let memory_table_provider = MemTable::try_new(memory_schema, vec![vec![memory_batch]])?;
    session_ctx.register_table("memory", Arc::new(memory_table_provider))?;
    println!("  ✓ 注册 memory 表到 DataFusion");

    // 创建分布式组件
    let dist_analyzer = DistPlannerAnalyzer::new();
    let dist_planner = DistributedPlanner::new_with_context(
        meta_service.clone(),
        &session_ctx,
    );

    println!("  ✓ 初始化分布式查询组件");

    println!("\n✅ 系统初始化完成！\n");

    // 5. 执行测试查询
    println!("🔍 开始执行查询测试");
    println!("================================\n");

    // 查询 1: 简单查询
    println!("查询 1: SELECT * FROM cpu WHERE usage > 70 LIMIT 5");
    println!("---------------------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT * FROM cpu WHERE usage > 70 LIMIT 10").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 2: 聚合查询
    println!("查询 2: SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host");
    println!("-----------------------------------------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 3: 全局统计
    println!("查询 3: SELECT COUNT(*) as count, AVG(usage) as avg, MAX(usage) as max FROM cpu");
    println!("---------------------------------------------------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT COUNT(*) as count, AVG(usage) as avg, MAX(usage) as max FROM cpu").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 4: JOIN 查询
    println!("查询 4: SELECT c.host, c.usage, m.used_mb FROM cpu c JOIN memory m ON c.host = m.host LIMIT 5");
    println!("------------------------------------------------------------------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT * FROM cpu c JOIN memory m ON c.host = m.host LIMIT 10").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 5: 排序查询
    println!("查询 5: SELECT host, usage FROM cpu ORDER BY usage DESC LIMIT 3");
    println!("-------------------------------------------------------------------");
    match execute_and_display_query(&session_ctx, &dist_analyzer, &dist_planner, "SELECT host, usage FROM cpu ORDER BY usage DESC LIMIT 10").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    println!("\n✅ 所有查询测试完成！");

    Ok(())
}

async fn execute_and_display_query(
    session_ctx: &Arc<SessionContext>,
    _dist_analyzer: &DistPlannerAnalyzer,
    dist_planner: &DistributedPlanner,
    sql: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    use std::time::Instant;

    let start_time = Instant::now();

    // 1. Parse SQL to logical plan
    let parse_start = Instant::now();
    let logical_plan = session_ctx
        .sql(sql)
        .await?
        .logical_plan()
        .clone();
    let parse_duration = parse_start.elapsed();

    println!("Original logical plan:");
    println!("{:?}\n", logical_plan);

    // 2. Skip distributed analysis for now - go directly to distributed planning
    // The DistPlannerAnalyzer creates MergeScan nodes which DataFusion doesn't know
    // how to convert to physical plans. Instead, we let DistributedPlanner handle
    // the original logical plan and create the physical plan directly.

    // 3. Generate distributed physical plan
    let plan_start = Instant::now();
    let dist_plan = dist_planner.plan(&logical_plan).await?;
    let plan_duration = plan_start.elapsed();

    println!("Distributed plan with {} remote plans", dist_plan.remote_plans.len());
    for (i, remote_plan) in dist_plan.remote_plans.iter().enumerate() {
        println!("  Remote plan {}: node={}, regions={:?}",
            i + 1, remote_plan.node_id, remote_plan.regions);
    }
    println!();

    // 4. Execute coordinator plan
    let exec_start = Instant::now();
    let task_ctx = session_ctx.task_ctx();
    let mut stream = dist_plan.coordinator_plan.execute(0, task_ctx)?;

    let mut total_rows = 0;
    let mut batch_count = 0;

    while let Some(batch_result) = stream.next().await {
        match batch_result {
            Ok(batch) => {
                batch_count += 1;
                total_rows += batch.num_rows();

                if batch_count == 1 {
                    // 打印 schema
                    println!("Schema:");
                    for field in batch.schema().fields() {
                        println!("  - {}: {:?}", field.name(), field.data_type());
                    }
                    println!();
                }

                // 打印数据
                println!("Batch {}:", batch_count);
                print_batch(&batch);
                println!();
            }
            Err(e) => {
                return Err(format!("Error reading batch: {}", e).into());
            }
        }
    }

    let exec_duration = exec_start.elapsed();
    let total_duration = start_time.elapsed();

    // 打印时间统计
    println!("⏱️  执行时间统计:");
    println!("  - SQL 解析: {:?}", parse_duration);
    println!("  - 分布式规划: {:?}", plan_duration);
    println!("  - 查询执行: {:?}", exec_duration);
    println!("  - 总时间: {:?} ({} ms)", total_duration, total_duration.as_millis());

    Ok(total_rows)
}

fn print_batch(batch: &RecordBatch) {
    use arrow::util::pretty::print_batches;
    let _ = print_batches(&[batch.clone()]);
}

