//! 分布式查询测试示例
//!
//! 这个示例程序演示如何使用 influxdb3_distributed 进行分布式查询

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use futures::StreamExt;

use influxdb3_distributed::meta::{InMemoryMetaService, MetaService, NodeInfo, RegionMeta, TableMeta};
use influxdb3_distributed::query_engine::DistributedQueryEngine;
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

    // 2. 创建测试数据
    println!("\n📋 步骤 2: 准备测试数据");
    
    // CPU 使用率数据
    let cpu_schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("host", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("usage", DataType::Float64, false),
        Field::new("cores", DataType::Int64, false),
    ]));

    let cpu_batch = RecordBatch::try_new(
        cpu_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                1609459200, 1609459260, 1609459320,
                1609459200, 1609459260, 1609459320,
                1609459200, 1609459260, 1609459320,
            ])),
            Arc::new(StringArray::from(vec![
                "server1", "server1", "server1",
                "server2", "server2", "server2",
                "server3", "server3", "server3",
            ])),
            Arc::new(StringArray::from(vec![
                "us-east", "us-east", "us-east",
                "us-west", "us-west", "us-west",
                "eu-west", "eu-west", "eu-west",
            ])),
            Arc::new(Float64Array::from(vec![
                85.0, 90.5, 78.2,
                65.3, 72.1, 68.9,
                55.7, 61.2, 58.4,
            ])),
            Arc::new(Int64Array::from(vec![
                8, 8, 8,
                16, 16, 16,
                4, 4, 4,
            ])),
        ],
    )?;

    println!("  ✓ 创建 CPU 数据表 (9 行)");

    // 内存使用率数据
    let memory_schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("host", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("used_mb", DataType::Int64, false),
        Field::new("total_mb", DataType::Int64, false),
    ]));

    let memory_batch = RecordBatch::try_new(
        memory_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                1609459200, 1609459260,
                1609459200, 1609459260,
                1609459200, 1609459260,
            ])),
            Arc::new(StringArray::from(vec![
                "server1", "server1",
                "server2", "server2",
                "server3", "server3",
            ])),
            Arc::new(StringArray::from(vec![
                "us-east", "us-east",
                "us-west", "us-west",
                "eu-west", "eu-west",
            ])),
            Arc::new(Int64Array::from(vec![
                4096, 4512,
                8192, 9000,
                2048, 2500,
            ])),
            Arc::new(Int64Array::from(vec![
                8192, 8192,
                16384, 16384,
                4096, 4096,
            ])),
        ],
    )?;

    println!("  ✓ 创建 Memory 数据表 (6 行)");

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

    // 4. 创建查询引擎
    println!("\n📋 步骤 4: 初始化查询引擎");
    let engine = DistributedQueryEngine::new(meta_service.clone());

    // 注册表到 DataFusion
    let cpu_table_provider = MemTable::try_new(cpu_schema, vec![vec![cpu_batch]])?;
    engine.register_table("cpu", Arc::new(cpu_table_provider)).await?;
    println!("  ✓ 注册 cpu 表到查询引擎");

    let memory_table_provider = MemTable::try_new(memory_schema, vec![vec![memory_batch]])?;
    engine.register_table("memory", Arc::new(memory_table_provider)).await?;
    println!("  ✓ 注册 memory 表到查询引擎");

    println!("\n✅ 系统初始化完成！\n");

    // 5. 执行测试查询
    println!("🔍 开始执行查询测试");
    println!("================================\n");

    // 查询 1: 简单查询
    println!("查询 1: SELECT * FROM cpu WHERE usage > 70 LIMIT 5");
    println!("---------------------------------------------------");
    match execute_and_display_query(&engine, "SELECT * FROM cpu WHERE usage > 70 LIMIT 5").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 2: 聚合查询
    println!("查询 2: SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host");
    println!("-----------------------------------------------------------------------");
    match execute_and_display_query(&engine, "SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 3: 全局统计
    println!("查询 3: SELECT COUNT(*) as count, AVG(usage) as avg, MAX(usage) as max FROM cpu");
    println!("---------------------------------------------------------------------------------");
    match execute_and_display_query(&engine, "SELECT COUNT(*) as count, AVG(usage) as avg, MAX(usage) as max FROM cpu").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 4: JOIN 查询
    println!("查询 4: SELECT c.host, c.usage, m.used_mb FROM cpu c JOIN memory m ON c.host = m.host LIMIT 5");
    println!("------------------------------------------------------------------------------------------------");
    match execute_and_display_query(&engine, "SELECT c.host, c.usage, m.used_mb FROM cpu c JOIN memory m ON c.host = m.host LIMIT 5").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    // 查询 5: 排序查询
    println!("查询 5: SELECT host, usage FROM cpu ORDER BY usage DESC LIMIT 3");
    println!("-------------------------------------------------------------------");
    match execute_and_display_query(&engine, "SELECT host, usage FROM cpu ORDER BY usage DESC LIMIT 3").await {
        Ok(count) => println!("✓ 返回 {} 行\n", count),
        Err(e) => println!("✗ 查询失败: {}\n", e),
    }

    println!("\n✅ 所有查询测试完成！");

    Ok(())
}

async fn execute_and_display_query(
    engine: &DistributedQueryEngine,
    sql: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut stream = engine.execute_sql(sql).await?;
    
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

    Ok(total_rows)
}

fn print_batch(batch: &RecordBatch) {
    use arrow::util::pretty::print_batches;
    let _ = print_batches(&[batch.clone()]);
}

