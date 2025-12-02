//! Test distributed JOIN query using DistributedTableProvider
//!
//! This example demonstrates the distributed query capabilities we just implemented.
//! It will:
//! 1. Create DistributedTableProvider for cpu (node1) and mem (node2)
//! 2. Register them with DataFusion
//! 3. Execute a JOIN query that automatically uses predicate pushdown

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::*;
use influxdb3_cluster::rpc::client::ClusterRpcClient;
use influxdb3_cluster::types::NodeId;
use influxdb3_query_executor::distributed::DistributedTableProvider;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("========================================");
    println!("  分布式 JOIN 查询测试");
    println!("========================================");
    println!();

    // Create gRPC client
    let rpc_client = Arc::new(ClusterRpcClient::new());

    // Define schema for cpu table
    let cpu_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("value", DataType::Float64, true),
    ]));

    // Define schema for mem table
    let mem_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("total", DataType::Float64, true),
        Field::new("used", DataType::Float64, true),
    ]));

    println!("Step 1: 创建分布式表提供者");
    println!("  - cpu 表在节点1 (127.0.0.1:8181)");
    println!("  - mem 表在节点2 (127.0.0.1:8182)");
    println!();

    // Create DistributedTableProvider for cpu table (on node1)
    let cpu_table = Arc::new(DistributedTableProvider::new(
        "cpu".to_string(),
        "testdb".to_string(),
        cpu_schema,
        vec![NodeId::new(1)], // cpu data is on node1
        rpc_client.clone(),
    ));

    // Create DistributedTableProvider for mem table (on node2)
    let mem_table = Arc::new(DistributedTableProvider::new(
        "mem".to_string(),
        "testdb".to_string(),
        mem_schema,
        vec![NodeId::new(2)], // mem data is on node2
        rpc_client.clone(),
    ));

    println!("Step 2: 创建 DataFusion SessionContext 并注册表");
    println!();

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Register distributed tables
    ctx.register_table("cpu", cpu_table)?;
    ctx.register_table("mem", mem_table)?;

    println!("✓ 表已注册到 DataFusion");
    println!();

    println!("========================================");
    println!("测试 1: 简单查询 (验证谓词下推)");
    println!("========================================");
    println!();

    let query1 = "SELECT host, value FROM cpu WHERE host = 'server01'";
    println!("SQL: {}", query1);
    println!();

    match ctx.sql(query1).await {
        Ok(df) => {
            println!("查询计划:");
            println!("{}", df.logical_plan());
            println!();

            println!("执行中...");
            match df.collect().await {
                Ok(batches) => {
                    println!("✓ 查询成功! 结果:");
                    arrow::util::pretty::print_batches(&batches)?;
                }
                Err(e) => {
                    println!("✗ 查询执行失败: {}", e);
                }
            }
        }
        Err(e) => {
            println!("✗ 查询规划失败: {}", e);
        }
    }
    println!();

    println!("========================================");
    println!("测试 2: 跨节点 JOIN 查询");
    println!("========================================");
    println!();

    let query2 = "SELECT c.host, c.value as cpu_value, m.used as mem_used \
                  FROM cpu c JOIN mem m ON c.host = m.host \
                  WHERE c.host = 'server01'";

    println!("SQL: {}", query2);
    println!();
    println!("这个查询会:");
    println!("  1. 从节点1查询: SELECT host, value FROM cpu WHERE host = 'server01'");
    println!("  2. 从节点2查询: SELECT host, used FROM mem WHERE host = 'server01'");
    println!("  3. 在本地执行 JOIN (数据量很小)");
    println!();

    match ctx.sql(query2).await {
        Ok(df) => {
            println!("✓ 查询规划成功!");
            println!();
            println!("逻辑计划:");
            println!("{}", df.logical_plan());
            println!();

            println!("执行中...");
            match df.collect().await {
                Ok(batches) => {
                    println!("✓ JOIN 查询成功! 结果:");
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                    println!("✓✓✓ 分布式 JOIN 测试成功！ ✓✓✓");
                }
                Err(e) => {
                    println!("✗ 查询执行失败: {}", e);
                    println!("错误详情: {:?}", e);
                }
            }
        }
        Err(e) => {
            println!("✗ 查询规划失败: {}", e);
        }
    }

    println!();
    println!("========================================");
    println!("测试完成");
    println!("========================================");

    Ok(())
}

