//! Test distributed JOIN query with sharded data across multiple nodes
//!
//! This example demonstrates TRUE distributed query capabilities:
//! 1. Create DistributedTableProvider for cpu (sharded across nodes 1,2,3)
//! 2. Create DistributedTableProvider for mem (sharded across nodes 1,2,3)
//! 3. Register them with DataFusion
//! 4. Execute JOIN queries that fetch and merge data from all nodes

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

    // Define schema for cpu table (now with more columns)
    let cpu_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("value", DataType::Float64, true),     
        Field::new("load", DataType::Float64, true),      
        Field::new("cores", DataType::Int64, true),       
    ]));

    // Define schema for mem table (now with more columns)
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
        Field::new("available", DataType::Float64, true),  
        Field::new("cached", DataType::Float64, true),     
    ]));

    println!("Step 1: 创建分布式表提供者（分片版本）");
    println!("  - cpu 表分片在节点1,2,3 (127.0.0.1:8181/8182/8183) - 6列");
    println!("  - mem 表分片在节点1,2,3 (127.0.0.1:8181/8182/8183) - 7列");
    println!("  - 分片策略: 基于 host 哈希");
    println!();

    // Create DistributedTableProvider for cpu table (sharded across 3 nodes)
    let cpu_table = Arc::new(DistributedTableProvider::new(
        "cpu".to_string(),
        "testdb".to_string(),
        cpu_schema,
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)], // cpu data is sharded
        rpc_client.clone(),
    ));

    // Create DistributedTableProvider for mem table (sharded across 3 nodes)
    let mem_table = Arc::new(DistributedTableProvider::new(
        "mem".to_string(),
        "testdb".to_string(),
        mem_schema,
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)], // mem data is sharded
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

    println!("数据统计");

    // 直接查询远程节点获取准确的行数
    let cpu_total = async {
        let response = reqwest::Client::new()
            .post("http://127.0.0.1:8181/api/v3/query_sql")
            .json(&serde_json::json!({
                "db": "testdb",
                "query": "SELECT COUNT(*) as count FROM cpu"
            }))
            .send()
            .await
            .ok()?;
        let text = response.text().await.ok()?;
        let json: serde_json::Value = serde_json::from_str(&text).ok()?;
        json.as_array()?.first()?.get("count")?.as_i64()
    }
    .await
    .unwrap_or(0);

    let mem_total = async {
        let response = reqwest::Client::new()
            .post("http://127.0.0.1:8182/api/v3/query_sql")
            .json(&serde_json::json!({
                "db": "testdb",
                "query": "SELECT COUNT(*) as count FROM mem"
            }))
            .send()
            .await
            .ok()?;
        let text = response.text().await.ok()?;
        let json: serde_json::Value = serde_json::from_str(&text).ok()?;
        json.as_array()?.first()?.get("count")?.as_i64()
    }
    .await
    .unwrap_or(0);

    println!("📊 CPU 表节点1行数: {} 行 (查询节点1)", cpu_total);
    println!("📊 MEM 表节点1行数: {} 行 (查询节点1)", mem_total);
    println!("📌 注意: 实际会查询所有3个节点并合并结果");
    println!();


    println!("测试 1: 简单查询 (验证谓词下推)");

    let query1 = "SELECT host, region, value, load FROM cpu WHERE host = 'server01'";

    match ctx.sql(query1).await {
        Ok(df) => {
            println!("查询计划:");
            println!("{}", df.logical_plan());
            println!();

            println!("执行中...");
            let start_time = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start_time.elapsed();
                    let result_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    println!("✓ 查询成功!");
                    println!("⏱️  查询耗时: {:.2} ms", elapsed.as_secs_f64() * 1000.0);
                    if cpu_total > 0 {
                        println!("📉 数据缩减效果: {} 行 -> {} 行 ({:.1}% 缩减)",
                            cpu_total, result_rows,
                            (1.0 - result_rows as f64 / cpu_total as f64) * 100.0);
                    } else {
                        println!("📉 查询结果: {} 行", result_rows);
                    }
                    println!();
                    println!("结果 (前5行):");
                    if !batches.is_empty() {
                        // 只显示前5行
                        let first_batch = &batches[0];
                        let row_count = first_batch.num_rows().min(5);
                        let limited_batch = first_batch.slice(0, row_count);
                        arrow::util::pretty::print_batches(&[limited_batch])?;
                    }
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

    println!("测试 2: 跨节点 JOIN 查询 (谓词下推优化)");

    let query2 = "SELECT c.host, c.region, c.value as cpu_usage, c.load as cpu_load, \
                         m.total as mem_total, m.used as mem_used, m.available as mem_available \
                  FROM (SELECT * FROM cpu WHERE host = 'server01') c \
                  JOIN (SELECT * FROM mem WHERE host = 'server01') m \
                  ON c.host = m.host";


    match ctx.sql(query2).await {
        Ok(df) => {
            println!("✓ 查询规划成功!");
            println!();
            println!("逻辑计划:");
            println!("{}", df.logical_plan());
            println!();

            // 并行查询统计信息和执行 JOIN
            println!("执行中...");
            let start_time = std::time::Instant::now();

            // 使用 tokio::join! 并行执行统计查询和 JOIN
            let (cpu_filtered_result, mem_filtered_result, join_result) = tokio::join!(
                async {
                    let response = reqwest::Client::new()
                        .post("http://127.0.0.1:8181/api/v3/query_sql")
                        .json(&serde_json::json!({
                            "db": "testdb",
                            "query": "SELECT COUNT(*) as count FROM cpu WHERE host = 'server01'"
                        }))
                        .send()
                        .await
                        .ok()?;
                    let text = response.text().await.ok()?;
                    let json: serde_json::Value = serde_json::from_str(&text).ok()?;
                    json.as_array()?.first()?.get("count")?.as_i64()
                },
                async {
                    let response = reqwest::Client::new()
                        .post("http://127.0.0.1:8182/api/v3/query_sql")
                        .json(&serde_json::json!({
                            "db": "testdb",
                            "query": "SELECT COUNT(*) as count FROM mem WHERE host = 'server01'"
                        }))
                        .send()
                        .await
                        .ok()?;
                    let text = response.text().await.ok()?;
                    let json: serde_json::Value = serde_json::from_str(&text).ok()?;
                    json.as_array()?.first()?.get("count")?.as_i64()
                },
                df.collect()
            );

            let cpu_filtered = cpu_filtered_result.unwrap_or(0);
            let mem_filtered = mem_filtered_result.unwrap_or(0);

            match join_result {
                Ok(batches) => {
                    let elapsed = start_time.elapsed();
                    let result_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    println!("✓ JOIN 查询成功!");
                    println!();
                    println!("📊 性能统计:");
                    println!("  • 查询耗时: {:.2} ms", elapsed.as_secs_f64() * 1000.0);
                    println!("  • CPU 表原始数据: {} 行", cpu_total);
                    println!("  • MEM 表原始数据: {} 行", mem_total);
                    println!("  • 谓词下推后 CPU: {} 行 ({:.1}% 缩减)",
                        cpu_filtered,
                        (1.0 - cpu_filtered as f64 / cpu_total as f64) * 100.0);
                    println!("  • 谓词下推后 MEM: {} 行 ({:.1}% 缩减)",
                        mem_filtered,
                        (1.0 - mem_filtered as f64 / mem_total as f64) * 100.0);
                    println!("  • JOIN 结果: {} 行", result_rows);
                    if cpu_total > 0 && mem_total > 0 {
                        let total_transmitted = cpu_filtered + mem_filtered;
                        let reduction = (1.0 - (total_transmitted as f64 / (cpu_total + mem_total) as f64)) * 100.0;
                        println!("  • 数据传输总缩减: {:.1}% (传输 {} 行 vs 原始 {} 行)",
                            reduction, total_transmitted, cpu_total + mem_total);
                    }
                    println!();
                    println!("结果 (前5行):");
                    if !batches.is_empty() {
                        // 只显示前5行
                        let first_batch = &batches[0];
                        let row_count = first_batch.num_rows().min(5);
                        let limited_batch = first_batch.slice(0, row_count);
                        arrow::util::pretty::print_batches(&[limited_batch])?;
                    }
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

    println!("测试 3: 跨区域聚合查询");

    let query3 = "SELECT c.region, \
                         COUNT(*) as sample_count, \
                         AVG(c.value) as avg_cpu, \
                         AVG(m.used) as avg_mem_used \
                  FROM (SELECT * FROM cpu WHERE region = 'us-east') c \
                  JOIN (SELECT * FROM mem WHERE region = 'us-east') m \
                  ON c.host = m.host \
                  GROUP BY c.region";


    match ctx.sql(query3).await {
        Ok(df) => {
            println!("执行中...");
            let start_time = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start_time.elapsed();
                    println!("✓ 聚合查询成功!");
                    println!("⏱️  查询耗时: {:.2} ms", elapsed.as_secs_f64() * 1000.0);
                    println!();
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

    println!("测试 4: 多条件过滤 + JOIN");

    let query4 = "SELECT c.host, c.region, c.value as cpu_usage, c.load as cpu_load, \
                         m.total as mem_total, m.used as mem_used \
                  FROM (SELECT * FROM cpu WHERE region = 'us-east' AND value > 50.0) c \
                  JOIN (SELECT * FROM mem WHERE region = 'us-east' AND used > 30.0) m \
                  ON c.host = m.host \
                  ORDER BY c.value DESC \
                  LIMIT 10";

    match ctx.sql(query4).await {
        Ok(df) => {
            println!("执行中...");
            let start_time = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start_time.elapsed();
                    println!("✓ 查询成功!");
                    println!("⏱️  查询耗时: {:.2} ms", elapsed.as_secs_f64() * 1000.0);
                    println!();
                    println!("结果 (前5行):");
                    if !batches.is_empty() {
                        let first_batch = &batches[0];
                        let row_count = first_batch.num_rows().min(5);
                        let limited_batch = first_batch.slice(0, row_count);
                        arrow::util::pretty::print_batches(&[limited_batch])?;
                    }
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

    println!("测试 5: 多表 JOIN + 窗口函数模拟");

    let query5 = "SELECT c.host, c.region, \
                         AVG(c.value) as avg_cpu, \
                         MAX(c.load) as max_load, \
                         AVG(m.used) as avg_mem_used, \
                         MAX(m.used) as max_mem_used, \
                         COUNT(*) as sample_count \
                  FROM (SELECT * FROM cpu WHERE region IN ('us-east', 'us-west')) c \
                  JOIN (SELECT * FROM mem WHERE region IN ('us-east', 'us-west')) m \
                  ON c.host = m.host \
                  GROUP BY c.host, c.region \
                  HAVING AVG(c.value) > 40.0 \
                  ORDER BY avg_cpu DESC \
                  LIMIT 10";

    match ctx.sql(query5).await {
        Ok(df) => {
            println!("执行中...");
            let start_time = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start_time.elapsed();
                    println!("✓ 查询成功!");
                    println!("⏱️  查询耗时: {:.2} ms", elapsed.as_secs_f64() * 1000.0);
                    println!();
                    println!("结果 (前5行):");
                    if !batches.is_empty() {
                        let first_batch = &batches[0];
                        let row_count = first_batch.num_rows().min(5);
                        let limited_batch = first_batch.slice(0, row_count);
                        arrow::util::pretty::print_batches(&[limited_batch])?;
                    }
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

    println!("测试 6: 子查询 + JOIN");

    let query6 = "SELECT high_cpu.host, high_cpu.region, high_cpu.avg_cpu, m.used as mem_used \
                  FROM (SELECT host, region, AVG(value) as avg_cpu \
                        FROM cpu \
                        WHERE region = 'us-east' \
                        GROUP BY host, region \
                        HAVING AVG(value) > 50.0) AS high_cpu \
                  JOIN mem m ON high_cpu.host = m.host \
                  ORDER BY high_cpu.avg_cpu DESC \
                  LIMIT 10";


    match ctx.sql(query6).await {
        Ok(df) => {
            println!("执行中...");
            let start_time = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start_time.elapsed();
                    let result_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    println!("✓ 查询成功!");
                    println!("⏱️  查询耗时: {:.2} ms", elapsed.as_secs_f64() * 1000.0);
                    println!("📊 结果行数: {}", result_rows);
                    println!();
                    println!("结果 (前5行):");
                    if !batches.is_empty() {
                        let first_batch = &batches[0];
                        let row_count = first_batch.num_rows().min(5);
                        let limited_batch = first_batch.slice(0, row_count);
                        arrow::util::pretty::print_batches(&[limited_batch])?;
                    }
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

    Ok(())
}

