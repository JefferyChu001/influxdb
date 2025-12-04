//! 真正的分布式查询测试
//!
//! 场景：cpu 和 mem 表的数据分散在3个节点上
//! - 节点1 (8181): server01-10 的 CPU 和 MEM 数据
//! - 节点2 (8182): server11-20 的 CPU 和 MEM 数据
//! - 节点3 (8183): server21-30 的 CPU 和 MEM 数据
//!
//! 测试内容：
//! 1. 分组聚合查询
//! 2. 排序查询（按 CPU value 排序）
//! 3. 过滤查询
//! 4. 分布式 JOIN（8181 的 cpu 和 8182 的 mem JOIN）

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::*;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::collect;
use influxdb3_cluster::rpc::client::ClusterRpcClient;
use influxdb3_cluster::types::NodeId;
use influxdb3_query_executor::distributed::{DistributedTableProvider, DistributedJoinOptimizer};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("========================================");
    println!("  真正的分布式查询测试");
    println!("========================================");
    println!();
    
    // 创建 gRPC 客户端
    let rpc_client = Arc::new(ClusterRpcClient::new());

    // 定义 cpu 表的 schema
    let cpu_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("time", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("load", DataType::Float64, true),
        Field::new("cores", DataType::Int64, true),
    ]));

    // 定义 mem 表的 schema
    let mem_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("time", DataType::Utf8, true),
        Field::new("total", DataType::Float64, true),
        Field::new("used", DataType::Float64, true),
        Field::new("available", DataType::Float64, true),
        Field::new("cached", DataType::Float64, true),
    ]));


    // 创建分布式表提供者 - cpu 表跨3个节点分片
    let cpu_table = Arc::new(DistributedTableProvider::new(
        "cpu".to_string(),
        "testdb".to_string(),
        cpu_schema.clone(),
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        rpc_client.clone(),
    ));

    // 创建分布式表提供者 - mem 表跨3个节点分片
    let mem_table = Arc::new(DistributedTableProvider::new(
        "mem".to_string(),
        "testdb".to_string(),
        mem_schema.clone(),
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        rpc_client.clone(),
    ));

    // // 单节点 CPU 表（只从节点1查询）
    // let cpu_node1 = Arc::new(DistributedTableProvider::new(
    //     "cpu".to_string(),
    //     "testdb".to_string(),
    //     cpu_schema.clone(),
    //     vec![NodeId::new(1)],
    //     rpc_client.clone(),
    // ));

    // // 单节点 MEM 表（只从节点2查询）
    // let mem_node2 = Arc::new(DistributedTableProvider::new(
    //     "mem".to_string(),
    //     "testdb".to_string(),
    //     mem_schema.clone(),
    //     vec![NodeId::new(2)],
    //     rpc_client.clone(),
    // ));

    // 创建 DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("cpu", cpu_table)?;
    ctx.register_table("mem", mem_table)?;
    // ctx.register_table("cpu_node1", cpu_node1)?;
    // ctx.register_table("mem_node2", mem_node2)?;
    
    let query1 = "SELECT region, COUNT(*) as count FROM cpu GROUP BY region";
    match ctx.sql(query1).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                }
                Err(e) => println!("✗ 查询失败: {}", e),
            }
        }
        Err(e) => println!("✗ 规划失败: {}", e),
    }
    
    let query2 = "SELECT * FROM cpu WHERE host = 'server05' LIMIT 5";
    match ctx.sql(query2).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                }
                Err(e) => println!("✗ 查询失败: {}", e),
            }
        }
        Err(e) => println!("✗ 规划失败: {}", e),
    }
       
    let query3 = "SELECT host, AVG(value) as avg_cpu FROM cpu WHERE host IN ('server05', 'server15', 'server25') GROUP BY host";
    match ctx.sql(query3).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    println!("说明: server05在节点1, server15在节点2, server25在节点3");
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                }
                Err(e) => println!("✗ 查询失败: {}", e),
            }
        }
        Err(e) => println!("✗ 规划失败: {}", e),
    }

    let query4 = "SELECT AVG(value) as avg_cpu, MAX(value) as max_cpu, MIN(value) as min_cpu FROM cpu";
    match ctx.sql(query4).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                }
                Err(e) => println!("✗ 查询失败: {}", e),
            }
        }
        Err(e) => println!("✗ 规划失败: {}", e),
    }

    let query5 = "SELECT host, region, value FROM cpu ORDER BY value DESC LIMIT 10";
    match ctx.sql(query5).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    println!("说明：从3个节点获取数据，协调节点进行全局排序，取 TOP 10");
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                }
                Err(e) => println!("✗ 查询失败: {}", e),
            }
        }
        Err(e) => println!("✗ 规划失败: {}", e),
    }


    let query6 = r#"
        SELECT c.host, c.value as cpu_value, m.used as mem_used
        FROM cpu_node1 c
        INNER JOIN mem_node2 m ON c.region = m.region
        LIMIT 10
    "#;
    match ctx.sql(query6).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                }
                Err(e) => println!("✗ 查询失败: {}", e),
            }
        }
        Err(e) => println!("✗ 规划失败: {}", e),
    }


    let query7 = r#"
        SELECT c.host, AVG(c.value) as avg_cpu, AVG(m.used) as avg_mem
        FROM cpu c
        INNER JOIN mem m ON c.host = m.host
        GROUP BY c.host
        ORDER BY avg_cpu DESC
        LIMIT 10
    "#;

    println!("--- 未优化版本 ---");
    let unoptimized_time = match ctx.sql(query7).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 未优化查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    println!("  结果行数: {}", batches.iter().map(|b| b.num_rows()).sum::<usize>());
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                    elapsed
                }
                Err(e) => {
                    println!("✗ 查询失败: {}", e);
                    std::time::Duration::from_secs(0)
                }
            }
        }
        Err(e) => {
            println!("✗ 规划失败: {}", e);
            std::time::Duration::from_secs(0)
        }
    };

    println!("--- Hash Join 优化版本 ---");
    let optimized_time = match ctx.sql(query7).await {
        Ok(df) => {
            // 获取物理计划
            let physical_plan = df.create_physical_plan().await?;

            // 应用 Hash Join 优化
            let optimizer = DistributedJoinOptimizer::new()
                .with_broadcast_threshold(50 * 1024); // 50MB

            let optimized_plan = optimizer.optimize(physical_plan.clone())?;


            // 执行优化后的计划
            let start = std::time::Instant::now();
            let task_ctx = Arc::new(TaskContext::default());

            match collect(optimized_plan, task_ctx).await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 优化查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    println!("  结果行数: {}", batches.iter().map(|b| b.num_rows()).sum::<usize>());
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                    elapsed
                }
                Err(e) => {
                    println!("✗ 优化查询失败: {}", e);
                    std::time::Duration::from_secs(0)
                }
            }
        }
        Err(e) => {
            println!("✗ 规划失败: {}", e);
            std::time::Duration::from_secs(0)
        }
    };


    Ok(())
}

