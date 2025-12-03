//! 测试协调节点架构
//!
//! 架构：
//! - 1个协调节点（Coordinator）：接收写入和查询，负责分片路由和结果聚合
//! - 3个数据节点（Data Nodes）：存储实际数据
//!
//! 写入流程：Client → Coordinator → ShardManager 计算分片 → 写入到 Data Node
//! 查询流程：Client → Coordinator → 下发到各个 Data Node → 收集结果 → 聚合 → 返回

mod coordinator_node;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::*;
use influxdb3_cluster::rpc::client::ClusterRpcClient;
use influxdb3_cluster::types::{NodeId, NodeInfo, NodeRole, NodeStatus, NodeCapacity, ShardRange};
use influxdb3_cluster::shard_manager::ShardManager;
use influxdb3_cluster::node_registry::NodeRegistry;
use influxdb3_cluster::meta_store::InMemoryMetaStore;
use influxdb3_id::DbId;
use influxdb3_query_executor::distributed::DistributedTableProvider;
use coordinator_node::CoordinatorNode;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("========================================");
    println!("  协调节点架构测试");
    println!("========================================");
    println!();
    
    println!("说明：");
    println!("  - 需要先启动集群：./scripts/start_distributed_cluster.sh");
    println!("  - etcd: http://127.0.0.1:2379");
    println!("  - 节点1: http://127.0.0.1:8181");
    println!("  - 节点2: http://127.0.0.1:8182");
    println!("  - 节点3: http://127.0.0.1:8183");
    println!();
    
    // ========== 第一部分：初始化集群基础设施 ==========
    println!("【第一步】初始化集群基础设施");
    println!();
    
    // 1. 创建元数据存储（使用内存实现）
    let meta_store = Arc::new(InMemoryMetaStore::new());
    
    // 2. 创建节点注册表
    let node_registry = Arc::new(NodeRegistry::new(meta_store.clone()));
    
    // 3. 创建分片管理器（4个分片，副本因子为3）
    let shard_manager = Arc::new(ShardManager::new(
        4,  // 4个分片
        3,  // 每个分片有3个副本
        meta_store.clone(),
    ));
    
    // 4. 创建 RPC 客户端
    let rpc_client = Arc::new(ClusterRpcClient::new());
    
    println!("✓ 元数据存储已创建");
    println!("✓ 节点注册表已创建");
    println!("✓ 分片管理器已创建（4个分片，副本因子=3）");
    println!("✓ RPC 客户端已创建");
    println!();
    
    // ========== 第二部分：注册数据节点 ==========
    println!("【第二步】注册3个数据节点");
    println!();
    
    for i in 1..=3 {
        let node_info = NodeInfo {
            node_id: NodeId::new(i),
            address: "127.0.0.1".to_string(),
            grpc_port: 8180 + i as u16,  // 8181, 8182, 8183
            http_port: 8180 + i as u16,  // 8181, 8182, 8183
            role: NodeRole::DataNode,
            status: NodeStatus::Active,
            capacity: NodeCapacity {
                cpu_cores: 8,
                memory_bytes: 8 * 1024 * 1024 * 1024,
                disk_bytes: 100 * 1024 * 1024 * 1024,
                current_shards: 0,
                max_shards: 10,
            },
            last_heartbeat_nanos: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
        };
        
        node_registry.register_node(node_info).await?;
        println!("  ✓ 数据节点 {} 已注册: 127.0.0.1:{}", i, 8180 + i);
    }
    println!();
    
    // ========== 第三部分：创建协调节点 ==========
    println!("【第三步】创建协调节点");
    println!();
    
    let mut coordinator = CoordinatorNode::new(
        node_registry.clone(),
        shard_manager.clone(),
        meta_store.clone(),
        rpc_client.clone(),
    );
    
    // 注册数据库
    let database_name = "testdb";
    let database_id = DbId::from(1);
    coordinator.register_database(database_name.to_string(), database_id);
    
    println!("✓ 协调节点已创建");
    println!("✓ 数据库 '{}' 已注册 (ID: {})", database_name, database_id);
    println!();
    
    // ========== 第四部分：创建分片 ==========
    println!("【第四步】创建分片并分配到数据节点");
    println!();
    
    for i in 0..4 {
        let shard_range = ShardRange::Time {
            start_nanos: i * 1_000_000_000 * 3600 * 24,
            end_nanos: (i + 1) * 1_000_000_000 * 3600 * 24,
        };

        let shard_id = shard_manager
            .create_shard(database_id, shard_range, &node_registry)
            .await?;

        let replicas = shard_manager.get_shard_replicas(shard_id).await?;
        let node_ids: Vec<_> = replicas.iter().map(|r| r.node_id).collect();

        println!("  ✓ 分片 {} 已创建", shard_id);
        match shard_range {
            ShardRange::Time { start_nanos, end_nanos } => {
                println!("    时间范围: [{}, {})", start_nanos, end_nanos);
            }
            _ => {}
        }
        println!("    副本节点: {:?}", node_ids);
    }
    println!();

    // ========== 第五部分：通过协调节点写入数据 ==========
    println!("【第五步】通过协调节点写入数据（分片路由）");
    println!();

    // 先创建数据库
    println!("创建数据库 '{}'...", database_name);
    for i in 1..=3 {
        let url = format!("http://127.0.0.1:{}/api/v3/configure/db", 8180 + i);
        let client = reqwest::Client::new();
        let _ = client.post(&url)
            .json(&serde_json::json!({
                "name": database_name
            }))
            .send()
            .await;
    }
    println!("✓ 数据库已在所有节点创建");
    println!();

    let test_data = vec![
        "cpu,host=server01,region=us-east value=45.2,load=0.8,cores=8i",
        "cpu,host=server02,region=us-east value=52.1,load=0.9,cores=8i",
        "cpu,host=server03,region=us-east value=38.7,load=0.6,cores=16i",
        "cpu,host=server04,region=us-west value=61.3,load=1.2,cores=8i",
        "cpu,host=server05,region=us-west value=48.9,load=0.7,cores=16i",
        "cpu,host=server06,region=us-west value=55.4,load=1.0,cores=8i",
        "cpu,host=server07,region=eu-central value=42.8,load=0.5,cores=16i",
        "cpu,host=server08,region=eu-central value=67.2,load=1.5,cores=8i",
        "cpu,host=server09,region=eu-central value=39.1,load=0.6,cores=16i",
    ];

    println!("准备写入 {} 条记录（通过协调节点路由）...", test_data.len());

    for (i, line) in test_data.iter().enumerate() {
        match coordinator.write_lp(database_name, line).await {
            Ok(_) => println!("  [{}/{}] ✓", i + 1, test_data.len()),
            Err(e) => eprintln!("  [{}/{}] ✗ 写入失败: {}", i + 1, test_data.len(), e),
        }
    }

    println!();
    println!("等待数据传播...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!();

    // ========== 第六部分：通过协调节点查询数据 ==========
    println!("【第六步】通过协调节点查询数据（分布式聚合）");
    println!();

    // 定义 cpu 表的 schema
    let cpu_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("value", DataType::Float64, true),
        Field::new("load", DataType::Float64, true),
        Field::new("cores", DataType::Int64, true),
    ]));

    // 从协调节点获取表所在的节点
    let table_nodes = coordinator.get_table_nodes(database_name, "cpu").await?;
    println!("cpu 表的数据分布在节点: {:?}", table_nodes);
    println!();

    // 创建分布式表提供者
    let cpu_table = Arc::new(DistributedTableProvider::new(
        "cpu".to_string(),
        database_name.to_string(),
        cpu_schema,
        table_nodes,
        coordinator.rpc_client().clone(),
    ));

    // 创建 DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("cpu", cpu_table)?;

    println!("✓ 分布式表已注册到查询引擎");
    println!();

    // 测试查询1: 按 region 分组统计
    println!("【查询1】按 region 分组统计");
    println!("SQL: SELECT region, COUNT(*) as count FROM cpu GROUP BY region");
    println!();

    let query1 = "SELECT region, COUNT(*) as count FROM cpu GROUP BY region";
    match ctx.sql(query1).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    println!("说明：协调节点从多个数据节点收集数据并聚合");
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                }
                Err(e) => println!("✗ 查询失败: {}", e),
            }
        }
        Err(e) => println!("✗ 规划失败: {}", e),
    }

    // 测试查询2: 全局聚合
    println!("【查询2】全局聚合 - 平均值、最大值、最小值");
    println!("SQL: SELECT AVG(value) as avg_cpu, MAX(value) as max_cpu, MIN(value) as min_cpu FROM cpu");
    println!();

    let query2 = "SELECT AVG(value) as avg_cpu, MAX(value) as max_cpu, MIN(value) as min_cpu FROM cpu";
    match ctx.sql(query2).await {
        Ok(df) => {
            let start = std::time::Instant::now();
            match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    println!("✓ 查询成功! 耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
                    println!("说明：协调节点收集各节点的部分聚合结果，再进行全局聚合");
                    arrow::util::pretty::print_batches(&batches)?;
                    println!();
                }
                Err(e) => println!("✗ 查询失败: {}", e),
            }
        }
        Err(e) => println!("✗ 规划失败: {}", e),
    }

    println!("========================================");
    println!("测试完成！");
    println!("========================================");

    Ok(())
}

