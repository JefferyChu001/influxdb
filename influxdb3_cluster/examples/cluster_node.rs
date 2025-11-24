//! Example cluster node that can be run to test distributed functionality

use influxdb3_cluster::{
    clustered_write_buffer::ClusteredWriteBuffer,
    meta_store::InMemoryMetaStore,
    node_registry::NodeRegistry,
    query::join::join_optimizer::{JoinOptimizer, TableStatistics},
    replication::WriteBatch,
    shard_manager::ShardManager,
    types::{ConsistencyLevel, DbId, NodeCapacity, NodeId, NodeInfo, NodeRole, NodeStatus, ShardRange},
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting InfluxDB 3 Cluster Demo\n");

    // Step 1: Create shared metadata store (in production, this would be etcd)
    println!("📦 Step 1: Creating metadata store...");
    let meta_store = Arc::new(InMemoryMetaStore::new());
    println!("   ✓ Metadata store created\n");

    // Step 2: Create node registry
    println!("🔧 Step 2: Creating node registry...");
    let node_registry = Arc::new(NodeRegistry::new(meta_store.clone()));
    println!("   ✓ Node registry created\n");

    // Step 3: Register 3 data nodes
    println!("📡 Step 3: Registering cluster nodes...");
    for i in 1..=3 {
        let node = NodeInfo {
            node_id: NodeId::new(i),
            address: format!("192.168.1.{}", i),
            grpc_port: 8087,
            http_port: 8086,
            role: NodeRole::DataNode,
            status: NodeStatus::Active,
            capacity: NodeCapacity {
                cpu_cores: 8,
                memory_bytes: 16 * 1024 * 1024 * 1024,
                disk_bytes: 1024 * 1024 * 1024 * 1024,
                current_shards: 0,
                max_shards: 100,
            },
            last_heartbeat_nanos: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos() as i64,
        };
        node_registry.register_node(node).await?;
        println!("   ✓ Node {} registered at {}", i, format!("192.168.1.{}", i));
    }
    println!();

    // Step 4: Verify nodes
    println!("🔍 Step 4: Verifying cluster nodes...");
    let nodes = node_registry.list_nodes().await;
    println!("   Active nodes: {}", nodes.len());
    for node in &nodes {
        println!("   - Node {}: {} ({}:{})", 
            node.node_id.as_u64(), 
            node.address, 
            node.grpc_port, 
            node.http_port
        );
    }
    println!();

    // Step 5: Create shard manager with 16 shards and 3 replicas
    println!("🗂️  Step 5: Creating shard manager...");
    let shard_manager = Arc::new(ShardManager::new(16, 3, meta_store.clone()));
    println!("   ✓ Shard manager created (16 shards, 3 replicas)\n");

    // Step 6: Create shards for database
    println!("💾 Step 6: Creating shards for database 'mydb'...");
    let db_id = DbId::new(1);
    
    for i in 0..4 {
        let start = i * 4096;
        let end = (i + 1) * 4096;
        let shard_id = shard_manager
            .create_shard(
                db_id,
                ShardRange::Hash { start, end },
                &node_registry,
            )
            .await?;
        
        let shard = shard_manager.get_shard(shard_id).await?;
        println!("   ✓ Shard {} created with {} replicas", 
            shard_id.as_u64(), 
            shard.replicas.len()
        );
        
        // Show replica distribution
        for replica in &shard.replicas {
            println!("     - Replica on Node {} ({:?})", 
                replica.node_id.as_u64(), 
                replica.role
            );
        }
    }
    println!();

    // Step 7: Test write routing
    println!("✍️  Step 7: Testing write routing...");
    let test_writes = vec![
        ("cpu", vec![("host", "server1"), ("region", "us-east")]),
        ("cpu", vec![("host", "server2"), ("region", "us-west")]),
        ("memory", vec![("host", "server1"), ("region", "us-east")]),
        ("disk", vec![("host", "server3"), ("region", "eu-west")]),
    ];

    for (measurement, tags) in test_writes {
        let tags_ref: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (*k, *v)).collect();
        let shard_id = shard_manager.route_write("mydb", measurement, &tags_ref);
        println!("   → Write to {},{:?} → Shard {}", 
            measurement, 
            tags, 
            shard_id.as_u64()
        );
    }
    println!();

    // Step 8: Test JOIN optimization
    println!("🔗 Step 8: Testing JOIN optimization...");
    let optimizer = JoinOptimizer::new(100 * 1024 * 1024, 256);

    // Small table stats
    let small_table = TableStatistics {
        row_count: 1000,
        estimated_size_bytes: 10 * 1024 * 1024, // 10MB
    };

    // Large table stats
    let large_table = TableStatistics {
        row_count: 10_000_000,
        estimated_size_bytes: 1024 * 1024 * 1024, // 1GB
    };

    let strategy = optimizer.optimize_join(
        "metadata", 
        "sensor_data", 
        &small_table, 
        &large_table
    )?;
    
    println!("   JOIN Strategy: {:?}", strategy);
    println!();

    // Step 9: Simulate heartbeats
    println!("💓 Step 9: Simulating node heartbeats...");
    for i in 1..=3 {
        node_registry.heartbeat(NodeId::new(i)).await?;
        println!("   ✓ Heartbeat from Node {}", i);
    }
    println!();

    // Step 10: Test failure detection
    println!("🔍 Step 10: Testing failure detection...");
    sleep(Duration::from_millis(100)).await;
    let failed = node_registry.detect_failures(Duration::from_millis(50)).await;
    println!("   Failed nodes detected: {}", failed.len());
    println!();

    // Summary
    println!("📊 Cluster Summary:");
    println!("   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("   Total Nodes:        {}", nodes.len());
    println!("   Total Shards:       4");
    println!("   Replication Factor: 3");
    println!("   Consistency Level:  QUORUM");
    println!("   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();

    println!("✅ Cluster demo completed successfully!");
    
    Ok(())
}

