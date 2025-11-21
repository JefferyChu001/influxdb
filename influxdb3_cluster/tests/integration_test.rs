//! Integration tests for the cluster module

use influxdb3_cluster::{
    meta_store::InMemoryMetaStore,
    node_registry::NodeRegistry,
    query::join::join_optimizer::{JoinOptimizer, JoinStrategy, TableStatistics},
    replication::{WriteBatch, WriteReplicator},
    shard_manager::ShardManager,
    types::{ConsistencyLevel, DbId, NodeCapacity, NodeInfo, NodeRole, NodeStatus, ShardRange},
};
use std::sync::Arc;

fn create_test_node(id: u64) -> NodeInfo {
    NodeInfo {
        node_id: influxdb3_cluster::types::NodeId::new(id),
        address: format!("192.168.1.{}", id),
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
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64,
    }
}

#[tokio::test]
async fn test_cluster_setup() {
    // Create metadata store
    let meta_store = Arc::new(InMemoryMetaStore::new());

    // Create node registry
    let node_registry = Arc::new(NodeRegistry::new(meta_store.clone()));

    // Register 3 nodes
    for i in 1..=3 {
        let node = create_test_node(i);
        node_registry.register_node(node).await.unwrap();
    }

    // Verify nodes are registered
    let nodes = node_registry.list_nodes().await;
    assert_eq!(nodes.len(), 3);

    // Create shard manager
    let shard_manager = Arc::new(ShardManager::new(16, 3, meta_store.clone()));

    // Create a shard
    let db_id = DbId::new(1);
    let shard_range = ShardRange::Hash {
        start: 0,
        end: 1000,
    };

    let shard_id = shard_manager
        .create_shard(db_id, shard_range, &node_registry)
        .await
        .unwrap();

    // Verify shard was created with 3 replicas
    let shard = shard_manager.get_shard(shard_id).await.unwrap();
    assert_eq!(shard.replicas.len(), 3);

    println!("✓ Cluster setup test passed");
}

#[tokio::test]
async fn test_write_replication() {
    let meta_store = Arc::new(InMemoryMetaStore::new());
    let node_registry = Arc::new(NodeRegistry::new(meta_store.clone()));
    let shard_manager = Arc::new(ShardManager::new(16, 3, meta_store.clone()));

    // Register nodes
    for i in 1..=3 {
        node_registry.register_node(create_test_node(i)).await.unwrap();
    }

    // Create shard
    let shard_id = shard_manager
        .create_shard(
            DbId::new(1),
            ShardRange::Hash {
                start: 0,
                end: 1000,
            },
            &node_registry,
        )
        .await
        .unwrap();

    // Create replicator
    let replicator = WriteReplicator::new(shard_manager, node_registry);

    // Test write with different consistency levels
    let batch = WriteBatch::new("test_db".to_string(), vec![1, 2, 3], 1);

    // Write with ONE consistency
    let result = replicator
        .replicate_write(shard_id, batch.clone(), ConsistencyLevel::One)
        .await;
    assert!(result.is_ok());

    // Write with QUORUM consistency
    let result = replicator
        .replicate_write(shard_id, batch.clone(), ConsistencyLevel::Quorum)
        .await;
    assert!(result.is_ok());

    println!("✓ Write replication test passed");
}

#[tokio::test]
async fn test_join_optimization() {
    let optimizer = JoinOptimizer::new(100 * 1024 * 1024, 256);

    // Test broadcast JOIN (small x large)
    let small_stats = TableStatistics {
        row_count: 1000,
        estimated_size_bytes: 10 * 1024 * 1024, // 10MB
    };

    let large_stats = TableStatistics {
        row_count: 10_000_000,
        estimated_size_bytes: 1024 * 1024 * 1024, // 1GB
    };

    let strategy = optimizer
        .optimize_join("small_table", "large_table", &small_stats, &large_stats)
        .unwrap();

    assert!(matches!(strategy, JoinStrategy::Broadcast { .. }));

    // Test shuffle JOIN (large x large)
    let large_stats1 = TableStatistics {
        row_count: 5_000_000,
        estimated_size_bytes: 512 * 1024 * 1024, // 512MB
    };

    let large_stats2 = TableStatistics {
        row_count: 10_000_000,
        estimated_size_bytes: 1024 * 1024 * 1024, // 1GB
    };

    let strategy = optimizer
        .optimize_join("table1", "table2", &large_stats1, &large_stats2)
        .unwrap();

    assert!(matches!(strategy, JoinStrategy::Shuffle { .. }));

    println!("✓ JOIN optimization test passed");
}

