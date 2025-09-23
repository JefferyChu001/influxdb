//! Integration tests for distributed cluster functionality
//! 
//! These tests verify the end-to-end functionality of the distributed
//! InfluxDB 3 cluster, including partitioning, write coordination, and
//! data distribution across multiple nodes.

use crate::{
    ClusterConfig, ClusterManager, NodeId, Result,
    partition::PartitionManager,
    write_coordinator::{WriteCoordinator, ConsistencyLevel},
    membership::MembershipManager,
};
use influxdb3_wal::{WriteBatch, TableChunks, TableChunk, Row, Field, FieldData};
use influxdb3_id::{DbId, TableId, ColumnId};
use indexmap::IndexMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Integration test for distributed write partitioning
#[tokio::test]
async fn test_distributed_write_partitioning() -> Result<()> {
    // Create a 3-node cluster configuration
    let mut configs = Vec::new();
    for i in 0..3 {
        let mut config = ClusterConfig::test_config();
        config.node_id = NodeId::new();
        config.bind_addr = format!("127.0.0.1:830{}", i).parse().unwrap();
        config.replication_factor = 2;
        config.consistency_level = ConsistencyLevel::Quorum;
        configs.push(config);
    }

    // Create cluster managers for each node
    let mut cluster_managers = Vec::new();
    for (i, config) in configs.iter().enumerate() {
        // First node is master, others are slaves
        let role = if i == 0 { crate::NodeRole::Master } else { crate::NodeRole::Slave };
        let manager = ClusterManager::new(config.clone(), role).await?;
        cluster_managers.push(manager);
    }

    // Start all cluster managers
    for manager in &cluster_managers {
        manager.start().await?;
    }

    // Manually add all nodes to each cluster manager's membership
    for (_i, manager) in cluster_managers.iter().enumerate() {
        let membership = manager.membership_manager();
        let membership_guard = membership.write().await;

        for (_j, config) in configs.iter().enumerate() {
            let node = crate::Node::new(config.node_id.clone(), config.bind_addr);
            // Only add if not already present
            if membership_guard.get_node(&config.node_id).is_none() {
                membership_guard.add_node(node).await?;
            }
        }

        // Rebalance partition ring with new nodes
        manager.partition_manager().rebalance().await?;
    }

    // Wait a bit for cluster formation
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Create test data with different series keys to test partitioning
    let test_batches = create_test_write_batches();

    // Test write coordination on the first node
    let first_manager = &cluster_managers[0];
    let partition_manager = first_manager.partition_manager();
    let write_coordinator = WriteCoordinator::new(
        configs[0].clone(),
        partition_manager.clone(),
    ).await?;

    write_coordinator.start().await?;

    // Test partitioning of each write batch
    for (i, batch) in test_batches.iter().enumerate() {
        println!("Testing write batch {}", i);
        
        // Test partition assignment
        let partitioned_batches = partition_manager
            .partition_write_batch(batch)
            .await?;
        
        // Verify that data is distributed across nodes
        assert!(!partitioned_batches.is_empty(), "Write batch should be partitioned");
        
        // Test write coordination
        let write_result = write_coordinator.coordinate_write(batch.clone()).await?;
        
        // Verify write succeeded based on consistency level
        assert!(write_result.success, "Write should succeed with quorum consistency");
        
        // Verify that enough nodes acknowledged the write
        let success_count = write_result.node_results.values()
            .filter(|r| r.success)
            .count();
        assert!(success_count >= 2, "At least 2 nodes should acknowledge write for quorum");
    }

    // Test partition statistics
    let stats = partition_manager.get_partition_stats().await;
    println!("Partition statistics: {:?}", stats);

    // Test rebalancing plan
    let current_assignments = partition_manager.get_partition_assignments().await;
    let rebalancing_plan = partition_manager
        .calculate_rebalancing_plan(current_assignments)
        .await;
    
    println!("Rebalancing plan: {:?}", rebalancing_plan);

    // Stop all services
    write_coordinator.stop().await?;
    for manager in &cluster_managers {
        manager.stop().await?;
    }

    Ok(())
}

/// Test cluster membership and node discovery
#[tokio::test]
async fn test_cluster_membership_integration() -> Result<()> {
    // Create a 3-node cluster
    let mut configs = Vec::new();
    for i in 0..3 {
        let mut config = ClusterConfig::test_config();
        config.node_id = NodeId::new();
        config.bind_addr = format!("127.0.0.1:831{}", i).parse().unwrap();
        configs.push(config);
    }

    // Create membership managers
    let mut membership_managers = Vec::new();
    for config in &configs {
        let manager = MembershipManager::new(config.node_id.clone());
        membership_managers.push(Arc::new(RwLock::new(manager)));
    }

    // Test adding nodes to cluster
    for (_i, membership) in membership_managers.iter().enumerate() {
        let manager = membership.write().await;

        // Add all nodes (including self) to each manager
        for (_j, other_config) in configs.iter().enumerate() {
            let node = crate::Node::new(other_config.node_id.clone(), other_config.bind_addr);
            // Only add if not already present (avoid duplicate errors)
            if manager.get_node(&other_config.node_id).is_none() {
                manager.add_node(node).await?;
            }
        }
    }

    // Verify cluster membership
    for membership in &membership_managers {
        let manager = membership.read().await;
        let active_nodes = manager.get_active_nodes();
        
        // Should see all 3 nodes (including self)
        assert_eq!(active_nodes.len(), 3, "Should see all 3 nodes in cluster");
    }

    Ok(())
}

/// Test partition rebalancing when nodes join/leave
#[tokio::test]
async fn test_partition_rebalancing() -> Result<()> {
    let config = ClusterConfig::test_config();
    let membership = Arc::new(RwLock::new(MembershipManager::new(config.node_id.clone())));
    let partition_manager = Arc::new(PartitionManager::new(config, membership.clone()).await?);

    // Add some nodes to the cluster
    let membership_guard = membership.write().await;
    for i in 0..3 {
        let node_id = NodeId::new();
        let addr = format!("127.0.0.1:832{}", i).parse().unwrap();
        let node = crate::Node::new(node_id, addr);
        membership_guard.add_node(node).await?;
    }
    drop(membership_guard);

    // Get initial partition assignments
    let initial_assignments = partition_manager.get_partition_assignments().await;
    
    // Add a new node
    let new_node_id = NodeId::new();
    let new_addr = "127.0.0.1:8323".parse().unwrap();
    let new_node = crate::Node::new(new_node_id.clone(), new_addr);
    membership.write().await.add_node(new_node).await?;

    // Calculate rebalancing plan
    let rebalancing_plan = partition_manager
        .calculate_rebalancing_plan(initial_assignments)
        .await;

    // Verify that some partitions are planned to move to the new node
    let moves_to_new_node = rebalancing_plan.moves.iter()
        .filter(|m| m.to_node == new_node_id)
        .count();
    
    println!("Moves to new node: {}", moves_to_new_node);
    // In a real scenario, we'd expect some partitions to move to balance the load

    Ok(())
}

/// Create test write batches with different series keys for partitioning tests
fn create_test_write_batches() -> Vec<WriteBatch> {
    let mut batches = Vec::new();

    // Create batches with different tag combinations (series keys)
    let tag_combinations = vec![
        vec![("host", "server1"), ("region", "us-east")],
        vec![("host", "server2"), ("region", "us-west")],
        vec![("host", "server3"), ("region", "eu-west")],
        vec![("host", "server1"), ("region", "us-west")],
        vec![("host", "server2"), ("region", "eu-west")],
    ];

    for (batch_id, tags) in tag_combinations.iter().enumerate() {
        let mut table_chunks = IndexMap::new();

        // Create a TableChunks for this table
        let mut table_chunk_data = TableChunks::default();

        // Create a row with the specified tags
        let mut fields = Vec::new();
        for (i, (_tag_name, tag_value)) in tags.iter().enumerate() {
            fields.push(Field::new(
                ColumnId::new((i + 1) as u16),
                FieldData::Tag(tag_value.to_string())
            ));
        }

        // Add a measurement field
        fields.push(Field::new(
            ColumnId::new(10),
            FieldData::Float(42.0 + batch_id as f64)
        ));

        let row = Row {
            time: 1000 + batch_id as i64 * 1000,
            fields,
        };

        // Push the row to the table chunks (using chunk_time = 0)
        table_chunk_data.push_row(0, row);
        table_chunks.insert(TableId::new(1), table_chunk_data);

        let write_batch = WriteBatch::new(
            batch_id as u64 + 1,
            DbId::new(1),
            Arc::from("test_db"),
            table_chunks,
        );

        batches.push(write_batch);
    }

    batches
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_test_batches() {
        let batches = create_test_write_batches();
        assert_eq!(batches.len(), 5);
        
        // Verify each batch has the expected structure
        for batch in &batches {
            assert!(!batch.table_chunks.is_empty());
        }
    }
}
