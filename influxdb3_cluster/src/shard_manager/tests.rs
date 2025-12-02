//! Tests for shard manager

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::meta_store::InMemoryMetaStore;
    use crate::node_registry::NodeRegistry;
    use crate::types::{NodeCapacity, NodeInfo, NodeRole, NodeStatus, ShardRange};
use influxdb3_id::DbId;
    use std::sync::Arc;

    fn create_test_node(id: u64) -> NodeInfo {
        NodeInfo {
            node_id: NodeId::new(id),
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
    async fn test_route_write() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let shard_manager = ShardManager::new(16, 3, meta_store);

        // Test that same series key always routes to same shard
        let shard1 = shard_manager.route_write(
            "mydb",
            "cpu",
            &[("host", "server1"), ("region", "us-east")],
        );

        let shard2 = shard_manager.route_write(
            "mydb",
            "cpu",
            &[("host", "server1"), ("region", "us-east")],
        );

        assert_eq!(shard1, shard2);

        // Different series key should route to different shard (most likely)
        let shard3 = shard_manager.route_write(
            "mydb",
            "cpu",
            &[("host", "server2"), ("region", "us-west")],
        );

        // Note: There's a small chance they could hash to the same shard
        // but with 16 shards, it's unlikely
        println!("shard1: {:?}, shard3: {:?}", shard1, shard3);
    }

    #[tokio::test]
    async fn test_create_shard() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let shard_manager = ShardManager::new(16, 3, meta_store.clone());
        let node_registry = NodeRegistry::new(meta_store);

        // Register enough nodes for replication
        for i in 1..=3 {
            let node = create_test_node(i);
            node_registry.register_node(node).await.unwrap();
        }

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

        // Verify shard was created
        let shard = shard_manager.get_shard(shard_id).await.unwrap();
        assert_eq!(shard.database_id, db_id);
        assert_eq!(shard.replicas.len(), 3);
        assert_eq!(shard.status, ShardStatus::Active);

        // Verify one replica is leader
        let leaders: Vec<_> = shard
            .replicas
            .iter()
            .filter(|r| r.role == ReplicaRole::Leader)
            .collect();
        assert_eq!(leaders.len(), 1);
    }

    #[tokio::test]
    async fn test_get_shard_leader() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let shard_manager = ShardManager::new(16, 3, meta_store.clone());
        let node_registry = NodeRegistry::new(meta_store);

        // Register nodes
        for i in 1..=3 {
            let node = create_test_node(i);
            node_registry.register_node(node).await.unwrap();
        }

        // Create shard
        let db_id = DbId::new(1);
        let shard_range = ShardRange::Hash {
            start: 0,
            end: 1000,
        };
        let shard_id = shard_manager
            .create_shard(db_id, shard_range, &node_registry)
            .await
            .unwrap();

        // Get leader
        let leader_id = shard_manager.get_shard_leader(shard_id).await.unwrap();
        assert!(leader_id.as_u64() >= 1 && leader_id.as_u64() <= 3);
    }

    #[tokio::test]
    async fn test_get_shard_replicas() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let shard_manager = ShardManager::new(16, 3, meta_store.clone());
        let node_registry = NodeRegistry::new(meta_store);

        // Register nodes
        for i in 1..=3 {
            let node = create_test_node(i);
            node_registry.register_node(node).await.unwrap();
        }

        // Create shard
        let db_id = DbId::new(1);
        let shard_range = ShardRange::Hash {
            start: 0,
            end: 1000,
        };
        let shard_id = shard_manager
            .create_shard(db_id, shard_range, &node_registry)
            .await
            .unwrap();

        // Get replicas
        let replicas = shard_manager.get_shard_replicas(shard_id).await.unwrap();
        assert_eq!(replicas.len(), 3);
    }
}

