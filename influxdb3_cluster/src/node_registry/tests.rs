//! Tests for node registry

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::meta_store::InMemoryMetaStore;
    use crate::types::{NodeCapacity, NodeRole, NodeStatus};
    use std::sync::Arc;
    use std::time::Duration;

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
    async fn test_register_node() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let registry = NodeRegistry::new(meta_store);

        let node = create_test_node(1);
        let result = registry.register_node(node.clone()).await;
        assert!(result.is_ok());

        // Verify node was registered
        let retrieved = registry.get_node(node.node_id).await;
        assert!(retrieved.is_ok());
        let retrieved_node = retrieved.unwrap();
        assert_eq!(retrieved_node.node_id, node.node_id);
        assert_eq!(retrieved_node.address, node.address);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let registry = NodeRegistry::new(meta_store);

        let node = create_test_node(1);
        registry.register_node(node.clone()).await.unwrap();

        // Sleep a bit to ensure timestamp changes
        tokio::time::sleep(Duration::from_millis(10)).await;

        let old_heartbeat = registry
            .get_node(node.node_id)
            .await
            .unwrap()
            .last_heartbeat_nanos;

        // Send heartbeat
        registry.heartbeat(node.node_id).await.unwrap();

        let new_heartbeat = registry
            .get_node(node.node_id)
            .await
            .unwrap()
            .last_heartbeat_nanos;

        assert!(new_heartbeat > old_heartbeat);
    }

    #[tokio::test]
    async fn test_get_active_nodes() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let registry = NodeRegistry::new(meta_store);

        // Register multiple nodes
        for i in 1..=3 {
            let node = create_test_node(i);
            registry.register_node(node).await.unwrap();
        }

        let active_nodes = registry.get_active_nodes(None).await;
        assert_eq!(active_nodes.len(), 3);

        // Filter by role
        let data_nodes = registry.get_active_nodes(Some(NodeRole::DataNode)).await;
        assert_eq!(data_nodes.len(), 3);

        let coordinators = registry.get_active_nodes(Some(NodeRole::Coordinator)).await;
        assert_eq!(coordinators.len(), 0);
    }

    #[tokio::test]
    async fn test_detect_failures() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let registry = NodeRegistry::new(meta_store);

        // Register a node with old heartbeat
        let mut node = create_test_node(1);
        node.last_heartbeat_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            - Duration::from_secs(60).as_nanos() as i64;

        registry.register_node(node.clone()).await.unwrap();

        // Detect failures with 30 second timeout
        let failed_nodes = registry.detect_failures(Duration::from_secs(30)).await;
        assert_eq!(failed_nodes.len(), 1);
        assert_eq!(failed_nodes[0], node.node_id);
    }

    #[tokio::test]
    async fn test_remove_node() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let registry = NodeRegistry::new(meta_store);

        let node = create_test_node(1);
        registry.register_node(node.clone()).await.unwrap();

        // Remove the node
        registry.remove_node(node.node_id).await.unwrap();

        // Verify node was removed
        let result = registry.get_node(node.node_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_nodes() {
        let meta_store = Arc::new(InMemoryMetaStore::new());
        let registry = NodeRegistry::new(meta_store);

        // Register multiple nodes
        for i in 1..=5 {
            let node = create_test_node(i);
            registry.register_node(node).await.unwrap();
        }

        let all_nodes = registry.list_nodes().await;
        assert_eq!(all_nodes.len(), 5);
    }
}

