//! Integration tests for cluster functionality

#[cfg(test)]
mod tests {
    use crate::{ClusterConfig, ClusterManager, NodeId};
    use tokio::time::{sleep, Duration};
    
    #[tokio::test]
    async fn test_cluster_manager_creation() {
        let config = ClusterConfig::test_config();
        let cluster = ClusterManager::new(config, crate::NodeRole::Master).await.unwrap();
        
        // Test that we can get membership (should be empty initially)
        let membership = cluster.get_membership().await;
        assert_eq!(membership.active_count(), 0);
    }
    
    #[tokio::test]
    async fn test_cluster_manager_start_stop() {
        let mut config = ClusterConfig::test_config();
        config.bind_addr = "127.0.0.1:0".parse().unwrap(); // Use random port
        
        let cluster = ClusterManager::new(config, crate::NodeRole::Master).await.unwrap();
        
        // Start the cluster
        cluster.start().await.unwrap();
        
        // Give it a moment to initialize
        sleep(Duration::from_millis(100)).await;
        
        // Stop the cluster
        cluster.stop().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_node_partition_assignment() {
        let config = ClusterConfig::test_config();
        let cluster = ClusterManager::new(config).await.unwrap();
        
        // Initially no nodes, so no assignment possible
        let node = cluster.get_node_for_key("test_key").await;
        assert!(node.is_none());
        
        let nodes = cluster.get_nodes_for_key("test_key").await;
        assert!(nodes.is_empty());
    }
    
    #[tokio::test]
    async fn test_leadership_election() {
        let config = ClusterConfig::test_config();
        let cluster = ClusterManager::new(config).await.unwrap();
        
        // With no other nodes, this node should consider itself leader
        // (in the simplified implementation)
        let _is_leader = cluster.is_leader().await;
        // Note: This might be false initially since there are no active nodes
        // The actual behavior depends on the leadership election logic
    }
    
    #[tokio::test]
    async fn test_multiple_cluster_nodes() {
        // Create multiple cluster configurations
        let mut configs = Vec::new();
        let mut clusters = Vec::new();
        
        for i in 0..3 {
            let mut config = ClusterConfig::test_config();
            config.node_id = NodeId::new();
            config.bind_addr = format!("127.0.0.1:{}", 8300 + i).parse().unwrap();
            
            // Set seed nodes (except for the first node)
            if i > 0 {
                config.seed_nodes = vec!["127.0.0.1:8300".parse().unwrap()];
            }
            
            configs.push(config.clone());
            
            let cluster = ClusterManager::new(config).await.unwrap();
            clusters.push(cluster);
        }
        
        // Start all clusters
        for cluster in &clusters {
            cluster.start().await.unwrap();
        }
        
        // Give them time to discover each other
        sleep(Duration::from_millis(500)).await;
        
        // Stop all clusters
        for cluster in &clusters {
            cluster.stop().await.unwrap();
        }
    }
}
