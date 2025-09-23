//! Gossip protocol implementation for node discovery and information dissemination

use crate::{
    ClusterConfig, Component, Node, NodeId, Result,
    error::{ClusterError, NetworkError},
    membership::MembershipManager,
};
use async_trait::async_trait;
use futures::future::join_all;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, mpsc};
use tokio::time::interval;

/// Gossip protocol implementation for cluster communication
#[derive(Debug)]
pub struct GossipProtocol {
    config: ClusterConfig,
    membership: Arc<RwLock<MembershipManager>>,
    local_node: Arc<RwLock<Node>>,
    running: Arc<RwLock<bool>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    client: reqwest::Client,
}

impl GossipProtocol {
    /// Create a new gossip protocol instance
    pub async fn new(
        config: ClusterConfig,
        membership: Arc<RwLock<MembershipManager>>,
    ) -> Result<Self> {
        let local_node = if let Some(http_endpoint) = &config.http_endpoint {
            Node::new_with_http(config.node_id.clone(), config.bind_addr, http_endpoint.clone())
        } else {
            Node::new(config.node_id.clone(), config.bind_addr)
        };
        
        let client = reqwest::Client::builder()
            .timeout(config.gossip.gossip_timeout)
            .build()
            .map_err(NetworkError::RequestFailed)?;
        
        Ok(Self {
            config,
            membership,
            local_node: Arc::new(RwLock::new(local_node)),
            running: Arc::new(RwLock::new(false)),
            shutdown_tx: None,
            client,
        })
    }
    
    /// Connect to a seed node to join the cluster
    pub async fn connect_to_seed(&self, seed_addr: SocketAddr) -> Result<()> {
        let local_node = self.local_node.read().await.clone();
        
        // Send join request to seed node
        let join_request = GossipMessage::JoinRequest {
            node: local_node.clone(),
            timestamp: current_timestamp(),
        };
        
        let response = self.send_message(seed_addr, &join_request).await?;
        
        match response {
            GossipMessage::JoinResponse { nodes, .. } => {
                // Add all nodes from the response to our membership
                let membership = self.membership.write().await;
                for node in nodes {
                    if node.id != local_node.id {
                        if let Err(e) = membership.add_node(node.clone()).await {
                            observability_deps::tracing::warn!(
                                node_id = %node.id,
                                error = %e,
                                "Failed to add node from join response"
                            );
                        }
                    }
                }
                
                observability_deps::tracing::info!(
                    seed_addr = %seed_addr,
                    "Successfully joined cluster via seed node"
                );
                
                Ok(())
            }
            _ => Err(ClusterError::Generic(
                "Unexpected response to join request".to_string()
            )),
        }
    }
    
    /// Send a gossip message to a specific node
    async fn send_message(
        &self,
        addr: SocketAddr,
        message: &GossipMessage,
    ) -> Result<GossipMessage> {
        // Convert gossip bind address to HTTP endpoint
        // Assume HTTP port is gossip port - 10 (e.g., 8191 -> 8181)
        let http_port = addr.port() - 10;
        let http_addr = SocketAddr::new(addr.ip(), http_port);
        let url = format!("http://{}/cluster/gossip", http_addr);
        let body = serde_json::to_vec(message)?;
        
        if body.len() > self.config.gossip.max_message_size {
            return Err(NetworkError::MessageTooLarge { size: body.len() }.into());
        }
        
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(NetworkError::RequestFailed)?;
        
        if !response.status().is_success() {
            return Err(NetworkError::RequestFailed(
                reqwest::Error::from(response.error_for_status().unwrap_err())
            ).into());
        }
        
        let response_body = response.bytes().await
            .map_err(NetworkError::RequestFailed)?;
        
        let message: GossipMessage = serde_json::from_slice(&response_body)?;
        Ok(message)
    }
    
    /// Start the gossip protocol background tasks
    async fn start_gossip_loop(&self) -> Result<()> {
        let mut gossip_interval = interval(self.config.gossip.gossip_interval);
        let (_shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        
        // Store shutdown sender for later use
        // Note: In a real implementation, we'd need to handle this more carefully
        // as we can't modify self here. This is a simplified version.
        
        let membership = Arc::clone(&self.membership);
        let local_node = Arc::clone(&self.local_node);
        let config = self.config.clone();
        let client = self.client.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = gossip_interval.tick() => {
                        if let Err(e) = Self::gossip_round(
                            &membership,
                            &local_node,
                            &config,
                            &client,
                        ).await {
                            observability_deps::tracing::error!(
                                error = %e,
                                "Gossip round failed"
                            );
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        observability_deps::tracing::info!("Gossip protocol shutting down");
                        break;
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Perform one round of gossip with random nodes
    async fn gossip_round(
        membership: &Arc<RwLock<MembershipManager>>,
        local_node: &Arc<RwLock<Node>>,
        config: &ClusterConfig,
        client: &reqwest::Client,
    ) -> Result<()> {
        let membership_guard = membership.read().await;
        let active_nodes = membership_guard.get_active_nodes();
        drop(membership_guard);

        if active_nodes.is_empty() {
            return Ok(());
        }

        // Select random nodes to gossip with
        let gossip_targets: Vec<_> = {
            let mut rng = rand::thread_rng();
            active_nodes
                .choose_multiple(&mut rng, config.gossip.gossip_fanout)
                .cloned()
                .collect()
        };

        let local_node_clone = local_node.read().await.clone();
        
        // Send gossip messages to selected nodes
        let gossip_futures = gossip_targets.into_iter().map(|target| {
            let local_node = local_node_clone.clone();
            let client = client.clone();
            let config = config.clone();
            
            async move {
                let gossip_msg = GossipMessage::Gossip {
                    sender: local_node,
                    timestamp: current_timestamp(),
                    membership_version: 0, // TODO: Get actual version
                };
                
                Self::send_gossip_message(&client, target.addr, &gossip_msg, &config).await
            }
        });
        
        // Wait for all gossip messages to complete
        let results = join_all(gossip_futures).await;
        
        // Process responses
        for result in results {
            if let Err(e) = result {
                observability_deps::tracing::debug!(
                    error = %e,
                    "Gossip message failed"
                );
            }
        }
        
        Ok(())
    }
    
    /// Send a single gossip message
    async fn send_gossip_message(
        client: &reqwest::Client,
        addr: SocketAddr,
        message: &GossipMessage,
        config: &ClusterConfig,
    ) -> Result<()> {
        // Convert gossip bind address to HTTP endpoint
        // Assume HTTP port is gossip port - 10 (e.g., 8191 -> 8181)
        let http_port = addr.port() - 10;
        let http_addr = SocketAddr::new(addr.ip(), http_port);
        let url = format!("http://{}/cluster/gossip", http_addr);
        let body = serde_json::to_vec(message)?;
        
        if body.len() > config.gossip.max_message_size {
            return Err(NetworkError::MessageTooLarge { size: body.len() }.into());
        }
        
        let _response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .timeout(config.gossip.gossip_timeout)
            .send()
            .await
            .map_err(NetworkError::RequestFailed)?;
        
        Ok(())
    }
    
    /// Handle incoming gossip messages
    pub async fn handle_message(&self, message: GossipMessage) -> Result<GossipMessage> {
        match message {
            GossipMessage::JoinRequest { node, .. } => {
                // Add the joining node to our membership
                let membership = self.membership.write().await;
                if let Err(e) = membership.add_node(node.clone()).await {
                    observability_deps::tracing::warn!(
                        node_id = %node.id,
                        error = %e,
                        "Failed to add joining node"
                    );
                }
                
                // Return current membership
                let all_nodes = membership.get_all_nodes();
                Ok(GossipMessage::JoinResponse {
                    nodes: all_nodes,
                    timestamp: current_timestamp(),
                })
            }
            
            GossipMessage::Gossip { sender, .. } => {
                // Update sender's information in our membership
                let membership = self.membership.write().await;
                if let Err(e) = membership.update_node(sender).await {
                    observability_deps::tracing::debug!(
                        error = %e,
                        "Failed to update node from gossip"
                    );
                }
                
                Ok(GossipMessage::GossipAck {
                    timestamp: current_timestamp(),
                })
            }
            
            _ => Ok(GossipMessage::GossipAck {
                timestamp: current_timestamp(),
            }),
        }
    }
}

#[async_trait]
impl Component for GossipProtocol {
    async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        
        *running = true;
        self.start_gossip_loop().await?;
        
        observability_deps::tracing::info!("Gossip protocol started");
        Ok(())
    }
    
    async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        
        *running = false;
        
        // TODO: Send shutdown signal
        
        observability_deps::tracing::info!("Gossip protocol stopped");
        Ok(())
    }
}

/// Messages exchanged in the gossip protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Request to join the cluster
    JoinRequest {
        node: Node,
        timestamp: u64,
    },
    /// Response to join request with current membership
    JoinResponse {
        nodes: Vec<Node>,
        timestamp: u64,
    },
    /// Regular gossip message with node information
    Gossip {
        sender: Node,
        timestamp: u64,
        membership_version: u64,
    },
    /// Acknowledgment of gossip message
    GossipAck {
        timestamp: u64,
    },
    /// Ping message for health checking
    Ping {
        sender: NodeId,
        timestamp: u64,
    },
    /// Pong response to ping
    Pong {
        sender: NodeId,
        timestamp: u64,
    },
}

/// Get current timestamp in seconds since Unix epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
