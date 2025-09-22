//! Health monitoring for cluster nodes

use crate::{
    ClusterConfig, Component, Node, NodeId, NodeState, Result,
    error::{HealthError, NetworkError},
    membership::MembershipManager,
    gossip::GossipMessage,
};
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, mpsc};
use tokio::time::interval;

/// Health monitoring system for cluster nodes
#[derive(Debug)]
pub struct HealthMonitor {
    config: ClusterConfig,
    membership: Arc<RwLock<MembershipManager>>,
    health_status: Arc<DashMap<NodeId, HealthStatus>>,
    running: Arc<RwLock<bool>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    client: reqwest::Client,
}

impl HealthMonitor {
    /// Create a new health monitor
    pub async fn new(
        config: ClusterConfig,
        membership: Arc<RwLock<MembershipManager>>,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.health.health_check_timeout)
            .build()
            .map_err(NetworkError::RequestFailed)?;
        
        let monitor = Self {
            config,
            membership,
            health_status: Arc::new(DashMap::new()),
            running: Arc::new(RwLock::new(false)),
            shutdown_tx: None,
            client,
        };
        
        Ok(monitor)
    }
    
    /// Start the health monitoring background task
    async fn start_health_loop(&self) -> Result<()> {
        let mut health_interval = interval(self.config.health.health_check_interval);
        let (_shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        
        let membership = Arc::clone(&self.membership);
        let health_status = Arc::clone(&self.health_status);
        let config = self.config.clone();
        let client = self.client.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = health_interval.tick() => {
                        if let Err(e) = Self::health_check_round(
                            &membership,
                            &health_status,
                            &config,
                            &client,
                        ).await {
                            observability_deps::tracing::error!(
                                error = %e,
                                "Health check round failed"
                            );
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        observability_deps::tracing::info!("Health monitor shutting down");
                        break;
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Perform one round of health checks
    async fn health_check_round(
        membership: &Arc<RwLock<MembershipManager>>,
        health_status: &Arc<DashMap<NodeId, HealthStatus>>,
        config: &ClusterConfig,
        client: &reqwest::Client,
    ) -> Result<()> {
        let membership_guard = membership.read().await;
        let all_nodes = membership_guard.get_all_nodes();
        drop(membership_guard);
        
        // Check health of all nodes except ourselves
        for node in all_nodes {
            if node.id == config.node_id {
                continue;
            }
            
            let health_result = Self::check_node_health(client, &node, config).await;
            let mut current_status = health_status
                .entry(node.id.clone())
                .or_insert_with(|| HealthStatus::new(node.id.clone()));
            
            match health_result {
                Ok(()) => {
                    current_status.record_success();
                    
                    // If node was unhealthy and now recovered
                    if current_status.should_mark_healthy(config) {
                        let membership_guard = membership.write().await;
                        if let Some(mut recovered_node) = membership_guard.get_node(&node.id) {
                            let node_id = recovered_node.id.clone();
                            recovered_node.mark_active();
                            if let Err(e) = membership_guard.update_node(recovered_node).await {
                                observability_deps::tracing::warn!(
                                    node_id = %node_id,
                                    error = %e,
                                    "Failed to mark node as healthy"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    current_status.record_failure();
                    
                    observability_deps::tracing::debug!(
                        node_id = %node.id,
                        error = %e,
                        "Health check failed"
                    );
                    
                    // If node should be marked as unhealthy
                    if current_status.should_mark_unhealthy(config) {
                        let membership_guard = membership.write().await;
                        if let Some(mut unhealthy_node) = membership_guard.get_node(&node.id) {
                            let node_id = unhealthy_node.id.clone();
                            match unhealthy_node.status.state {
                                NodeState::Active => {
                                    unhealthy_node.mark_suspected();
                                    observability_deps::tracing::warn!(
                                        node_id = %node_id,
                                        "Node marked as suspected due to health check failures"
                                    );
                                }
                                NodeState::Suspected => {
                                    unhealthy_node.mark_down();
                                    observability_deps::tracing::error!(
                                        node_id = %node_id,
                                        "Node marked as down due to continued health check failures"
                                    );
                                }
                                _ => {}
                            }

                            if let Err(e) = membership_guard.update_node(unhealthy_node).await {
                                observability_deps::tracing::warn!(
                                    node_id = %node_id,
                                    error = %e,
                                    "Failed to update node health status"
                                );
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Check the health of a specific node
    async fn check_node_health(
        client: &reqwest::Client,
        node: &Node,
        config: &ClusterConfig,
    ) -> Result<()> {
        let ping_message = GossipMessage::Ping {
            sender: config.node_id.clone(),
            timestamp: current_timestamp(),
        };
        
        let url = format!("http://{}/cluster/health", node.addr);
        let body = serde_json::to_vec(&ping_message)?;
        
        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .timeout(config.health.health_check_timeout)
            .send()
            .await
            .map_err(|e| HealthError::CheckFailed(e.to_string()))?;
        
        if !response.status().is_success() {
            return Err(HealthError::CheckFailed(
                format!("HTTP {}", response.status())
            ).into());
        }
        
        // Verify the response is a valid pong
        let response_body = response.bytes().await
            .map_err(|e| HealthError::CheckFailed(e.to_string()))?;
        
        let pong: GossipMessage = serde_json::from_slice(&response_body)
            .map_err(|e| HealthError::CheckFailed(e.to_string()))?;
        
        match pong {
            GossipMessage::Pong { .. } => Ok(()),
            _ => Err(HealthError::CheckFailed(
                "Invalid response to health check".to_string()
            ).into()),
        }
    }
    
    /// Handle incoming health check requests
    pub async fn handle_health_check(&self, message: GossipMessage) -> Result<GossipMessage> {
        match message {
            GossipMessage::Ping { sender, .. } => {
                // Update the sender's last seen time
                let membership = self.membership.read().await;
                if let Some(mut node) = membership.get_node(&sender) {
                    node.status.update_last_seen();
                    if let Err(e) = membership.update_node(node).await {
                        observability_deps::tracing::debug!(
                            node_id = %sender,
                            error = %e,
                            "Failed to update node last seen time"
                        );
                    }
                }
                
                Ok(GossipMessage::Pong {
                    sender: self.config.node_id.clone(),
                    timestamp: current_timestamp(),
                })
            }
            _ => Err(HealthError::CheckFailed(
                "Invalid health check message".to_string()
            ).into()),
        }
    }
    
    /// Get the health status of a specific node
    pub fn get_node_health(&self, node_id: &NodeId) -> Option<HealthStatus> {
        self.health_status.get(node_id).map(|entry| entry.clone())
    }
    
    /// Clean up health status for nodes that are no longer in the cluster
    pub async fn cleanup_health_status(&self) -> Result<()> {
        let membership = self.membership.read().await;
        let current_nodes: std::collections::HashSet<_> = membership
            .get_all_nodes()
            .into_iter()
            .map(|node| node.id)
            .collect();
        
        // Remove health status for nodes not in current membership
        self.health_status.retain(|node_id, _| current_nodes.contains(node_id));
        
        Ok(())
    }
}

#[async_trait]
impl Component for HealthMonitor {
    async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        
        *running = true;
        self.start_health_loop().await?;
        
        observability_deps::tracing::info!("Health monitor started");
        Ok(())
    }
    
    async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        
        *running = false;
        
        // TODO: Send shutdown signal
        
        observability_deps::tracing::info!("Health monitor stopped");
        Ok(())
    }
}

/// Health status tracking for a node
#[derive(Debug, Clone)]
pub struct HealthStatus {
    node_id: NodeId,
    consecutive_failures: usize,
    consecutive_successes: usize,
    last_check: u64,
    total_checks: usize,
    total_failures: usize,
}

impl HealthStatus {
    /// Create a new health status tracker
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            consecutive_failures: 0,
            consecutive_successes: 0,
            last_check: current_timestamp(),
            total_checks: 0,
            total_failures: 0,
        }
    }
    
    /// Record a successful health check
    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.consecutive_successes += 1;
        self.last_check = current_timestamp();
        self.total_checks += 1;
    }
    
    /// Record a failed health check
    pub fn record_failure(&mut self) {
        self.consecutive_successes = 0;
        self.consecutive_failures += 1;
        self.last_check = current_timestamp();
        self.total_checks += 1;
        self.total_failures += 1;
    }
    
    /// Check if the node should be marked as unhealthy
    pub fn should_mark_unhealthy(&self, config: &ClusterConfig) -> bool {
        self.consecutive_failures >= config.health.failure_threshold
    }
    
    /// Check if the node should be marked as healthy
    pub fn should_mark_healthy(&self, config: &ClusterConfig) -> bool {
        self.consecutive_successes >= config.health.recovery_threshold
    }
    
    /// Get the failure rate
    pub fn failure_rate(&self) -> f64 {
        if self.total_checks == 0 {
            0.0
        } else {
            self.total_failures as f64 / self.total_checks as f64
        }
    }
}

/// Get current timestamp in seconds since Unix epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
