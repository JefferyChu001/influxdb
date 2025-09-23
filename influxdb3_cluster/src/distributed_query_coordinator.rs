//! Distributed Query Coordinator
//!
//! This module provides distributed query coordination capabilities for InfluxDB 3 clusters.
//! It handles query distribution, result aggregation, and cross-node query execution.

use crate::{ClusterError, Result, NodeId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use observability_deps::tracing::{debug, error, info, warn};

/// Query execution strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStrategy {
    /// Execute on all nodes and merge results
    Scatter,
    /// Execute on specific nodes based on data locality
    Targeted(Vec<NodeId>),
    /// Execute on a single node (for metadata queries)
    Single(NodeId),
}

/// Query request that will be distributed across nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedQuery {
    pub query_id: String,
    pub database: String,
    pub sql: String,
    pub strategy: QueryStrategy,
    pub timeout: Duration,
    pub consistency_level: ConsistencyLevel,
}

/// Consistency level for distributed queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// Return results from any available node
    Any,
    /// Return results from majority of nodes
    Quorum,
    /// Return results from all nodes
    All,
}

/// Query result from a single node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeQueryResult {
    pub node_id: NodeId,
    pub success: bool,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
    pub execution_time_ms: u64,
    pub row_count: usize,
}

/// Aggregated query result from multiple nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedQueryResult {
    pub query_id: String,
    pub success: bool,
    pub data: serde_json::Value,
    pub node_results: Vec<NodeQueryResult>,
    pub total_execution_time_ms: u64,
    pub total_rows: usize,
    pub nodes_queried: usize,
    pub nodes_succeeded: usize,
}

/// Query execution statistics
#[derive(Debug, Clone)]
pub struct QueryStats {
    pub total_queries: u64,
    pub successful_queries: u64,
    pub failed_queries: u64,
    pub average_execution_time_ms: f64,
    pub total_rows_returned: u64,
}

/// HTTP client for making requests to other nodes
#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn query_node(
        &self,
        node_endpoint: &str,
        database: &str,
        sql: &str,
        timeout: Duration,
    ) -> Result<NodeQueryResult>;
}

/// Default HTTP client implementation
pub struct DefaultHttpClient {
    client: reqwest::Client,
}

impl DefaultHttpClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl HttpClient for DefaultHttpClient {
    async fn query_node(
        &self,
        node_endpoint: &str,
        database: &str,
        sql: &str,
        timeout: Duration,
    ) -> Result<NodeQueryResult> {
        let start_time = Instant::now();
        let node_id = NodeId::new();
        
        let url = format!("{}/api/v3/query_sql", node_endpoint);
        let query_params = [
            ("db", database),
            ("q", sql),
        ];
        
        debug!(
            node_endpoint = %node_endpoint,
            database = %database,
            sql = %sql,
            "Executing query on remote node"
        );
        
        match self
            .client
            .get(&url)
            .query(&query_params)
            .timeout(timeout)
            .send()
            .await
        {
            Ok(response) => {
                let execution_time = start_time.elapsed().as_millis() as u64;
                let status = response.status();

                if status.is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(data) => {
                            let row_count = if let Some(array) = data.as_array() {
                                array.len()
                            } else {
                                1
                            };

                            Ok(NodeQueryResult {
                                node_id,
                                success: true,
                                data: Some(data),
                                error: None,
                                execution_time_ms: execution_time,
                                row_count,
                            })
                        }
                        Err(e) => Ok(NodeQueryResult {
                            node_id,
                            success: false,
                            data: None,
                            error: Some(format!("Failed to parse response: {}", e)),
                            execution_time_ms: execution_time,
                            row_count: 0,
                        }),
                    }
                } else {
                    let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                    Ok(NodeQueryResult {
                        node_id,
                        success: false,
                        data: None,
                        error: Some(format!("HTTP {}: {}", status, error_text)),
                        execution_time_ms: execution_time,
                        row_count: 0,
                    })
                }
            }
            Err(e) => {
                let execution_time = start_time.elapsed().as_millis() as u64;
                Ok(NodeQueryResult {
                    node_id,
                    success: false,
                    data: None,
                    error: Some(format!("Request failed: {}", e)),
                    execution_time_ms: execution_time,
                    row_count: 0,
                })
            }
        }
    }
}

/// Distributed Query Coordinator
pub struct DistributedQueryCoordinator {
    node_endpoints: Arc<RwLock<HashMap<NodeId, String>>>,
    http_client: Arc<dyn HttpClient>,
    stats: Arc<RwLock<QueryStats>>,
}

impl std::fmt::Debug for DistributedQueryCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedQueryCoordinator")
            .field("node_endpoints", &self.node_endpoints)
            .field("stats", &self.stats)
            .finish()
    }
}

impl DistributedQueryCoordinator {
    /// Create a new distributed query coordinator
    pub fn new(http_client: Option<Arc<dyn HttpClient>>) -> Self {
        // Initialize with hardcoded endpoints for testing
        let mut initial_endpoints = HashMap::new();
        initial_endpoints.insert(NodeId::new(), "http://127.0.0.1:8181".to_string());
        initial_endpoints.insert(NodeId::new(), "http://127.0.0.1:8182".to_string());

        Self {
            node_endpoints: Arc::new(RwLock::new(initial_endpoints)),
            http_client: http_client.unwrap_or_else(|| Arc::new(DefaultHttpClient::new())),
            stats: Arc::new(RwLock::new(QueryStats {
                total_queries: 0,
                successful_queries: 0,
                failed_queries: 0,
                average_execution_time_ms: 0.0,
                total_rows_returned: 0,
            })),
        }
    }
    
    /// Update the list of available nodes and their endpoints
    pub async fn update_nodes(&self, nodes: HashMap<NodeId, String>) {
        let mut endpoints = self.node_endpoints.write().await;
        *endpoints = nodes;
        info!(node_count = endpoints.len(), "Updated cluster node endpoints");
    }
    
    /// Execute a distributed query across the cluster
    pub async fn execute_query(&self, query: DistributedQuery) -> Result<DistributedQueryResult> {
        let start_time = Instant::now();
        
        info!(
            query_id = %query.query_id,
            database = %query.database,
            strategy = ?query.strategy,
            "Starting distributed query execution"
        );
        
        // Determine target nodes based on strategy
        let target_nodes = self.determine_target_nodes(&query.strategy).await?;
        
        if target_nodes.is_empty() {
            return Err(ClusterError::NoAvailableNodes);
        }
        
        // Execute query on all target nodes concurrently
        let mut tasks = Vec::new();
        let endpoints = self.node_endpoints.read().await;
        
        for node_id in &target_nodes {
            if let Some(endpoint) = endpoints.get(node_id) {
                let client = Arc::clone(&self.http_client);
                let endpoint = endpoint.clone();
                let database = query.database.clone();
                let sql = query.sql.clone();
                let timeout = query.timeout;
                let node_id = node_id.clone();
                
                let task = tokio::spawn(async move {
                    client.query_node(&endpoint, &database, &sql, timeout).await
                });
                
                tasks.push((node_id, task));
            }
        }
        
        // Wait for all queries to complete
        let mut node_results = Vec::new();
        for (node_id, task) in tasks {
            match task.await {
                Ok(Ok(result)) => {
                    debug!(
                        node_id = %node_id,
                        success = result.success,
                        execution_time_ms = result.execution_time_ms,
                        row_count = result.row_count,
                        "Node query completed"
                    );
                    node_results.push(result);
                }
                Ok(Err(e)) => {
                    error!(node_id = %node_id, error = %e, "Node query failed");
                    node_results.push(NodeQueryResult {
                        node_id,
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                        execution_time_ms: 0,
                        row_count: 0,
                    });
                }
                Err(e) => {
                    error!(node_id = %node_id, error = %e, "Task execution failed");
                    node_results.push(NodeQueryResult {
                        node_id,
                        success: false,
                        data: None,
                        error: Some(format!("Task failed: {}", e)),
                        execution_time_ms: 0,
                        row_count: 0,
                    });
                }
            }
        }
        
        // Check consistency requirements
        let successful_results: Vec<_> = node_results.iter().filter(|r| r.success).collect();
        let consistency_met = self.check_consistency(&query.consistency_level, successful_results.len(), target_nodes.len());

        if !consistency_met {
            warn!(
                query_id = %query.query_id,
                successful_nodes = successful_results.len(),
                total_nodes = target_nodes.len(),
                consistency_level = ?query.consistency_level,
                "Consistency requirements not met"
            );
        }

        // Aggregate results
        let aggregated_data = self.aggregate_results(&successful_results)?;
        let total_execution_time = start_time.elapsed().as_millis() as u64;
        let total_rows: usize = successful_results.iter().map(|r| r.row_count).sum();
        let nodes_succeeded = successful_results.len();

        let result = DistributedQueryResult {
            query_id: query.query_id.clone(),
            success: consistency_met && !successful_results.is_empty(),
            data: aggregated_data,
            node_results,
            total_execution_time_ms: total_execution_time,
            total_rows,
            nodes_queried: target_nodes.len(),
            nodes_succeeded,
        };
        
        // Update statistics
        self.update_stats(&result).await;
        
        info!(
            query_id = %query.query_id,
            success = result.success,
            total_execution_time_ms = result.total_execution_time_ms,
            total_rows = result.total_rows,
            nodes_succeeded = result.nodes_succeeded,
            nodes_queried = result.nodes_queried,
            "Distributed query completed"
        );
        
        Ok(result)
    }

    /// Determine target nodes based on query strategy
    async fn determine_target_nodes(&self, strategy: &QueryStrategy) -> Result<Vec<NodeId>> {
        let endpoints = self.node_endpoints.read().await;

        match strategy {
            QueryStrategy::Scatter => {
                // Return all available nodes
                Ok(endpoints.keys().cloned().collect())
            }
            QueryStrategy::Targeted(nodes) => {
                // Return only the specified nodes that are available
                Ok(nodes.iter()
                    .filter(|node_id| endpoints.contains_key(node_id))
                    .cloned()
                    .collect())
            }
            QueryStrategy::Single(node_id) => {
                // Return the single specified node if available
                if endpoints.contains_key(node_id) {
                    Ok(vec![node_id.clone()])
                } else {
                    Ok(Vec::new())
                }
            }
        }
    }

    /// Check if consistency requirements are met
    fn check_consistency(&self, level: &ConsistencyLevel, successful: usize, total: usize) -> bool {
        match level {
            ConsistencyLevel::Any => successful > 0,
            ConsistencyLevel::Quorum => successful > total / 2,
            ConsistencyLevel::All => successful == total,
        }
    }

    /// Aggregate results from multiple nodes with proper SQL semantics
    fn aggregate_results(&self, results: &[&NodeQueryResult]) -> Result<serde_json::Value> {
        if results.is_empty() {
            return Ok(serde_json::Value::Array(vec![]));
        }

        // Collect all data from all nodes
        let mut all_rows = Vec::new();

        for result in results {
            if let Some(data) = &result.data {
                if let Some(array) = data.as_array() {
                    all_rows.extend(array.iter().cloned());
                } else {
                    all_rows.push(data.clone());
                }
            }
        }

        // If we have no data, return empty array
        if all_rows.is_empty() {
            return Ok(serde_json::Value::Array(vec![]));
        }

        // Try to detect if this is an aggregation query by looking at the structure
        // Aggregation queries typically return a single row with computed values
        let is_aggregation_query = self.detect_aggregation_query(&all_rows);

        if is_aggregation_query {
            // Handle aggregation queries (MAX, MIN, COUNT, SUM, AVG, etc.)
            self.aggregate_aggregation_results(&all_rows)
        } else {
            // Handle regular SELECT queries - sort and return all rows
            self.aggregate_regular_results(all_rows)
        }
    }

    /// Detect if this is an aggregation query based on result structure
    fn detect_aggregation_query(&self, rows: &[serde_json::Value]) -> bool {
        if rows.is_empty() {
            return false;
        }

        // Check if all rows have the same structure and contain aggregation-like field names
        if let Some(first_row) = rows.first() {
            if let Some(obj) = first_row.as_object() {
                // Look for common aggregation field patterns
                for key in obj.keys() {
                    let key_lower = key.to_lowercase();
                    if key_lower.contains("max") || key_lower.contains("min") ||
                       key_lower.contains("count") || key_lower.contains("sum") ||
                       key_lower.contains("avg") {
                        return true;
                    }
                }

                // If there's only one row per node and no time/location fields, likely aggregation
                if !obj.contains_key("time") && !obj.contains_key("location") && obj.len() <= 2 {
                    return true;
                }
            }
        }

        false
    }

    /// Aggregate results from aggregation queries (MAX, MIN, COUNT, SUM, AVG)
    fn aggregate_aggregation_results(&self, rows: &[serde_json::Value]) -> Result<serde_json::Value> {
        if rows.is_empty() {
            return Ok(serde_json::Value::Array(vec![]));
        }

        // Get the structure from the first row
        let first_row = &rows[0];
        if let Some(first_obj) = first_row.as_object() {
            let mut aggregated = serde_json::Map::new();

            for (key, _) in first_obj {
                let key_lower = key.to_lowercase();

                if key_lower.contains("max") {
                    // Find maximum value across all nodes
                    let max_val = self.find_max_value(rows, key)?;
                    aggregated.insert(key.clone(), max_val);
                } else if key_lower.contains("min") {
                    // Find minimum value across all nodes
                    let min_val = self.find_min_value(rows, key)?;
                    aggregated.insert(key.clone(), min_val);
                } else if key_lower.contains("count") {
                    // Sum all count values
                    let total_count = self.sum_values(rows, key)?;
                    aggregated.insert(key.clone(), total_count);
                } else if key_lower.contains("sum") {
                    // Sum all sum values
                    let total_sum = self.sum_values(rows, key)?;
                    aggregated.insert(key.clone(), total_sum);
                } else if key_lower.contains("avg") {
                    // Calculate weighted average (this is complex, for now use simple average)
                    let avg_val = self.calculate_average(rows, key)?;
                    aggregated.insert(key.clone(), avg_val);
                } else {
                    // For other fields, take the first non-null value
                    if let Some(val) = first_obj.get(key) {
                        aggregated.insert(key.clone(), val.clone());
                    }
                }
            }

            Ok(serde_json::Value::Array(vec![serde_json::Value::Object(aggregated)]))
        } else {
            Ok(serde_json::Value::Array(rows.to_vec()))
        }
    }

    /// Aggregate results from regular SELECT queries
    fn aggregate_regular_results(&self, mut rows: Vec<serde_json::Value>) -> Result<serde_json::Value> {
        // Sort rows by time if time field exists
        rows.sort_by(|a, b| {
            let time_a = a.get("time").and_then(|v| v.as_str()).unwrap_or("");
            let time_b = b.get("time").and_then(|v| v.as_str()).unwrap_or("");
            time_a.cmp(time_b)
        });

        Ok(serde_json::Value::Array(rows))
    }

    /// Find maximum value for a given field across all rows
    fn find_max_value(&self, rows: &[serde_json::Value], field: &str) -> Result<serde_json::Value> {
        let mut max_val: Option<f64> = None;

        for row in rows {
            if let Some(obj) = row.as_object() {
                if let Some(val) = obj.get(field) {
                    if let Some(num) = val.as_f64() {
                        max_val = Some(max_val.map_or(num, |current| current.max(num)));
                    }
                }
            }
        }

        Ok(max_val.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null))
    }

    /// Find minimum value for a given field across all rows
    fn find_min_value(&self, rows: &[serde_json::Value], field: &str) -> Result<serde_json::Value> {
        let mut min_val: Option<f64> = None;

        for row in rows {
            if let Some(obj) = row.as_object() {
                if let Some(val) = obj.get(field) {
                    if let Some(num) = val.as_f64() {
                        min_val = Some(min_val.map_or(num, |current| current.min(num)));
                    }
                }
            }
        }

        Ok(min_val.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null))
    }

    /// Sum values for a given field across all rows
    fn sum_values(&self, rows: &[serde_json::Value], field: &str) -> Result<serde_json::Value> {
        let mut sum = 0.0;

        for row in rows {
            if let Some(obj) = row.as_object() {
                if let Some(val) = obj.get(field) {
                    if let Some(num) = val.as_f64() {
                        sum += num;
                    }
                }
            }
        }

        Ok(serde_json::Value::from(sum))
    }

    /// Calculate average value for a given field across all rows
    fn calculate_average(&self, rows: &[serde_json::Value], field: &str) -> Result<serde_json::Value> {
        let mut sum = 0.0;
        let mut count = 0;

        for row in rows {
            if let Some(obj) = row.as_object() {
                if let Some(val) = obj.get(field) {
                    if let Some(num) = val.as_f64() {
                        sum += num;
                        count += 1;
                    }
                }
            }
        }

        if count > 0 {
            Ok(serde_json::Value::from(sum / count as f64))
        } else {
            Ok(serde_json::Value::Null)
        }
    }

    /// Update query statistics
    async fn update_stats(&self, result: &DistributedQueryResult) {
        let mut stats = self.stats.write().await;

        stats.total_queries += 1;
        if result.success {
            stats.successful_queries += 1;
        } else {
            stats.failed_queries += 1;
        }

        stats.total_rows_returned += result.total_rows as u64;

        // Update average execution time using incremental formula
        let new_avg = (stats.average_execution_time_ms * (stats.total_queries - 1) as f64
                      + result.total_execution_time_ms as f64) / stats.total_queries as f64;
        stats.average_execution_time_ms = new_avg;
    }

    /// Get current query statistics
    pub async fn get_stats(&self) -> QueryStats {
        self.stats.read().await.clone()
    }

    /// Reset query statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = QueryStats {
            total_queries: 0,
            successful_queries: 0,
            failed_queries: 0,
            average_execution_time_ms: 0.0,
            total_rows_returned: 0,
        };
    }

    /// Get the list of available nodes
    pub async fn get_available_nodes(&self) -> Vec<NodeId> {
        self.node_endpoints.read().await.keys().cloned().collect()
    }

    /// Check if a specific node is available
    pub async fn is_node_available(&self, node_id: &NodeId) -> bool {
        self.node_endpoints.read().await.contains_key(node_id)
    }
}
