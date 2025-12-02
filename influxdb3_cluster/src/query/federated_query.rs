//! Federated query execution for cross-node JOINs
//!
//! This module provides a simple federated query mechanism that allows
//! querying tables from different nodes and joining them locally.
//!
//! For the initial implementation, we use HTTP to query remote nodes,
//! then join the results locally using DataFusion.

use crate::error::{Error, Result};
use crate::node_registry::NodeRegistry;
use crate::types::NodeId;
use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use observability_deps::tracing::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;

/// Federated query executor for cross-node queries
#[derive(Debug)]
pub struct FederatedQueryExecutor {
    node_registry: Arc<NodeRegistry>,
}

/// Standalone federated query executor that doesn't depend on NodeRegistry
/// Used for direct HTTP queries to known node addresses
#[derive(Debug, Clone)]
pub struct StandaloneFederatedExecutor {
    /// Map of node names to their HTTP addresses (e.g., "node1" -> "127.0.0.1:8181")
    nodes: HashMap<String, String>,
}

impl FederatedQueryExecutor {
    pub fn new(node_registry: Arc<NodeRegistry>) -> Self {
        Self { node_registry }
    }

    /// Query a remote node via HTTP and get JSON response
    pub async fn query_remote_node_http(
        &self,
        node_id: NodeId,
        database: &str,
        query: &str,
    ) -> Result<String> {
        info!(
            "FederatedQuery: Querying remote node {} for database {} with query: {}",
            node_id, database, query
        );

        // Get node info from registry
        let node_info = self.node_registry.get_node(node_id).await?;

        debug!(
            "FederatedQuery: Node info - address: {}, http_port: {}",
            node_info.address, node_info.http_port
        );

        // Build HTTP query URL
        let url = format!(
            "http://{}:{}/api/v3/query_sql",
            node_info.address, node_info.http_port
        );

        // Build request body
        let body = serde_json::json!({
            "db": database,
            "query": query
        });

        // Execute HTTP request
        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                error!(
                    "FederatedQuery: Failed to query remote node {}: {}",
                    node_id, e
                );
                Error::InternalError {
                    message: format!("Failed to query remote node: {}", e),
                }
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            error!(
                "FederatedQuery: Remote query failed with status {}: {}",
                status, error_text
            );
            return Err(Error::InternalError {
                message: format!("Remote query failed: {}", status),
            });
        }

        let result = response.text().await.map_err(|e| Error::InternalError {
            message: format!("Failed to read response: {}", e),
        })?;

        info!(
            "FederatedQuery: Received response from node {} ({} bytes)",
            node_id,
            result.len()
        );

        Ok(result)
    }

    /// Check which nodes have data for a given table
    pub async fn find_nodes_with_table(
        &self,
        _database: &str,
        table: &str,
    ) -> Result<Vec<NodeId>> {
        info!("FederatedQuery: Finding nodes with table {}", table);

        // For now, we'll query all active nodes
        // In a real implementation, we'd maintain metadata about table locations
        let nodes = self.node_registry.list_nodes().await;

        let active_node_ids: Vec<NodeId> = nodes
            .iter()
            .filter(|n| n.status == crate::types::NodeStatus::Active)
            .map(|n| n.node_id)
            .collect();

        info!("FederatedQuery: Found {} active nodes", active_node_ids.len());
        Ok(active_node_ids)
    }

    /// Get table data from all nodes that have it
    pub async fn fetch_table_from_nodes(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Vec<(NodeId, String)>> {
        info!("FederatedQuery: Fetching table {} from all nodes", table);

        let nodes = self.find_nodes_with_table(database, table).await?;
        let mut results = Vec::new();

        for node_id in nodes {
            // Build a query for just this table
            let table_query = format!("SELECT * FROM {}", table);

            match self
                .query_remote_node_http(node_id, database, &table_query)
                .await
            {
                Ok(data) => {
                    info!(
                        "FederatedQuery: Successfully queried {} from node {}",
                        table, node_id
                    );
                    results.push((node_id, data));
                }
                Err(e) => {
                    warn!(
                        "FederatedQuery: Failed to query {} from node {}: {}",
                        table, node_id, e
                    );
                    // Continue with other nodes
                }
            }
        }

        Ok(results)
    }
}

impl StandaloneFederatedExecutor {
    /// Create a new standalone executor with the given node addresses
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Add a node to the executor
    pub fn add_node(&mut self, name: &str, address: &str) {
        self.nodes.insert(name.to_string(), address.to_string());
    }

    /// Query a node by its address and get JSON response
    pub async fn query_node(&self, address: &str, database: &str, query: &str) -> Result<String> {
        info!(
            "StandaloneFederated: Querying {} for database {} with query: {}",
            address, database, query
        );

        let url = format!("http://{}/api/v3/query_sql", address);
        let body = serde_json::json!({
            "db": database,
            "query": query
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::InternalError {
                message: format!("Failed to query node {}: {}", address, e),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::InternalError {
                message: format!("Query failed with status {}: {}", status, error_text),
            });
        }

        response.text().await.map_err(|e| Error::InternalError {
            message: format!("Failed to read response: {}", e),
        })
    }

    /// Convert JSON response to Arrow RecordBatch
    /// This handles the common InfluxDB JSON format with arrays of objects
    pub fn json_to_record_batch(&self, json_str: &str, table_name: &str) -> Result<RecordBatch> {
        info!(
            "Converting JSON to RecordBatch for table {}, json length: {}",
            table_name,
            json_str.len()
        );

        let rows: Vec<serde_json::Value> =
            serde_json::from_str(json_str).map_err(|e| Error::InternalError {
                message: format!("Failed to parse JSON: {}", e),
            })?;

        if rows.is_empty() {
            // Return empty batch with minimal schema
            let schema = Schema::new(vec![Field::new("_empty", DataType::Utf8, true)]);
            return Ok(RecordBatch::new_empty(Arc::new(schema)));
        }

        // Infer schema from first row
        let first_row = &rows[0];
        let obj = first_row.as_object().ok_or_else(|| Error::InternalError {
            message: "Expected JSON object".to_string(),
        })?;

        let mut fields = Vec::new();
        let mut column_names: Vec<String> = Vec::new();

        for (key, value) in obj.iter() {
            let data_type = match value {
                serde_json::Value::Number(n) => {
                    if n.is_f64() {
                        DataType::Float64
                    } else {
                        DataType::Float64 // Treat all numbers as f64 for simplicity
                    }
                }
                serde_json::Value::String(s) => {
                    // Check if it looks like a timestamp
                    if s.contains('T') && s.contains(':') {
                        DataType::Timestamp(TimeUnit::Nanosecond, None)
                    } else {
                        DataType::Utf8
                    }
                }
                serde_json::Value::Bool(_) => DataType::Boolean,
                _ => DataType::Utf8,
            };
            fields.push(Field::new(key, data_type, true));
            column_names.push(key.clone());
        }

        let schema = Arc::new(Schema::new(fields));

        // Build columns
        let mut columns: Vec<ArrayRef> = Vec::new();

        for col_name in &column_names {
            let field = schema.field_with_name(col_name).map_err(|e| {
                Error::InternalError {
                    message: format!("Field not found: {}", e),
                }
            })?;

            let array: ArrayRef = match field.data_type() {
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| row.get(col_name).and_then(|v| v.as_f64()))
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                DataType::Utf8 => {
                    let values: Vec<Option<&str>> = rows
                        .iter()
                        .map(|row| row.get(col_name).and_then(|v| v.as_str()))
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            row.get(col_name).and_then(|v| {
                                v.as_str().and_then(|s| {
                                    // Parse ISO 8601 timestamp
                                    chrono::DateTime::parse_from_rfc3339(s)
                                        .ok()
                                        .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0))
                                        .or_else(|| {
                                            // Try parsing without timezone
                                            chrono::NaiveDateTime::parse_from_str(
                                                s,
                                                "%Y-%m-%dT%H:%M:%S",
                                            )
                                            .ok()
                                            .map(|dt| dt.and_utc().timestamp_nanos_opt().unwrap_or(0))
                                        })
                                })
                            })
                        })
                        .collect();
                    Arc::new(TimestampNanosecondArray::from(values))
                }
                _ => {
                    // Default to string
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| {
                            row.get(col_name)
                                .map(|v| v.to_string().trim_matches('"').to_string())
                        })
                        .collect();
                    Arc::new(StringArray::from(
                        values
                            .iter()
                            .map(|o| o.as_deref())
                            .collect::<Vec<Option<&str>>>(),
                    ))
                }
            };
            columns.push(array);
        }

        RecordBatch::try_new(schema, columns).map_err(|e| Error::InternalError {
            message: format!("Failed to create RecordBatch: {}", e),
        })
    }

    /// Execute a federated JOIN query across multiple nodes
    pub async fn execute_federated_join(
        &self,
        database: &str,
        table1_node: &str,
        table1_name: &str,
        table2_node: &str,
        table2_name: &str,
        join_column: &str,
        select_columns: &str,
    ) -> Result<Vec<RecordBatch>> {
        info!(
            "Executing federated JOIN: {} from {} JOIN {} from {} ON {}",
            table1_name, table1_node, table2_name, table2_node, join_column
        );

        // Query both nodes
        let query1 = format!("SELECT * FROM {}", table1_name);
        let query2 = format!("SELECT * FROM {}", table2_name);

        let json1 = self.query_node(table1_node, database, &query1).await?;
        let json2 = self.query_node(table2_node, database, &query2).await?;

        info!(
            "Received data: {} bytes from {}, {} bytes from {}",
            json1.len(),
            table1_node,
            json2.len(),
            table2_node
        );

        // Convert to RecordBatches
        let batch1 = self.json_to_record_batch(&json1, table1_name)?;
        let batch2 = self.json_to_record_batch(&json2, table2_name)?;

        info!(
            "Converted to RecordBatch: {} has {} rows, {} has {} rows",
            table1_name,
            batch1.num_rows(),
            table2_name,
            batch2.num_rows()
        );

        // Create DataFusion context and register tables
        let ctx = SessionContext::new();

        ctx.register_batch(table1_name, batch1)
            .map_err(|e| Error::InternalError {
                message: format!("Failed to register {}: {}", table1_name, e),
            })?;

        ctx.register_batch(table2_name, batch2)
            .map_err(|e| Error::InternalError {
                message: format!("Failed to register {}: {}", table2_name, e),
            })?;

        // Build and execute JOIN query
        let join_sql = format!(
            "SELECT {} FROM {} t1 JOIN {} t2 ON t1.{} = t2.{}",
            select_columns, table1_name, table2_name, join_column, join_column
        );

        info!("Executing local JOIN: {}", join_sql);

        let df = ctx
            .sql(&join_sql)
            .await
            .map_err(|e| Error::InternalError {
                message: format!("Failed to execute JOIN: {}", e),
            })?;

        df.collect().await.map_err(|e| Error::InternalError {
            message: format!("Failed to collect results: {}", e),
        })
    }
}

impl Default for StandaloneFederatedExecutor {
    fn default() -> Self {
        Self::new()
    }
}