//! gRPC client for inter-node communication

use crate::error::{Error, Result};
use crate::proto::cluster_service_client::ClusterServiceClient;
use crate::proto::{ConsistencyLevel as ProtoConsistency, WriteRequest};
use crate::types::NodeId;
use arrow::record_batch::RecordBatch;
use futures::stream::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tonic::transport::Channel;

#[derive(Clone, Copy, Debug)]
pub struct ClusterRpcClient {}

impl ClusterRpcClient {
    pub fn new() -> Self { Self {} }

    async fn client_for(&self, addr: &str) -> Result<ClusterServiceClient<Channel>> {
        let endpoint = format!("http://{}", addr);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| Error::InternalError { message: e.to_string() })?
            .connect()
            .await
            .map_err(|e| Error::InternalError { message: e.to_string() })?;
        Ok(ClusterServiceClient::new(channel))
    }

    pub async fn write_to_node(
        &self,
        addr: &str,
        shard_id: u64,
        database: &str,
        data: Vec<u8>,
        consistency: crate::types::ConsistencyLevel,
    ) -> Result<()> {
        let mut client = self.client_for(addr).await?;
        let consistency_proto = match consistency {
            crate::types::ConsistencyLevel::One => ProtoConsistency::One as i32,
            crate::types::ConsistencyLevel::Quorum => ProtoConsistency::Quorum as i32,
            crate::types::ConsistencyLevel::All => ProtoConsistency::All as i32,
        };
        let req = WriteRequest {
            shard_id,
            data,
            consistency: consistency_proto,
            database: database.to_string(),
            forwarded: false,
        };
        client.write(req).await.map_err(|e| Error::RpcError { source: e })?;
        Ok(())
    }

    /// Query a node via HTTP and get a stream of record batches
    pub async fn query_node(
        &self,
        node_id: NodeId,
        database: &str,
        query: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        // Map node_id to HTTP port
        // node1 -> 8181, node2 -> 8182, node3 -> 8183
        let http_port = 8180 + node_id.as_u64();
        let url = format!("http://127.0.0.1:{}/api/v3/query_sql", http_port);

        // Build HTTP request
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
                message: format!("HTTP request failed: {}", e),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::InternalError {
                message: format!("Query failed with status {}: {}", status, error_text),
            });
        }

        // Get JSON response
        let json_text = response.text().await.map_err(|e| Error::InternalError {
            message: format!("Failed to read response: {}", e),
        })?;

        // Parse JSON to RecordBatch
        // The response is a JSON array of objects
        let json_value: serde_json::Value =
            serde_json::from_str(&json_text).map_err(|e| Error::InternalError {
                message: format!("Failed to parse JSON: {}", e),
            })?;

        let rows = json_value.as_array().ok_or_else(|| Error::InternalError {
            message: "Expected JSON array".to_string(),
        })?;

        if rows.is_empty() {
            // Return empty stream
            return Ok(Box::pin(futures::stream::empty()));
        }

        // Convert JSON to RecordBatch
        let record_batch = self.json_to_record_batch(&json_text)?;

        // Create a single-item stream
        let stream = futures::stream::once(async move { Ok(record_batch) });

        Ok(Box::pin(stream))
    }

    /// Convert JSON array to RecordBatch using Arrow's built-in JSON reader
    fn json_to_record_batch(&self, json_text: &str) -> Result<RecordBatch> {
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use std::io::Cursor;

        // Parse the JSON array
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(json_text).map_err(|e| Error::InternalError {
                message: format!("Failed to parse JSON: {}", e),
            })?;

        if rows.is_empty() {
            return Err(Error::InternalError {
                message: "Empty result set".to_string(),
            });
        }

        // Convert JSON array to newline-delimited JSON (required by Arrow's JSON reader)
        let mut ndjson = String::new();
        for row in &rows {
            let row_str = serde_json::to_string(row).map_err(|e| Error::InternalError {
                message: format!("Failed to serialize row: {}", e),
            })?;
            ndjson.push_str(&row_str);
            ndjson.push('\n');
        }

        // Use Arrow's JSON schema inference (only scan first 100 rows for performance)
        let mut cursor = Cursor::new(ndjson.as_bytes());
        let (inferred_schema, _) = arrow::json::reader::infer_json_schema(&mut cursor, Some(100))
            .map_err(|e| Error::InternalError {
                message: format!("Failed to infer schema: {}", e),
            })?;

        // Sort fields by name for deterministic ordering
        let mut fields: Vec<_> = inferred_schema.fields().iter().cloned().collect();
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        let schema = Arc::new(Schema::new(fields));

        // Use Arrow's JSON reader for efficient parsing
        cursor.set_position(0);
        let mut reader = arrow::json::ReaderBuilder::new(schema.clone())
            .build(cursor)
            .map_err(|e| Error::InternalError {
                message: format!("Failed to create JSON reader: {}", e),
            })?;

        // Read the batch
        let batch = reader
            .next()
            .ok_or_else(|| Error::InternalError {
                message: "No batch returned from JSON reader".to_string(),
            })?
            .map_err(|e| Error::InternalError {
                message: format!("Failed to read JSON batch: {}", e),
            })?;

        Ok(batch)
    }
}

