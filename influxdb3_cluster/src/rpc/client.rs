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

        println!("🔍 Remote query to node {}: {}", node_id.as_u64(), query);
        println!("📥 Response JSON: {}", json_text);

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
        println!("📊 RecordBatch schema: {:?}", record_batch.schema());

        // Create a single-item stream
        let stream = futures::stream::once(async move { Ok(record_batch) });

        Ok(Box::pin(stream))
    }

    /// Convert JSON array to RecordBatch
    fn json_to_record_batch(&self, json_text: &str) -> Result<RecordBatch> {
        use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let rows: Vec<serde_json::Value> =
            serde_json::from_str(json_text).map_err(|e| Error::InternalError {
                message: format!("Failed to parse JSON: {}", e),
            })?;

        if rows.is_empty() {
            return Err(Error::InternalError {
                message: "Empty result set".to_string(),
            });
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
                serde_json::Value::Number(_) => DataType::Float64,
                serde_json::Value::String(s) if s.contains('T') && s.contains(':') => {
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                }
                _ => DataType::Utf8,
            };
            fields.push(Field::new(key, data_type, true));
            column_names.push(key.clone());
        }

        let schema = Arc::new(Schema::new(fields));

        // Build columns
        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        for col_name in &column_names {
            let field = schema
                .field_with_name(col_name)
                .map_err(|e| Error::InternalError {
                    message: format!("Field not found: {}", e),
                })?;

            let array: Arc<dyn arrow::array::Array> = match field.data_type() {
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
                                    chrono::DateTime::parse_from_rfc3339(s)
                                        .ok()
                                        .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0))
                                })
                            })
                        })
                        .collect();
                    Arc::new(TimestampNanosecondArray::from(values))
                }
                _ => {
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
}

