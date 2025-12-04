//! Remote query client for executing queries on data nodes
//!
//! This implementation uses Arrow Flight for efficient data transfer

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor, Ticket};
use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use tonic::transport::{Channel, Endpoint};

use crate::error::*;
use crate::types::RegionId;

/// Client for executing queries on remote nodes
#[derive(Debug, Clone)]
pub struct RemoteQueryClient {
    endpoint: String,
    channel: Option<Channel>,
}

impl RemoteQueryClient {
    /// Create a new remote query client
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            channel: None,
        }
    }

    /// Connect to the remote endpoint
    pub async fn connect(endpoint: String) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.clone())
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Invalid endpoint: {}", e),
                }
                .build()
            })?
            .connect()
            .await
            .map_err(|e| {
                NetworkSnafu {
                    reason: format!("Failed to connect to {}: {}", endpoint, e),
                }
                .build()
            })?;

        let flight_client = FlightServiceClient::new(channel.clone());

        Ok(Self {
            endpoint,
            channel: Some(channel.clone()),
        })
    }

    /// Get or create Flight client
    async fn get_flight_client(&self) -> Result<FlightServiceClient<Channel>> {
        let channel = self.channel.as_ref().ok_or_else(|| {
            InternalSnafu {
                reason: "Client not connected",
            }
            .build()
        })?;

        Ok(FlightServiceClient::new(channel.clone()))
    }

    /// Execute a query and stream results using Arrow Flight
    ///
    /// This uses Arrow Flight's DoGet RPC to stream query results efficiently.
    pub async fn execute_query(
        &self,
        query_id: String,
        serialized_plan: Vec<u8>,
        region_ids: Vec<RegionId>,
        schema: SchemaRef,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let mut client = self.get_flight_client().await?;

        tracing::info!(
            query_id = %query_id,
            endpoint = %self.endpoint,
            num_regions = region_ids.len(),
            plan_size = serialized_plan.len(),
            "Executing query on remote node via Arrow Flight"
        );

        // Create a Ticket that encodes the query information
        // Format: query_id|region_ids|serialized_plan
        let ticket_data = self.encode_ticket(&query_id, &region_ids, &serialized_plan)?;
        let ticket = Ticket {
            ticket: ticket_data.into(),
        };

        // Execute DoGet to stream results
        let response = client
            .do_get(ticket)
            .await
            .map_err(|e| {
                RemoteQuerySnafu {
                    reason: format!("Flight DoGet failed: {}", e),
                }
                .build()
            })?;

        let flight_stream = response.into_inner();

        // Convert Flight stream to RecordBatch stream
        let batch_stream = flight_stream
            .map_err(|e| {
                crate::Error::remote_query(format!("Flight stream error: {}", e))
            })
            .and_then(move |flight_data| {
                let schema_clone = schema.clone();
                async move {
                    Self::decode_flight_data(flight_data, schema_clone).await
                }
            });

        Ok(Box::pin(batch_stream))
    }

    /// Encode query information into a Ticket
    fn encode_ticket(
        &self,
        query_id: &str,
        region_ids: &[RegionId],
        serialized_plan: &[u8],
    ) -> Result<Vec<u8>> {
        use std::io::Write;

        let mut ticket_data = Vec::new();

        // Write query_id length and data
        let query_id_bytes = query_id.as_bytes();
        ticket_data.write_all(&(query_id_bytes.len() as u32).to_le_bytes())
            .map_err(|e| InternalSnafu { reason: format!("Failed to write query_id length: {}", e) }.build())?;
        ticket_data.write_all(query_id_bytes)
            .map_err(|e| InternalSnafu { reason: format!("Failed to write query_id: {}", e) }.build())?;

        // Write region_ids count and data
        ticket_data.write_all(&(region_ids.len() as u32).to_le_bytes())
            .map_err(|e| InternalSnafu { reason: format!("Failed to write region count: {}", e) }.build())?;
        for region_id in region_ids {
            ticket_data.write_all(&region_id.as_u64().to_le_bytes())
                .map_err(|e| InternalSnafu { reason: format!("Failed to write region_id: {}", e) }.build())?;
        }

        // Write serialized_plan length and data
        ticket_data.write_all(&(serialized_plan.len() as u32).to_le_bytes())
            .map_err(|e| InternalSnafu { reason: format!("Failed to write plan length: {}", e) }.build())?;
        ticket_data.write_all(serialized_plan)
            .map_err(|e| InternalSnafu { reason: format!("Failed to write plan: {}", e) }.build())?;

        Ok(ticket_data)
    }

    /// Decode FlightData into RecordBatch
    async fn decode_flight_data(
        flight_data: FlightData,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;

        // FlightData contains the Arrow IPC stream
        let data_body = flight_data.data_body;

        if data_body.is_empty() {
            // Schema-only message, skip
            return Err(InternalSnafu {
                reason: "Received empty FlightData".to_string(),
            }
            .build());
        }

        // Decode using Arrow IPC
        let cursor = Cursor::new(data_body);
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Failed to create IPC reader: {}", e),
                }
                .build()
            })?;

        // Read the first batch
        reader
            .next()
            .ok_or_else(|| {
                InternalSnafu {
                    reason: "No batch in IPC stream".to_string(),
                }
                .build()
            })?
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Failed to read batch: {}", e),
                }
                .build()
            })
    }

    /// Get the status of a query using Flight's GetFlightInfo
    pub async fn get_query_status(&self, query_id: String) -> Result<QueryStatus> {
        let mut client = self.get_flight_client().await?;

        tracing::debug!(
            query_id = %query_id,
            endpoint = %self.endpoint,
            "Getting query status"
        );

        // Create a FlightDescriptor for the query status request
        let descriptor = FlightDescriptor {
            r#type: 0, // CMD type
            cmd: format!("status:{}", query_id).into_bytes().into(),
            path: vec![],
        };

        // Use GetFlightInfo to check query status
        match client.get_flight_info(descriptor).await {
            Ok(response) => {
                let info = response.into_inner();
                // Check if the flight has endpoints (query is ready)
                if info.endpoint.is_empty() {
                    Ok(QueryStatus::Pending)
                } else {
                    // Query has results available
                    Ok(QueryStatus::Completed)
                }
            }
            Err(status) => {
                // Parse error to determine status
                let error_msg = status.message();
                if error_msg.contains("not found") {
                    Ok(QueryStatus::Pending)
                } else if error_msg.contains("failed") || error_msg.contains("error") {
                    Ok(QueryStatus::Failed)
                } else if error_msg.contains("cancelled") {
                    Ok(QueryStatus::Cancelled)
                } else {
                    Ok(QueryStatus::Running)
                }
            }
        }
    }

    /// Cancel a running query using Flight's DoPut
    pub async fn cancel_query(&self, query_id: String) -> Result<()> {
        let mut client = self.get_flight_client().await?;

        tracing::info!(
            query_id = %query_id,
            endpoint = %self.endpoint,
            "Cancelling query"
        );

        // Create a FlightDescriptor for the cancel command
        let descriptor = FlightDescriptor {
            r#type: 0, // CMD type
            cmd: format!("cancel:{}", query_id).into_bytes().into(),
            path: vec![],
        };

        // Send cancel command via DoPut
        let stream = stream::iter(vec![FlightData {
            flight_descriptor: Some(descriptor),
            ..Default::default()
        }]);

        let response = client.do_put(stream).await.map_err(|e| {
            RemoteQuerySnafu {
                reason: format!("Failed to cancel query: {}", e),
            }
            .build()
        })?;

        // Consume the response
        let mut result_stream = response.into_inner();
        while let Some(_) = result_stream.next().await {}

        tracing::info!(
            query_id = %query_id,
            "Query cancelled successfully"
        );

        Ok(())
    }
}

/// Query execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = RemoteQueryClient::new("http://localhost:8080".to_string());
        assert_eq!(client.endpoint, "http://localhost:8080");
        assert!(client.channel.is_none());
    }

    #[tokio::test]
    async fn test_client_connect_failure() {
        let result = RemoteQueryClient::connect("http://invalid-host:9999".to_string()).await;
        assert!(result.is_err());

        if let Err(e) = result {
            // Should be a network error
            assert!(format!("{:?}", e).contains("Network"));
        }
    }

    #[tokio::test]
    async fn test_execute_query_without_connection() {
        use arrow::datatypes::{Field, DataType, Schema};

        let client = RemoteQueryClient::new("http://localhost:8080".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let result = client.execute_query(
            "test-query-id".to_string(),
            vec![],
            vec![],
            schema,
        ).await;

        // Should fail because not connected
        assert!(result.is_err());
    }

    #[test]
    fn test_query_status_enum() {
        // Test QueryStatus variants
        assert_eq!(QueryStatus::Pending, QueryStatus::Pending);
        assert_ne!(QueryStatus::Pending, QueryStatus::Running);

        // Test Debug
        let status = QueryStatus::Running;
        assert_eq!(format!("{:?}", status), "Running");
    }
}

