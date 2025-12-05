//! Arrow Flight service for Datanode.
//!
//! This module implements an Arrow Flight service for efficient query result
//! streaming from Datanodes to Frontends.

use crate::common::{NodeId, RegionId, RegionInfo};
use crate::datanode::executor::LocalExecutor;
use crate::error::{DistributedError, Result};
use crate::frontend::plan_serde::ArrowStreamSerializer;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::{FlightService, FlightServiceServer},
};
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use observability_deps::tracing::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

/// Arrow Flight service implementation for Datanode.
///
/// This service allows Frontends to execute queries and stream results
/// using the Arrow Flight protocol.
pub struct DatanodeFlightService {
    /// Node ID of this datanode
    node_id: NodeId,
    /// Local query executor
    executor: Arc<LocalExecutor>,
    /// Active query streams (query_id -> stream info)
    active_queries: Arc<RwLock<HashMap<String, QueryStreamInfo>>>,
    /// Configuration
    config: FlightServiceConfig,
}

/// Configuration for the Flight service.
#[derive(Debug, Clone)]
pub struct FlightServiceConfig {
    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,
    /// Query timeout in seconds
    pub query_timeout_secs: u64,
    /// Maximum batch size for streaming
    pub max_batch_size: usize,
}

impl Default for FlightServiceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: 100,
            query_timeout_secs: 300,
            max_batch_size: 8192,
        }
    }
}

/// Information about an active query stream.
struct QueryStreamInfo {
    /// Query ID
    query_id: String,
    /// Database name
    database: String,
    /// Region ID being queried
    region_id: RegionId,
    /// Schema of the result
    schema: SchemaRef,
    /// Start time
    start_time: std::time::Instant,
}

impl DatanodeFlightService {
    /// Create a new Flight service.
    pub fn new(node_id: NodeId, executor: Arc<LocalExecutor>) -> Self {
        Self {
            node_id,
            executor,
            active_queries: Arc::new(RwLock::new(HashMap::new())),
            config: FlightServiceConfig::default(),
        }
    }

    /// Create a Flight service with custom configuration.
    pub fn with_config(
        node_id: NodeId,
        executor: Arc<LocalExecutor>,
        config: FlightServiceConfig,
    ) -> Self {
        Self {
            node_id,
            executor,
            active_queries: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Create a gRPC server for this service.
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Parse a ticket to extract query information.
    fn parse_ticket(&self, ticket: &Ticket) -> Result<QueryTicket> {
        let data = std::str::from_utf8(&ticket.ticket)
            .map_err(|e| DistributedError::InvalidRequest(e.to_string()))?;

        serde_json::from_str(data).map_err(|e| DistributedError::InvalidRequest(e.to_string()))
    }

    /// Generate a unique query ID.
    fn generate_query_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Execute a SQL query and return a result stream.
    async fn execute_sql_query(
        &self,
        database: &str,
        query: &str,
        region_id: RegionId,
    ) -> Result<SendableRecordBatchStream> {
        info!(
            database = %database,
            query = %query,
            region_id = %region_id.get(),
            "Executing SQL query via Flight"
        );

        self.executor
            .execute_sql(database, query, None, None, None)
            .await
    }

    /// Execute an InfluxQL query and return a result stream.
    async fn execute_influxql_query(
        &self,
        database: &str,
        query: &str,
        region_id: RegionId,
    ) -> Result<SendableRecordBatchStream> {
        info!(
            database = %database,
            query = %query,
            region_id = %region_id.get(),
            "Executing InfluxQL query via Flight"
        );

        // Parse the InfluxQL statement
        let statement = influxdb_influxql_parser::parse_statements(query)
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| DistributedError::InvalidRequest("Empty InfluxQL query".to_string()))?;

        self.executor
            .execute_influxql(database, query, statement, None, None, None)
            .await
    }

    /// Convert a Result to a tonic Status.
    fn to_status<T>(result: Result<T>) -> std::result::Result<T, Status> {
        result.map_err(|e| match e {
            DistributedError::InvalidRequest(msg) => Status::invalid_argument(msg),
            DistributedError::DatabaseNotFound { database } => {
                Status::not_found(format!("Database not found: {}", database))
            }
            DistributedError::TableNotFound { database, table } => {
                Status::not_found(format!("Table not found: {}.{}", database, table))
            }
            DistributedError::RegionNotFound { region_id } => {
                Status::not_found(format!("Region not found: {}", region_id))
            }
            _ => Status::internal(e.to_string()),
        })
    }
}

/// Ticket data for query execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryTicket {
    /// Query ID for tracking
    pub query_id: String,
    /// Database to query
    pub database: String,
    /// Query string
    pub query: String,
    /// Query type
    pub query_type: QueryType,
    /// Region to query (if specified)
    pub region_id: Option<u64>,
}

/// Type of query.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum QueryType {
    Sql,
    InfluxQL,
}

#[tonic::async_trait]
impl FlightService for DatanodeFlightService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = std::result::Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = std::result::Result<PutResult, Status>> + Send>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = std::result::Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = std::result::Result<ActionType, Status>> + Send>>;

    /// Handshake for authentication (simplified - no auth for now).
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        debug!("Flight handshake request");

        // Simple handshake response
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::new(),
        };

        let stream = stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }

    /// List available flights (queries that can be retrieved).
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        debug!("Flight list_flights request");

        // Return empty list for now
        let stream = stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }

    /// Get flight info for a query descriptor.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        debug!(descriptor = ?descriptor, "Flight get_flight_info request");

        // Parse the descriptor to understand what flight is requested
        let path = descriptor.path.join("/");

        // For now, return a simple flight info
        let ticket = Ticket {
            ticket: Bytes::from(path),
        };

        let info = FlightInfo::new()
            .with_descriptor(descriptor)
            .try_with_schema(&arrow::datatypes::Schema::empty())
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(info))
    }

    /// Get schema for a flight.
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        debug!(descriptor = ?descriptor, "Flight get_schema request");

        // Return empty schema for now
        let schema = arrow::datatypes::Schema::empty();
        let schema_flight = arrow_flight::SchemaAsIpc::new(&schema, &Default::default());
        let schema_bytes: Bytes = schema_flight.try_into().unwrap_or_default();

        Ok(Response::new(SchemaResult {
            schema: schema_bytes.into(),
        }))
    }

    /// Execute a query and stream results.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        debug!(ticket_len = ticket.ticket.len(), "Flight do_get request");

        // Parse the ticket
        let query_ticket: QueryTicket = Self::to_status(self.parse_ticket(&ticket))?;

        info!(
            query_id = %query_ticket.query_id,
            database = %query_ticket.database,
            query_type = ?query_ticket.query_type,
            "Executing Flight query"
        );

        let region_id = RegionId::new(query_ticket.region_id.unwrap_or(0));

        // Execute the query
        let stream = match query_ticket.query_type {
            QueryType::Sql => Self::to_status(
                self.execute_sql_query(&query_ticket.database, &query_ticket.query, region_id)
                    .await,
            )?,
            QueryType::InfluxQL => Self::to_status(
                self.execute_influxql_query(&query_ticket.database, &query_ticket.query, region_id)
                    .await,
            )?,
        };

        // Get schema
        let schema = stream.schema();

        // Convert the RecordBatch stream to FlightData stream
        let flight_stream = RecordBatchStreamToFlightData::new(stream, schema);

        Ok(Response::new(Box::pin(flight_stream)))
    }

    /// Handle data upload (for write operations).
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        debug!("Flight do_put request");

        // Collect flight data
        let mut batches = Vec::new();
        let mut schema: Option<SchemaRef> = None;

        while let Some(data) = stream.next().await {
            let data = data?;

            // Try to decode as Arrow IPC
            if let Ok(decoded_batches) = ArrowStreamSerializer::deserialize_batches(&data.data_body)
            {
                for batch in decoded_batches {
                    if schema.is_none() {
                        schema = Some(batch.schema());
                    }
                    batches.push(batch);
                }
            }
        }

        info!(
            num_batches = batches.len(),
            "Received data via Flight do_put"
        );

        // For now, just acknowledge receipt
        let result = PutResult {
            app_metadata: Bytes::from("OK"),
        };

        let stream = stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(stream)))
    }

    /// Bidirectional data exchange.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        debug!("Flight do_exchange request");
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    /// Execute an action.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        debug!(action_type = %action.r#type, "Flight do_action request");

        match action.r#type.as_str() {
            "health_check" => {
                let result = arrow_flight::Result {
                    body: Bytes::from("OK"),
                };
                let stream = stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "cancel_query" => {
                // Parse query_id from action body
                let query_id = std::str::from_utf8(&action.body)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;

                info!(query_id = %query_id, "Cancelling query");

                // Remove from active queries
                self.active_queries.write().await.remove(query_id);

                let result = arrow_flight::Result {
                    body: Bytes::from("Cancelled"),
                };
                let stream = stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            _ => Err(Status::unimplemented(format!(
                "Action '{}' not implemented",
                action.r#type
            ))),
        }
    }

    /// List available actions.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        debug!("Flight list_actions request");

        let actions = vec![
            ActionType {
                r#type: "health_check".to_string(),
                description: "Check if the service is healthy".to_string(),
            },
            ActionType {
                r#type: "cancel_query".to_string(),
                description: "Cancel an executing query".to_string(),
            },
        ];

        let stream = stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    /// Poll for flight status.
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        debug!("Flight poll_flight_info request");
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
}

/// Stream adapter that converts RecordBatches to FlightData.
struct RecordBatchStreamToFlightData {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    schema_sent: bool,
}

impl RecordBatchStreamToFlightData {
    fn new(inner: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        Self {
            inner,
            schema,
            schema_sent: false,
        }
    }
}

impl Stream for RecordBatchStreamToFlightData {
    type Item = std::result::Result<FlightData, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        // First, send schema
        if !self.schema_sent {
            self.schema_sent = true;
            let schema_flight = arrow_flight::SchemaAsIpc::new(&self.schema, &Default::default());
            let schema_bytes: Bytes = schema_flight.try_into().unwrap_or_default();
            let flight_data = FlightData {
                data_header: schema_bytes.into(),
                data_body: Bytes::new(),
                app_metadata: Bytes::new(),
                flight_descriptor: None,
            };
            return Poll::Ready(Some(Ok(flight_data)));
        }

        // Then stream batches
        let inner = Pin::new(&mut self.inner);
        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Serialize the batch
                match ArrowStreamSerializer::serialize_batch(&batch) {
                    Ok(data) => {
                        let flight_data = FlightData {
                            data_header: Bytes::new(),
                            data_body: Bytes::from(data),
                            app_metadata: Bytes::new(),
                            flight_descriptor: None,
                        };
                        Poll::Ready(Some(Ok(flight_data)))
                    }
                    Err(e) => Poll::Ready(Some(Err(Status::internal(e.to_string())))),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Status::internal(e.to_string())))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Flight client for connecting to Datanode Flight services.
#[derive(Debug, Clone)]
pub struct DatanodeFlightClient {
    /// Target address
    addr: String,
}

impl DatanodeFlightClient {
    /// Create a new Flight client.
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    /// Execute a SQL query and return results.
    pub async fn execute_sql(
        &self,
        database: &str,
        query: &str,
        region_id: Option<u64>,
    ) -> Result<Vec<RecordBatch>> {
        let ticket = QueryTicket {
            query_id: uuid::Uuid::new_v4().to_string(),
            database: database.to_string(),
            query: query.to_string(),
            query_type: QueryType::Sql,
            region_id,
        };

        self.execute_with_ticket(ticket).await
    }

    /// Execute an InfluxQL query and return results.
    pub async fn execute_influxql(
        &self,
        database: &str,
        query: &str,
        region_id: Option<u64>,
    ) -> Result<Vec<RecordBatch>> {
        let ticket = QueryTicket {
            query_id: uuid::Uuid::new_v4().to_string(),
            database: database.to_string(),
            query: query.to_string(),
            query_type: QueryType::InfluxQL,
            region_id,
        };

        self.execute_with_ticket(ticket).await
    }

    /// Execute a query with a specific ticket.
    async fn execute_with_ticket(&self, ticket: QueryTicket) -> Result<Vec<RecordBatch>> {
        use arrow_flight::flight_service_client::FlightServiceClient;

        // Connect to the server
        let mut client = FlightServiceClient::connect(self.addr.clone())
            .await
            .map_err(|e| DistributedError::NetworkError(e.to_string()))?;

        // Serialize ticket
        let ticket_json = serde_json::to_string(&ticket)
            .map_err(|e| DistributedError::SerializationError(e.to_string()))?;

        let ticket = Ticket {
            ticket: Bytes::from(ticket_json),
        };

        // Execute do_get
        let response = client
            .do_get(ticket)
            .await
            .map_err(|e| DistributedError::NetworkError(e.to_string()))?;

        let mut stream = response.into_inner();
        let mut batches = Vec::new();
        let mut _schema: Option<SchemaRef> = None;

        while let Some(flight_data) = stream.next().await {
            let data = flight_data.map_err(|e| DistributedError::NetworkError(e.to_string()))?;

            // Check if this is schema data
            if !data.data_header.is_empty() && data.data_body.is_empty() {
                // This is schema data
                continue;
            }

            // Deserialize batch
            if !data.data_body.is_empty() {
                let decoded = ArrowStreamSerializer::deserialize_batches(&data.data_body)?;
                batches.extend(decoded);
            }
        }

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flight_config_default() {
        let config = FlightServiceConfig::default();
        assert_eq!(config.max_concurrent_queries, 100);
        assert_eq!(config.query_timeout_secs, 300);
    }

    #[test]
    fn test_query_ticket_serialization() {
        let ticket = QueryTicket {
            query_id: "test-123".to_string(),
            database: "mydb".to_string(),
            query: "SELECT * FROM cpu".to_string(),
            query_type: QueryType::Sql,
            region_id: Some(1),
        };

        let json = serde_json::to_string(&ticket).unwrap();
        let parsed: QueryTicket = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.query_id, ticket.query_id);
        assert_eq!(parsed.database, ticket.database);
    }
}
