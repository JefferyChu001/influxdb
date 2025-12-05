//! gRPC server implementation for Datanode.
//!
//! This module provides the gRPC service implementation for Datanode,
//! handling query execution and write requests.

use crate::common::{NodeId, RegionId, RegionInfo, RegionStatus};
use crate::datanode::DatanodeApi;
use crate::datanode::executor::LocalExecutor;
use crate::datanode::server::DatanodeServer;
use crate::error::{DistributedError, Result};
use crate::frontend::plan_serde::ArrowStreamSerializer;
use crate::proto::{GetRegionStatusRequest, GetRegionStatusResponse, WriteRequest, WriteResponse};
use arrow::array::RecordBatch;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use observability_deps::tracing::{debug, error, info, trace, warn};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, Streaming, transport::Server};

/// gRPC server for Datanode.
pub struct DatanodeGrpcServer {
    /// Inner Datanode server
    inner: Arc<DatanodeServer>,
    /// Local executor for query execution
    executor: Arc<LocalExecutor>,
    /// Server address
    addr: SocketAddr,
}

impl DatanodeGrpcServer {
    /// Create a new DatanodeGrpcServer.
    pub fn new(inner: Arc<DatanodeServer>, executor: Arc<LocalExecutor>, addr: SocketAddr) -> Self {
        Self {
            inner,
            executor,
            addr,
        }
    }

    /// Start the gRPC server.
    ///
    /// This starts the Datanode's gRPC server which uses Arrow Flight for query execution.
    /// The Flight service is the main interface for executing queries and streaming results.
    pub async fn run(self, shutdown: oneshot::Receiver<()>) -> Result<()> {
        info!(addr = %self.addr, "Starting Datanode gRPC server");

        // Create the Flight service for query execution
        let flight_service = crate::datanode::flight_service::DatanodeFlightService::new(
            self.inner.node_id(),
            Arc::clone(&self.executor),
        );

        // Create the gRPC service handler for other operations
        let _grpc_service =
            DatanodeGrpcService::new(Arc::clone(&self.inner), Arc::clone(&self.executor));

        // Start the server with the Flight service
        // Note: In the full implementation, we would also add the generated proto service
        // once the proto files are compiled. For now, we use Flight for queries.
        Server::builder()
            .add_service(flight_service.into_server())
            .serve_with_shutdown(self.addr, async {
                let _ = shutdown.await;
                info!("Datanode gRPC server shutting down");
            })
            .await
            .map_err(|e| DistributedError::NetworkError(e.to_string()))?;

        Ok(())
    }
}

/// gRPC service implementation for Datanode.
///
/// This provides the handler methods for Datanode gRPC operations.
/// When the proto files are compiled, these methods will be called by the generated service.
pub struct DatanodeGrpcService {
    inner: Arc<DatanodeServer>,
    executor: Arc<LocalExecutor>,
}

impl DatanodeGrpcService {
    /// Create a new DatanodeGrpcService.
    pub fn new(inner: Arc<DatanodeServer>, executor: Arc<LocalExecutor>) -> Self {
        Self { inner, executor }
    }

    /// Handle SQL query execution request.
    pub async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> std::result::Result<Response<QueryResultStream>, Status> {
        let req = request.into_inner();
        info!(
            database = %req.database,
            query = %req.query,
            region_id = req.region_id,
            "Executing query"
        );

        let stream = match req.query_type {
            0 | 1 => {
                // SQL query
                self.executor
                    .execute_sql(&req.database, &req.query, None, None, None)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            }
            2 => {
                // InfluxQL query - parse the statement first
                let statements = influxdb_influxql_parser::parse_statements(&req.query)
                    .map_err(|e| Status::invalid_argument(format!("Invalid InfluxQL: {}", e)))?;
                let statement = statements
                    .into_iter()
                    .next()
                    .ok_or_else(|| Status::invalid_argument("Empty InfluxQL query"))?;
                self.executor
                    .execute_influxql(&req.database, &req.query, statement, None, None, None)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            }
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unknown query type: {}",
                    req.query_type
                )));
            }
        };

        // Convert to stream of QueryResultBatch
        let result_stream = stream.map(|batch_result| {
            batch_result
                .map(|batch| {
                    let serialized =
                        ArrowStreamSerializer::serialize_batch(&batch).unwrap_or_default();
                    QueryResultBatch {
                        arrow_data: Bytes::from(serialized),
                        num_rows: batch.num_rows() as u64,
                        is_last: false,
                        error_message: None,
                    }
                })
                .map_err(|e| Status::internal(e.to_string()))
        });

        Ok(Response::new(Box::pin(result_stream)))
    }

    /// Handle physical plan execution request.
    pub async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequestLocal>,
    ) -> std::result::Result<Response<QueryResultStream>, Status> {
        let req = request.into_inner();
        info!(
            database = %req.database,
            region_id = req.region_id,
            plan_size = req.plan_bytes.len(),
            "Executing physical plan"
        );

        // Construct the proto request
        let proto_request = crate::proto::ExecutePlanRequest {
            plan_bytes: req.plan_bytes.clone(),
            database: req.database.clone(),
            region_id: RegionId::new(req.region_id),
            query_id: req.query_id.clone(),
        };

        // Deserialize and execute the plan
        let result = self.inner.execute_plan(proto_request).await;

        match result {
            Ok(stream) => {
                // Convert the RecordBatchStream to QueryResultBatch stream
                let result_stream = stream.map(|batch_result| {
                    batch_result
                        .map(|batch| {
                            let serialized =
                                ArrowStreamSerializer::serialize_batch(&batch).unwrap_or_default();
                            QueryResultBatch {
                                arrow_data: Bytes::from(serialized),
                                num_rows: batch.num_rows() as u64,
                                is_last: false,
                                error_message: None,
                            }
                        })
                        .map_err(|e| Status::internal(e.to_string()))
                });

                Ok(Response::new(Box::pin(result_stream))).collect();

                let stream = stream::iter(batch_results);
                Ok(Response::new(Box::pin(stream)))
            }
            Err(e) => {
                let error_batch = QueryResultBatch {
                    arrow_data: Bytes::new(),
                    num_rows: 0,
                    is_last: true,
                    error_message: Some(e.to_string()),
                };
                let stream = stream::once(async { Ok(error_batch) });
                Ok(Response::new(Box::pin(stream)))
            }
        }
    }

    /// Handle write request.
    pub async fn write(
        &self,
        request: Request<WriteRequestProto>,
    ) -> std::result::Result<Response<WriteResponseProto>, Status> {
        let req = request.into_inner();
        info!(
            database = %req.database,
            region_id = req.region_id,
            data_size = req.data.len(),
            "Processing write request"
        );

        let write_req = WriteRequest {
            database: req.database,
            region_id: RegionId::new(req.region_id),
            data: req.data,
            precision: crate::proto::WritePrecision::Nanoseconds,
        };

        match self.inner.write(&write_req).await {
            Ok(response) => {
                let proto_response = WriteResponseProto {
                    success: response.success,
                    points_written: response.points_written,
                    error_message: response.error_message,
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// Handle batch write request (streaming).
    pub async fn batch_write(
        &self,
        request: Request<Streaming<WriteRequestProto>>,
    ) -> std::result::Result<Response<WriteResponseProto>, Status> {
        let mut stream = request.into_inner();
        let mut total_points = 0u64;
        let mut errors = Vec::new();

        while let Some(req_result) = stream.next().await {
            match req_result {
                Ok(req) => {
                    let write_req = WriteRequest {
                        database: req.database,
                        region_id: RegionId::new(req.region_id),
                        data: req.data,
                        precision: crate::proto::WritePrecision::Nanoseconds,
                    };

                    match self.inner.write(&write_req).await {
                        Ok(response) => {
                            total_points += response.points_written;
                        }
                        Err(e) => {
                            errors.push(e.to_string());
                        }
                    }
                }
                Err(e) => {
                    errors.push(e.to_string());
                }
            }
        }

        let error_message = if errors.is_empty() {
            None
        } else {
            Some(errors.join("; "))
        };

        Ok(Response::new(WriteResponseProto {
            success: errors.is_empty(),
            points_written: total_points,
            error_message,
        }))
    }

    /// Handle get region status request.
    pub async fn get_region_status(
        &self,
        request: Request<GetRegionStatusRequestProto>,
    ) -> std::result::Result<Response<GetRegionStatusResponseProto>, Status> {
        let req = request.into_inner();
        debug!(region_id = req.region_id, "Getting region status");

        match self
            .inner
            .get_region_status(RegionId::new(req.region_id))
            .await
        {
            Ok(response) => {
                let proto_response = GetRegionStatusResponseProto {
                    region_id: response.region_id.get(),
                    status: match response.status {
                        RegionStatus::Active => 1,
                        RegionStatus::ReadOnly => 2,
                        RegionStatus::Migrating => 3,
                        RegionStatus::Offline => 4,
                        RegionStatus::Creating => 5,
                        RegionStatus::Deleting => 6,
                    },
                    row_count: response.row_count,
                    size_bytes: response.size_bytes,
                    last_write_time: response.last_write_time,
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// Handle get managed regions request.
    pub async fn get_managed_regions(
        &self,
        _request: Request<GetManagedRegionsRequest>,
    ) -> std::result::Result<Response<GetManagedRegionsResponse>, Status> {
        debug!("Getting managed regions");

        let regions = self.inner.managed_regions().await;

        let proto_regions: Vec<_> = regions
            .into_iter()
            .map(|r| RegionInfoProto {
                region_id: r.region_id.get(),
                database: r.database,
                table: r.table,
                node_id: r.node_id.get(),
                partition_range: Some(PartitionRangeProto {
                    time_start_ns: r.partition_range.time_start_ns,
                    time_end_ns: r.partition_range.time_end_ns,
                    hash_start: r.partition_range.hash_start as u32,
                    hash_end: r.partition_range.hash_end as u32,
                }),
                status: match r.status {
                    RegionStatus::Active => 1,
                    RegionStatus::ReadOnly => 2,
                    RegionStatus::Migrating => 3,
                    RegionStatus::Offline => 4,
                    RegionStatus::Creating => 5,
                    RegionStatus::Deleting => 6,
                },
                epoch: r.epoch,
            })
            .collect();

        Ok(Response::new(GetManagedRegionsResponse {
            regions: proto_regions,
        }))
    }

    /// Handle health check request.
    pub async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> std::result::Result<Response<HealthCheckResponse>, Status> {
        debug!("Health check");

        let status = self.inner.get_status().await;

        Ok(Response::new(HealthCheckResponse {
            healthy: status.status == crate::common::NodeStatus::Online,
            status: match status.status {
                crate::common::NodeStatus::Starting => 1,
                crate::common::NodeStatus::Online => 2,
                crate::common::NodeStatus::Offline => 3,
                crate::common::NodeStatus::Maintenance => 4,
                crate::common::NodeStatus::Draining => 5,
            },
            metrics: status.metrics.map(|m| NodeMetricsProto {
                cpu_usage: m.cpu_usage,
                memory_usage: m.memory_usage,
                disk_usage: m.disk_usage,
                active_connections: m.active_connections,
                queries_per_second: m.queries_per_second,
            }),
            message: None,
        }))
    }
}

// Type alias for query result stream
type QueryResultStream =
    Pin<Box<dyn Stream<Item = std::result::Result<QueryResultBatch, Status>> + Send>>;

// Placeholder proto message types

#[derive(Debug, Clone)]
pub struct ExecuteQueryRequest {
    pub database: String,
    pub query: String,
    pub query_type: i32, // 0=unspecified, 1=SQL, 2=InfluxQL
    pub region_id: u64,
    pub query_id: String,
    pub params: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ExecutePlanRequest {
    pub plan_bytes: Bytes,
    pub database: String,
    pub region_id: u64,
    pub query_id: String,
}

#[derive(Debug, Clone)]
pub struct QueryResultBatch {
    pub arrow_data: Bytes,
    pub num_rows: u64,
    pub is_last: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WriteRequestProto {
    pub database: String,
    pub region_id: u64,
    pub data: Bytes,
    pub precision: i32,
    pub no_sync: bool,
}

#[derive(Debug, Clone)]
pub struct WriteResponseProto {
    pub success: bool,
    pub points_written: u64,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetRegionStatusRequestProto {
    pub region_id: u64,
}

#[derive(Debug, Clone)]
pub struct GetRegionStatusResponseProto {
    pub region_id: u64,
    pub status: i32,
    pub row_count: u64,
    pub size_bytes: u64,
    pub last_write_time: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct GetManagedRegionsRequest {}

#[derive(Debug, Clone)]
pub struct GetManagedRegionsResponse {
    pub regions: Vec<RegionInfoProto>,
}

#[derive(Debug, Clone)]
pub struct HealthCheckRequest {}

#[derive(Debug, Clone)]
pub struct HealthCheckResponse {
    pub healthy: bool,
    pub status: i32,
    pub metrics: Option<NodeMetricsProto>,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RegionInfoProto {
    pub region_id: u64,
    pub database: String,
    pub table: String,
    pub node_id: u64,
    pub partition_range: Option<PartitionRangeProto>,
    pub status: i32,
    pub epoch: u64,
}

#[derive(Debug, Clone)]
pub struct PartitionRangeProto {
    pub time_start_ns: Option<i64>,
    pub time_end_ns: Option<i64>,
    pub hash_start: u32,
    pub hash_end: u32,
}

#[derive(Debug, Clone)]
pub struct NodeMetricsProto {
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub disk_usage: f32,
    pub active_connections: u32,
    pub queries_per_second: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_type_handling() {
        // SQL type
        assert_eq!(0, 0); // unspecified treated as SQL
        assert_eq!(1, 1); // explicit SQL
        assert_eq!(2, 2); // InfluxQL
    }
}
