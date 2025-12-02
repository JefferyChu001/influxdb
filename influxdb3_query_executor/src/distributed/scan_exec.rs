//! Remote table scan execution plan
//!
//! This module provides the RemoteTableScanExec, which is a DataFusion
//! ExecutionPlan that fetches data from remote nodes via gRPC.

use crate::distributed::expr_converter::ExprToSqlConverter;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DataFusionResult, Statistics};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::StreamExt;
use futures::Stream;
use influxdb3_cluster::rpc::client::ClusterRpcClient;
use influxdb3_cluster::types::NodeId;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Remote table scan execution plan
///
/// This ExecutionPlan fetches data from a remote node by:
/// 1. Converting filters and projections to SQL
/// 2. Sending the SQL query via gRPC
/// 3. Streaming results back as RecordBatches
#[derive(Debug)]
pub struct RemoteTableScanExec {
    /// Remote node to query
    node_id: NodeId,
    /// Database name
    database: String,
    /// Table name
    table_name: String,
    /// Schema of the table
    schema: SchemaRef,
    /// Columns to project (indices into schema)
    projection: Option<Vec<usize>>,
    /// Filter expressions for predicate pushdown
    filters: Vec<Expr>,
    /// Limit for early termination
    limit: Option<usize>,
    /// gRPC client for remote communication
    rpc_client: Arc<ClusterRpcClient>,
    /// Cached plan properties
    plan_properties: PlanProperties,
}

impl RemoteTableScanExec {
    /// Create a new remote table scan
    pub fn new(
        node_id: NodeId,
        database: String,
        table_name: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        rpc_client: Arc<ClusterRpcClient>,
    ) -> Self {
        // Calculate the output schema (after projection)
        // This is what the ExecutionPlan will actually produce
        let output_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|&i| schema.field(i).clone()).collect();
            Arc::new(arrow::datatypes::Schema::new(fields))
        } else {
            schema.clone()
        };

        // Build plan properties using the output schema
        let eq_properties = EquivalenceProperties::new(output_schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Final;
        let boundedness = Boundedness::Bounded;

        let plan_properties = PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);

        Self {
            node_id,
            database,
            table_name,
            schema,
            projection,
            filters,
            limit,
            rpc_client,
            plan_properties,
        }
    }

    /// Build the SQL query for the remote node
    fn build_query(&self) -> DataFusionResult<String> {
        // Build SELECT clause with projection
        let select_clause = if let Some(ref proj) = self.projection {
            let columns: Vec<String> = proj
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect();
            columns.join(", ")
        } else {
            "*".to_string()
        };

        // Build WHERE clause from filters
        let where_clause = if !self.filters.is_empty() {
            let where_str = ExprToSqlConverter::filters_to_where_clause(&self.filters)
                .map_err(|e| DataFusionError::Plan(format!("Failed to convert filters: {}", e)))?;
            format!(" WHERE {}", where_str)
        } else {
            String::new()
        };

        // Build LIMIT clause
        let limit_clause = self
            .limit
            .map(|l| format!(" LIMIT {}", l))
            .unwrap_or_default();

        // Combine into full query
        let query = format!(
            "SELECT {} FROM {}{}{}",
            select_clause, self.table_name, where_clause, limit_clause
        );

        Ok(query)
    }

    /// Execute the remote query and return a stream of record batches
    async fn execute_remote_query(
        node_id: NodeId,
        database: String,
        query: String,
        schema: SchemaRef,
        rpc_client: Arc<ClusterRpcClient>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Execute query via gRPC
        let stream = rpc_client
            .query_node(node_id, &database, &query)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Remote query failed: {}", e)))?;

        // Map the error type from influxdb3_cluster::Error to String
        let mapped_stream = stream.map(|result| {
            result.map_err(|e| format!("{}", e))
        });

        // Convert the stream to a SendableRecordBatchStream
        Ok(Box::pin(RemoteRecordBatchStream::new(
            Box::pin(mapped_stream),
            schema,
        )))
    }
}

impl DisplayAs for RemoteTableScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RemoteTableScanExec: node={}, table={}",
            self.node_id.as_u64(),
            self.table_name
        )
    }
}

impl ExecutionPlan for RemoteTableScanExec {
    fn name(&self) -> &str {
        "RemoteTableScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf node, no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Leaf node, return self
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "RemoteTableScanExec only supports partition 0, got {}",
                partition
            )));
        }

        // Build the query
        let query = self.build_query()?;

        // Clone necessary data for async execution
        let node_id = self.node_id;
        let database = self.database.clone();
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let rpc_client = self.rpc_client.clone();

        // Calculate the output schema (after projection)
        // Note: The SQL query already includes the projection, so the remote
        // query will return batches with the projected schema
        let output_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|&i| schema.field(i).clone()).collect();
            Arc::new(arrow::datatypes::Schema::new(fields))
        } else {
            schema.clone()
        };

        // Clone for use in the spawned task
        let output_schema_for_task = output_schema.clone();

        // Create a channel to convert the async operation into a stream
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        // Spawn a task to execute the query and send results through the channel
        tokio::spawn(async move {
            // Note: We pass output_schema instead of the full schema because the SQL
            // query already includes the projection in the SELECT clause
            match Self::execute_remote_query(node_id, database, query, output_schema_for_task, rpc_client).await {
                Ok(mut stream) => {
                    while let Some(batch_result) = stream.next().await {
                        // No need to apply projection here - the SQL query already did it
                        // Just forward the results as-is
                        if tx.send(batch_result).await.is_err() {
                            // Receiver dropped, stop sending
                            break;
                        }
                    }
                }
                Err(e) => {
                    // Send the error and close the stream
                    let _ = tx.send(Err(e)).await;
                }
            }
        });

        // Convert the receiver into a SendableRecordBatchStream with the output schema
        Ok(Box::pin(RemoteRecordBatchChannelStream::new(rx, output_schema)))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        // Return unknown statistics for now
        // TODO: Implement remote statistics fetching
        Ok(Statistics {
            num_rows: datafusion::common::stats::Precision::Absent,
            total_byte_size: datafusion::common::stats::Precision::Absent,
            column_statistics: vec![],
        })
    }
}

/// Stream adapter for remote query results using tokio channel
struct RemoteRecordBatchChannelStream {
    /// Receiver from the async task
    receiver: tokio::sync::mpsc::Receiver<DataFusionResult<RecordBatch>>,
    /// Schema of the stream
    schema: SchemaRef,
}

impl RemoteRecordBatchChannelStream {
    fn new(receiver: tokio::sync::mpsc::Receiver<DataFusionResult<RecordBatch>>, schema: SchemaRef) -> Self {
        Self { receiver, schema }
    }
}

impl Stream for RemoteRecordBatchChannelStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl RecordBatchStream for RemoteRecordBatchChannelStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Stream adapter for remote query results
struct RemoteRecordBatchStream {
    /// Inner stream from gRPC
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, String>> + Send>>,
    /// Schema of the stream
    schema: SchemaRef,
}

impl RemoteRecordBatchStream {
    fn new(
        inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, String>> + Send>>,
        schema: SchemaRef,
    ) -> Self {
        Self { inner, schema }
    }
}

impl Stream for RemoteRecordBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner
            .as_mut()
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(|e| DataFusionError::Execution(e))))
    }
}

impl RecordBatchStream for RemoteRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}


