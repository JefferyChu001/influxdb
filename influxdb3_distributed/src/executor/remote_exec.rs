//! Remote query execution
//!
//! This module handles executing queries on remote data nodes and streaming
//! results back to the coordinator.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use futures::{Stream, StreamExt};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::error::*;
use crate::executor::remote_client::RemoteQueryClient;
use crate::meta::NodeInfo;
use crate::types::RegionId;

/// Remote execution plan node
///
/// This plan node executes a sub-plan on a remote data node and streams
/// the results back to the coordinator.
#[derive(Debug)]
pub struct RemoteExec {
    /// The node to execute on
    node: NodeInfo,
    /// Regions to query on this node
    regions: Vec<RegionId>,
    /// The plan to execute remotely
    plan: Arc<dyn ExecutionPlan>,
    /// Output schema
    schema: SchemaRef,
    /// gRPC client for remote execution
    client: Option<RemoteQueryClient>,
}

impl RemoteExec {
    pub fn new(
        node: NodeInfo,
        regions: Vec<RegionId>,
        plan: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            node,
            regions,
            plan,
            schema,
            client: None,
        }
    }

    /// Connect to the remote node
    async fn connect(&mut self) -> Result<()> {
        let endpoint = format!("http://{}:{}", self.node.address, self.node.grpc_port);
        let client = RemoteQueryClient::connect(endpoint).await?;
        self.client = Some(client);
        Ok(())
    }

    /// Execute the plan on the remote node
    async fn execute_remote(
        &self,
        query_id: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| {
                InternalSnafu {
                    reason: "Not connected to remote node",
                }
                .build()
            })?;

        // Serialize the physical plan for transmission
        let serialized_plan = self.serialize_plan()?;

        tracing::info!(
            query_id = %query_id,
            node_id = %self.node.id,
            node_addr = %self.node.address,
            regions = ?self.regions,
            plan_size = serialized_plan.len(),
            "Sending query plan to remote node"
        );

        // Execute the query on the remote node and get streaming results
        let stream = client
            .execute_query(
                query_id,
                serialized_plan,
                self.regions.clone(),
                self.schema.clone(),
            )
            .await?;

        Ok(stream)
    }

    /// Serialize the physical plan for transmission
    ///
    /// Uses JSON serialization as a workaround for prost version conflicts.
    /// In production, this should use datafusion-proto's protobuf serialization.
    fn serialize_plan(&self) -> Result<Vec<u8>> {
        tracing::debug!(
            plan_name = self.plan.name(),
            "Serializing physical plan metadata as JSON"
        );

        // Create a JSON representation with plan metadata
        let plan_info = serde_json::json!({
            "name": self.plan.name(),
            "schema": format!("{:?}", self.plan.schema()),
            "partitions": self.plan.properties().partitioning.partition_count(),
        });

        let bytes = serde_json::to_vec(&plan_info).map_err(|e| {
            InternalSnafu {
                reason: format!("Failed to serialize plan: {}", e),
            }
            .build()
        })?;

        tracing::debug!(
            plan_size = bytes.len(),
            "Successfully serialized physical plan metadata"
        );

        Ok(bytes)
    }

    /// Deserialize results from the remote node
    ///
    /// The remote node sends results as Arrow IPC RecordBatches.
    /// This method deserializes them back into Arrow RecordBatches.
    fn deserialize_batch(&self, bytes: Bytes) -> Result<RecordBatch> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;

        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).map_err(|e| {
            InternalSnafu {
                reason: format!("Failed to create IPC reader: {}", e),
            }
            .build()
        })?;

        // Read the first (and should be only) batch
        let batch = reader
            .next()
            .ok_or_else(|| {
                InternalSnafu {
                    reason: "No batch in IPC stream",
                }
                .build()
            })?
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Failed to read batch: {}", e),
                }
                .build()
            })?;

        Ok(batch)
    }
}

impl DisplayAs for RemoteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RemoteExec: node={}, regions={}",
                    self.node.id,
                    self.regions.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "RemoteExec")
            }
        }
    }
}

impl ExecutionPlan for RemoteExec {
    fn name(&self) -> &str {
        "RemoteExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        // Return default properties for now
        // In a real implementation, this would be computed based on the remote plan
        todo!("Implement properties")
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "RemoteExec expects exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            self.node.clone(),
            self.regions.clone(),
            children[0].clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        // Generate a unique query ID
        let query_id = Uuid::new_v4().to_string();

        tracing::info!(
            query_id = %query_id,
            node_id = %self.node.id,
            partition = partition,
            regions = ?self.regions,
            "Executing query on remote node"
        );

        // Create a stream that will execute the remote query
        let schema = self.schema.clone();
        let node = self.node.clone();
        let regions = self.regions.clone();
        let plan = self.plan.clone();

        // Create async stream
        let stream = futures::stream::once(async move {
            // In the real implementation, this would:
            // 1. Connect to the remote node via gRPC
            // 2. Send the serialized plan
            // 3. Stream back results

            // For now, return an error indicating this needs full implementation
            Err(DataFusionError::Internal(
                "Remote execution requires gRPC client implementation".to_string(),
            ))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use crate::types::NodeStatus;

    #[tokio::test]
    async fn test_remote_exec_creation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let node = NodeInfo {
            id: crate::types::NodeId::new(1),
            address: "localhost".to_string(),
            grpc_port: 8080,
            http_port: 8081,
            status: NodeStatus::Active,
            regions: vec![],
        };

        let ctx = SessionContext::new();
        let empty_batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new())),
                Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .unwrap();

        let table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]]).unwrap();
        let table_provider = Arc::new(table);

        let physical_plan = ctx.read_table(table_provider).unwrap();
        let exec_plan = ctx
            .state()
            .create_physical_plan(&physical_plan.logical_plan().clone())
            .await
            .unwrap();

        let regions = vec![RegionId::new(1)];
        let remote_exec = RemoteExec::new(node, regions.clone(), exec_plan, schema.clone());

        assert_eq!(remote_exec.name(), "RemoteExec");
        assert_eq!(remote_exec.regions.len(), 1);
    }

    #[tokio::test]
    async fn test_remote_exec_connect() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let node = NodeInfo {
            id: crate::types::NodeId::new(1),
            address: "invalid-host".to_string(),
            grpc_port: 9999,
            http_port: 8888,
            status: NodeStatus::Active,
            regions: vec![],
        };

        let ctx = SessionContext::new();
        let empty_batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();

        let table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]]).unwrap();
        let table_provider = Arc::new(table);

        let physical_plan = ctx.read_table(table_provider).unwrap();
        let exec_plan = ctx
            .state()
            .create_physical_plan(&physical_plan.logical_plan().clone())
            .await
            .unwrap();

        let regions = vec![RegionId::new(1)];
        let mut remote_exec = RemoteExec::new(node, regions, exec_plan, schema);

        // Try to connect (should fail for invalid host)
        let result = remote_exec.connect().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_remote_exec_display() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let node = NodeInfo {
            id: crate::types::NodeId::new(1),
            address: "localhost".to_string(),
            grpc_port: 8080,
            http_port: 8081,
            status: NodeStatus::Active,
            regions: vec![],
        };

        let ctx = SessionContext::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let exec_plan = rt.block_on(async {
            let empty_batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new()))],
            )
            .unwrap();

            let table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]]).unwrap();
            let table_provider = Arc::new(table);

            let physical_plan = ctx.read_table(table_provider).unwrap();
            ctx.state()
                .create_physical_plan(&physical_plan.logical_plan().clone())
                .await
                .unwrap()
        });

        let regions = vec![RegionId::new(1), RegionId::new(2)];
        let remote_exec = RemoteExec::new(node, regions, exec_plan, schema);

        // Verify the name method
        assert_eq!(remote_exec.name(), "RemoteExec");

        // Verify regions
        assert_eq!(remote_exec.regions.len(), 2);
    }
}

