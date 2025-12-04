//! Region-level query execution
//!
//! This module handles executing queries on specific regions, whether local or remote.
//! Based on GreptimeDB's region_query module.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;

use crate::error::*;
use crate::executor::RemoteQueryClient;
use crate::meta::MetaServiceRef;
use crate::types::{NodeId, RegionId};

/// Handler for executing queries on regions
///
/// This trait abstracts the execution of queries on regions, whether they are
/// local or remote. Implementations handle routing, execution, and result streaming.
#[async_trait]
pub trait RegionQueryHandler: Send + Sync {
    /// Execute a query on specific regions
    ///
    /// # Arguments
    /// * `regions` - The regions to query
    /// * `plan` - The logical plan to execute
    ///
    /// # Returns
    /// A stream of record batches from all regions
    async fn execute_query(
        &self,
        regions: Vec<RegionId>,
        plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream>;

    /// Get the schema for a query without executing it
    async fn get_query_schema(&self, plan: &LogicalPlan) -> Result<arrow::datatypes::SchemaRef>;
}

pub type RegionQueryHandlerRef = Arc<dyn RegionQueryHandler>;

/// Default implementation of RegionQueryHandler
///
/// This implementation uses the MetaService to locate regions and route queries
/// to the appropriate nodes.
pub struct DefaultRegionQueryHandler {
    meta_service: MetaServiceRef,
    session_ctx: Arc<SessionContext>,
    /// Cache of remote clients by node ID
    remote_clients: Arc<parking_lot::RwLock<std::collections::HashMap<NodeId, RemoteQueryClient>>>,
}

impl DefaultRegionQueryHandler {
    pub fn new(meta_service: MetaServiceRef, session_ctx: Arc<SessionContext>) -> Self {
        Self {
            meta_service,
            session_ctx,
            remote_clients: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Get or create a remote client for a node
    async fn get_remote_client(&self, node_id: NodeId) -> Result<RemoteQueryClient> {
        // Check cache first
        {
            let clients = self.remote_clients.read();
            if let Some(client) = clients.get(&node_id) {
                return Ok(client.clone());
            }
        }

        // Not in cache, create new client
        let node = self.meta_service.get_node(node_id).await?;
        let endpoint = format!("http://{}:{}", node.address, node.grpc_port);

        let client = RemoteQueryClient::connect(endpoint).await?;

        // Cache it
        {
            let mut clients = self.remote_clients.write();
            clients.insert(node_id, client.clone());
        }

        Ok(client)
    }

    /// Group regions by their hosting node
    async fn group_regions_by_node(
        &self,
        regions: &[RegionId],
    ) -> Result<std::collections::HashMap<NodeId, Vec<RegionId>>> {
        let mut by_node = std::collections::HashMap::new();

        for &region_id in regions {
            let region_meta = self.meta_service.get_region(region_id).await?;
            by_node
                .entry(region_meta.node_id)
                .or_insert_with(Vec::new)
                .push(region_id);
        }

        Ok(by_node)
    }
}

#[async_trait]
impl RegionQueryHandler for DefaultRegionQueryHandler {
    async fn execute_query(
        &self,
        regions: Vec<RegionId>,
        plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream> {
        use futures::stream::{self, StreamExt};

        tracing::info!(
            num_regions = regions.len(),
            "Executing query on regions"
        );

        // Group regions by node
        let regions_by_node = self.group_regions_by_node(&regions).await?;

        // Create physical plan
        let physical_plan = self
            .session_ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Failed to create physical plan: {}", e),
                }
                .build()
            })?;

        // Collect streams from all nodes
        let mut streams = Vec::new();

        for (node_id, node_regions) in regions_by_node {
            tracing::debug!(
                node_id = %node_id,
                num_regions = node_regions.len(),
                "Executing query on node"
            );

            // Get remote client
            let client = self.get_remote_client(node_id).await?;

            // Serialize plan
            let serialized_plan = serde_json::to_vec(&serde_json::json!({
                "plan": format!("{:?}", physical_plan),
            }))
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Failed to serialize plan: {}", e),
                }
                .build()
            })?;

            // Execute query on remote node
            let query_id = uuid::Uuid::new_v4().to_string();
            let schema = physical_plan.schema();

            let stream = client
                .execute_query(query_id, serialized_plan, node_regions, schema)
                .await?;

            streams.push(stream);
        }

        // Merge all streams
        if streams.is_empty() {
            return Err(InternalSnafu {
                reason: "No streams to merge",
            }
            .build());
        }

        if streams.len() == 1 {
            // Single stream, return it directly
            let stream = streams.into_iter().next().unwrap();
            let schema = physical_plan.schema();

            // Convert error types
            let converted_stream = stream.map(|result| {
                result.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
            });

            Ok(Box::pin(
                datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    converted_stream,
                ),
            ))
        } else {
            // Multiple streams, merge them
            let schema = physical_plan.schema();

            // Convert to SendableRecordBatchStream and collect all batches
            let mut all_batches = Vec::new();

            for s in streams {
                // Convert error types
                let converted = s.map(|result| {
                    result.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
                });

                let stream = datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema.clone(),
                    converted,
                );

                // Collect all batches from this stream
                use datafusion::physical_plan::common;
                let batches = common::collect(Box::pin(stream))
                    .await
                    .map_err(|e| {
                        InternalSnafu {
                            reason: format!("Failed to collect stream: {}", e),
                        }
                        .build()
                    })?;

                all_batches.extend(batches);
            }

            // Convert back to stream
            let stream = futures::stream::iter(all_batches.into_iter().map(Ok));
            Ok(Box::pin(
                datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
            ))
        }
    }

    async fn get_query_schema(&self, plan: &LogicalPlan) -> Result<arrow::datatypes::SchemaRef> {
        Ok(Arc::new(plan.schema().as_arrow().clone()))
    }
}

