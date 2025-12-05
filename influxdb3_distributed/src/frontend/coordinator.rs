//! Query coordinator for distributed query execution.
//!
//! The QueryCoordinator handles the actual execution of distributed queries by:
//! - Sending sub-queries to Datanodes
//! - Collecting and merging results
//! - Handling failures and retries

use crate::common::{NodeId, NodeInfo, RegionId, RegionInfo};
use crate::datanode::DatanodeClient;
use crate::error::{DistributedError, Result};
use crate::frontend::planner::{DistributedPlan, ExecutionStage, ScanTask};
use crate::meta::MetaServiceApi;
use crate::proto::ExecutePlanRequest;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_util::MemoryStream;
use futures::FutureExt;
use futures::stream::StreamExt;
use observability_deps::tracing::{debug, error, info, warn};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Query coordinator for distributed query execution.
#[derive(Debug)]
pub struct QueryCoordinator<M: MetaServiceApi> {
    /// MetaServer client
    meta_client: Arc<M>,

    /// Datanode client
    datanode_client: DatanodeClient,

    /// Maximum concurrent queries per node
    max_concurrent_per_node: usize,

    /// Query timeout
    query_timeout: Duration,

    /// Semaphore for limiting total concurrent remote calls
    concurrency_semaphore: Arc<Semaphore>,
}

impl<M: MetaServiceApi> QueryCoordinator<M> {
    /// Create a new QueryCoordinator.
    pub fn new(meta_client: Arc<M>) -> Self {
        Self {
            meta_client,
            datanode_client: DatanodeClient::with_defaults(),
            max_concurrent_per_node: 10,
            query_timeout: Duration::from_secs(300),
            concurrency_semaphore: Arc::new(Semaphore::new(100)),
        }
    }

    /// Create a QueryCoordinator with custom settings.
    pub fn with_settings(
        meta_client: Arc<M>,
        datanode_client: DatanodeClient,
        max_concurrent: usize,
        query_timeout: Duration,
    ) -> Self {
        Self {
            meta_client,
            datanode_client,
            max_concurrent_per_node: max_concurrent,
            query_timeout,
            concurrency_semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    /// Execute a distributed plan.
    pub async fn execute(
        &self,
        database: &str,
        plan: DistributedPlan,
    ) -> Result<SendableRecordBatchStream> {
        match plan {
            DistributedPlan::Local { plan } => {
                // Execute locally using DataFusion
                self.execute_local(plan).await
            }
            DistributedPlan::SingleNode { node_id, plan } => {
                // Forward to single node
                self.execute_on_single_node(database, node_id, &plan).await
            }
            DistributedPlan::Distributed {
                stages,
                table_regions,
            } => {
                // Execute distributed plan
                self.execute_distributed(database, stages, table_regions)
                    .await
            }
        }
    }

    /// Execute a query locally.
    async fn execute_local(&self, plan: LogicalPlan) -> Result<SendableRecordBatchStream> {
        let ctx = SessionContext::new();

        let physical_plan = ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .map_err(|e| DistributedError::QueryPlanning(e.to_string()))?;

        let stream = datafusion::physical_plan::execute_stream(physical_plan, ctx.task_ctx())
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))?;

        Ok(stream)
    }

    /// Execute a query on a single node.
    async fn execute_on_single_node(
        &self,
        database: &str,
        node_id: NodeId,
        plan: &LogicalPlan,
    ) -> Result<SendableRecordBatchStream> {
        // Get node info
        let node_info = self.meta_client.get_node(node_id).await?;

        // Serialize the plan
        // For now, we'll send the SQL string instead of the serialized plan
        // In a full implementation, we'd use Substrait or DataFusion's proto serialization
        let sql = plan.to_string();

        debug!(
            node_id = %node_id,
            database = %database,
            "Forwarding query to single node"
        );

        // Execute on remote node
        // Note: This would use the actual query execution method
        // For now, we'll return an error indicating this path needs implementation
        Err(DistributedError::UnsupportedOperation(
            "Single-node forwarding not yet fully implemented".to_string(),
        ))
    }

    /// Execute a distributed plan across multiple nodes.
    async fn execute_distributed(
        &self,
        database: &str,
        stages: Vec<ExecutionStage>,
        table_regions: HashMap<String, Vec<RegionInfo>>,
    ) -> Result<SendableRecordBatchStream> {
        // For now, implement a simplified distributed execution:
        // 1. Extract scan tasks from the first stage
        // 2. Execute them in parallel on each datanode
        // 3. Merge the results

        let scan_tasks = match stages.first() {
            Some(ExecutionStage::Scan { tasks }) => tasks.clone(),
            _ => {
                return Err(DistributedError::QueryPlanning(
                    "Expected scan stage as first stage".to_string(),
                ));
            }
        };

        // Group tasks by node
        let mut tasks_by_node: HashMap<NodeId, Vec<ScanTask>> = HashMap::new();
        for task in scan_tasks {
            tasks_by_node
                .entry(task.node_id)
                .or_insert_with(Vec::new)
                .push(task);
        }

        // Get node info for all nodes
        let mut node_infos: HashMap<NodeId, NodeInfo> = HashMap::new();
        for node_id in tasks_by_node.keys() {
            let info = self.meta_client.get_node(*node_id).await?;
            node_infos.insert(*node_id, info);
        }

        // Execute scan tasks in parallel on each node
        let mut handles = Vec::new();

        for (node_id, tasks) in tasks_by_node {
            let node_info = node_infos.get(&node_id).cloned().ok_or_else(|| {
                DistributedError::NodeNotFound {
                    node_id: node_id.get(),
                }
            })?;

            let client = self.datanode_client.clone();
            let db = database.to_string();
            let semaphore = Arc::clone(&self.concurrency_semaphore);

            let handle = tokio::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|_| DistributedError::Internal("Semaphore closed".to_string()))?;

                let mut all_batches = Vec::new();

                for task in tasks {
                    // Execute a scan for this region
                    // In a full implementation, this would send the actual scan plan
                    let batches = client
                        .execute_sql(&node_info, &db, "SELECT 1", task.region_id)
                        .await?;
                    all_batches.extend(batches);
                }

                Ok::<Vec<RecordBatch>, DistributedError>(all_batches)
            });

            handles.push(handle);
        }

        // Collect results from all nodes
        let mut all_batches = Vec::new();

        for handle in handles {
            match handle.await {
                Ok(Ok(batches)) => all_batches.extend(batches),
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(DistributedError::Internal(format!(
                        "Task join error: {}",
                        e
                    )));
                }
            }
        }

        // Create a stream from the collected batches
        if all_batches.is_empty() {
            return Err(DistributedError::QueryExecution(
                "No results from distributed execution".to_string(),
            ));
        }

        let schema = all_batches[0].schema();
        let stream = Box::pin(MemoryStream::new(all_batches));

        Ok(stream)
    }

    /// Execute a simple SQL query across the cluster.
    ///
    /// This is a high-level method that handles the full query lifecycle:
    /// 1. Get table regions from MetaServer
    /// 2. Send query to all relevant nodes
    /// 3. Merge results
    pub async fn execute_sql(
        &self,
        database: &str,
        sql: &str,
    ) -> Result<SendableRecordBatchStream> {
        info!(database = %database, sql = %sql, "Executing distributed SQL query");

        // Get all online datanodes
        let nodes = self.meta_client.get_online_datanodes().await?;

        if nodes.is_empty() {
            return Err(DistributedError::ClusterNotInitialized);
        }

        // For a simple implementation, we'll send the query to all nodes
        // and merge the results. In a production implementation, we would:
        // 1. Parse the SQL to extract table names
        // 2. Get region distribution for those tables
        // 3. Only query nodes that have relevant regions

        let mut handles = Vec::new();

        for node in nodes {
            let client = self.datanode_client.clone();
            let db = database.to_string();
            let query = sql.to_string();
            let semaphore = Arc::clone(&self.concurrency_semaphore);

            let handle = tokio::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|_| DistributedError::Internal("Semaphore closed".to_string()))?;

                // For now, we use region 0 as a placeholder
                // In a real implementation, we'd query all relevant regions
                client
                    .execute_sql(&node, &db, &query, RegionId::new(0))
                    .await
            });

            handles.push(handle);
        }

        // Collect results
        let mut all_batches = Vec::new();
        let mut first_error: Option<DistributedError> = None;

        for handle in handles {
            match handle.await {
                Ok(Ok(batches)) => all_batches.extend(batches),
                Ok(Err(e)) => {
                    warn!(error = %e, "Error from one datanode, continuing with others");
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Task join error");
                }
            }
        }

        // If we got no results but had errors, return the first error
        if all_batches.is_empty() {
            if let Some(e) = first_error {
                return Err(e);
            }
            return Err(DistributedError::QueryExecution(
                "No results from any datanode".to_string(),
            ));
        }

        let schema = all_batches[0].schema();
        Ok(Box::pin(MemoryStream::new(all_batches)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full testing requires mocking MetaServiceApi and DatanodeClient
    // These are placeholder tests

    #[test]
    fn test_coordinator_creation() {
        // This would require a mock MetaServiceApi
        // For now, just verify the types compile
    }
}
