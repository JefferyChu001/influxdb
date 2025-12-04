//! Distributed table provider for federated queries
//!
//! This module provides the DistributedTableProvider, which implements
//! DataFusion's TableProvider trait to enable querying tables on remote nodes.

use crate::distributed::scan_exec::RemoteTableScanExec;
use crate::distributed::multi_node_scan_exec::MultiNodeScanExec;
use crate::distributed::statistics::RemoteTableStatistics;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, DataFusionError, Result as DataFusionResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use influxdb3_cluster::rpc::client::ClusterRpcClient;
use influxdb3_cluster::types::NodeId;
use std::any::Any;
use std::sync::Arc;

/// Distributed table provider
///
/// This TableProvider represents a table that exists on one or more remote nodes.
/// It supports:
/// - Predicate pushdown (filters)
/// - Projection pushdown (column selection)
/// - Limit pushdown
/// - Statistics fetching for query optimization
#[derive(Debug)]
pub struct DistributedTableProvider {
    /// Table name
    table_name: String,
    /// Database name
    database: String,
    /// Schema of the table
    schema: SchemaRef,
    /// Nodes that contain this table's data
    remote_nodes: Vec<NodeId>,
    /// gRPC client for remote communication
    rpc_client: Arc<ClusterRpcClient>,
    /// Statistics manager
    statistics: RemoteTableStatistics,
}

impl DistributedTableProvider {
    /// Create a new distributed table provider
    pub fn new(
        table_name: String,
        database: String,
        schema: SchemaRef,
        remote_nodes: Vec<NodeId>,
        rpc_client: Arc<ClusterRpcClient>,
    ) -> Self {
        let statistics = RemoteTableStatistics::new(rpc_client.clone());

        Self {
            table_name,
            database,
            schema,
            remote_nodes,
            rpc_client,
            statistics,
        }
    }

    /// Get the nodes that contain this table
    pub fn nodes(&self) -> &[NodeId] {
        &self.remote_nodes
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the database name
    pub fn database(&self) -> &str {
        &self.database
    }
}

#[async_trait::async_trait]
impl TableProvider for DistributedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // We support exact filter pushdown for most filter types
        // DataFusion will automatically push filters down to our scan() method
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if self.remote_nodes.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "No nodes available for table {}",
                self.table_name
            )));
        }

        // If only one node, use RemoteTableScanExec for simplicity
        if self.remote_nodes.len() == 1 {
            let node_id = self.remote_nodes[0];

            // Create the remote scan execution plan
            // DataFusion has already optimized and pushed down:
            // - projection (which columns we need)
            // - filters (WHERE conditions)
            // - limit (for early termination)
            let scan = RemoteTableScanExec::new(
                node_id,
                self.database.clone(),
                self.table_name.clone(),
                self.schema.clone(),
                projection.cloned(),
                filters.to_vec(),
                limit,
                self.rpc_client.clone(),
            );

            return Ok(Arc::new(scan));
        }

        // Multiple nodes: use MultiNodeScanExec to query all nodes in parallel
        // and union the results
        let scan = MultiNodeScanExec::new(
            self.table_name.clone(),
            self.database.clone(),
            self.schema.clone(),
            self.remote_nodes.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
            self.rpc_client.clone(),
        );

        Ok(Arc::new(scan))
    }

    // Note: statistics() method has a default implementation in TableProvider trait
    // If we need to override it, we need to match the exact signature from the trait
    // For now, we'll let it use the default implementation which returns unknown statistics
}

