//! Multi-node scan execution plan
//!
//! This module implements a physical execution plan that queries multiple nodes
//! in parallel and unions the results.

use crate::distributed::scan_exec::RemoteTableScanExec;
use arrow::datatypes::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use influxdb3_cluster::rpc::client::ClusterRpcClient;
use influxdb3_cluster::types::NodeId;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Multi-node scan execution plan
///
/// This execution plan queries multiple nodes in parallel and returns
/// the union of all results.
#[derive(Debug)]
pub struct MultiNodeScanExec {
    /// Table name
    table_name: String,
    /// Database name
    database: String,
    /// Schema of the table
    schema: SchemaRef,
    /// Nodes to query
    nodes: Vec<NodeId>,
    /// Column projection
    projection: Option<Vec<usize>>,
    /// Filter predicates
    filters: Vec<Expr>,
    /// Limit
    limit: Option<usize>,
    /// RPC client
    rpc_client: Arc<ClusterRpcClient>,
    /// Cached plan properties
    properties: PlanProperties,
}

impl MultiNodeScanExec {
    /// Create a new multi-node scan execution plan
    pub fn new(
        table_name: String,
        database: String,
        schema: SchemaRef,
        nodes: Vec<NodeId>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        rpc_client: Arc<ClusterRpcClient>,
    ) -> Self {
        // Calculate the output schema (after projection)
        let output_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|&i| schema.field(i).clone()).collect();
            Arc::new(arrow::datatypes::Schema::new(fields))
        } else {
            schema.clone()
        };

        let properties = Self::compute_properties(&output_schema, nodes.len());

        Self {
            table_name,
            database,
            schema,
            nodes,
            projection,
            filters,
            limit,
            rpc_client,
            properties,
        }
    }

    fn compute_properties(output_schema: &SchemaRef, num_nodes: usize) -> PlanProperties {
        // Each node becomes a partition
        let partitioning = Partitioning::UnknownPartitioning(num_nodes);

        // No ordering guarantees across nodes
        let eq_properties = EquivalenceProperties::new(output_schema.clone());

        // Emission type: Final (all data in one go from each partition)
        let emission_type = EmissionType::Final;

        // Boundedness: Bounded (finite data from each node)
        let boundedness = Boundedness::Bounded;

        PlanProperties::new(eq_properties, partitioning, emission_type, boundedness)
    }

    /// Get the nodes being queried
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }
}

impl DisplayAs for MultiNodeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MultiNodeScanExec: table={}, nodes={:?}, filters={:?}",
                    self.table_name,
                    self.nodes,
                    self.filters.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "MultiNodeScan[{}]", self.table_name)
            }
        }
    }
}

impl ExecutionPlan for MultiNodeScanExec {
    fn name(&self) -> &str {
        "MultiNodeScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // No children - this is a leaf node
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Each partition corresponds to one node
        if partition >= self.nodes.len() {
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "Invalid partition {} for {} nodes",
                partition,
                self.nodes.len()
            )));
        }

        let node_id = self.nodes[partition];

        // Create a RemoteTableScanExec for this specific node
        let scan = RemoteTableScanExec::new(
            node_id,
            self.database.clone(),
            self.table_name.clone(),
            self.schema.clone(),
            self.projection.clone(),
            self.filters.clone(),
            self.limit,
            self.rpc_client.clone(),
        );

        // Execute the scan for this node
        scan.execute(0, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_multi_node_scan_properties() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let nodes = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let rpc_client = Arc::new(ClusterRpcClient::new());

        let exec = MultiNodeScanExec::new(
            "test_table".to_string(),
            "test_db".to_string(),
            schema,
            nodes.clone(),
            None,
            vec![],
            None,
            rpc_client,
        );

        assert_eq!(exec.nodes(), &nodes);
        assert_eq!(exec.properties().partitioning.partition_count(), 3);
    }
}

