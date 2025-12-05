//! Distributed query planner.
//!
//! The DistributedPlanner transforms single-node query plans into distributed
//! query plans that can be executed across multiple Datanodes.

use crate::common::{NodeId, RegionId, RegionInfo};
use crate::error::{DistributedError, Result};
use crate::meta::MetaServiceApi;
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use observability_deps::tracing::{debug, info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Distributed query planner.
///
/// Transforms logical plans into distributed execution plans by:
/// 1. Identifying tables involved in the query
/// 2. Getting region distribution from MetaServer
/// 3. Creating sub-plans for each relevant region
/// 4. Adding merge/aggregate nodes for result combination
#[derive(Debug)]
pub struct DistributedPlanner<M: MetaServiceApi> {
    /// MetaServer client for region information
    meta_client: Arc<M>,
}

impl<M: MetaServiceApi> DistributedPlanner<M> {
    /// Create a new DistributedPlanner.
    pub fn new(meta_client: Arc<M>) -> Self {
        Self { meta_client }
    }

    /// Plan a distributed query.
    ///
    /// This analyzes the logical plan and creates a distributed execution strategy.
    pub async fn plan(
        &self,
        database: &str,
        logical_plan: &LogicalPlan,
    ) -> Result<DistributedPlan> {
        // Extract tables from the logical plan
        let tables = self.extract_tables(logical_plan)?;

        if tables.is_empty() {
            // No tables involved, can be executed locally
            return Ok(DistributedPlan::Local {
                plan: logical_plan.clone(),
            });
        }

        // Get region information for all tables
        let mut table_regions: HashMap<String, Vec<RegionInfo>> = HashMap::new();

        for table in &tables {
            let regions = self.meta_client.get_table_regions(database, table).await?;
            table_regions.insert(table.clone(), regions);
        }

        // Check if all data is on a single node
        let all_nodes: HashSet<NodeId> = table_regions
            .values()
            .flatten()
            .map(|r| r.node_id)
            .collect();

        if all_nodes.len() == 1 {
            // All data on one node, can forward the entire query
            let node_id = all_nodes.into_iter().next().unwrap();
            return Ok(DistributedPlan::SingleNode {
                node_id,
                plan: logical_plan.clone(),
            });
        }

        // Multiple nodes involved, need distributed execution
        let stages = self.create_execution_stages(logical_plan, &table_regions)?;

        Ok(DistributedPlan::Distributed {
            stages,
            table_regions,
        })
    }

    /// Extract table names from a logical plan.
    fn extract_tables(&self, plan: &LogicalPlan) -> Result<Vec<String>> {
        let mut tables = Vec::new();

        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                tables.push(scan.table_name.table().to_string());
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .map_err(|e: DataFusionError| DistributedError::QueryPlanning(e.to_string()))?;

        // Remove duplicates
        tables.sort();
        tables.dedup();

        Ok(tables)
    }

    /// Create execution stages for distributed execution.
    fn create_execution_stages(
        &self,
        plan: &LogicalPlan,
        table_regions: &HashMap<String, Vec<RegionInfo>>,
    ) -> Result<Vec<ExecutionStage>> {
        let mut stages = Vec::new();

        // Stage 0: Scan stage - executed on each datanode
        // For each table, create scan tasks for each region
        let mut scan_tasks = Vec::new();

        for (table, regions) in table_regions {
            for region in regions {
                scan_tasks.push(ScanTask {
                    table: table.clone(),
                    region_id: region.region_id,
                    node_id: region.node_id,
                });
            }
        }

        stages.push(ExecutionStage::Scan { tasks: scan_tasks });

        // Stage 1: Merge stage - collect results from scan stage
        stages.push(ExecutionStage::Merge {
            input_stage: 0,
            // For now, we don't add additional processing in merge stage
            // In a full implementation, this would include aggregation pushdown
        });

        // Stage 2: Final stage - execute remaining operations locally
        stages.push(ExecutionStage::Final {
            plan: plan.clone(),
        });

        Ok(stages)
    }

    /// Check if a plan can be fully pushed down to a single node.
    ///
    /// This is true when:
    /// - All tables in the query are in regions on the same node
    /// - The query doesn't require cross-node operations
    pub async fn can_push_down_complete(
        &self,
        database: &str,
        plan: &LogicalPlan,
    ) -> Result<Option<NodeId>> {
        let tables = self.extract_tables(plan)?;

        if tables.is_empty() {
            return Ok(None);
        }

        let mut all_node_ids: HashSet<NodeId> = HashSet::new();

        for table in &tables {
            let regions = self.meta_client.get_table_regions(database, table).await?;
            for region in regions {
                all_node_ids.insert(region.node_id);
            }
        }

        if all_node_ids.len() == 1 {
            Ok(all_node_ids.into_iter().next())
        } else {
            Ok(None)
        }
    }
}

/// A distributed execution plan.
#[derive(Debug, Clone)]
pub enum DistributedPlan {
    /// Can be executed entirely locally (no table access).
    Local { plan: LogicalPlan },

    /// Can be forwarded to a single node.
    SingleNode { node_id: NodeId, plan: LogicalPlan },

    /// Requires distributed execution across multiple nodes.
    Distributed {
        stages: Vec<ExecutionStage>,
        table_regions: HashMap<String, Vec<RegionInfo>>,
    },
}

impl DistributedPlan {
    /// Check if this is a local-only plan.
    pub fn is_local(&self) -> bool {
        matches!(self, DistributedPlan::Local { .. })
    }

    /// Check if this can be executed on a single node.
    pub fn is_single_node(&self) -> bool {
        matches!(self, DistributedPlan::SingleNode { .. })
    }

    /// Check if this requires distributed execution.
    pub fn is_distributed(&self) -> bool {
        matches!(self, DistributedPlan::Distributed { .. })
    }

    /// Get the target node ID for single-node plans.
    pub fn single_node_target(&self) -> Option<NodeId> {
        match self {
            DistributedPlan::SingleNode { node_id, .. } => Some(*node_id),
            _ => None,
        }
    }

    /// Get all nodes involved in this plan.
    pub fn involved_nodes(&self) -> Vec<NodeId> {
        match self {
            DistributedPlan::Local { .. } => Vec::new(),
            DistributedPlan::SingleNode { node_id, .. } => vec![*node_id],
            DistributedPlan::Distributed { table_regions, .. } => {
                let mut nodes: Vec<NodeId> = table_regions
                    .values()
                    .flatten()
                    .map(|r| r.node_id)
                    .collect();
                nodes.sort();
                nodes.dedup();
                nodes
            }
        }
    }
}

/// An execution stage in a distributed plan.
#[derive(Debug, Clone)]
pub enum ExecutionStage {
    /// Scan stage - executed on each responsible datanode.
    Scan { tasks: Vec<ScanTask> },

    /// Merge stage - collects results from previous stage.
    Merge { input_stage: usize },

    /// Final stage - executes remaining operations on the frontend.
    Final { plan: LogicalPlan },
}

/// A scan task to be executed on a datanode.
#[derive(Debug, Clone)]
pub struct ScanTask {
    /// Table to scan
    pub table: String,

    /// Region to scan
    pub region_id: RegionId,

    /// Node that hosts this region
    pub node_id: NodeId,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full testing requires a mock MetaServiceApi
    // These are placeholder tests

    #[test]
    fn test_distributed_plan_is_methods() {
        // Test helper methods on DistributedPlan
        // In a full test, we'd create actual plans

        // This is a compile-time check that the types work correctly
        fn check_plan_methods(plan: DistributedPlan) {
            let _ = plan.is_local();
            let _ = plan.is_single_node();
            let _ = plan.is_distributed();
            let _ = plan.single_node_target();
            let _ = plan.involved_nodes();
        }
    }
}
