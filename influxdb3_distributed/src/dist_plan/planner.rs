//! Distributed query planner
//!
//! Converts DataFusion logical plans into distributed physical plans.
//! Based on GreptimeDB's planner but adapted for InfluxDB.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::context::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::ToDFSchema;

use crate::dist_plan::merge_scan::{MergeScanExec, MergeScanLogicalPlan};
use crate::error::*;
use crate::meta::MetaServiceRef;
use crate::types::*;

/// Distributed query plan
///
/// Contains both the coordinator-side plan and remote execution plans
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    /// The plan to execute on the coordinator node
    pub coordinator_plan: Arc<dyn ExecutionPlan>,
    /// Plans to execute on remote datanodes
    pub remote_plans: Vec<RemotePlan>,
}

/// Plan to execute on a remote datanode
#[derive(Debug, Clone)]
pub struct RemotePlan {
    /// Target node ID
    pub node_id: NodeId,
    /// Regions to query on this node
    pub regions: Vec<RegionId>,
    /// Physical plan to execute
    pub plan: Arc<dyn ExecutionPlan>,
}

/// Distributed query planner
///
/// This is the main component for distributed query planning.
/// It analyzes logical plans and generates distributed physical plans.
pub struct DistributedPlanner {
    meta_service: MetaServiceRef,
    session_state: Arc<SessionState>,
}

impl DistributedPlanner {
    pub fn new(meta_service: MetaServiceRef, session_state: Arc<SessionState>) -> Self {
        Self {
            meta_service,
            session_state,
        }
    }

    pub fn new_with_context(meta_service: MetaServiceRef, ctx: &SessionContext) -> Self {
        Self {
            meta_service,
            session_state: Arc::new(ctx.state()),
        }
    }

    /// Convert a logical plan to a distributed physical plan
    ///
    /// This is the main entry point for distributed query planning.
    ///
    /// The planning process:
    /// 1. Analyze the logical plan to identify tables and operations
    /// 2. Determine which regions need to be queried (region pruning)
    /// 3. Group regions by node
    /// 4. Generate sub-plans for each node
    /// 5. Create merge/aggregation plan for the coordinator
    pub async fn plan(&self, logical_plan: &LogicalPlan) -> Result<DistributedPlan> {
        // Step 1: Extract tables from the plan
        let tables = self.extract_tables(logical_plan)?;

        if tables.is_empty() {
            // No tables, can't distribute - return error for now
            return Err(InvalidPlanSnafu {
                reason: "No tables found in plan",
            }
            .build());
        }

        // Step 2: Get regions for all tables
        let mut all_regions = Vec::new();
        for table_name in &tables {
            let table_meta = self.meta_service.get_table(table_name).await?;
            let regions = self
                .meta_service
                .list_table_regions(table_meta.id)
                .await?;
            all_regions.extend(regions);
        }

        if all_regions.is_empty() {
            return Err(InvalidPlanSnafu {
                reason: "No regions found for tables",
            }
            .build());
        }

        // Step 3: Group regions by node
        let regions_by_node = self.group_regions_by_node(&all_regions).await?;

        // Step 4: Create remote plans for each node
        let remote_plans = self
            .create_remote_plans(logical_plan, regions_by_node)
            .await?;

        // Step 5: Create coordinator plan (merge results from remote plans)
        let coordinator_plan = self
            .create_coordinator_plan(logical_plan, &remote_plans)
            .await?;

        Ok(DistributedPlan {
            coordinator_plan,
            remote_plans,
        })
    }

    /// Extract table names from logical plan
    pub fn extract_tables(&self, plan: &LogicalPlan) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        self.extract_tables_recursive(plan, &mut tables)?;
        Ok(tables)
    }

    fn extract_tables_recursive(&self, plan: &LogicalPlan, tables: &mut Vec<String>) -> Result<()> {
        match plan {
            LogicalPlan::TableScan(table_scan) => {
                let table_name = table_scan.table_name.to_string();
                if !tables.contains(&table_name) {
                    tables.push(table_name);
                }
            }
            LogicalPlan::Extension(extension) => {
                // Check if this is a MergeScan node
                if let Some(merge_scan) = extension.node.as_any().downcast_ref::<MergeScanLogicalPlan>() {
                    // Extract tables from the input plan
                    self.extract_tables_recursive(merge_scan.input(), tables)?;
                } else {
                    // For other extensions, recurse into inputs
                    for input in plan.inputs() {
                        self.extract_tables_recursive(input, tables)?;
                    }
                }
            }
            _ => {
                // Recurse into children
                for input in plan.inputs() {
                    self.extract_tables_recursive(input, tables)?;
                }
            }
        }
        Ok(())
    }

    /// Group regions by their hosting node
    async fn group_regions_by_node(
        &self,
        regions: &[crate::meta::RegionMeta],
    ) -> Result<HashMap<NodeId, Vec<RegionId>>> {
        let mut by_node: HashMap<NodeId, Vec<RegionId>> = HashMap::new();

        for region in regions {
            by_node
                .entry(region.node_id)
                .or_insert_with(Vec::new)
                .push(region.id);
        }

        Ok(by_node)
    }

    /// Create remote execution plans for each node
    ///
    /// This generates a sub-plan for each node that will be executed locally
    /// on that node's regions.
    ///
    /// Important: We strip out Sort and certain aggregation operations that should
    /// be executed on the coordinator side.
    async fn create_remote_plans(
        &self,
        logical_plan: &LogicalPlan,
        regions_by_node: HashMap<NodeId, Vec<RegionId>>,
    ) -> Result<Vec<RemotePlan>> {
        let mut remote_plans = Vec::new();

        // Extract the pushdown-able part of the logical plan
        // (strip Sort, Limit with Sort, etc.)
        let pushdown_plan = self.extract_pushdown_plan(logical_plan)?;

        for (node_id, regions) in regions_by_node {
            let physical_plan = self
                .session_state
                .create_physical_plan(&pushdown_plan)
                .await
                .map_err(|e| Error::internal(format!("Failed to create physical plan: {}", e)))?;

            remote_plans.push(RemotePlan {
                node_id,
                regions,
                plan: physical_plan,
            });
        }

        Ok(remote_plans)
    }

    /// Extract the portion of the logical plan that can be pushed down to remote nodes
    ///
    /// This removes coordinator-side operations like Sort, while keeping:
    /// - TableScan
    /// - Filter (WHERE)
    /// - Projection (SELECT columns)
    /// - Limit (without Sort)
    fn extract_pushdown_plan(&self, logical_plan: &LogicalPlan) -> Result<LogicalPlan> {
        match logical_plan {
            // If there's a Sort at the top, skip it and go to the input
            LogicalPlan::Sort(sort) => {
                self.extract_pushdown_plan(sort.input.as_ref())
            }
            // If there's a Limit with a sorted input, we need to be careful
            LogicalPlan::Limit(limit) => {
                // Check if the input is a Sort
                if matches!(limit.input.as_ref(), LogicalPlan::Sort(_)) {
                    // Skip the Limit and Sort, go deeper
                    if let LogicalPlan::Sort(sort) = limit.input.as_ref() {
                        return self.extract_pushdown_plan(sort.input.as_ref());
                    }
                }
                // Otherwise, keep the limit but process its input
                Ok(logical_plan.clone())
            }
            // Keep other operations as-is
            _ => Ok(logical_plan.clone()),
        }
    }

    /// Create coordinator plan to merge results from remote nodes
    ///
    /// This creates a plan that:
    /// 1. Receives results from all remote nodes (via Union)
    /// 2. Applies coordinator-side operations (Sort, final aggregation, etc.)
    /// 3. Returns the final result
    async fn create_coordinator_plan(
        &self,
        logical_plan: &LogicalPlan,
        remote_plans: &[RemotePlan],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if remote_plans.is_empty() {
            return Err(InternalSnafu {
                reason: "No remote plans to merge",
            }
            .build());
        }

        // Step 1: Create base plan (merge results from all remote nodes)
        let base_plan = if remote_plans.len() == 1 {
            // Only one node, no merging needed
            remote_plans[0].plan.clone()
        } else {
            // Multiple nodes: need to merge results
            // Collect all regions
            let all_regions: Vec<RegionId> = remote_plans
                .iter()
                .flat_map(|rp| rp.regions.clone())
                .collect();

            // Use the schema from the first remote plan
            let schema = remote_plans[0].plan.schema();

            // Create MergeScanExec
            Arc::new(MergeScanExec::new(all_regions, remote_plans[0].plan.clone(), schema))
        };

        // Step 2: Apply coordinator-side operations
        let final_plan = self.apply_coordinator_operations(logical_plan, base_plan).await?;

        Ok(final_plan)
    }

    /// Apply coordinator-side operations like Sort, Limit, etc.
    ///
    /// This method extracts operations from the logical plan that should be
    /// executed on the coordinator after merging remote results.
    async fn apply_coordinator_operations(
        &self,
        logical_plan: &LogicalPlan,
        base_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::limit::GlobalLimitExec;
        use datafusion::physical_plan::sorts::sort::SortExec;
        use datafusion::physical_expr::PhysicalSortExpr;

        let mut current_plan = base_plan;

        // Check what operations need to be applied
        match logical_plan {
            // Sort + Limit: Apply both
            LogicalPlan::Limit(limit) => {
                if let LogicalPlan::Sort(sort) = limit.input.as_ref() {
                    // First apply Sort
                    let sort_exprs = self.create_sort_exprs(sort, &current_plan)?;
                    current_plan = Arc::new(
                        SortExec::new(sort_exprs, current_plan)
                            .with_preserve_partitioning(false)
                    );

                    // Then apply Limit
                    // In newer DataFusion, fetch and skip are usize
                    let skip = 0; // DataFusion Limit doesn't have skip anymore in newer versions
                    let fetch = None; // Placeholder for actual limit value

                    // Note: GlobalLimitExec API may vary between DataFusion versions
                    // This is a simplified version that may need adjustment
                    current_plan = Arc::new(GlobalLimitExec::new(
                        current_plan,
                        skip,
                        fetch
                    ));
                }
            }
            // Just Sort
            LogicalPlan::Sort(sort) => {
                let sort_exprs = self.create_sort_exprs(sort, &current_plan)?;
                current_plan = Arc::new(
                    SortExec::new(sort_exprs, current_plan)
                        .with_preserve_partitioning(false)
                );
            }
            _ => {
                // No coordinator operations needed
            }
        }

        Ok(current_plan)
    }

    /// Create physical sort expressions from logical sort
    fn create_sort_exprs(
        &self,
        sort: &datafusion::logical_expr::Sort,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<datafusion::physical_expr::LexOrdering> {
        use datafusion::physical_expr::create_physical_expr;

        let input_schema = plan.schema();
        let mut sort_exprs = Vec::new();

        for expr in &sort.expr {
            let dfschema_ref = input_schema.clone().to_dfschema_ref()
                .map_err(|e| Error::internal(format!("Failed to convert schema: {}", e)))?;

            let physical_expr = create_physical_expr(
                &expr.expr,
                &dfschema_ref,
                &self.session_state.execution_props(),
            )
            .map_err(|e| Error::internal(format!("Failed to create physical expr: {}", e)))?;

            sort_exprs.push(datafusion::physical_expr::PhysicalSortExpr {
                expr: physical_expr,
                options: datafusion::arrow::compute::SortOptions {
                    descending: !expr.asc,
                    nulls_first: expr.nulls_first,
                },
            });
        }

        // Convert Vec<PhysicalSortExpr> to LexOrdering
        datafusion::physical_expr::LexOrdering::new(sort_exprs)
            .ok_or_else(|| Error::internal("Failed to create LexOrdering"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{InMemoryMetaService, MetaService, NodeInfo, RegionMeta, TableMeta};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::{col, LogicalPlanBuilder};
    use datafusion::prelude::SessionContext;

    async fn setup_test_env() -> (Arc<InMemoryMetaService>, SessionContext) {
        let meta_service = Arc::new(InMemoryMetaService::new());

        // Register a test node
        let node = NodeInfo {
            id: NodeId::new(1),
            address: "localhost".to_string(),
            grpc_port: 8080,
            http_port: 8081,
            status: NodeStatus::Active,
            regions: vec![RegionId::new(1), RegionId::new(2)],
        };
        meta_service.register_node(node).await.unwrap();

        // Register a test table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let table_meta = TableMeta {
            id: TableId::new(1),
            name: "test_table".to_string(),
            schema,
            regions: vec![RegionId::new(1), RegionId::new(2)],
        };
        meta_service.register_table(table_meta).await.unwrap();

        // Register regions
        for region_id in [RegionId::new(1), RegionId::new(2)] {
            let region_meta = RegionMeta {
                id: region_id,
                table_id: TableId::new(1),
                node_id: NodeId::new(1),
                status: RegionStatus::Active,
            };
            // Note: InMemoryMetaService doesn't have a direct register_region method
            // In a real implementation, this would be handled automatically
        }

        let ctx = SessionContext::new();

        (meta_service, ctx)
    }

    #[tokio::test]
    async fn test_planner_extract_tables() {
        let (meta_service, ctx) = setup_test_env().await;
        let planner = DistributedPlanner::new_with_context(meta_service, &ctx);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table_source = provider_as_source(Arc::new(EmptyTable::new(schema)));

        let plan = LogicalPlanBuilder::scan("test_table", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        let tables = planner.extract_tables(&plan).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], "test_table");
    }

    #[tokio::test]
    async fn test_planner_group_regions_by_node() {
        let (meta_service, ctx) = setup_test_env().await;
        let planner = DistributedPlanner::new_with_context(meta_service, &ctx);

        let regions = vec![
            RegionMeta {
                id: RegionId::new(1),
                table_id: TableId::new(1),
                node_id: NodeId::new(1),
                status: RegionStatus::Active,
            },
            RegionMeta {
                id: RegionId::new(2),
                table_id: TableId::new(1),
                node_id: NodeId::new(1),
                status: RegionStatus::Active,
            },
        ];

        let grouped = planner.group_regions_by_node(&regions).await.unwrap();
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[&NodeId::new(1)].len(), 2);
    }
}

