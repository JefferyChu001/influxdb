//! Distributed query optimizer.
//!
//! This module implements query optimizations for distributed execution:
//! - Predicate pushdown: Push filters to datanodes
//! - Projection pushdown: Reduce data transferred by projecting early
//! - Aggregate pushdown: Compute partial aggregates on datanodes
//! - Limit pushdown: Apply limits at datanodes when possible

use crate::common::{RegionId, RegionInfo};
use crate::error::{DistributedError, Result};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{
    Aggregate, Extension, Filter, Join, Limit, LogicalPlan, Projection, Sort, TableScan,
};
use datafusion::prelude::Expr;
use observability_deps::tracing::{debug, info, trace};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Optimizer for distributed query plans.
///
/// This optimizer analyzes logical plans and determines which operations
/// can be pushed down to datanodes for more efficient distributed execution.
#[derive(Debug)]
pub struct DistributedOptimizer {
    /// Configuration for optimization
    config: OptimizerConfig,
}

/// Configuration for the distributed optimizer.
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    /// Enable predicate pushdown
    pub enable_predicate_pushdown: bool,
    /// Enable projection pushdown
    pub enable_projection_pushdown: bool,
    /// Enable aggregate pushdown (partial aggregates)
    pub enable_aggregate_pushdown: bool,
    /// Enable limit pushdown
    pub enable_limit_pushdown: bool,
    /// Enable sort pushdown
    pub enable_sort_pushdown: bool,
    /// Maximum number of regions to query in parallel
    pub max_parallel_regions: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enable_predicate_pushdown: true,
            enable_projection_pushdown: true,
            enable_aggregate_pushdown: true,
            enable_limit_pushdown: true,
            enable_sort_pushdown: true,
            max_parallel_regions: 32,
        }
    }
}

/// Result of analyzing a query plan for distributed execution.
#[derive(Debug, Clone)]
pub struct OptimizedPlan {
    /// The original logical plan
    pub original_plan: LogicalPlan,
    /// Per-region plans (plan that should run on each datanode)
    pub region_plans: HashMap<RegionId, RegionPlan>,
    /// Final aggregation plan (runs on frontend after collecting results)
    pub final_plan: Option<FinalPlan>,
    /// Tables referenced by the query
    pub referenced_tables: HashSet<String>,
    /// Whether the query can potentially benefit from distribution
    pub is_distributable: bool,
    /// Optimization statistics
    pub stats: OptimizationStats,
}

/// A plan to execute on a specific region.
#[derive(Debug, Clone)]
pub struct RegionPlan {
    /// Region this plan targets
    pub region_id: RegionId,
    /// The logical plan to execute on this region
    pub plan: LogicalPlan,
    /// Predicates pushed down to this region
    pub pushed_predicates: Vec<Expr>,
    /// Columns projected at this region
    pub pushed_projections: Vec<String>,
    /// Whether partial aggregation is performed
    pub has_partial_aggregate: bool,
    /// Limit applied at this region (if any)
    pub pushed_limit: Option<usize>,
}

/// Final aggregation/merge plan to run on frontend.
#[derive(Debug, Clone)]
pub struct FinalPlan {
    /// Type of final operation
    pub operation: FinalOperation,
    /// The logical plan for final processing
    pub plan: LogicalPlan,
}

/// Type of final operation after collecting distributed results.
#[derive(Debug, Clone)]
pub enum FinalOperation {
    /// Simple union of results (no additional processing)
    Union,
    /// Final aggregation (combine partial aggregates)
    FinalAggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
    },
    /// Final sort and limit
    SortLimit {
        sort_exprs: Vec<Expr>,
        limit: Option<usize>,
    },
    /// Just apply a limit
    Limit(usize),
    /// No-op (single region case)
    Passthrough,
}

/// Statistics about optimization.
#[derive(Debug, Clone, Default)]
pub struct OptimizationStats {
    /// Number of predicates pushed down
    pub predicates_pushed: usize,
    /// Number of projections applied early
    pub projections_pushed: usize,
    /// Whether aggregate was partially pushed
    pub aggregate_pushed: bool,
    /// Whether limit was pushed
    pub limit_pushed: bool,
    /// Estimated data reduction ratio (0.0-1.0, lower is better)
    pub estimated_reduction: f64,
}

impl DistributedOptimizer {
    /// Create a new optimizer with default configuration.
    pub fn new() -> Self {
        Self {
            config: OptimizerConfig::default(),
        }
    }

    /// Create a new optimizer with custom configuration.
    pub fn with_config(config: OptimizerConfig) -> Self {
        Self { config }
    }

    /// Optimize a logical plan for distributed execution.
    ///
    /// This analyzes the plan and determines:
    /// 1. Which predicates can be pushed to datanodes
    /// 2. Which projections can be applied early
    /// 3. Whether aggregates can be partially computed on datanodes
    /// 4. Whether limits can be pushed down
    pub fn optimize(
        &self,
        plan: &LogicalPlan,
        table_regions: &HashMap<String, Vec<RegionInfo>>,
    ) -> Result<OptimizedPlan> {
        info!("Optimizing plan for distributed execution");
        trace!(plan = ?plan, "Input plan");

        // Extract referenced tables
        let referenced_tables = self.extract_tables(plan);
        debug!(tables = ?referenced_tables, "Referenced tables");

        // Check if the plan is distributable
        let is_distributable = self.is_distributable(plan, table_regions);
        if !is_distributable {
            debug!("Plan is not distributable, will execute locally");
            return Ok(OptimizedPlan {
                original_plan: plan.clone(),
                region_plans: HashMap::new(),
                final_plan: None,
                referenced_tables,
                is_distributable: false,
                stats: OptimizationStats::default(),
            });
        }

        // Collect all regions that need to be queried
        let target_regions = self.collect_target_regions(&referenced_tables, table_regions);
        debug!(num_regions = target_regions.len(), "Target regions");

        // Analyze the plan structure
        let analysis = self.analyze_plan(plan)?;

        // Create optimized per-region plans
        let mut region_plans = HashMap::new();
        let mut stats = OptimizationStats::default();

        for region in &target_regions {
            let region_plan = self.create_region_plan(plan, region, &analysis, &mut stats)?;
            region_plans.insert(region.region_id, region_plan);
        }

        // Create the final aggregation plan
        let final_plan = self.create_final_plan(plan, &analysis, target_regions.len())?;

        Ok(OptimizedPlan {
            original_plan: plan.clone(),
            region_plans,
            final_plan,
            referenced_tables,
            is_distributable: true,
            stats,
        })
    }

    /// Extract all table names referenced in the plan.
    fn extract_tables(&self, plan: &LogicalPlan) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.extract_tables_recursive(plan, &mut tables);
        tables
    }

    fn extract_tables_recursive(&self, plan: &LogicalPlan, tables: &mut HashSet<String>) {
        match plan {
            LogicalPlan::TableScan(scan) => {
                tables.insert(scan.table_name.to_string());
            }
            _ => {
                for input in plan.inputs() {
                    self.extract_tables_recursive(input, tables);
                }
            }
        }
    }

    /// Check if the plan can benefit from distributed execution.
    fn is_distributable(
        &self,
        plan: &LogicalPlan,
        table_regions: &HashMap<String, Vec<RegionInfo>>,
    ) -> bool {
        let tables = self.extract_tables(plan);

        // If any table has multiple regions, the plan is distributable
        for table in &tables {
            if let Some(regions) = table_regions.get(table) {
                if regions.len() > 1 {
                    return true;
                }
            }
        }

        // Also distributable if there are multiple tables in different regions
        let mut seen_nodes = HashSet::new();
        for table in &tables {
            if let Some(regions) = table_regions.get(table) {
                for region in regions {
                    seen_nodes.insert(region.node_id);
                }
            }
        }

        seen_nodes.len() > 1
    }

    /// Collect all regions that need to be queried.
    fn collect_target_regions(
        &self,
        tables: &HashSet<String>,
        table_regions: &HashMap<String, Vec<RegionInfo>>,
    ) -> Vec<RegionInfo> {
        let mut regions = Vec::new();
        let mut seen_ids = HashSet::new();

        for table in tables {
            if let Some(table_regs) = table_regions.get(table) {
                for region in table_regs {
                    if seen_ids.insert(region.region_id) {
                        regions.push(region.clone());
                    }
                }
            }
        }

        regions
    }

    /// Analyze the plan structure to determine optimization opportunities.
    fn analyze_plan(&self, plan: &LogicalPlan) -> Result<PlanAnalysis> {
        let mut analysis = PlanAnalysis::default();
        self.analyze_recursive(plan, &mut analysis)?;
        Ok(analysis)
    }

    fn analyze_recursive(&self, plan: &LogicalPlan, analysis: &mut PlanAnalysis) -> Result<()> {
        match plan {
            LogicalPlan::Filter(filter) => {
                // Collect predicates that can be pushed down
                let predicates = self.extract_predicates(&filter.predicate);
                for pred in predicates {
                    if self.can_push_predicate(&pred) {
                        analysis.pushable_predicates.push(pred);
                    } else {
                        analysis.non_pushable_predicates.push(pred);
                    }
                }
                self.analyze_recursive(&filter.input, analysis)?;
            }
            LogicalPlan::Projection(proj) => {
                // Collect projection columns
                for expr in &proj.expr {
                    if let Some(col_name) = self.extract_column_name(expr) {
                        analysis.projected_columns.insert(col_name);
                    }
                }
                analysis.has_projection = true;
                self.analyze_recursive(&proj.input, analysis)?;
            }
            LogicalPlan::Aggregate(agg) => {
                analysis.has_aggregate = true;
                analysis.group_by_exprs = agg.group_expr.clone();
                analysis.aggregate_exprs = agg.aggr_expr.clone();

                // Check if aggregates can be partially computed
                analysis.can_partial_aggregate = self.can_partial_aggregate(&agg.aggr_expr);

                self.analyze_recursive(&agg.input, analysis)?;
            }
            LogicalPlan::Sort(sort) => {
                analysis.has_sort = true;
                analysis.sort_exprs = sort.expr.clone();
                self.analyze_recursive(&sort.input, analysis)?;
            }
            LogicalPlan::Limit(limit) => {
                analysis.has_limit = true;
                analysis.limit_skip = limit.skip;
                analysis.limit_fetch = limit.fetch;
                self.analyze_recursive(&limit.input, analysis)?;
            }
            LogicalPlan::Join(join) => {
                analysis.has_join = true;
                analysis.join_type = Some(join.join_type);
                self.analyze_recursive(&join.left, analysis)?;
                self.analyze_recursive(&join.right, analysis)?;
            }
            LogicalPlan::TableScan(scan) => {
                analysis.table_scans.push(scan.table_name.to_string());
            }
            _ => {
                for input in plan.inputs() {
                    self.analyze_recursive(input, analysis)?;
                }
            }
        }
        Ok(())
    }

    /// Extract individual predicates from a compound predicate.
    fn extract_predicates(&self, expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
                let mut left = self.extract_predicates(&binary.left);
                let mut right = self.extract_predicates(&binary.right);
                left.append(&mut right);
                left
            }
            _ => vec![expr.clone()],
        }
    }

    /// Check if a predicate can be pushed down to datanodes.
    fn can_push_predicate(&self, _expr: &Expr) -> bool {
        // For now, we push down most predicates
        // TODO: Add checks for UDFs, subqueries, etc. that can't be pushed
        true
    }

    /// Extract column name from an expression if it's a simple column reference.
    fn extract_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(col) => Some(col.name.clone()),
            Expr::Alias(alias) => self.extract_column_name(&alias.expr),
            _ => None,
        }
    }

    /// Check if aggregates can be partially computed on datanodes.
    fn can_partial_aggregate(&self, aggr_exprs: &[Expr]) -> bool {
        // We can do partial aggregation for decomposable aggregates:
        // SUM, COUNT, MIN, MAX, AVG (as SUM/COUNT)
        for expr in aggr_exprs {
            if let Expr::AggregateFunction(agg) = expr {
                let func_name = agg.func.name().to_uppercase();
                match func_name.as_str() {
                    "SUM" | "COUNT" | "MIN" | "MAX" | "AVG" => continue,
                    _ => return false,
                }
            }
        }
        true
    }

    /// Create an optimized plan for a specific region.
    fn create_region_plan(
        &self,
        original_plan: &LogicalPlan,
        region: &RegionInfo,
        analysis: &PlanAnalysis,
        stats: &mut OptimizationStats,
    ) -> Result<RegionPlan> {
        let mut plan = original_plan.clone();
        let mut pushed_predicates = Vec::new();
        let mut pushed_projections = Vec::new();
        let mut has_partial_aggregate = false;
        let mut pushed_limit = None;

        // Apply predicate pushdown
        if self.config.enable_predicate_pushdown && !analysis.pushable_predicates.is_empty() {
            for pred in &analysis.pushable_predicates {
                // Check if predicate can be applied to this region
                if self.predicate_applies_to_region(pred, region) {
                    pushed_predicates.push(pred.clone());
                    stats.predicates_pushed += 1;
                }
            }
        }

        // Apply projection pushdown
        if self.config.enable_projection_pushdown && !analysis.projected_columns.is_empty() {
            pushed_projections = analysis.projected_columns.iter().cloned().collect();
            stats.projections_pushed = pushed_projections.len();
        }

        // Apply aggregate pushdown (partial aggregation)
        if self.config.enable_aggregate_pushdown
            && analysis.has_aggregate
            && analysis.can_partial_aggregate
        {
            has_partial_aggregate = true;
            stats.aggregate_pushed = true;
        }

        // Apply limit pushdown
        // We can push limit if there's no aggregation or sorting that would change results
        if self.config.enable_limit_pushdown
            && analysis.has_limit
            && !analysis.has_aggregate
            && !analysis.has_sort
        {
            if let Some(fetch) = analysis.limit_fetch {
                pushed_limit = Some(fetch);
                stats.limit_pushed = true;
            }
        }

        // Rebuild the plan with optimizations applied
        // For now, we use the original plan and let the executor apply optimizations
        // A more sophisticated implementation would actually rewrite the plan

        Ok(RegionPlan {
            region_id: region.region_id,
            plan,
            pushed_predicates,
            pushed_projections,
            has_partial_aggregate,
            pushed_limit,
        })
    }

    /// Check if a predicate applies to a specific region based on partition ranges.
    fn predicate_applies_to_region(&self, _pred: &Expr, _region: &RegionInfo) -> bool {
        // TODO: Implement partition pruning based on the predicate
        // For now, assume all predicates apply to all regions
        true
    }

    /// Create the final plan for aggregating results from all regions.
    fn create_final_plan(
        &self,
        original_plan: &LogicalPlan,
        analysis: &PlanAnalysis,
        num_regions: usize,
    ) -> Result<Option<FinalPlan>> {
        // If only one region, no need for a final plan
        if num_regions <= 1 {
            return Ok(Some(FinalPlan {
                operation: FinalOperation::Passthrough,
                plan: original_plan.clone(),
            }));
        }

        // Determine the final operation based on the plan structure
        let operation = if analysis.has_aggregate && analysis.can_partial_aggregate {
            FinalOperation::FinalAggregate {
                group_by: analysis.group_by_exprs.clone(),
                aggregates: analysis.aggregate_exprs.clone(),
            }
        } else if analysis.has_sort && analysis.has_limit {
            FinalOperation::SortLimit {
                sort_exprs: analysis.sort_exprs.clone(),
                limit: analysis.limit_fetch,
            }
        } else if analysis.has_limit {
            FinalOperation::Limit(analysis.limit_fetch.unwrap_or(usize::MAX))
        } else {
            FinalOperation::Union
        };

        Ok(Some(FinalPlan {
            operation,
            plan: original_plan.clone(),
        }))
    }
}

impl Default for DistributedOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal structure for plan analysis.
#[derive(Debug, Default)]
struct PlanAnalysis {
    /// Predicates that can be pushed down
    pushable_predicates: Vec<Expr>,
    /// Predicates that cannot be pushed down
    non_pushable_predicates: Vec<Expr>,
    /// Columns in projections
    projected_columns: HashSet<String>,
    /// Whether the plan has a projection
    has_projection: bool,
    /// Whether the plan has an aggregate
    has_aggregate: bool,
    /// Group by expressions
    group_by_exprs: Vec<Expr>,
    /// Aggregate expressions
    aggregate_exprs: Vec<Expr>,
    /// Whether partial aggregation is possible
    can_partial_aggregate: bool,
    /// Whether the plan has a sort
    has_sort: bool,
    /// Sort expressions
    sort_exprs: Vec<datafusion::logical_expr::SortExpr>,
    /// Whether the plan has a limit
    has_limit: bool,
    /// Skip value for limit
    limit_skip: usize,
    /// Fetch value for limit
    limit_fetch: Option<usize>,
    /// Whether the plan has a join
    has_join: bool,
    /// Join type if present
    join_type: Option<datafusion::logical_expr::JoinType>,
    /// Table scans in the plan
    table_scans: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{NodeId, PartitionRange, RegionStatus};
    use datafusion::prelude::*;

    fn create_test_region(id: u64, table: &str) -> RegionInfo {
        RegionInfo {
            region_id: RegionId::new(id),
            database: "test_db".to_string(),
            table: table.to_string(),
            node_id: NodeId::new(1),
            partition_range: PartitionRange::full(),
            status: RegionStatus::Active,
            epoch: 1,
        }
    }

    #[tokio::test]
    async fn test_optimizer_creation() {
        let optimizer = DistributedOptimizer::new();
        assert!(optimizer.config.enable_predicate_pushdown);
        assert!(optimizer.config.enable_projection_pushdown);
        assert!(optimizer.config.enable_aggregate_pushdown);
    }

    #[tokio::test]
    async fn test_extract_tables() {
        let optimizer = DistributedOptimizer::new();
        let ctx = SessionContext::new();

        // Create a simple table scan
        ctx.register_csv("test_table", "nonexistent.csv", CsvReadOptions::default())
            .await
            .ok();

        // This is a simplified test - in practice we'd use a real plan
    }

    #[test]
    fn test_can_partial_aggregate() {
        let optimizer = DistributedOptimizer::new();

        // Test with SUM - should be able to do partial
        let sum_expr = col("value").sum();
        assert!(optimizer.can_partial_aggregate(&[sum_expr]));

        // Test with COUNT - should be able to do partial
        let count_expr = col("value").count();
        assert!(optimizer.can_partial_aggregate(&[count_expr]));
    }

    #[test]
    fn test_extract_predicates() {
        let optimizer = DistributedOptimizer::new();

        // Simple predicate
        let pred = col("x").gt(lit(5));
        let predicates = optimizer.extract_predicates(&pred);
        assert_eq!(predicates.len(), 1);

        // Compound predicate with AND
        let compound = col("x").gt(lit(5)).and(col("y").lt(lit(10)));
        let predicates = optimizer.extract_predicates(&compound);
        assert_eq!(predicates.len(), 2);
    }

    #[test]
    fn test_is_distributable() {
        let optimizer = DistributedOptimizer::new();

        // Single region - not distributable
        let mut table_regions = HashMap::new();
        table_regions.insert(
            "test_table".to_string(),
            vec![create_test_region(1, "test_table")],
        );

        // Multiple regions - distributable
        table_regions.insert(
            "multi_region_table".to_string(),
            vec![
                create_test_region(1, "multi_region_table"),
                create_test_region(2, "multi_region_table"),
            ],
        );

        // The actual test would need a proper LogicalPlan
    }
}
