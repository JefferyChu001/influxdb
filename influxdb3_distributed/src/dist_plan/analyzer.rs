//! Distributed plan analyzer
//!
//! Analyzes DataFusion logical plans to determine distributed execution strategy.
//! Based on GreptimeDB's dist_plan/analyzer.rs but adapted for InfluxDB.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_optimizer::analyzer::AnalyzerRule;

use crate::dist_plan::merge_scan::MergeScanLogicalPlan;
use crate::error::Result;

/// Options for distributed query planning
#[derive(Debug, Clone)]
pub struct DistPlannerOptions {
    /// Whether to allow fallback when push down fails
    pub allow_query_fallback: bool,
}

impl Default for DistPlannerOptions {
    fn default() -> Self {
        Self {
            allow_query_fallback: true,
        }
    }
}

/// Rewriter status tracking
#[derive(Debug, Clone, PartialEq, Eq)]
enum RewriterStatus {
    /// Initial state
    Initial,
    /// Found a table scan, can start rewriting
    FoundTableScan,
    /// Created a MergeScan node
    CreatedMergeScan,
}

/// Distributed planner analyzer
///
/// This analyzer transforms logical plans to enable distributed execution.
/// It identifies parts of the plan that can be pushed down to data nodes.
#[derive(Debug)]
pub struct DistPlannerAnalyzer {
    options: DistPlannerOptions,
}

impl DistPlannerAnalyzer {
    pub fn new() -> Self {
        Self {
            options: DistPlannerOptions::default(),
        }
    }

    pub fn with_options(options: DistPlannerOptions) -> Self {
        Self { options }
    }
}

impl Default for DistPlannerAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalyzerRule for DistPlannerAnalyzer {
    fn name(&self) -> &str {
        "DistPlannerAnalyzer"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DfResult<LogicalPlan> {
        // Try to push down as many operations as possible to data nodes
        match self.try_push_down(plan.clone()) {
            Ok(transformed_plan) => Ok(transformed_plan),
            Err(err) => {
                if self.options.allow_query_fallback {
                    tracing::warn!(
                        error = %err,
                        "Failed to push down plan, using fallback"
                    );
                    // Use fallback: only push down table scans
                    self.use_fallback(plan)
                } else {
                    Err(err)
                }
            }
        }
    }
}

impl DistPlannerAnalyzer {
    /// Try to push down as many operations as possible
    fn try_push_down(&self, plan: LogicalPlan) -> DfResult<LogicalPlan> {
        let mut rewriter = PushDownRewriter::new();
        let result = plan.rewrite(&mut rewriter)?.data;
        Ok(result)
    }

    /// Fallback: only push down table scans
    fn use_fallback(&self, plan: LogicalPlan) -> DfResult<LogicalPlan> {
        let mut rewriter = FallbackRewriter::new();
        let result = plan.rewrite(&mut rewriter)?.data;
        Ok(result)
    }
}

/// Rewriter that pushes down operations to data nodes
///
/// This rewriter traverses the plan tree and identifies operations that
/// can be pushed down to data nodes for parallel execution.
struct PushDownRewriter {
    /// Track which tables we've seen
    seen_tables: HashSet<String>,
    /// Current rewriter status
    status: RewriterStatus,
    /// Plans that have been accumulated for push-down
    accumulated_plans: Vec<LogicalPlan>,
}

impl PushDownRewriter {
    fn new() -> Self {
        Self {
            seen_tables: HashSet::new(),
            status: RewriterStatus::Initial,
            accumulated_plans: Vec::new(),
        }
    }

    /// Check if a plan node can be pushed down
    fn can_push_down(&self, node: &LogicalPlan) -> bool {
        match node {
            // These operations can typically be pushed down
            LogicalPlan::TableScan(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Limit(_) => true,

            // Sort can be pushed down, but needs special handling (MergeSort)
            LogicalPlan::Sort(_) => true,

            // Aggregate needs two-phase execution
            LogicalPlan::Aggregate(_) => true,

            // Join is complex - for now, don't push down
            LogicalPlan::Join(_) => false,

            // Subqueries and CTEs are complex
            LogicalPlan::SubqueryAlias(_) => false,

            _ => false,
        }
    }
}

impl TreeNodeRewriter for PushDownRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        match &node {
            LogicalPlan::TableScan(table_scan) => {
                // Extract table name from table reference
                let table_name = table_scan.table_name.to_string();
                self.seen_tables.insert(table_name);
                self.status = RewriterStatus::FoundTableScan;

                // Wrap the table scan in a MergeScan
                let merge_scan = MergeScanLogicalPlan::new(node.clone());
                self.status = RewriterStatus::CreatedMergeScan;

                Ok(Transformed::yes(merge_scan.into_logical_plan()))
            }
            LogicalPlan::Projection(proj) => {
                if self.status == RewriterStatus::CreatedMergeScan {
                    // Already created MergeScan, this projection stays on coordinator
                    Ok(Transformed::no(node))
                } else if self.can_push_down(&node) {
                    // Can push down, continue traversing
                    Ok(Transformed::no(node))
                } else {
                    Ok(Transformed::no(node))
                }
            }
            LogicalPlan::Filter(filter) => {
                if self.status == RewriterStatus::CreatedMergeScan {
                    // Already created MergeScan, this filter stays on coordinator
                    Ok(Transformed::no(node))
                } else if self.can_push_down(&node) {
                    // Can push down, continue traversing
                    Ok(Transformed::no(node))
                } else {
                    Ok(Transformed::no(node))
                }
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

/// Fallback rewriter that only pushes down table scans
///
/// This is a conservative rewriter used when the full push-down fails.
/// It only wraps table scans in MergeScan nodes.
struct FallbackRewriter {
    /// Track whether we've found any table scans
    found_table_scan: bool,
}

impl FallbackRewriter {
    fn new() -> Self {
        Self {
            found_table_scan: false,
        }
    }
}

impl TreeNodeRewriter for FallbackRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        match &node {
            LogicalPlan::TableScan(_) => {
                self.found_table_scan = true;
                // Only push down the table scan
                let merge_scan = MergeScanLogicalPlan::new(node.clone());
                Ok(Transformed::yes(merge_scan.into_logical_plan()))
            }
            _ => {
                // Everything else stays on coordinator
                Ok(Transformed::no(node))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};
    use std::sync::Arc;

    fn create_test_table_source(schema: Arc<Schema>) -> Arc<dyn datafusion::catalog::TableProvider> {
        Arc::new(EmptyTable::new(schema))
    }

    #[test]
    fn test_analyzer_with_table_scan() {
        // Create a simple table scan plan
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let table_source = provider_as_source(create_test_table_source(schema.clone()));

        let plan = LogicalPlanBuilder::scan("test_table", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        let analyzer = DistPlannerAnalyzer::new();
        let result = analyzer.try_push_down(plan).unwrap();

        // The plan should be transformed to include a MergeScan
        assert!(format!("{:?}", result).contains("MergeScan"));
    }

    #[test]
    fn test_analyzer_with_filter() {
        // Create a plan with filter
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let table_source = provider_as_source(create_test_table_source(schema.clone()));

        let plan = LogicalPlanBuilder::scan("test_table", table_source, None)
            .unwrap()
            .filter(col("value").gt(lit(100)))
            .unwrap()
            .build()
            .unwrap();

        let analyzer = DistPlannerAnalyzer::new();
        let result = analyzer.try_push_down(plan).unwrap();

        // The plan should include MergeScan
        let plan_str = format!("{:?}", result);
        assert!(plan_str.contains("MergeScan"));
    }

    #[test]
    fn test_analyzer_with_projection() {
        // Create a plan with projection
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let table_source = provider_as_source(create_test_table_source(schema.clone()));

        let plan = LogicalPlanBuilder::scan("test_table", table_source, None)
            .unwrap()
            .project(vec![col("id"), col("name")])
            .unwrap()
            .build()
            .unwrap();

        let analyzer = DistPlannerAnalyzer::new();
        let result = analyzer.try_push_down(plan).unwrap();

        let plan_str = format!("{:?}", result);
        assert!(plan_str.contains("MergeScan"));
        assert!(plan_str.contains("Projection"));
    }

    #[test]
    fn test_fallback_rewriter() {
        // Test fallback rewriter
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let table_source = provider_as_source(create_test_table_source(schema.clone()));

        let plan = LogicalPlanBuilder::scan("test_table", table_source, None)
            .unwrap()
            .filter(col("id").gt(lit(10)))
            .unwrap()
            .build()
            .unwrap();

        let mut rewriter = FallbackRewriter::new();
        let result = plan.rewrite(&mut rewriter).unwrap().data;

        // Should have MergeScan at the bottom
        let plan_str = format!("{:?}", result);
        assert!(plan_str.contains("MergeScan"));
        assert!(rewriter.found_table_scan);
    }

    #[test]
    fn test_push_down_rewriter_can_push_down() {
        let rewriter = PushDownRewriter::new();

        // Test which plans can be pushed down
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let table_source = provider_as_source(create_test_table_source(schema.clone()));

        let table_scan = LogicalPlanBuilder::scan("test", table_source.clone(), None)
            .unwrap()
            .build()
            .unwrap();

        assert!(rewriter.can_push_down(&table_scan));

        let projection = LogicalPlanBuilder::scan("test", table_source, None)
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap();

        assert!(rewriter.can_push_down(&projection));
    }
}

