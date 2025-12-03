//! MergeScan logical and physical plan
//!
//! MergeScan is a special plan node that executes a sub-plan on multiple
//! data nodes and merges the results.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::DFSchemaRef;
use datafusion_physical_expr::EquivalenceProperties;


use crate::types::RegionId;

/// MergeScan logical plan node
///
/// This represents a sub-plan that will be executed on remote data nodes,
/// with results merged at the coordinator.
#[derive(Debug, Clone)]
pub struct MergeScanLogicalPlan {
    /// The input plan to execute on each data node
    input: LogicalPlan,
    /// Whether this is a placeholder (not yet resolved)
    is_placeholder: bool,
}

// Manual implementations of traits required by UserDefinedLogicalNodeCore
impl PartialEq for MergeScanLogicalPlan {
    fn eq(&self, other: &Self) -> bool {
        self.is_placeholder == other.is_placeholder
            && self.input == other.input
    }
}

impl Eq for MergeScanLogicalPlan {}

impl std::hash::Hash for MergeScanLogicalPlan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.is_placeholder.hash(state);
        // Note: LogicalPlan doesn't implement Hash, so we use its display representation
        format!("{:?}", self.input).hash(state);
    }
}

impl PartialOrd for MergeScanLogicalPlan {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeScanLogicalPlan {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by placeholder status first, then by input plan representation
        match self.is_placeholder.cmp(&other.is_placeholder) {
            std::cmp::Ordering::Equal => {
                format!("{:?}", self.input).cmp(&format!("{:?}", other.input))
            }
            other => other,
        }
    }
}

impl MergeScanLogicalPlan {
    pub fn new(input: LogicalPlan) -> Self {
        Self {
            input,
            is_placeholder: false,
        }
    }

    pub fn new_placeholder(input: LogicalPlan) -> Self {
        Self {
            input,
            is_placeholder: true,
        }
    }

    pub fn name() -> &'static str {
        "MergeScan"
    }

    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub fn is_placeholder(&self) -> bool {
        self.is_placeholder
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
}

impl UserDefinedLogicalNodeCore for MergeScanLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        // Return empty to prevent further optimization of the input
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        // Return empty to prevent further optimization
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MergeScan [is_placeholder={}, input=[\n{}\n]]",
            self.is_placeholder, self.input
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        Ok(self.clone())
    }
}

/// MergeScan physical execution plan
///
/// Executes a sub-plan on multiple regions and merges the results.
#[derive(Debug)]
pub struct MergeScanExec {
    /// Regions to query
    regions: Vec<RegionId>,
    /// The plan to execute on each region
    input_plan: Arc<dyn ExecutionPlan>,
    /// Output schema
    schema: ArrowSchemaRef,
    /// Plan properties
    properties: PlanProperties,
}

impl MergeScanExec {
    pub fn new(
        regions: Vec<RegionId>,
        input_plan: Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let boundedness = datafusion::physical_plan::execution_plan::Boundedness::Unbounded {
            requires_infinite_memory: false,
        };
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(regions.len()),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            boundedness,
        );

        Self {
            regions,
            input_plan,
            schema,
            properties,
        }
    }

    pub fn regions(&self) -> &[RegionId] {
        &self.regions
    }

    pub fn input_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input_plan
    }
}

impl DisplayAs for MergeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MergeScanExec: regions={}", self.regions.len())
            }
            DisplayFormatType::TreeRender => {
                write!(f, "MergeScanExec")
            }
        }
    }
}

impl ExecutionPlan for MergeScanExec {
    fn name(&self) -> &str {
        "MergeScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input_plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "MergeScanExec expects exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            self.regions.clone(),
            children[0].clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition >= self.regions.len() {
            return Err(DataFusionError::Internal(format!(
                "Invalid partition {} for {} regions",
                partition,
                self.regions.len()
            )));
        }

        // Get the region for this partition
        let region_id = self.regions[partition];

        tracing::debug!(
            "Executing MergeScanExec for partition {} (region {})",
            partition,
            region_id
        );

        // Execute the input plan for this partition
        // In a real distributed implementation, this would:
        // 1. Send the plan to the remote node hosting this region
        // 2. Execute it there
        // 3. Stream results back
        // For now, we execute locally
        self.input_plan.execute(partition, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_merge_scan_logical_plan_creation() {
        // Test creating a MergeScanLogicalPlan
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Create empty batch to satisfy MemTable requirement
        let empty_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]]).unwrap();
        ctx.register_table("test", Arc::new(table)).unwrap();

        let plan = ctx
            .sql("SELECT * FROM test")
            .await
            .unwrap()
            .logical_plan()
            .clone();

        let merge_scan = MergeScanLogicalPlan::new(plan.clone());

        assert!(!merge_scan.is_placeholder());
        assert_eq!(merge_scan.name(), "MergeScan");
        assert_eq!(merge_scan.input(), &plan);
    }

    #[tokio::test]
    async fn test_merge_scan_logical_plan_placeholder() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Create empty batch
        let empty_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]]).unwrap();
        ctx.register_table("test", Arc::new(table)).unwrap();

        let plan = ctx
            .sql("SELECT * FROM test")
            .await
            .unwrap()
            .logical_plan()
            .clone();

        let merge_scan = MergeScanLogicalPlan::new_placeholder(plan);

        assert!(merge_scan.is_placeholder());
    }

    #[tokio::test]
    async fn test_merge_scan_exec_creation() {
        // Test creating a MergeScanExec
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Create some test data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let table_provider = Arc::new(table);

        // Create a simple physical plan
        let physical_plan = ctx.read_table(table_provider.clone()).unwrap();
        let exec_plan = ctx
            .state()
            .create_physical_plan(&physical_plan.logical_plan().clone())
            .await
            .unwrap();

        // Create MergeScanExec
        let regions = vec![RegionId::new(1), RegionId::new(2)];
        let merge_exec = MergeScanExec::new(regions.clone(), exec_plan, schema.clone());

        assert_eq!(merge_exec.regions().len(), 2);
        assert_eq!(merge_exec.regions(), &regions);
    }

    #[tokio::test]
    async fn test_merge_scan_exec_execute() {
        // Test executing MergeScanExec
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Create test data with multiple batches (simulating multiple regions)
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 4])),
                Arc::new(Int64Array::from(vec![30, 40])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema.clone(), vec![vec![batch1], vec![batch2]]).unwrap();
        let table_provider = Arc::new(table);

        let physical_plan = ctx.read_table(table_provider).unwrap();
        let exec_plan = ctx
            .state()
            .create_physical_plan(&physical_plan.logical_plan().clone())
            .await
            .unwrap();

        let regions = vec![RegionId::new(1), RegionId::new(2)];
        let merge_exec = Arc::new(MergeScanExec::new(regions, exec_plan, schema));

        // Execute partition 0
        let task_ctx = Arc::new(TaskContext::default());
        let stream = merge_exec.execute(0, task_ctx).unwrap();

        // Verify we can get the stream (actual data reading would require more setup)
        assert!(stream.schema().fields().len() == 2);
    }

    #[test]
    fn test_merge_scan_logical_plan_traits() {
        // Test trait implementations
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let ctx = SessionContext::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let plan = rt.block_on(async {
            // Create empty batch
            let empty_batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
            )
            .unwrap();

            let table = MemTable::try_new(schema.clone(), vec![vec![empty_batch]]).unwrap();
            ctx.register_table("test", Arc::new(table)).unwrap();
            ctx.sql("SELECT * FROM test")
                .await
                .unwrap()
                .logical_plan()
                .clone()
        });

        let merge_scan1 = MergeScanLogicalPlan::new(plan.clone());
        let merge_scan2 = MergeScanLogicalPlan::new(plan.clone());

        // Test PartialEq
        assert_eq!(merge_scan1, merge_scan2);

        // Test Clone
        let merge_scan3 = merge_scan1.clone();
        assert_eq!(merge_scan1, merge_scan3);

        // Test Debug
        let debug_str = format!("{:?}", merge_scan1);
        assert!(debug_str.contains("MergeScanLogicalPlan"));
    }
}

