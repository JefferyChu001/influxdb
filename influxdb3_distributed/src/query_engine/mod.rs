//! Distributed Query Engine
//!
//! This module implements the complete distributed query engine that orchestrates
//! query planning, execution, and result collection across multiple nodes.
//! Based on GreptimeDB's query_engine module.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_optimizer::analyzer::AnalyzerRule;

use crate::dist_plan::{DistPlannerAnalyzer, DistributedPlanner};
use crate::error::*;
use crate::meta::MetaServiceRef;
use crate::region_query::{DefaultRegionQueryHandler, RegionQueryHandler, RegionQueryHandlerRef};

/// Distributed Query Engine
///
/// This is the main entry point for distributed query execution. It coordinates:
/// 1. SQL parsing and logical planning (via DataFusion)
/// 2. Distributed plan analysis and optimization
/// 3. Physical plan generation
/// 4. Query execution across multiple nodes
/// 5. Result merging and streaming
pub struct DistributedQueryEngine {
    /// MetaService for cluster metadata
    meta_service: MetaServiceRef,
    /// DataFusion session context
    session_ctx: Arc<SessionContext>,
    /// Distributed planner
    dist_planner: Arc<DistributedPlanner>,
    /// Distributed plan analyzer
    dist_analyzer: Arc<DistPlannerAnalyzer>,
    /// Region query handler
    region_handler: RegionQueryHandlerRef,
}

impl DistributedQueryEngine {
    /// Create a new distributed query engine
    pub fn new(meta_service: MetaServiceRef) -> Self {
        let session_ctx = Arc::new(SessionContext::new());
        let dist_planner = Arc::new(DistributedPlanner::new_with_context(
            meta_service.clone(),
            &session_ctx,
        ));
        let dist_analyzer = Arc::new(DistPlannerAnalyzer::new());
        let region_handler = Arc::new(DefaultRegionQueryHandler::new(
            meta_service.clone(),
            session_ctx.clone(),
        ));

        Self {
            meta_service,
            session_ctx,
            dist_planner,
            dist_analyzer,
            region_handler,
        }
    }

    /// Execute a SQL query
    ///
    /// This is the main entry point for SQL queries. It:
    /// 1. Parses SQL to logical plan
    /// 2. Applies distributed optimizations
    /// 3. Generates distributed physical plan
    /// 4. Executes across nodes
    /// 5. Returns merged results
    pub async fn execute_sql(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        tracing::info!(sql = %sql, "Executing SQL query");

        // Parse SQL to logical plan
        let logical_plan = self
            .session_ctx
            .sql(sql)
            .await
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Failed to parse SQL: {}", e),
                }
                .build()
            })?
            .logical_plan()
            .clone();

        // Execute the logical plan
        self.execute_logical_plan(logical_plan).await
    }

    /// Execute a logical plan
    ///
    /// This method takes a logical plan and executes it in a distributed manner.
    pub async fn execute_logical_plan(
        &self,
        logical_plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream> {
        tracing::debug!(
            plan = ?logical_plan,
            "Executing logical plan"
        );

        // Skip distributed analysis - go directly to distributed planning
        // The DistPlannerAnalyzer is for optimization, not for basic execution

        // Generate distributed physical plan
        let dist_plan = self.dist_planner.plan(&logical_plan).await?;

        tracing::info!(
            num_remote_plans = dist_plan.remote_plans.len(),
            "Generated distributed plan"
        );

        // Execute coordinator plan
        let task_ctx = self.session_ctx.task_ctx();
        let stream = dist_plan
            .coordinator_plan
            .execute(0, task_ctx)
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Failed to execute coordinator plan: {}", e),
                }
                .build()
            })?;

        Ok(stream)
    }

    /// Register a table in the query engine
    pub async fn register_table(
        &self,
        name: &str,
        table: Arc<dyn TableProvider>,
    ) -> Result<()> {
        self.session_ctx
            .register_table(name, table)
            .map_err(|e| {
                InternalSnafu {
                    reason: format!("Failed to register table: {}", e),
                }
                .build()
            })?;
        Ok(())
    }

    /// Get the session context
    pub fn session_context(&self) -> &Arc<SessionContext> {
        &self.session_ctx
    }

    /// Get the meta service
    pub fn meta_service(&self) -> &MetaServiceRef {
        &self.meta_service
    }

    /// Get the region query handler
    pub fn region_handler(&self) -> &RegionQueryHandlerRef {
        &self.region_handler
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::InMemoryMetaService;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    #[tokio::test]
    async fn test_query_engine_creation() {
        let meta_service = Arc::new(InMemoryMetaService::new());
        let engine = DistributedQueryEngine::new(meta_service);

        // Verify the session context is valid
        let state = engine.session_context().state();
        let catalog_list = state.catalog_list();
        assert!(catalog_list.catalog("datafusion").is_some());
    }

    #[tokio::test]
    async fn test_query_engine_register_table() {
        let meta_service = Arc::new(InMemoryMetaService::new());
        let engine = DistributedQueryEngine::new(meta_service);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        
        engine
            .register_table("test_table", Arc::new(table))
            .await
            .unwrap();

        // Verify table is registered
        let result = engine.session_context().sql("SELECT COUNT(*) FROM test_table").await;
        assert!(result.is_ok());
    }
}

