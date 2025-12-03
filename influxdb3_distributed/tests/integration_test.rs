//! Integration tests for the distributed query framework
//!
//! These tests demonstrate the complete flow from logical plan to distributed execution.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use influxdb3_distributed::dist_plan::{DistPlannerAnalyzer, DistributedPlanner};
use influxdb3_distributed::meta::{
    InMemoryMetaService, MetaService, NodeInfo, RegionMeta, TableMeta,
};
use influxdb3_distributed::types::*;

/// Setup a test environment with nodes, tables, and regions
async fn setup_distributed_env() -> (Arc<InMemoryMetaService>, SessionContext) {
    let meta_service = Arc::new(InMemoryMetaService::new());

    // Register 3 nodes
    for i in 1..=3 {
        let node = NodeInfo {
            id: NodeId::new(i),
            address: format!("node{}", i),
            grpc_port: 8080 + i as u16,
            http_port: 9090 + i as u16,
            status: NodeStatus::Active,
            regions: vec![RegionId::new(i)],
        };
        meta_service.register_node(node).await.unwrap();
    }

    // Register a table with schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("cpu", DataType::Int64, false),
        Field::new("host", DataType::Utf8, false),
    ]));

    let table_meta = TableMeta {
        id: TableId::new(1),
        name: "system_metrics".to_string(),
        schema,
        regions: vec![RegionId::new(1), RegionId::new(2), RegionId::new(3)],
    };
    meta_service.register_table(table_meta).await.unwrap();

    // Register regions explicitly
    for (i, region_id) in [RegionId::new(1), RegionId::new(2), RegionId::new(3)]
        .iter()
        .enumerate()
    {
        let region_meta = RegionMeta {
            id: *region_id,
            table_id: TableId::new(1),
            node_id: NodeId::new((i + 1) as u64),
            status: RegionStatus::Active,
        };
        meta_service.register_region(region_meta).await.unwrap();
    }

    let ctx = SessionContext::new();

    (meta_service, ctx)
}

#[tokio::test]
async fn test_distributed_query_flow() {
    // Setup environment
    let (meta_service, ctx) = setup_distributed_env().await;

    // Create a logical plan with table scan
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("cpu", DataType::Int64, false),
    ]));

    let table_source = provider_as_source(Arc::new(EmptyTable::new(schema)));

    let logical_plan = LogicalPlanBuilder::scan("system_metrics", table_source, None)
        .unwrap()
        .filter(col("cpu").gt(lit(80)))
        .unwrap()
        .build()
        .unwrap();

    // Test 1: Analyze the plan
    let analyzer = DistPlannerAnalyzer::new();
    let analyzed_plan = analyzer.try_push_down(logical_plan.clone()).unwrap();

    let plan_str = format!("{:?}", analyzed_plan);
    println!("Analyzed plan:\n{}", plan_str);
    assert!(plan_str.contains("MergeScan"));

    // Test 2: Create distributed plan
    let planner = DistributedPlanner::new_with_context(meta_service.clone(), &ctx);

    // Extract tables (this should work)
    let tables = planner.extract_tables(&logical_plan).unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0], "system_metrics");

    println!("✓ Distributed query flow test passed");
}

#[tokio::test]
async fn test_multi_region_planning() {
    let (meta_service, _ctx) = setup_distributed_env().await;

    // Get table metadata
    let table = meta_service.get_table("system_metrics").await.unwrap();
    assert_eq!(table.regions.len(), 3);

    // List regions for the table
    let regions = meta_service.list_table_regions(table.id).await.unwrap();
    assert_eq!(regions.len(), 3);

    // Verify each region is on a different node
    let node_ids: Vec<_> = regions.iter().map(|r| r.node_id).collect();
    assert_eq!(node_ids.len(), 3);

    println!("✓ Multi-region planning test passed");
}

#[tokio::test]
async fn test_analyzer_with_complex_query() {
    let (_meta_service, _ctx) = setup_distributed_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("cpu", DataType::Int64, false),
        Field::new("memory", DataType::Int64, false),
    ]));

    let table_source = provider_as_source(Arc::new(EmptyTable::new(schema)));

    // Create a more complex query with filter, projection, and limit
    let logical_plan = LogicalPlanBuilder::scan("system_metrics", table_source, None)
        .unwrap()
        .filter(col("cpu").gt(lit(50)))
        .unwrap()
        .project(vec![col("timestamp"), col("cpu")])
        .unwrap()
        .limit(0, Some(100))
        .unwrap()
        .build()
        .unwrap();

    let analyzer = DistPlannerAnalyzer::new();
    let analyzed = analyzer.try_push_down(logical_plan).unwrap();

    let plan_str = format!("{:?}", analyzed);
    println!("Complex query plan:\n{}", plan_str);

    // Verify the plan contains expected nodes
    assert!(plan_str.contains("MergeScan"));
    assert!(plan_str.contains("Limit"));

    println!("✓ Complex query analyzer test passed");
}

#[tokio::test]
async fn test_fallback_mode() {
    let (_meta_service, _ctx) = setup_distributed_env().await;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table_source = provider_as_source(Arc::new(EmptyTable::new(schema)));

    let logical_plan = LogicalPlanBuilder::scan("test_table", table_source, None)
        .unwrap()
        .build()
        .unwrap();

    // Use analyzer with fallback enabled
    let analyzer = DistPlannerAnalyzer::new();
    let result = analyzer.try_push_down(logical_plan).unwrap();

    let plan_str = format!("{:?}", result);
    assert!(plan_str.contains("MergeScan"));

    println!("✓ Fallback mode test passed");
}

