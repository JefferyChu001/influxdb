//! Test federated JOIN query across multiple InfluxDB nodes
//!
//! This example demonstrates how to:
//! 1. Query different tables from different nodes
//! 2. Convert JSON responses to Arrow RecordBatch
//! 3. Execute a local JOIN using DataFusion
//!
//! Prerequisites:
//! - Start 3 InfluxDB nodes on ports 8181, 8182, 8183 with --without-auth
//! - Write cpu data to node1 (8181)
//! - Write mem data to node2 (8182)
//! - Write disk data to node3 (8183)

use arrow::util::pretty::print_batches;
use influxdb3_cluster::query::StandaloneFederatedExecutor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("===========================================");
    println!("   Federated Query Test - Cross-Node JOIN");
    println!("===========================================\n");

    let executor = StandaloneFederatedExecutor::new();

    // Test 1: Query cpu table from node1
    println!(">>> Test 1: Query cpu table from node1 (127.0.0.1:8181)");
    let cpu_json = executor
        .query_node("127.0.0.1:8181", "testdb", "SELECT * FROM cpu")
        .await?;
    println!("CPU data (JSON): {}\n", &cpu_json[..cpu_json.len().min(200)]);

    // Test 2: Query mem table from node2
    println!(">>> Test 2: Query mem table from node2 (127.0.0.1:8182)");
    let mem_json = executor
        .query_node("127.0.0.1:8182", "testdb", "SELECT * FROM mem")
        .await?;
    println!("MEM data (JSON): {}\n", &mem_json[..mem_json.len().min(200)]);

    // Test 3: Convert JSON to RecordBatch
    println!(">>> Test 3: Convert JSON to Arrow RecordBatch");
    let cpu_batch = executor.json_to_record_batch(&cpu_json, "cpu")?;
    let mem_batch = executor.json_to_record_batch(&mem_json, "mem")?;

    println!("CPU RecordBatch schema: {:?}", cpu_batch.schema());
    println!("CPU RecordBatch rows: {}", cpu_batch.num_rows());
    println!("MEM RecordBatch schema: {:?}", mem_batch.schema());
    println!("MEM RecordBatch rows: {}\n", mem_batch.num_rows());

    // Test 4: Execute federated JOIN
    println!(">>> Test 4: Execute federated JOIN (cpu JOIN mem ON host)");
    println!("    - cpu from node1 (127.0.0.1:8181)");
    println!("    - mem from node2 (127.0.0.1:8182)");
    println!("    - JOIN ON host column\n");

    let results = executor
        .execute_federated_join(
            "testdb",
            "127.0.0.1:8181",
            "cpu",
            "127.0.0.1:8182",
            "mem",
            "host",
            "t1.host, t1.value as cpu_value, t2.used as mem_used, t2.total as mem_total",
        )
        .await?;

    println!(">>> JOIN Results:");
    print_batches(&results)?;

    // Test 5: Three-way JOIN (cpu + mem + disk)
    println!("\n>>> Test 5: Three-way JOIN (cpu + mem + disk)");
    
    // First get disk data
    let disk_json = executor
        .query_node("127.0.0.1:8183", "testdb", "SELECT * FROM disk")
        .await?;
    println!("DISK data (JSON): {}\n", &disk_json[..disk_json.len().min(200)]);

    // For three-way join, we'll do it in DataFusion directly
    use datafusion::prelude::*;

    let ctx = SessionContext::new();
    
    let cpu_batch = executor.json_to_record_batch(&cpu_json, "cpu")?;
    let mem_batch = executor.json_to_record_batch(&mem_json, "mem")?;
    let disk_batch = executor.json_to_record_batch(&disk_json, "disk")?;

    ctx.register_batch("cpu", cpu_batch)?;
    ctx.register_batch("mem", mem_batch)?;
    ctx.register_batch("disk", disk_batch)?;

    let three_way_join = ctx
        .sql(
            "SELECT 
                c.host, 
                c.region,
                c.value as cpu_value, 
                m.used as mem_used, 
                m.total as mem_total,
                d.used as disk_used,
                d.total as disk_total
             FROM cpu c 
             JOIN mem m ON c.host = m.host 
             JOIN disk d ON c.host = d.host",
        )
        .await?;

    let results = three_way_join.collect().await?;
    println!(">>> Three-way JOIN Results (cpu + mem + disk):");
    print_batches(&results)?;

    println!("\n===========================================");
    println!("   Federated Query Test COMPLETED!");
    println!("===========================================");

    Ok(())
}

