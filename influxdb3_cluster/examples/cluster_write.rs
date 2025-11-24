//! Example client to send a cluster write via gRPC
use influxdb3_cluster::rpc::client::ClusterRpcClient;
use influxdb3_cluster::types::ConsistencyLevel;
use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    /// gRPC address of the node, e.g. 127.0.0.1:8087
    #[arg(long)]
    addr: String,
    /// Database name
    #[arg(long, default_value="mydb")]
    db: String,
    /// Line protocol string
    #[arg(long)]
    lp: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = ClusterRpcClient::new();
    client
        .write_to_node(&args.addr, 0, &args.db, args.lp.into_bytes(), ConsistencyLevel::One)
        .await?;
    println!("✓ write sent to {} db={} lp=...", args.addr, args.db);
    Ok(())
}

