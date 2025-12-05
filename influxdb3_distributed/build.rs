//! Build script for influxdb3_distributed
//!
//! This compiles the protobuf definitions for the distributed services.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = PathBuf::from("src/proto");
    let proto_file = proto_path.join("distributed.proto");

    // Only rebuild if proto file changes
    println!("cargo:rerun-if-changed=src/proto/distributed.proto");

    // Check if proto file exists before trying to compile
    if proto_file.exists() {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .out_dir("src/proto")
            .compile_protos(&[proto_file], &[proto_path])?;
    }

    Ok(())
}
