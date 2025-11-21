fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use tonic-build with explicit prost version
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/cluster.proto"], &["proto"])?;
    Ok(())
}

