fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate gRPC code from proto files
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &["proto/query_execution.proto"],
            &["proto"],
        )?;
    
    Ok(())
}

