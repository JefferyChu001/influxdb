//! gRPC client for inter-node communication

use crate::error::{Error, Result};
use crate::proto::cluster_service_client::ClusterServiceClient;
use crate::proto::{WriteRequest, ConsistencyLevel as ProtoConsistency};
use tonic::transport::Channel;

#[derive(Clone, Copy, Debug)]
pub struct ClusterRpcClient {}

impl ClusterRpcClient {
    pub fn new() -> Self { Self {} }

    async fn client_for(&self, addr: &str) -> Result<ClusterServiceClient<Channel>> {
        let endpoint = format!("http://{}", addr);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| Error::InternalError { message: e.to_string() })?
            .connect()
            .await
            .map_err(|e| Error::InternalError { message: e.to_string() })?;
        Ok(ClusterServiceClient::new(channel))
    }

    pub async fn write_to_node(
        &self,
        addr: &str,
        shard_id: u64,
        database: &str,
        data: Vec<u8>,
        consistency: crate::types::ConsistencyLevel,
    ) -> Result<()> {
        let mut client = self.client_for(addr).await?;
        let consistency_proto = match consistency {
            crate::types::ConsistencyLevel::One => ProtoConsistency::One as i32,
            crate::types::ConsistencyLevel::Quorum => ProtoConsistency::Quorum as i32,
            crate::types::ConsistencyLevel::All => ProtoConsistency::All as i32,
        };
        let req = WriteRequest {
            shard_id,
            data,
            consistency: consistency_proto,
            database: database.to_string(),
            forwarded: false,
        };
        client.write(req).await.map_err(|e| Error::RpcError { source: e })?;
        Ok(())
    }
}

