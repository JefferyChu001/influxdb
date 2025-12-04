//! Error types for distributed query execution
//!
//! Based on GreptimeDB's error handling patterns using snafu

use snafu::{Location, Snafu};
use std::any::Any;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("DataFusion error"))]
    DataFusion {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Arrow error"))]
    Arrow {
        #[snafu(source)]
        error: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} not found", region_id))]
    RegionNotFound {
        region_id: crate::types::RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Node {} not found", node_id))]
    NodeNotFound {
        node_id: crate::types::NodeId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table '{}' not found", table_name))]
    TableNotFound {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid plan: {}", reason))]
    InvalidPlan {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Remote query failed: {}", reason))]
    RemoteQuery {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Serialization error: {}", reason))]
    Serialization {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Network error: {}", reason))]
    Network {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Timeout: {}", reason))]
    Timeout {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Internal error: {}", reason))]
    Internal {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Not implemented: {}", feature))]
    NotImplemented {
        feature: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid region request: {}", reason))]
    InvalidRegionRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Meta service error: {}", reason))]
    MetaService {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("RPC error"))]
    Rpc {
        #[snafu(source)]
        error: tonic::Status,
        #[snafu(implicit)]
        location: Location,
    },
}

impl Error {
    /// Create a new not implemented error
    pub fn not_implemented(feature: impl Into<String>) -> Self {
        NotImplementedSnafu {
            feature: feature.into(),
        }
        .build()
    }

    /// Create a new internal error
    pub fn internal(reason: impl Into<String>) -> Self {
        InternalSnafu {
            reason: reason.into(),
        }
        .build()
    }

    /// Create a new remote query error
    pub fn remote_query(reason: impl Into<String>) -> Self {
        RemoteQuerySnafu {
            reason: reason.into(),
        }
        .build()
    }
}

// Implement conversion to DataFusion error
impl From<Error> for datafusion::error::DataFusionError {
    fn from(err: Error) -> Self {
        datafusion::error::DataFusionError::External(Box::new(err))
    }
}

