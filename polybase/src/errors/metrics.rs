//! This module handles various metrics data returned from handlers and meant to be
//! processed in the logger middleware.

use slog::{Record, Result, Serializer, KV};
use std::fmt;

pub enum MetricsData {
    NumberOfRecordsBeingReturned { req_uri: String, num_records: usize },
}

impl KV for MetricsData {
    fn serialize(&self, _rec: &Record, serializer: &mut dyn Serializer) -> Result {
        match self {
            MetricsData::NumberOfRecordsBeingReturned {
                req_uri,
                num_records,
            } => {
                serializer.emit_str("req_uri", req_uri)?;
                serializer.emit_usize("num_records", *num_records)?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for MetricsData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MetricsData::NumberOfRecordsBeingReturned { .. } => "request_response_metrics",
            }
        )
    }
}
