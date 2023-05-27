//! This module handles various metrics data returned from handlers and meant to be
//! processed in the logger middleware.

use std::fmt;

pub enum MetricsData {
    NumberOfRecordsBeingReturned { req_uri: String, num_records: usize },
}

impl fmt::Display for MetricsData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MetricsData::NumberOfRecordsBeingReturned {
                    req_uri,
                    num_records,
                } => format!("request uri = {req_uri}, number of records = {num_records}"),
            }
        )
    }
}
