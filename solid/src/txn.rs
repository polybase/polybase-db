use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Txn {
    /// Id of the change, to enable deduplication
    pub id: Vec<u8>,

    /// Data of the change (opaque to solid protocol)
    pub data: Vec<u8>,
}
