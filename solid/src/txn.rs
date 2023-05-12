use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Txn {
    /// Id of the change, to enable deduplication
    pub(crate) id: Vec<u8>,

    /// Data of the change (opaque to solid protocol)
    pub(crate) data: Vec<u8>,
}
