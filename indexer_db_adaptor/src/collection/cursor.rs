use super::index::Index;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("serde_json error")]
    SerdeJSONError(#[from] serde_json::Error),

    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor<'a> {
    collection_id: String,
    record_id: String,
    index: Index<'a>,
    values: Vec<String>,
}

/// where age >= age_value && id > id_value

impl<'a> Cursor<'a> {
    pub fn as_base64(&self) -> Result<String> {
        // serialize the cursor to bytes
        let buf = bincode::serialize(&self)?;
        // encode to base64
        Ok(STANDARD.encode(buf))
    }
}
