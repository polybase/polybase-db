use super::{field_path::FieldPath, record::IndexValue};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    pub record_id: String,
    pub values: HashMap<FieldPath, IndexValue<'a>>,
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
// #[serde(transparent)]
// pub struct CursorValues<'a>(pub HashMap<FieldPath, IndexValue<'a>>);

#[derive(Debug, PartialEq, Eq)]
pub enum CursorDirection {
    Before,
    After,
}

impl<'a> Cursor<'a> {
    pub fn as_base64(&self) -> Result<String> {
        // serialize the cursor to bytes
        let buf = bincode::serialize(&self)?;
        // encode to base64
        Ok(STANDARD.encode(buf))
    }

    // pub fn

    // todo - handle or remove this
    pub fn immediate_successor(&self) -> Result<Self> {
        todo!()
    }
}

// impl CursorValues<'_> {
//     pub fn with_static(self) -> CursorValues<'static> {
//         CursorValues(
//             self.0
//                 .into_iter()
//                 .map(|(k, v)| (k, v.with_static()))
//                 .collect(),
//         )
//     }
// }
