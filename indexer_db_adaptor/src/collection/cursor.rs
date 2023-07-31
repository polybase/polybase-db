use super::{
    field_path::FieldPath,
    record::{IndexValue, IndexValueError, RecordRoot},
    where_query::{WhereNode, WhereQuery},
};
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

    #[error("record missing id")]
    RecordMissingID,

    #[error("index value error")]
    IndexValueError(#[from] IndexValueError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor<'a> {
    pub record_id: IndexValue<'a>,
    pub values: HashMap<FieldPath, IndexValue<'a>>,
}

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

    pub fn from_record(record: &RecordRoot, query: &WhereQuery) -> Result<Self> {
        let mut values = HashMap::new();

        for (key, node) in query.0.iter() {
            if let WhereNode::Inequality(_) = node {
                if let Some(value) = record.get(&key.to_string()) {
                    values.insert(key.clone(), value.clone().try_into()?);
                } else {
                    values.insert(key.clone(), IndexValue::Null);
                }
            }
        }

        Ok(Cursor {
            record_id: record
                .get("id")
                .ok_or(Error::RecordMissingID)?
                .clone()
                .try_into()?,
            values,
        })
    }
}
