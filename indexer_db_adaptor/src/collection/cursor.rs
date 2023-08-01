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

#[derive(Debug, Clone, PartialEq)]
pub struct Cursor<'a>(pub WrappedCursor<'a>);

impl<'a> Serialize for Cursor<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let buf = bincode::serialize(&self.0).unwrap();
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(&buf))
    }
}

impl<'de, 'a> Deserialize<'de> for Cursor<'a> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let buf = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Self(
            bincode::deserialize(&buf).map_err(serde::de::Error::custom)?,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WrappedCursor<'a> {
    pub record_id: IndexValue<'a>,
    pub values: HashMap<FieldPath, IndexValue<'a>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CursorDirection {
    Before,
    After,
}

impl<'a> WrappedCursor<'a> {
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

        Ok(WrappedCursor {
            record_id: record
                .get("id")
                .ok_or(Error::RecordMissingID)?
                .clone()
                .try_into()?,
            values,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn test_cursor_serialization_deserialization() {
        let cursor = Cursor(WrappedCursor {
            record_id: IndexValue::String(Cow::Owned("1".to_string())),
            values: HashMap::new(),
        });

        let serialized = serde_json::to_string(&cursor).unwrap();
        //println!("serialized = {serialized:?}");
        let deserialized: Cursor = serde_json::from_str(&serialized).unwrap();
        //println!("{deserialized:?}");

        assert_eq!(cursor, deserialized);
    }
}
