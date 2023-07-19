use crate::publickey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type Result<T> = std::result::Result<T, RecordError>;

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error(transparent)]
    UserError(#[from] RecordUserError),

    #[error("invalid boolean byte {b}")]
    InvalidBooleanByte { b: u8 },

    #[error("invalid type prefix {b}")]
    InvalidTypePrefix { b: u8 },

    #[error("failed to convert number to f64")]
    FailedToConvertNumberToF64,

    #[error("failed to convert f64 ({f:?}) to serde number")]
    FailedToConvertF64ToSerdeNumber { f: f64 },

    #[error("foreign record reference has wrong collection id {collection_id:?}")]
    ForeignRecordReferenceHasWrongCollectionId { collection_id: String },

    #[error("unknown type")]
    UnknownType,

    #[error(transparent)]
    PublicKeyError(#[from] publickey::PublicKeyError),

    #[error("try from int error")]
    TryFromIntError(#[from] std::num::TryFromIntError),

    #[error("try from slice error")]
    TryFromSliceError(#[from] std::array::TryFromSliceError),

    #[error("from utf8 error")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("IO error")]
    IOError(#[from] std::io::Error),

    #[error("base64 decode error")]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("serde_json error")]
    SerdeJSONError(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum RecordUserError {
    #[error("record is missing field {field:?}")]
    MissingField { field: String },

    #[error("record root should be an object, got: {got}")]
    RecordRootShouldBeAnObject { got: serde_json::Value },

    #[error("value at field \"{}\" does not match the schema type, expected type: {expected_type}, got value: {value}", .field.as_deref().unwrap_or("unknown"))]
    InvalidFieldValueType {
        value: serde_json::Value,
        expected_type: String,
        field: Option<String>,
    },

    #[error("unexpected fields: {}", .fields.join(", "))]
    UnexpectedFields { fields: Vec<String> },
}

pub type RecordRoot = HashMap<String, RecordValue>;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum RecordValue {
    Number(f64),
    Boolean(bool),
    Null,
    String(String),
    PublicKey(publickey::PublicKey),
    Bytes(Vec<u8>),
    Map(HashMap<String, RecordValue>),
    Array(Vec<RecordValue>),
    RecordReference(RecordReference),
    ForeignRecordReference(ForeignRecordReference),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct RecordReference {
    pub id: String,
}

impl From<RecordReference> for serde_json::Value {
    fn from(r: RecordReference) -> Self {
        serde_json::json!({
            "id": r.id,
        })
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct ForeignRecordReference {
    pub id: String,
    pub collection_id: String,
}

impl ForeignRecordReference {
    fn to_indexable(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&u32::to_be_bytes(self.collection_id.as_bytes().len() as u32));
        v.extend_from_slice(self.collection_id.as_bytes());
        v.extend_from_slice(&u32::to_be_bytes(self.id.as_bytes().len() as u32));
        v.extend_from_slice(self.id.as_bytes());
        v
    }

    fn from_indexable(v: &[u8]) -> Result<Self> {
        let mut v = v;
        let collection_id_len = u32::from_be_bytes(v[..4].try_into()?) as usize;
        v = &v[4..];
        let collection_id = String::from_utf8(v[..collection_id_len].to_vec())?;
        v = &v[collection_id_len..];
        let id_len = u32::from_be_bytes(v[..4].try_into()?) as usize;
        v = &v[4..];
        let id = String::from_utf8(v[..id_len].to_vec())?;
        Ok(ForeignRecordReference { id, collection_id })
    }
}

impl From<ForeignRecordReference> for serde_json::Value {
    fn from(r: ForeignRecordReference) -> Self {
        serde_json::json!({
            "id": r.id,
            "collectionId": r.collection_id,
        })
    }
}
