use crate::index::Index;

use super::publickey::PublicKey;
use super::record::{self, ForeignRecordReference, RecordError, RecordValue};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, thiserror::Error)]
pub enum IndexValueError {
    #[error("record value cannnot be indexed")]
    TryFromRecordValue,
}

// TODO: refactor this into own module
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum IndexValue<'a> {
    Number(f64),
    Boolean(bool),
    Null,
    String(Cow<'a, str>),
    PublicKey(Cow<'a, PublicKey>),
    ForeignRecordReference(Cow<'a, ForeignRecordReference>),
}

impl From<u64> for IndexValue<'_> {
    fn from(n: u64) -> Self {
        IndexValue::Number(n as f64)
    }
}

impl From<f64> for IndexValue<'_> {
    fn from(n: f64) -> Self {
        IndexValue::Number(n)
    }
}

impl From<bool> for IndexValue<'_> {
    fn from(b: bool) -> Self {
        IndexValue::Boolean(b)
    }
}

impl<'a> From<&'a str> for IndexValue<'a> {
    fn from(s: &'a str) -> Self {
        IndexValue::String(Cow::Borrowed(s))
    }
}

impl IndexValue<'_> {
    pub fn with_static(self) -> IndexValue<'static> {
        match self {
            IndexValue::String(s) => IndexValue::String(Cow::Owned(s.into_owned())),
            IndexValue::PublicKey(p) => IndexValue::PublicKey(Cow::Owned(p.into_owned())),
            IndexValue::ForeignRecordReference(frr) => {
                IndexValue::ForeignRecordReference(Cow::Owned(frr.into_owned()))
            }
            IndexValue::Number(n) => IndexValue::Number(n),
            IndexValue::Boolean(b) => IndexValue::Boolean(b),
            IndexValue::Null => IndexValue::Null,
        }
    }
}

impl From<IndexValue<'_>> for RecordValue {
    fn from(value: IndexValue) -> Self {
        match value {
            IndexValue::Null => RecordValue::Null,
            IndexValue::Boolean(b) => RecordValue::Boolean(b),
            IndexValue::Number(n) => RecordValue::Number(n),
            IndexValue::String(s) => RecordValue::String(s.into_owned()),
            IndexValue::PublicKey(p) => RecordValue::PublicKey(p.into_owned()),
            IndexValue::ForeignRecordReference(fr) => {
                RecordValue::ForeignRecordReference(fr.into_owned())
            }
        }
    }
}

impl TryFrom<RecordValue> for IndexValue<'_> {
    type Error = IndexValueError;

    fn try_from(value: RecordValue) -> std::result::Result<Self, IndexValueError> {
        match value {
            RecordValue::Null => Ok(IndexValue::Null),
            RecordValue::Boolean(b) => Ok(IndexValue::Boolean(b)),
            RecordValue::Number(n) => Ok(IndexValue::Number(n)),
            RecordValue::String(s) => Ok(IndexValue::String(Cow::Owned(s))),
            RecordValue::PublicKey(p) => Ok(IndexValue::PublicKey(Cow::Owned(p))),
            RecordValue::ForeignRecordReference(fr) => {
                Ok(IndexValue::ForeignRecordReference(Cow::Owned(fr)))
            }
            RecordValue::Bytes(_) => Err(IndexValueError::TryFromRecordValue),
            RecordValue::RecordReference(_) => Err(IndexValueError::TryFromRecordValue),
            RecordValue::Map(_) => Err(IndexValueError::TryFromRecordValue),
            RecordValue::Array(_) => Err(IndexValueError::TryFromRecordValue),
        }
    }
}

impl TryFrom<IndexValue<'_>> for serde_json::Value {
    type Error = RecordError;

    fn try_from(value: IndexValue) -> record::Result<Self> {
        Ok(match value {
            IndexValue::String(s) => serde_json::Value::String(s.into_owned()),
            IndexValue::Number(n) => serde_json::Value::Number(
                serde_json::Number::from_f64(n)
                    .ok_or(RecordError::FailedToConvertF64ToSerdeNumber { f: n })?,
            ),
            IndexValue::Boolean(b) => serde_json::Value::Bool(b),
            IndexValue::PublicKey(p) => serde_json::Value::from(p.into_owned()),
            IndexValue::ForeignRecordReference(r) => serde_json::Value::from(r.into_owned()),
            IndexValue::Null => serde_json::Value::Null,
        })
    }
}
