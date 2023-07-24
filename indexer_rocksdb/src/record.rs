use std::borrow::Cow;

use crate::keys;
use indexer_db_adaptor::{
    publickey,
    record::{ForeignRecordReference, IndexValue, RecordError},
};

pub type Result<T> = std::result::Result<T, RecordError>;

pub trait RocksDBIndexValue {
    fn byte_prefix(&self) -> u8;
    fn deserialize(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

impl RocksDBIndexValue for IndexValue<'_> {
    fn byte_prefix(&self) -> u8 {
        match self {
            IndexValue::Null => keys::BYTE_NULL,
            IndexValue::String(_) => keys::BYTE_STRING,
            IndexValue::Number(_) => keys::BYTE_NUMBER,
            IndexValue::Boolean(_) => keys::BYTE_BOOLEAN,
            IndexValue::PublicKey(_) => keys::BYTE_PUBLIC_KEY,
            IndexValue::ForeignRecordReference(_) => keys::BYTE_FOREIGN_RECORD_REFERENCE,
        }
    }

    fn deserialize(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let type_prefix = bytes[0];
        let value = &bytes[1..];
        let value = match type_prefix {
            keys::BYTE_STRING => IndexValue::String(Cow::Owned(String::from_utf8(value.to_vec())?)),
            keys::BYTE_NUMBER => IndexValue::Number(f64::from_be_bytes(value.try_into()?)),
            keys::BYTE_BOOLEAN => IndexValue::Boolean(match value[0] {
                0x00 => false,
                0x01 => true,
                b => return Err(RecordError::InvalidBooleanByte { b }),
            }),
            keys::BYTE_NULL => IndexValue::Null,
            keys::BYTE_PUBLIC_KEY => {
                IndexValue::PublicKey(Cow::Owned(publickey::PublicKey::from_indexable(value)?))
            }
            keys::BYTE_FOREIGN_RECORD_REFERENCE => IndexValue::ForeignRecordReference(Cow::Owned(
                ForeignRecordReference::from_indexable(value)?,
            )),
            b => return Err(RecordError::InvalidTypePrefix { b }),
        };

        Ok(value)
    }
}
