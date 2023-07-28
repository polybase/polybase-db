use crate::keys;
use indexer_db_adaptor::{
    collection::record::{self, ForeignRecordReference, IndexValue},
    publickey,
};
use std::borrow::Cow;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid boolean byte {b}")]
    InvalidBooleanByte { b: u8 },

    #[error("invalid type prefix {b}")]
    InvalidTypePrefix { b: u8 },

    #[error("record error")]
    Record(#[from] record::RecordError),

    #[error("public key error")]
    PublicKey(#[from] indexer_db_adaptor::publickey::PublicKeyError),

    #[error("try from int error")]
    TryFromIntError(#[from] std::num::TryFromIntError),

    #[error("try from slice error")]
    TryFromSliceError(#[from] std::array::TryFromSliceError),

    #[error("from utf8 error")]
    FromUtf8(#[from] std::string::FromUtf8Error),

    #[error("IO error")]
    IO(#[from] std::io::Error),
}

pub(crate) fn byte_prefix(index_value: &IndexValue) -> u8 {
    match index_value {
        IndexValue::Null => keys::BYTE_NULL,
        IndexValue::String(_) => keys::BYTE_STRING,
        IndexValue::Number(_) => keys::BYTE_NUMBER,
        IndexValue::Boolean(_) => keys::BYTE_BOOLEAN,
        IndexValue::PublicKey(_) => keys::BYTE_PUBLIC_KEY,
        IndexValue::ForeignRecordReference(_) => keys::BYTE_FOREIGN_RECORD_REFERENCE,
    }
}

pub(crate) fn serialize(index_value: &IndexValue, mut w: impl std::io::Write) -> Result<()> {
    let number_bytes;
    let value: Cow<[u8]> = match index_value {
        IndexValue::String(s) => Cow::Borrowed(s.as_bytes()),
        IndexValue::Number(n) => {
            number_bytes = n.to_be_bytes();
            Cow::Borrowed(&number_bytes[..])
        }
        IndexValue::Boolean(b) => match b {
            false => Cow::Borrowed(&[0x00]),
            true => Cow::Borrowed(&[0x01]),
        },
        IndexValue::Null => Cow::Borrowed(&[0x00]),
        IndexValue::PublicKey(jwk) => Cow::Owned(jwk.to_indexable()),
        IndexValue::ForeignRecordReference(frr) => Cow::Owned(frr.to_indexable()),
    };

    let len = 1 + u16::try_from(value.len())?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&[byte_prefix(index_value)])?;
    w.write_all(&value[..])?;

    Ok(())
}

pub(crate) fn deserialize(bytes: &[u8]) -> Result<IndexValue> {
    let type_prefix = bytes[0];
    let value = &bytes[1..];
    let value = match type_prefix {
        keys::BYTE_STRING => IndexValue::String(Cow::Owned(String::from_utf8(value.to_vec())?)),
        keys::BYTE_NUMBER => IndexValue::Number(f64::from_be_bytes(value.try_into()?)),
        keys::BYTE_BOOLEAN => IndexValue::Boolean(match value[0] {
            0x00 => false,
            0x01 => true,
            b => return Err(Error::InvalidBooleanByte { b }),
        }),
        keys::BYTE_NULL => IndexValue::Null,
        keys::BYTE_PUBLIC_KEY => {
            IndexValue::PublicKey(Cow::Owned(publickey::PublicKey::from_indexable(value)?))
        }
        keys::BYTE_FOREIGN_RECORD_REFERENCE => IndexValue::ForeignRecordReference(Cow::Owned(
            ForeignRecordReference::from_indexable(value)?,
        )),
        b => return Err(Error::InvalidTypePrefix { b }),
    };

    Ok(value)
}
