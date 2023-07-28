use std::{borrow::Cow, collections::HashMap, error::Error};

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::publickey;

pub type Result<T> = std::result::Result<T, RecordError>;

// TODO: review if we still need all these errors
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

pub fn json_to_record(
    collection: &polylang::stableast::Collection,
    value: serde_json::Value,
    always_cast: bool,
) -> Result<RecordRoot> {
    let mut map = HashMap::new();
    let serde_json::Value::Object(mut value) = value else {
        return Err(RecordUserError::RecordRootShouldBeAnObject { got: value }.into());
    };

    for (field, ty, required) in collection.attributes.iter().filter_map(|a| match a {
        polylang::stableast::CollectionAttribute::Property(p) => {
            Some((&p.name, &p.type_, &p.required))
        }
        _ => None,
    }) {
        let Some((name, value)) = value.remove_entry(field.as_ref())
        else {
            if *required {
                if always_cast {
                    // Insert default for the type
                    map.insert(field.to_string(), Converter::convert((ty, serde_json::Value::Null), &mut vec![Cow::Borrowed(field.as_ref())], always_cast)?);
                    continue;
                }

                return Err(RecordUserError::MissingField { field: field.to_string() }.into());
            } else {
                continue;
            }
        };

        let converted =
            Converter::convert((ty, value), &mut vec![Cow::Borrowed(&name)], always_cast)?;
        map.insert(name, converted);
    }

    Ok(map)
}

pub fn record_to_json(value: RecordRoot) -> Result<serde_json::Value> {
    let mut map = serde_json::Map::new();

    for (field, value) in value {
        map.insert(field, value.try_into()?);
    }

    Ok(serde_json::Value::Object(map))
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum RecordValue {
    Number(f64),
    Boolean(bool),
    Null,
    String(String),
    PublicKey(crate::publickey::PublicKey),
    Bytes(Vec<u8>),
    Map(HashMap<String, RecordValue>),
    Array(Vec<RecordValue>),
    RecordReference(RecordReference),
    ForeignRecordReference(ForeignRecordReference),
}

/// JSON to RecordValue converter
pub trait Converter {
    /// If always_cast is true, the converter will try to cast values of mismatched types,
    /// if it fails, then it will set them to the default value for the schema type.
    fn convert(self, path: &mut Vec<Cow<str>>, always_cast: bool) -> Result<RecordValue>;
}

impl Converter for (&polylang::stableast::Type<'_>, serde_json::Value) {
    fn convert(self, path: &mut Vec<Cow<str>>, always_cast: bool) -> Result<RecordValue> {
        use polylang::stableast::{PrimitiveType, Type};

        let (ty, value) = self;
        match ty {
            Type::Primitive(p) => match (&p.value, value) {
                (PrimitiveType::String, value) => match value {
                    serde_json::Value::String(s) => Ok(RecordValue::String(s)),
                    serde_json::Value::Null if always_cast => {
                        Ok(RecordValue::String("".to_string()))
                    }
                    // cast user-provided boolean to string
                    serde_json::Value::Bool(b) if always_cast => {
                        Ok(RecordValue::String(b.to_string()))
                    }
                    // cast user-provided number to string
                    serde_json::Value::Number(n) if always_cast => {
                        Ok(RecordValue::String(n.to_string()))
                    }
                    serde_json::Value::Array(a) if always_cast => {
                        Ok(RecordValue::String(serde_json::to_string(&a)?))
                    }
                    serde_json::Value::Object(o) if always_cast => {
                        Ok(RecordValue::String(serde_json::to_string(&o)?))
                    }
                    x => {
                        if always_cast {
                            Ok(RecordValue::String("".to_string()))
                        } else {
                            Err(RecordUserError::InvalidFieldValueType {
                                value: x,
                                expected_type: ty.to_string(),
                                field: Some(path.join(".")),
                            }
                            .into())
                        }
                    }
                },
                (PrimitiveType::Bytes, value) => match value {
                    serde_json::Value::String(s) => Ok(RecordValue::Bytes({
                        let mut r = base64::engine::general_purpose::STANDARD.decode(s.as_bytes());

                        if r.is_err() && always_cast {
                            r = Ok(vec![]);
                        }

                        r?
                    })),
                    serde_json::Value::Null if always_cast => Ok(RecordValue::Bytes(vec![])),
                    serde_json::Value::Bool(b) if always_cast => {
                        Ok(RecordValue::Bytes(vec![b as u8]))
                    }
                    serde_json::Value::Number(n) if always_cast => {
                        let mut r = n.as_f64().ok_or(RecordError::FailedToConvertNumberToF64);
                        if r.is_err() && always_cast {
                            r = Ok(0.0);
                        }

                        let r = r?;

                        Ok(RecordValue::Bytes({
                            let mut r =
                                base64::engine::general_purpose::STANDARD.decode(r.to_le_bytes());

                            if r.is_err() && always_cast {
                                r = Ok(vec![]);
                            }

                            r?
                        }))
                    }
                    serde_json::Value::Array(a) if always_cast => {
                        Ok(RecordValue::Bytes(serde_json::to_vec(&a)?))
                    }
                    serde_json::Value::Object(o) if always_cast => {
                        Ok(RecordValue::Bytes(serde_json::to_vec(&o)?))
                    }
                    x => {
                        if always_cast {
                            Ok(RecordValue::Bytes(vec![]))
                        } else {
                            Err(RecordUserError::InvalidFieldValueType {
                                value: x,
                                expected_type: ty.to_string(),
                                field: Some(path.join(".")),
                            }
                            .into())
                        }
                    }
                },
                (PrimitiveType::Number, value) => match value {
                    serde_json::Value::Number(n) => Ok(RecordValue::Number({
                        let mut r = n.as_f64().ok_or(RecordError::FailedToConvertNumberToF64);
                        if r.is_err() && always_cast {
                            r = Ok(0.0);
                        }

                        r?
                    })),
                    serde_json::Value::Null if always_cast => Ok(RecordValue::Number(0.0)),
                    serde_json::Value::Bool(b) if always_cast => {
                        Ok(RecordValue::Number(if b { 1.0 } else { 0.0 }))
                    }
                    serde_json::Value::String(s) if always_cast => {
                        Ok(RecordValue::Number(s.parse::<f64>().unwrap_or(0.0)))
                    }
                    x => {
                        if always_cast {
                            Ok(RecordValue::Number(0.0))
                        } else {
                            Err(RecordUserError::InvalidFieldValueType {
                                value: x,
                                expected_type: ty.to_string(),
                                field: Some(path.join(".")),
                            }
                            .into())
                        }
                    }
                },
                (PrimitiveType::Boolean, value) => match value {
                    serde_json::Value::Bool(b) => Ok(RecordValue::Boolean(b)),
                    serde_json::Value::Null if always_cast => Ok(RecordValue::Boolean(false)),
                    serde_json::Value::Number(n) if always_cast => {
                        Ok(RecordValue::Boolean(n.as_f64().unwrap_or(0.0) != 0.0))
                    }
                    serde_json::Value::String(s) if always_cast => {
                        Ok(RecordValue::Boolean(s == "true"))
                    }
                    x => {
                        if always_cast {
                            Ok(RecordValue::Boolean(false))
                        } else {
                            Err(RecordUserError::InvalidFieldValueType {
                                value: x,
                                expected_type: ty.to_string(),
                                field: Some(path.join(".")),
                            }
                            .into())
                        }
                    }
                },
            },
            Type::Array(t) => {
                path.push(Cow::Borrowed("[]"));

                let res = match value {
                    serde_json::Value::Array(a) => {
                        let mut array = Vec::with_capacity(a.len());

                        for v in a {
                            array.push(Converter::convert(
                                (t.value.as_ref(), v),
                                path,
                                always_cast,
                            )?);
                        }

                        Ok(RecordValue::Array(array))
                    }
                    serde_json::Value::Null if always_cast => Ok(RecordValue::Array(vec![])),
                    serde_json::Value::Bool(b) if always_cast => {
                        Ok(RecordValue::Array(vec![Converter::convert(
                            (t.value.as_ref(), serde_json::Value::Bool(b)),
                            path,
                            always_cast,
                        )?]))
                    }
                    serde_json::Value::Number(n) if always_cast => {
                        Ok(RecordValue::Array(vec![Converter::convert(
                            (t.value.as_ref(), serde_json::Value::Number(n)),
                            path,
                            always_cast,
                        )?]))
                    }
                    serde_json::Value::String(s) if always_cast => {
                        Ok(RecordValue::Array(vec![Converter::convert(
                            (t.value.as_ref(), serde_json::Value::String(s)),
                            path,
                            always_cast,
                        )?]))
                    }
                    serde_json::Value::Object(_) if always_cast => {
                        // Turn this into an array with one object
                        let arr = vec![Converter::convert(
                            (t.value.as_ref(), value),
                            path,
                            always_cast,
                        )?];
                        Ok(RecordValue::Array(arr))
                    }
                    x => {
                        if always_cast {
                            Ok(RecordValue::Array(vec![]))
                        } else {
                            Err(RecordUserError::InvalidFieldValueType {
                                value: x,
                                expected_type: ty.to_string(),
                                field: Some(path.join(".")),
                            }
                            .into())
                        }
                    }
                };

                path.pop();

                res
            }
            Type::Map(t) => match value {
                serde_json::Value::Object(o) => {
                    let mut map = HashMap::with_capacity(o.len());
                    for (k, v) in o {
                        path.push(Cow::Owned(k.clone()));
                        map.insert(
                            k,
                            Converter::convert((t.value.as_ref(), v), path, always_cast)?,
                        );
                        path.pop();
                    }
                    Ok(RecordValue::Map(map))
                }
                x => {
                    if always_cast {
                        Ok(RecordValue::Map(HashMap::new()))
                    } else {
                        Err(RecordUserError::InvalidFieldValueType {
                            value: x,
                            expected_type: ty.to_string(),
                            field: Some(path.join(".")),
                        }
                        .into())
                    }
                }
            },
            Type::Object(t) => match value {
                serde_json::Value::Object(mut o) => {
                    let mut map = HashMap::with_capacity(o.len());

                    let path_len_before_loop = path.len();
                    for field in &t.fields {
                        path.truncate(path_len_before_loop);
                        path.push(Cow::Owned(field.name.to_string()));

                        let Some((k, v)) = o.remove_entry(field.name.as_ref()) else {
                            if field.required {
                                if always_cast {
                                    map.insert(field.name.to_string(), Converter::convert((&field.type_, serde_json::Value::Null), path, always_cast)?);
                                    continue;
                                }

                                return Err(RecordUserError::MissingField { field: path.join(".") }.into());
                            } else {
                                continue;
                            }
                        };

                        map.insert(k, Converter::convert((&field.type_, v), path, always_cast)?);
                    }
                    path.truncate(path_len_before_loop);

                    if !o.is_empty() && !always_cast {
                        let path = path.join(".");
                        return Err(RecordUserError::UnexpectedFields {
                            fields: o.keys().map(|k| path.clone() + "." + k).collect::<Vec<_>>(),
                        }
                        .into());
                    }

                    Ok(RecordValue::Map(map))
                }
                x => {
                    if always_cast {
                        Ok(RecordValue::Map(HashMap::new()))
                    } else {
                        Err(RecordUserError::InvalidFieldValueType {
                            value: x,
                            expected_type: ty.to_string(),
                            field: Some(path.join(".")),
                        }
                        .into())
                    }
                }
            },
            Type::PublicKey(_) => match value {
                serde_json::Value::Object(_) => Ok(RecordValue::PublicKey(
                    publickey::PublicKey::try_from(value)?,
                )),
                serde_json::Value::String(s)
                    if always_cast && s.starts_with("0x") && s.len() == (2 + 32 * 2 * 2) =>
                {
                    // s is 0x-prefixed hex-encoded x and y parameters, without 0x04 prefix
                    if let Ok(bytes) = hex::decode(s[2..].as_bytes()) {
                        #[allow(clippy::unwrap_used)]
                        // Unwrap is safe because we know the hex is 64 bytes
                        let bytes = <[u8; 64]>::try_from(bytes.as_slice()).unwrap();

                        Ok(RecordValue::PublicKey(
                            publickey::PublicKey::try_from(bytes)
                                .unwrap_or_else(|_| publickey::PublicKey::default()),
                        ))
                    } else {
                        Ok(RecordValue::PublicKey(publickey::PublicKey::default()))
                    }
                }
                _ if always_cast => Ok(RecordValue::PublicKey(publickey::PublicKey::default())),
                x => Err(RecordUserError::InvalidFieldValueType {
                    value: x,
                    expected_type: ty.to_string(),
                    field: Some(path.join(".")),
                }
                .into()),
            },
            Type::Record(_) => Ok(RecordValue::RecordReference({
                let mut r = match value {
                    serde_json::Value::Object(mut o) => {
                        let id = {
                            path.push(Cow::Borrowed("id"));

                            let r = match o.remove("id") {
                                Some(serde_json::Value::String(s)) => s,
                                Some(v) => {
                                    return Err(RecordUserError::InvalidFieldValueType {
                                        value: v,
                                        expected_type: ty.to_string(),
                                        field: Some(path.join(".")),
                                    }
                                    .into())
                                }
                                None => {
                                    return Err(RecordUserError::MissingField {
                                        field: path.join("."),
                                    }
                                    .into())
                                }
                            };

                            path.pop();
                            r
                        };

                        if !o.is_empty() && !always_cast {
                            let path = path.join(".");

                            return Err(RecordUserError::UnexpectedFields {
                                fields: o.keys().map(|k| path.clone() + "." + k).collect(),
                            }
                            .into());
                        }

                        Ok(RecordReference { id })
                    }
                    x => Err(RecordUserError::InvalidFieldValueType {
                        value: x,
                        expected_type: ty.to_string(),
                        field: Some(path.join(".")),
                    }),
                };
                if r.is_err() && always_cast {
                    r = Ok(RecordReference::default());
                }

                r?
            })),
            Type::ForeignRecord(fr) => {
                let convert = || {
                    let reference = match value {
                        serde_json::Value::Object(mut m) => {
                            let id = {
                                path.push(Cow::Borrowed("id"));

                                let r = match m.remove("id") {
                                    Some(serde_json::Value::String(s)) => s,
                                    Some(v) => {
                                        return Err(RecordUserError::InvalidFieldValueType {
                                            value: v,
                                            expected_type: ty.to_string(),
                                            field: Some(path.join(".")),
                                        }
                                        .into())
                                    }
                                    _ => {
                                        return Err(RecordUserError::MissingField {
                                            field: path.join("."),
                                        }
                                        .into())
                                    }
                                };

                                path.pop();
                                r
                            };

                            let collection_id = {
                                path.push(Cow::Borrowed("collectionId"));

                                let r = match m.remove("collectionId") {
                                    Some(serde_json::Value::String(s)) => s,
                                    Some(v) => {
                                        return Err(RecordUserError::InvalidFieldValueType {
                                            value: v,
                                            expected_type: ty.to_string(),
                                            field: Some(path.join(".")),
                                        }
                                        .into())
                                    }
                                    None => {
                                        return Err(RecordUserError::MissingField {
                                            field: path.join("."),
                                        }
                                        .into())
                                    }
                                };

                                path.pop();
                                r
                            };

                            if !m.is_empty() {
                                let path = path.join(".");
                                return Err(RecordUserError::UnexpectedFields {
                                    fields: m.keys().map(|k| path.clone() + "." + k).collect(),
                                }
                                .into());
                            }

                            Ok(ForeignRecordReference { id, collection_id })
                        }
                        v => Err(RecordUserError::InvalidFieldValueType {
                            value: v,
                            expected_type: ty.to_string(),
                            field: Some(path.join(".")),
                        }),
                    }?;

                    #[allow(clippy::unwrap_used)] // split always returns at least one element
                    let short_collection_name = reference.collection_id.split('/').last().unwrap();

                    if short_collection_name != fr.collection {
                        return Err::<_, RecordError>(
                            RecordError::ForeignRecordReferenceHasWrongCollectionId {
                                collection_id: fr.collection.clone().into_owned(),
                            },
                        );
                    }

                    Ok(reference)
                };

                let mut r = convert();
                if r.is_err() && always_cast {
                    r = Ok(ForeignRecordReference::default());
                }

                Ok(RecordValue::ForeignRecordReference(r?))
            }
            Type::Unknown => Err(RecordError::UnknownType),
        }
    }
}

impl TryFrom<RecordValue> for serde_json::Value {
    type Error = RecordError;

    fn try_from(value: RecordValue) -> Result<Self> {
        match value {
            RecordValue::String(s) => Ok(serde_json::Value::String(s)),
            RecordValue::Number(n) => Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(n)
                    .ok_or(RecordError::FailedToConvertF64ToSerdeNumber { f: n })?,
            )),
            RecordValue::Boolean(b) => Ok(serde_json::Value::Bool(b)),
            RecordValue::PublicKey(p) => Ok(serde_json::Value::from(p)),
            RecordValue::Null => Ok(serde_json::Value::Null),
            RecordValue::Bytes(b) => Ok(serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(b),
            )),
            RecordValue::Map(m) => {
                let mut map = serde_json::Map::with_capacity(m.len());
                for (k, v) in m {
                    map.insert(k, serde_json::Value::try_from(v)?);
                }
                Ok(serde_json::Value::Object(map))
            }
            RecordValue::Array(a) => {
                let mut array = Vec::with_capacity(a.len());
                for v in a {
                    array.push(serde_json::Value::try_from(v)?);
                }
                Ok(serde_json::Value::Array(array))
            }
            RecordValue::RecordReference(r) => Ok(serde_json::Value::from(r)),
            RecordValue::ForeignRecordReference(r) => Ok(serde_json::Value::from(r)),
        }
    }
}

impl TryFrom<IndexValue<'_>> for serde_json::Value {
    type Error = RecordError;

    fn try_from(value: IndexValue) -> Result<Self> {
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

impl RecordValue {
    pub fn walk<'a, E: Error>(
        &'a self,
        current_path: &mut Vec<Cow<'a, str>>,
        f: &mut impl FnMut(&[Cow<str>], IndexValue<'a>) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            RecordValue::Number(n) => {
                f(current_path, IndexValue::Number(*n))?;
            }
            RecordValue::Boolean(b) => {
                f(current_path, IndexValue::Boolean(*b))?;
            }
            RecordValue::Null => {
                f(current_path, IndexValue::Null)?;
            }
            RecordValue::String(s) => {
                f(current_path, IndexValue::String(Cow::Borrowed(s)))?;
            }
            RecordValue::PublicKey(p) => {
                f(current_path, IndexValue::PublicKey(Cow::Borrowed(p)))?;
            }
            RecordValue::Bytes(_) => {}
            RecordValue::Map(m) => {
                for (k, v) in m.iter() {
                    current_path.push(Cow::Borrowed(k));
                    v.walk(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::Array(a) => {
                for (i, v) in a.iter().enumerate() {
                    current_path.push(Cow::Owned(i.to_string()));
                    v.walk(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::RecordReference(_) => {}
            RecordValue::ForeignRecordReference(fr) => {
                f(
                    current_path,
                    IndexValue::ForeignRecordReference(Cow::Borrowed(fr)),
                )?;
            }
        }

        Ok(())
    }

    pub fn walk_all<'a, E: Error>(
        &'a self,
        current_path: &mut Vec<Cow<'a, str>>,
        f: &mut impl FnMut(&[Cow<str>], &'a RecordValue) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            RecordValue::String(_) => {
                f(current_path, self)?;
            }
            RecordValue::Number(_) => {
                f(current_path, self)?;
            }
            RecordValue::Boolean(_) => {
                f(current_path, self)?;
            }
            RecordValue::PublicKey(_) => {
                f(current_path, self)?;
            }
            RecordValue::Null => {
                f(current_path, self)?;
            }
            RecordValue::Bytes(_) => {
                f(current_path, self)?;
            }
            RecordValue::Map(m) => {
                f(current_path, self)?;

                for (k, v) in m.iter() {
                    current_path.push(Cow::Borrowed(k));
                    v.walk_all(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::Array(a) => {
                f(current_path, self)?;

                for (i, v) in a.iter().enumerate() {
                    current_path.push(Cow::Owned(i.to_string()));
                    v.walk_all(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::RecordReference(_) => {
                f(current_path, self)?;
            }
            RecordValue::ForeignRecordReference(_) => {
                f(current_path, self)?;
            }
        }

        Ok(())
    }

    pub fn walk_maps_mut<'rv, E>(
        &mut self,
        current_path: &mut Vec<Cow<'rv, str>>,
        f: &mut impl FnMut(
            &[Cow<'rv, str>],
            &mut HashMap<String, RecordValue>,
        ) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            RecordValue::String(_) => {}
            RecordValue::Number(_) => {}
            RecordValue::Boolean(_) => {}
            RecordValue::PublicKey(_) => {}
            RecordValue::Null => {}
            RecordValue::Bytes(_) => {}
            RecordValue::Map(m) => {
                f(current_path, m)?;
                let keys = m.keys().cloned().collect::<Vec<_>>();
                for (k, v) in keys.into_iter().zip(m.values_mut()) {
                    current_path.push(Cow::Owned(k));
                    v.walk_maps_mut(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::Array(a) => {
                for (i, v) in a.iter_mut().enumerate() {
                    current_path.push(Cow::Owned(i.to_string()));
                    v.walk_maps_mut(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::RecordReference(_) => {}
            RecordValue::ForeignRecordReference(_) => {}
        }

        Ok(())
    }
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
    pub fn to_indexable(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&u32::to_be_bytes(self.collection_id.as_bytes().len() as u32));
        v.extend_from_slice(self.collection_id.as_bytes());
        v.extend_from_slice(&u32::to_be_bytes(self.id.as_bytes().len() as u32));
        v.extend_from_slice(self.id.as_bytes());
        v
    }

    pub fn from_indexable(v: &[u8]) -> Result<Self> {
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

pub trait PathFinder {
    fn find_path<T>(&self, path: &[T]) -> Option<&RecordValue>
    where
        T: AsRef<str> + PartialEq + for<'other> PartialEq<&'other str>;

    fn find_path_mut<T>(&mut self, path: &[T]) -> Option<&mut RecordValue>
    where
        T: AsRef<str> + PartialEq + for<'other> PartialEq<&'other str>;
}

impl PathFinder for RecordRoot {
    fn find_path<T>(&self, path: &[T]) -> Option<&RecordValue>
    where
        T: AsRef<str> + PartialEq + for<'other> PartialEq<&'other str>,
    {
        let Some(head) = path.first() else {
            return None;
        };

        let Some(value) = self.get(head.as_ref()) else {
            return None;
        };

        if path.len() == 1 {
            return Some(value);
        }

        value.find_path(&path[1..])
    }

    fn find_path_mut<T>(&mut self, path: &[T]) -> Option<&mut RecordValue>
    where
        T: AsRef<str> + PartialEq + for<'other> PartialEq<&'other str>,
    {
        let Some(head) = path.first() else {
            return None;
        };

        let Some(value) = self.get_mut(head.as_ref()) else {
            return None;
        };

        if path.len() == 1 {
            return Some(value);
        }

        value.find_path_mut(&path[1..])
    }
}

impl PathFinder for RecordValue {
    fn find_path<T>(&self, path: &[T]) -> std::option::Option<&RecordValue>
    where
        T: AsRef<str> + PartialEq + for<'other> PartialEq<&'other str>,
    {
        let Some(head) = path.first() else {
            return None;
        };

        match self {
            RecordValue::Null => None,
            RecordValue::Boolean(_) => None,
            RecordValue::Number(_) => None,
            RecordValue::String(_) => None,
            RecordValue::PublicKey(_) => None,
            RecordValue::Bytes(_) => None,
            RecordValue::RecordReference(_) => None,
            RecordValue::ForeignRecordReference(_) => None,
            RecordValue::Map(m) => m.find_path(path),
            RecordValue::Array(a) => {
                if let Ok(index) = head.as_ref().parse::<usize>() {
                    if let Some(value) = a.get(index) {
                        if path.len() == 1 {
                            return Some(value);
                        }

                        value.find_path(&path[1..])
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    fn find_path_mut<T>(&mut self, path: &[T]) -> std::option::Option<&mut RecordValue>
    where
        T: AsRef<str> + PartialEq + for<'other> PartialEq<&'other str>,
    {
        let Some(head) = path.first() else {
            return None;
        };

        match self {
            RecordValue::Null => None,
            RecordValue::Boolean(_) => None,
            RecordValue::Number(_) => None,
            RecordValue::String(_) => None,
            RecordValue::PublicKey(_) => None,
            RecordValue::Bytes(_) => None,
            RecordValue::RecordReference(_) => None,
            RecordValue::ForeignRecordReference(_) => None,
            RecordValue::Map(m) => m.find_path_mut(path),
            RecordValue::Array(a) => {
                if let Ok(index) = head.as_ref().parse::<usize>() {
                    if let Some(value) = a.get_mut(index) {
                        if path.len() == 1 {
                            return Some(value);
                        }

                        value.find_path_mut(&path[1..])
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum IndexValue<'a> {
    Number(f64),
    Boolean(bool),
    Null,
    String(Cow<'a, str>),
    PublicKey(Cow<'a, crate::publickey::PublicKey>),
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

impl TryFrom<RecordValue> for IndexValue<'_> {
    type Error = ();

    fn try_from(value: RecordValue) -> std::result::Result<Self, ()> {
        match value {
            RecordValue::Null => Ok(IndexValue::Null),
            RecordValue::Boolean(b) => Ok(IndexValue::Boolean(b)),
            RecordValue::Number(n) => Ok(IndexValue::Number(n)),
            RecordValue::String(s) => Ok(IndexValue::String(Cow::Owned(s))),
            RecordValue::PublicKey(p) => Ok(IndexValue::PublicKey(Cow::Owned(p))),
            RecordValue::ForeignRecordReference(fr) => {
                Ok(IndexValue::ForeignRecordReference(Cow::Owned(fr)))
            }
            RecordValue::Bytes(_) => Err(()),
            RecordValue::RecordReference(_) => Err(()),
            RecordValue::Map(_) => Err(()),
            RecordValue::Array(_) => Err(()),
        }
    }
}
