use std::{borrow::Cow, collections::HashMap, error::Error};

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::{keys, publickey};

pub type Result<T> = std::result::Result<T, RecordError>;

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("invalid boolean byte {b}")]
    InvalidBooleanByte { b: u8 },

    #[error("invalid type prefix {b}")]
    InvalidTypePrefix { b: u8 },

    #[error("value {value:?} at field {field:?} does not match the schema type")]
    InvalidSerdeJSONType {
        value: serde_json::Value,
        field: Option<String>,
    },

    #[error("missing field {field:?}")]
    MissingField { field: String },

    #[error("unexpected fields {fields:?}")]
    UnexpectedFields { fields: Vec<String> },

    #[error("expected value to be an object, got {got:?}")]
    ExpectedObject { got: serde_json::Value },

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

pub type RecordRoot = HashMap<String, RecordValue>;

pub fn json_to_record(
    collection: &polylang::stableast::Collection,
    value: serde_json::Value,
    always_cast: bool,
) -> Result<RecordRoot> {
    let mut map = HashMap::new();
    let serde_json::Value::Object(mut value) = value else {
        return Err(RecordError::ExpectedObject { got: value });
    };

    for (field, ty, required) in collection.attributes.iter().filter_map(|a| match a {
        polylang::stableast::CollectionAttribute::Property(p) => {
            Some((&p.name, &p.type_, &p.required))
        }
        _ => None,
    }) {
        let Some((name, value)) = value.remove_entry(field.as_ref()) else {
            if *required {
                if always_cast {
                    // Insert default for the type
                    map.insert(field.to_string(), Converter::convert((ty, serde_json::Value::Null), always_cast)?);
                    continue;
                }

                return Err(RecordError::MissingField { field: field.to_string() });
            } else {
                continue;
            }
        };

        map.insert(name, Converter::convert((ty, value), always_cast)?);
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
    PublicKey(publickey::PublicKey),
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
    fn convert(self, always_cast: bool) -> Result<RecordValue>;
}

impl Converter for (&polylang::stableast::Type<'_>, serde_json::Value) {
    fn convert(self, always_cast: bool) -> Result<RecordValue> {
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
                            Err(RecordError::InvalidSerdeJSONType {
                                value: x,
                                field: None,
                            })
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
                            Err(RecordError::InvalidSerdeJSONType {
                                value: x,
                                field: None,
                            })
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
                            Err(RecordError::InvalidSerdeJSONType {
                                value: x,
                                field: None,
                            })
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
                            Err(RecordError::InvalidSerdeJSONType {
                                value: x,
                                field: None,
                            })
                        }
                    }
                },
            },
            Type::Array(t) => match value {
                serde_json::Value::Array(a) => {
                    let mut array = Vec::with_capacity(a.len());
                    for v in a {
                        array.push(Converter::convert((t.value.as_ref(), v), always_cast)?);
                    }

                    Ok(RecordValue::Array(array))
                }
                serde_json::Value::Null if always_cast => Ok(RecordValue::Array(vec![])),
                serde_json::Value::Bool(b) if always_cast => {
                    Ok(RecordValue::Array(vec![Converter::convert(
                        (t.value.as_ref(), serde_json::Value::Bool(b)),
                        always_cast,
                    )?]))
                }
                serde_json::Value::Number(n) if always_cast => {
                    Ok(RecordValue::Array(vec![Converter::convert(
                        (t.value.as_ref(), serde_json::Value::Number(n)),
                        always_cast,
                    )?]))
                }
                serde_json::Value::String(s) if always_cast => {
                    Ok(RecordValue::Array(vec![Converter::convert(
                        (t.value.as_ref(), serde_json::Value::String(s)),
                        always_cast,
                    )?]))
                }
                serde_json::Value::Object(_) if always_cast => {
                    // Turn this into an array with one object
                    let arr = vec![Converter::convert((t.value.as_ref(), value), always_cast)?];
                    Ok(RecordValue::Array(arr))
                }
                x => {
                    if always_cast {
                        Ok(RecordValue::Array(vec![]))
                    } else {
                        Err(RecordError::InvalidSerdeJSONType {
                            value: x,
                            field: None,
                        })
                    }
                }
            },
            Type::Map(t) => match value {
                serde_json::Value::Object(o) => {
                    let mut map = HashMap::with_capacity(o.len());
                    for (k, v) in o {
                        map.insert(k, Converter::convert((t.value.as_ref(), v), always_cast)?);
                    }
                    Ok(RecordValue::Map(map))
                }
                x => {
                    if always_cast {
                        Ok(RecordValue::Map(HashMap::new()))
                    } else {
                        Err(RecordError::InvalidSerdeJSONType {
                            value: x,
                            field: None,
                        })
                    }
                }
            },
            Type::Object(t) => match value {
                serde_json::Value::Object(mut o) => {
                    let mut map = HashMap::with_capacity(o.len());

                    for field in &t.fields {
                        let Some((k, v)) = o.remove_entry(field.name.as_ref()) else {
                            if field.required {
                                if always_cast {
                                    map.insert(field.name.to_string(), Converter::convert((&field.type_, serde_json::Value::Null), always_cast)?);
                                    continue;
                                }

                                return Err(RecordError::MissingField { field: field.name.to_string() });
                            } else {
                                continue;
                            }
                        };

                        map.insert(k, Converter::convert((&field.type_, v), always_cast)?);
                    }

                    if !o.is_empty() {
                        return Err(RecordError::UnexpectedFields {
                            fields: o.keys().map(|k| k.to_owned()).collect::<Vec<_>>(),
                        });
                    }

                    Ok(RecordValue::Map(map))
                }
                x => {
                    if always_cast {
                        Ok(RecordValue::Map(HashMap::new()))
                    } else {
                        Err(RecordError::InvalidSerdeJSONType {
                            value: x,
                            field: None,
                        })
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
                x => Err(RecordError::InvalidSerdeJSONType {
                    value: x,
                    field: None,
                }),
            },
            Type::Record(_) => Ok(RecordValue::RecordReference({
                let mut r = RecordReference::try_from(value);
                if r.is_err() && always_cast {
                    r = Ok(RecordReference::default());
                }

                r?
            })),
            Type::ForeignRecord(fr) => {
                let convert = || {
                    let reference = ForeignRecordReference::try_from(value)?;
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
            RecordValue::ForeignRecordReference(_) => {}
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

impl TryFrom<serde_json::Value> for RecordReference {
    type Error = RecordError;

    fn try_from(value: serde_json::Value) -> Result<Self> {
        match value {
            serde_json::Value::Object(mut o) => {
                let id = match o.remove("id") {
                    Some(serde_json::Value::String(s)) => s,
                    Some(v) => {
                        return Err(RecordError::InvalidSerdeJSONType {
                            value: v,
                            field: Some("id".to_string()),
                        })
                    }
                    None => {
                        return Err(RecordError::MissingField {
                            field: "id".to_string(),
                        })
                    }
                };

                if !o.is_empty() {
                    return Err(RecordError::UnexpectedFields {
                        fields: o.keys().map(|k| k.to_string()).collect(),
                    });
                }

                Ok(RecordReference { id })
            }
            x => Err(RecordError::ExpectedObject { got: x }),
        }
    }
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

impl TryFrom<serde_json::Value> for ForeignRecordReference {
    type Error = RecordError;

    fn try_from(value: serde_json::Value) -> Result<Self> {
        match value {
            serde_json::Value::Object(mut m) => {
                let id = match m.remove("id") {
                    Some(serde_json::Value::String(s)) => s,
                    Some(v) => {
                        return Err(RecordError::InvalidSerdeJSONType {
                            value: v,
                            field: Some("id".to_string()),
                        })
                    }
                    _ => {
                        return Err(RecordError::MissingField {
                            field: "id".to_string(),
                        })
                    }
                };

                let collection_id = match m.remove("collectionId") {
                    Some(serde_json::Value::String(s)) => s,
                    Some(v) => {
                        return Err(RecordError::InvalidSerdeJSONType {
                            value: v,
                            field: Some("collectionId".to_string()),
                        })
                    }
                    None => {
                        return Err(RecordError::MissingField {
                            field: "collectionId".to_string(),
                        })
                    }
                };

                if !m.is_empty() {
                    return Err(RecordError::UnexpectedFields {
                        fields: m.keys().map(|k| k.to_string()).collect(),
                    });
                }

                Ok(ForeignRecordReference { id, collection_id })
            }
            v => Err(RecordError::ExpectedObject { got: v }),
        }
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
    PublicKey(Cow<'a, publickey::PublicKey>),
}

impl IndexValue<'_> {
    pub(crate) fn byte_prefix(&self) -> u8 {
        match self {
            IndexValue::Null => keys::BYTE_NULL,
            IndexValue::String(_) => keys::BYTE_STRING,
            IndexValue::Number(_) => keys::BYTE_NUMBER,
            IndexValue::Boolean(_) => keys::BYTE_BOOLEAN,
            IndexValue::PublicKey(_) => keys::BYTE_PUBLIC_KEY,
        }
    }

    pub(crate) fn serialize(&self, mut w: impl std::io::Write) -> Result<()> {
        let number_bytes;
        let value: Cow<[u8]> = match self {
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
        };

        let len = 1 + u16::try_from(value.len())?;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&[self.byte_prefix()])?;
        w.write_all(&value[..])?;

        Ok(())
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self> {
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
            b => return Err(RecordError::InvalidTypePrefix { b }),
        };

        Ok(value)
    }

    pub(crate) fn with_static(self) -> IndexValue<'static> {
        match self {
            IndexValue::String(s) => IndexValue::String(Cow::Owned(s.into_owned())),
            IndexValue::PublicKey(p) => IndexValue::PublicKey(Cow::Owned(p.into_owned())),
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
            RecordValue::Bytes(_) => Err(()),
            RecordValue::RecordReference(_) => Err(()),
            RecordValue::ForeignRecordReference(_) => Err(()),
            RecordValue::Map(_) => Err(()),
            RecordValue::Array(_) => Err(()),
        }
    }
}
