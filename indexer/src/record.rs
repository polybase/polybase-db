use std::{borrow::Cow, collections::HashMap, error::Error};

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::{
    keys::{self, BYTE_BOOLEAN, BYTE_BYTES, BYTE_NULL, BYTE_NUMBER, BYTE_STRING},
    publickey,
};

pub type RecordRoot = HashMap<String, RecordValue>;

pub fn json_to_record(
    collection: &polylang::stableast::Collection,
    value: serde_json::Value,
    always_cast: bool,
) -> Result<RecordRoot, Box<dyn Error + Send + Sync + 'static>> {
    let mut map = HashMap::new();
    let serde_json::Value::Object(mut value) = value else {
        return Err("Expected value to be an object".into());
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

                return Err(format!("Missing field: {field}").into());
            } else {
                continue;
            }
        };

        map.insert(name, Converter::convert((ty, value), always_cast)?);
    }

    Ok(map)
}

pub fn record_to_json(
    value: RecordRoot,
) -> Result<serde_json::Value, Box<dyn Error + Send + Sync + 'static>> {
    let mut map = serde_json::Map::new();

    for (field, value) in value {
        map.insert(field, value.try_into()?);
    }

    Ok(serde_json::Value::Object(map))
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum RecordValue {
    IndexValue(IndexValue),
    Map(HashMap<String, RecordValue>),
    Array(Vec<RecordValue>),
    RecordReference(RecordReference),
    ForeignRecordReference(ForeignRecordReference),
}

/// JSON to RecordValue converter
pub trait Converter {
    /// If always_cast is true, the converter will try to cast values of mismatched types,
    /// if it fails, then it will set them to the default value for the schema type.
    fn convert(
        self,
        always_cast: bool,
    ) -> Result<RecordValue, Box<dyn Error + Send + Sync + 'static>>;
}

impl Converter for (&polylang::stableast::Type<'_>, serde_json::Value) {
    fn convert(
        self,
        always_cast: bool,
    ) -> Result<RecordValue, Box<dyn Error + Send + Sync + 'static>> {
        use polylang::stableast::{PrimitiveType, Type};

        let (ty, value) = self;
        match ty {
            Type::Primitive(p) => match (&p.value, value) {
                (PrimitiveType::String, value) => match value {
                    serde_json::Value::String(s) => {
                        Ok(RecordValue::IndexValue(IndexValue::String(s)))
                    }
                    serde_json::Value::Null if always_cast => {
                        Ok(RecordValue::IndexValue(IndexValue::String("".to_string())))
                    }
                    // cast user-provided boolean to string
                    serde_json::Value::Bool(b) if always_cast => {
                        Ok(RecordValue::IndexValue(IndexValue::String(b.to_string())))
                    }
                    // cast user-provided number to string
                    serde_json::Value::Number(n) if always_cast => {
                        Ok(RecordValue::IndexValue(IndexValue::String(n.to_string())))
                    }
                    serde_json::Value::Array(a) if always_cast => Ok(RecordValue::IndexValue(
                        IndexValue::String(serde_json::to_string(&a)?),
                    )),
                    serde_json::Value::Object(o) if always_cast => Ok(RecordValue::IndexValue(
                        IndexValue::String(serde_json::to_string(&o)?),
                    )),
                    x => {
                        if always_cast {
                            Ok(RecordValue::IndexValue(IndexValue::String("".to_string())))
                        } else {
                            Err(format!("type mismatch: {x:?}").into())
                        }
                    }
                },
                (PrimitiveType::Number, value) => match value {
                    serde_json::Value::Number(n) => {
                        Ok(RecordValue::IndexValue(IndexValue::Number({
                            let mut r = n.as_f64().ok_or("failed to convert number to f64");
                            if r.is_err() && always_cast {
                                r = Ok(0.0);
                            }

                            r?
                        })))
                    }
                    serde_json::Value::Null if always_cast => {
                        Ok(RecordValue::IndexValue(IndexValue::Number(0.0)))
                    }
                    serde_json::Value::Bool(b) if always_cast => {
                        Ok(RecordValue::IndexValue(IndexValue::Number(if b {
                            1.0
                        } else {
                            0.0
                        })))
                    }
                    serde_json::Value::String(s) if always_cast => Ok(RecordValue::IndexValue(
                        IndexValue::Number(s.parse::<f64>().unwrap_or(0.0)),
                    )),
                    x => {
                        if always_cast {
                            Ok(RecordValue::IndexValue(IndexValue::Number(0.0)))
                        } else {
                            Err(format!("type mismatch: {x:?}").into())
                        }
                    }
                },
                (PrimitiveType::Boolean, value) => match value {
                    serde_json::Value::Bool(b) => {
                        Ok(RecordValue::IndexValue(IndexValue::Boolean(b)))
                    }
                    serde_json::Value::Null if always_cast => {
                        Ok(RecordValue::IndexValue(IndexValue::Boolean(false)))
                    }
                    serde_json::Value::Number(n) if always_cast => Ok(RecordValue::IndexValue(
                        IndexValue::Boolean(n.as_f64().unwrap_or(0.0) != 0.0),
                    )),
                    serde_json::Value::String(s) if always_cast => {
                        Ok(RecordValue::IndexValue(IndexValue::Boolean(s == "true")))
                    }
                    x => {
                        if always_cast {
                            Ok(RecordValue::IndexValue(IndexValue::Boolean(false)))
                        } else {
                            Err(format!("type mismatch: {x:?}").into())
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
                    // Turn this into an array with one object (o)
                    let arr = vec![Converter::convert((t.value.as_ref(), value), always_cast)?];
                    Ok(RecordValue::Array(arr))
                }
                x => {
                    if always_cast {
                        Ok(RecordValue::Array(vec![]))
                    } else {
                        Err(format!("type mismatch: {x:?}").into())
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
                        Err(format!("type mismatch: {x:?}").into())
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

                                return Err(format!("missing field: {}", field.name).into());
                            } else {
                                continue;
                            }
                        };

                        map.insert(k, Converter::convert((&field.type_, v), always_cast)?);
                    }

                    if !o.is_empty() {
                        return Err(format!(
                            "unexpected fields: {:?}",
                            o.keys().map(|k| k.as_str()).collect::<Vec<_>>().join(", ")
                        )
                        .into());
                    }

                    Ok(RecordValue::Map(map))
                }
                x => {
                    if always_cast {
                        Ok(RecordValue::Map(HashMap::new()))
                    } else {
                        Err(format!("type mismatch: {x:?}").into())
                    }
                }
            },
            Type::PublicKey(_) => Ok(RecordValue::IndexValue(IndexValue::PublicKey(
                publickey::PublicKey::try_from(value)?,
            ))),
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
                    let short_collection_name = fr.collection.split('/').last().unwrap();

                    if short_collection_name != fr.collection {
                        return Err::<_, Box<dyn std::error::Error + Send + Sync>>(
                            format!(
                                "foreign record reference has wrong collection id: {}",
                                fr.collection
                            )
                            .into(),
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
            Type::Unknown => Err("unknown type".into()),
        }
    }
}

impl TryFrom<RecordValue> for serde_json::Value {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(value: RecordValue) -> Result<Self, Self::Error> {
        match value {
            RecordValue::IndexValue(v) => Ok(serde_json::Value::try_from(v)?),
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

impl TryFrom<IndexValue> for serde_json::Value {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(value: IndexValue) -> Result<Self, Self::Error> {
        Ok(match value {
            IndexValue::String(s) => serde_json::Value::String(s),
            IndexValue::Number(n) => serde_json::Value::Number(
                serde_json::Number::from_f64(n).ok_or_else(|| format!("invalid number: {n}"))?,
            ),
            IndexValue::Boolean(b) => serde_json::Value::Bool(b),
            IndexValue::PublicKey(p) => serde_json::Value::from(p),
            IndexValue::Bytes(b) => {
                serde_json::Value::String(base64::engine::general_purpose::URL_SAFE.encode(b))
            }
            IndexValue::Null => serde_json::Value::Null,
        })
    }
}

impl RecordValue {
    pub fn walk<'a, E: Error>(
        &'a self,
        current_path: &mut Vec<Cow<'a, str>>,
        f: &mut impl FnMut(&[Cow<str>], &'a IndexValue) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            RecordValue::IndexValue(v) => {
                f(current_path, v)?;
            }
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
        f: &mut impl FnMut(&[Cow<str>], &'a RecordValue) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            RecordValue::IndexValue(_) => {
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
        f: &mut impl FnMut(&[Cow<'rv, str>], &mut HashMap<String, RecordValue>) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            RecordValue::IndexValue(_) => {}
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
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Object(mut o) => {
                let id = match o.remove("id") {
                    Some(serde_json::Value::String(s)) => s,
                    Some(_) => return Err("id must be a string".into()),
                    None => return Err("missing id".into()),
                };

                if !o.is_empty() {
                    return Err(format!(
                        "unexpected fields: {:?}",
                        o.keys().map(|k| k.as_str()).collect::<Vec<_>>().join(", ")
                    )
                    .into());
                }

                Ok(RecordReference { id })
            }
            x => Err(format!("expected object, got {x:?}").into()),
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

impl TryFrom<&RecordValue> for ForeignRecordReference {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(value: &RecordValue) -> Result<Self, Self::Error> {
        match value {
            RecordValue::Map(m) => {
                let id = match m.get("id") {
                    Some(RecordValue::IndexValue(IndexValue::String(s))) => s.to_string(),
                    _ => return Err("foreign record reference must have an id".into()),
                };

                let collection_id = match m.get("collectionId") {
                    Some(RecordValue::IndexValue(IndexValue::String(s))) => s.to_string(),
                    Some(_) => return Err("collectionId must be a string".into()),
                    None => return Err("missing collectionId".into()),
                };

                if !m.is_empty() {
                    return Err(format!(
                        "unexpected fields: {:?}",
                        m.keys().map(|k| k.as_str()).collect::<Vec<_>>().join(", ")
                    )
                    .into());
                }

                Ok(ForeignRecordReference { id, collection_id })
            }
            x => Err(format!("expected a map, got {x:?}").into()),
        }
    }
}

impl TryFrom<serde_json::Value> for ForeignRecordReference {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Object(mut m) => {
                let id = match m.remove("id") {
                    Some(serde_json::Value::String(s)) => s,
                    _ => return Err("foreign record reference must have an id".into()),
                };

                let collection_id = match m.remove("collectionId") {
                    Some(serde_json::Value::String(s)) => s,
                    Some(_) => return Err("collectionId must be a string".into()),
                    None => return Err("missing collectionId".into()),
                };

                if !m.is_empty() {
                    return Err(format!(
                        "unexpected fields: {:?}",
                        m.keys().map(|k| k.as_str()).collect::<Vec<_>>().join(", ")
                    )
                    .into());
                }

                Ok(ForeignRecordReference { id, collection_id })
            }
            x => Err(format!("expected a map, got {x:?}").into()),
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
            RecordValue::IndexValue(_) => None,
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
            RecordValue::IndexValue(_) => None,
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
pub enum IndexValue {
    Number(f64),
    Boolean(bool),
    Null,
    String(String),
    Bytes(Vec<u8>),
    PublicKey(publickey::PublicKey),
}

impl IndexValue {
    pub(crate) fn byte_prefix(&self) -> u8 {
        match self {
            IndexValue::Null => keys::BYTE_NULL,
            IndexValue::String(_) => keys::BYTE_STRING,
            IndexValue::Number(_) => keys::BYTE_NUMBER,
            IndexValue::Boolean(_) => keys::BYTE_BOOLEAN,
            IndexValue::Bytes(_) => keys::BYTE_BYTES,
            IndexValue::PublicKey(_) => keys::BYTE_PUBLIC_KEY,
        }
    }

    pub(crate) fn serialize(
        &self,
        mut w: impl std::io::Write,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
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
            IndexValue::Bytes(b) => Cow::Borrowed(b),
            IndexValue::Null => Cow::Borrowed(&[0x00]),
            IndexValue::PublicKey(jwk) => Cow::Owned(jwk.to_indexable()),
        };

        let len = 1 + u16::try_from(value.len())?;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&[self.byte_prefix()])?;
        w.write_all(&value[..])?;

        Ok(())
    }

    pub(crate) fn deserialize(
        bytes: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let type_prefix = bytes[0];
        let value = &bytes[1..];
        let value = match type_prefix {
            BYTE_STRING => IndexValue::String(String::from_utf8(value.to_vec())?),
            BYTE_NUMBER => IndexValue::Number(f64::from_be_bytes(value.try_into()?)),
            BYTE_BOOLEAN => IndexValue::Boolean(match value[0] {
                0x00 => false,
                0x01 => true,
                _ => return Err("invalid boolean value".into()),
            }),
            BYTE_BYTES => IndexValue::Bytes(value.to_vec()),
            BYTE_NULL => IndexValue::Null,
            _ => return Err("invalid index value".into()),
        };

        Ok(value)
    }
}
