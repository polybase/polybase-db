use std::{borrow::Cow, collections::HashMap, error::Error};

use serde::{Deserialize, Serialize};

use crate::{
    keys::{self, BYTE_BOOLEAN, BYTE_BYTES, BYTE_NULL, BYTE_NUMBER, BYTE_STRING},
    publickey,
};

pub type RecordRoot = HashMap<String, RecordValue>;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum RecordValue {
    IndexValue(IndexValue),
    Map(HashMap<String, RecordValue>),
    Array(Vec<RecordValue>),
}

// TODO: use this to deserialize from a JSON provided by the user, to our RecordValue.
// The database will store RecordValue. Conversion only has to happen once.
impl TryFrom<(&polylang::stableast::Type<'_>, serde_json::Value)> for RecordValue {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(
        (ty, value): (&polylang::stableast::Type, serde_json::Value),
    ) -> Result<Self, Self::Error> {
        match (ty, value) {
            (polylang::stableast::Type::Primitive(p), value) => match (&p.value, value) {
                (polylang::stableast::PrimitiveType::String, serde_json::Value::String(s)) => {
                    Ok(RecordValue::IndexValue(IndexValue::String(s)))
                }
                (polylang::stableast::PrimitiveType::Number, serde_json::Value::Number(n)) => Ok(
                    RecordValue::IndexValue(IndexValue::Number(n.as_f64().unwrap())),
                ),
                (polylang::stableast::PrimitiveType::Boolean, serde_json::Value::Bool(b)) => {
                    Ok(RecordValue::IndexValue(IndexValue::Boolean(b)))
                }
                x => Err(format!("type mismatch: {x:?}").into()),
            },
            (polylang::stableast::Type::Array(t), serde_json::Value::Array(a)) => {
                let mut array = Vec::with_capacity(a.len());
                for v in a {
                    array.push(RecordValue::try_from((t.value.as_ref(), v))?);
                }
                Ok(RecordValue::Array(array))
            }
            (polylang::stableast::Type::Map(t), serde_json::Value::Object(o)) => {
                let mut map = HashMap::with_capacity(o.len());
                for (k, v) in o {
                    map.insert(k, RecordValue::try_from((t.value.as_ref(), v))?);
                }
                Ok(RecordValue::Map(map))
            }
            _ => todo!(),
        }
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
        }

        Ok(())
    }
}

pub struct RecordReference {
    pub id: String,
    pub collection_id: Option<String>,
}

impl TryFrom<&RecordValue> for RecordReference {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(value: &RecordValue) -> Result<Self, Self::Error> {
        match value {
            RecordValue::Map(m) => {
                let id = match m.get("id") {
                    Some(RecordValue::IndexValue(IndexValue::String(s))) => s.to_string(),
                    _ => return Err("record reference must have an id".into()),
                };

                let collection_id = match m.get("collectionId") {
                    Some(RecordValue::IndexValue(IndexValue::String(s))) => Some(s.to_string()),
                    Some(_) => return Err("collectionId must be a string".into()),
                    None => None,
                };

                Ok(RecordReference { id, collection_id })
            }
            _ => Err("not a record reference".into()),
        }
    }
}

pub trait PathFinder {
    fn find_path<T>(&self, path: &[T]) -> Option<&RecordValue>
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
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(untagged)]
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
