use std::{
    borrow::Cow, cmp::Ordering, collections::HashMap, error::Error, fmt, io::Write, ops::Deref,
};

use cid::multihash::{Hasher, MultihashDigest};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::value::Index;
use serde_with::{serde_as, BorrowCow};

use crate::proto;

const MULTICODEC_PROTOBUF: u64 = 0x50;

// 1 byte: prefix
// 36 bytes: CID
const KEY_COMPARE_PREFIX: usize = 1 + 36;

// Key type prefixes
const BYTE_DATA: u8 = 0x01;
const BYTE_INDEX: u8 = 0x02;
const BYTE_WILDCARD: u8 = 0x03;

// Data type prefixes
const BYTE_NULL: u8 = 0x00;
const BYTE_STRING: u8 = 0x04;
const BYTE_NUMBER: u8 = 0x05;
const BYTE_BOOLEAN: u8 = 0x06;
const BYTE_BYTES: u8 = 0x07;
const BYTE_PUBLIC_KEY: u8 = 0x08;

pub(crate) fn comparator(key1: &[u8], key2: &[u8]) -> Ordering {
    if key1.len() < KEY_COMPARE_PREFIX || key2.len() < KEY_COMPARE_PREFIX {
        return key1.cmp(key2);
    }

    let key1_is_wildcard = key1[0] == BYTE_WILDCARD;
    let key1 = if key1_is_wildcard { &key1[1..] } else { key1 };

    let key2_is_wildcard = key2[0] == BYTE_WILDCARD;
    let key2 = if key2_is_wildcard { &key2[1..] } else { key2 };

    match key1[..KEY_COMPARE_PREFIX].cmp(&key2[..KEY_COMPARE_PREFIX]) {
        Ordering::Equal => {}
        x => return x,
    };

    let mut k1 = &key1[KEY_COMPARE_PREFIX..];
    let mut k2 = &key2[KEY_COMPARE_PREFIX..];

    let mut directions: &[u8] = &[];
    if key1[0] == BYTE_INDEX {
        (directions, k1) = eat_field(k1);
    }
    if key2[0] == BYTE_INDEX {
        let dirs;
        (dirs, k2) = eat_field(k2);

        match directions.cmp(dirs) {
            Ordering::Equal => {}
            x => return x,
        };
    }

    for i in 0.. {
        if k1.is_empty() || k2.is_empty() {
            match (key1_is_wildcard, key2_is_wildcard) {
                (true, true) => return Ordering::Equal,
                (true, false) => return Ordering::Greater,
                (false, true) => return Ordering::Less,
                (false, false) => {}
            }
        }

        if k1.len() < 2 || k2.len() < 2 {
            return k1.cmp(k2);
        }

        let (field_1, rest_k1) = eat_field(k1);
        let (field_2, rest_k2) = eat_field(k2);

        match field_1.cmp(field_2) {
            Ordering::Equal => {}
            x if !directions.is_empty() && directions[i] == u8::from(Direction::Descending) => {
                return x.reverse()
            }
            x => return x,
        }

        k1 = rest_k1;
        k2 = rest_k2;
    }

    unreachable!()
}

/// Returns (field, rest)
fn eat_field(start: &[u8]) -> (&[u8], &[u8]) {
    if start.len() < 2 {
        return (&[], &[]);
    }

    let field_len = u16::from_le_bytes([start[0], start[1]]);
    let field = &start[2..field_len as usize + 2];
    let rest = &start[field_len as usize + 2..];

    (field, rest)
}

fn generate_cid(data: &[u8], out: &mut Vec<u8>) -> Result<(), cid::Error> {
    let mut hasher = cid::multihash::Sha2_256::default();
    hasher.update(data);
    let hash = cid::multihash::Code::Sha2_256.wrap(hasher.finalize())?;
    let cid = cid::Cid::new_v1(MULTICODEC_PROTOBUF, hash);

    cid.write_bytes(out)?;

    Ok(())
}

#[derive(PartialEq, Clone)]
pub(crate) enum Key<'a> {
    Wildcard(Box<Key<'a>>),
    Data {
        cid: Cow<'a, [u8]>,
    },
    Index {
        cid: Cow<'a, [u8]>,
        directions: Cow<'a, [Direction]>,
        values: Vec<Cow<'a, IndexValue<'a>>>,
    },
}

impl<'a> fmt::Debug for Key<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Wildcard(k) => write!(f, "Wildcard({k:?})"),
            Key::Data { cid } => write!(f, "Data({cid:?})"),
            Key::Index {
                cid,
                directions,
                values,
            } => write!(f, "Index({cid:?}, {directions:?}, {values:?})"),
        }
    }
}

impl<'a> Key<'a> {
    pub(crate) fn new_data(namespace: String, id: String) -> Result<Self, cid::Error> {
        let data = proto::DataKey { namespace, id };
        let mut cid = Vec::with_capacity(36);
        generate_cid(&data.encode_to_vec(), &mut cid)?;

        Ok(Key::Data {
            cid: Cow::Owned(cid),
        })
    }

    pub(crate) fn new_index(
        namespace: String,
        paths: &[&[impl AsRef<str>]],
        directions: &[Direction],
        values: Vec<Cow<'a, IndexValue<'a>>>,
    ) -> Result<Self, cid::Error> {
        let data = proto::IndexKey {
            namespace,
            path: paths
                .iter()
                .map(|path| {
                    let mut s = String::new();
                    for p in path.iter() {
                        s.push_str(p.as_ref());
                        s.push('.');
                    }
                    s.pop();
                    s
                })
                .collect(),
        };
        let mut cid = Vec::with_capacity(36);
        generate_cid(&data.encode_to_vec(), &mut cid)?;

        Ok(Key::Index {
            cid: Cow::Owned(cid),
            directions: Cow::Owned(directions.to_vec()),
            values,
        })
    }

    pub(crate) fn wildcard(self) -> Self {
        Key::Wildcard(Box::new(self))
    }

    pub(crate) fn serialize(
        &self,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        match self {
            Key::Wildcard(key) => {
                let mut key = key.serialize()?;
                key.insert(0, BYTE_WILDCARD);
                Ok(key)
            }
            Key::Data { cid } => {
                let mut key = Vec::with_capacity(cid.len() + 1);
                key.push(BYTE_DATA);
                key.extend_from_slice(cid);
                Ok(key)
            }
            Key::Index {
                cid,
                directions,
                values,
            } => {
                let mut key = Vec::with_capacity(cid.len() + 1 + directions.len());
                key.push(BYTE_INDEX);
                key.extend_from_slice(cid);

                key.extend_from_slice(u16::try_from(directions.len())?.to_le_bytes().as_ref());
                for dir in directions.iter() {
                    key.push((*dir).into());
                }

                for value in values.iter() {
                    value.as_ref().serialize(&mut key)?;
                }
                Ok(key)
            }
        }
    }

    pub(crate) fn deserialize(
        key: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let key_type = key[0];
        let cid = &key[1..37];

        match key_type {
            BYTE_DATA => Ok(Key::Data {
                cid: Cow::Borrowed(cid),
            }),
            BYTE_INDEX => {
                let directions_len = u16::from_le_bytes([key[37], key[38]]) as usize;

                Ok(Key::Index {
                    cid: Cow::Borrowed(cid),
                    directions: {
                        let mut directions = Vec::with_capacity(directions_len);
                        for i in 0..directions_len {
                            directions.push(Direction::try_from(key[39 + i])?);
                        }
                        Cow::Owned(directions)
                    },
                    values: {
                        let mut values = vec![];
                        let mut i = 39 + directions_len;
                        while i < key.len() {
                            let (field, _) = eat_field(&key[i..]);
                            let value = IndexValue::deserialize(field)?;
                            values.push(Cow::Owned(value));
                            i += 2 + field.len();
                        }
                        values
                    },
                })
            }
            _ => Err(format!("Invalid key type: {key_type}").into()),
        }
    }

    pub(crate) fn immediate_successor_value(
        self,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        match self {
            Key::Wildcard(_) => Err("Wildcard keys have no values".into()),
            Key::Data { .. } => Err("Data keys have no values".into()),
            Key::Index {
                cid,
                directions,
                mut values,
            } => {
                values.push(Cow::Borrowed(&IndexValue::Null));
                Ok(Key::Index {
                    cid,
                    directions,
                    values,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Direction {
    Ascending,
    Descending,
}

impl From<Direction> for u8 {
    fn from(d: Direction) -> Self {
        match d {
            Direction::Ascending => 0x00,
            Direction::Descending => 0x01,
        }
    }
}

impl TryFrom<u8> for Direction {
    type Error = String;

    fn try_from(d: u8) -> Result<Self, Self::Error> {
        match d {
            0x00 => Ok(Direction::Ascending),
            0x01 => Ok(Direction::Descending),
            _ => Err(format!("invalid direction: {d}")),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(untagged)]
#[serde_as]
pub enum IndexValue<'a> {
    #[serde(borrow)]
    String(Cow<'a, str>),
    Number(f64),
    Boolean(bool),
    Bytes(Cow<'a, [u8]>),
    Null,
    PublicKey(#[serde_as(as = "Box<BorrowCow>")] Box<Cow<'a, jsonwebtoken::jwk::Jwk>>),
}

trait SerializeStable {
    fn serialize_stable(
        &self,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>>;
}

impl SerializeStable for jsonwebtoken::jwk::Jwk {
    fn serialize_stable(
        &self,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        todo!()
    }
}

impl<'a> IndexValue<'a> {
    fn byte_prefix(&self) -> u8 {
        match self {
            IndexValue::Null => BYTE_NULL,
            IndexValue::String(_) => BYTE_STRING,
            IndexValue::Number(_) => BYTE_NUMBER,
            IndexValue::Boolean(_) => BYTE_BOOLEAN,
            IndexValue::Bytes(_) => BYTE_BYTES,
            IndexValue::PublicKey(_) => BYTE_PUBLIC_KEY,
        }
    }

    fn serialize(
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
            IndexValue::PublicKey(jwk) => Cow::Owned(jwk.serialize_stable()?),
        };

        let len = 1 + u16::try_from(value.len())?;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&[self.byte_prefix()])?;
        w.write_all(&value[..])?;

        Ok(())
    }

    fn deserialize(
        bytes: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let type_prefix = bytes[0];
        let value = &bytes[1..];
        let value = match type_prefix {
            BYTE_STRING => IndexValue::String(Cow::Owned(String::from_utf8(value.to_vec())?)),
            BYTE_NUMBER => IndexValue::Number(f64::from_be_bytes(value.try_into()?)),
            BYTE_BOOLEAN => IndexValue::Boolean(match value[0] {
                0x00 => false,
                0x01 => true,
                _ => return Err("invalid boolean value".into()),
            }),
            BYTE_BYTES => IndexValue::Bytes(Cow::Borrowed(value)),
            BYTE_NULL => IndexValue::Null,
            _ => return Err("invalid index value".into()),
        };

        Ok(value)
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum RecordValue<'a> {
    #[serde(borrow)]
    IndexValue(IndexValue<'a>),
    Map(#[serde_as(as = "HashMap<BorrowCow, _>")] HashMap<Cow<'a, str>, RecordValue<'a>>),
}

impl RecordValue<'_> {
    pub(crate) fn walk<'a, E: Error>(
        &'a self,
        current_path: &mut Vec<&'a str>,
        f: &mut impl FnMut(&[&str], &'a IndexValue) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            RecordValue::IndexValue(v) => {
                f(current_path, v)?;
            }
            RecordValue::Map(m) => {
                for (k, v) in m.iter() {
                    current_path.push(k);
                    v.walk(current_path, f)?;
                    current_path.pop();
                }
            }
        }

        Ok(())
    }
}

pub(crate) fn index_record_key_with_record<'a, T>(
    namespace: String,
    paths: &[&[T]],
    directions: &[Direction],
    record: &'a HashMap<Cow<str>, RecordValue>,
) -> Result<Key<'a>, Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: AsRef<str> + PartialEq + for<'other> PartialEq<&'other str>,
{
    if paths.len() != directions.len() {
        return Err("path and directions must be the same length".into());
    }

    let mut found_values = vec![];
    for (k, v) in record {
        v.walk::<std::convert::Infallible>(&mut vec![k], &mut |path, value| {
            if let Some(found) = paths.iter().find(|p| p == &&path) {
                found_values.push((found, value));
            }

            Ok(())
        })?;
    }

    if found_values.len() != paths.len() {
        let missing_fields = paths
            .iter()
            .filter(|p| !found_values.iter().any(|(fp, _)| fp == p));
        return Err(format!(
            "record is missing fields: {}",
            missing_fields
                .map(|x| {
                    let mut s = String::new();
                    for p in x.iter() {
                        s.push_str(p.as_ref());
                        s.push('.');
                    }
                    s.pop();
                    s
                })
                .collect::<Vec<_>>()
                .join(", ")
        )
        .into());
    }

    found_values.sort_by(|(p1, _), (p2, _)| {
        paths
            .iter()
            .position(|p| &p == p1)
            .cmp(&paths.iter().position(|p| p == *p2))
    });

    let found_values = found_values
        .into_iter()
        .map(|(_, v)| Cow::Borrowed(v))
        .collect::<Vec<_>>();

    let key = Key::new_index(namespace, paths, directions, found_values)?;

    Ok(key)
}

pub(crate) fn immediate_successor(key: Vec<u8>) -> Vec<u8> {
    let mut successor = key;
    for i in (0..successor.len()).rev() {
        if successor[i] == u8::MAX {
            successor[i] = 0;
        } else {
            successor[i] += 1;
            break;
        }
    }

    successor
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_index_value_number() {
        let value = IndexValue::Number(40.0);
        let mut serialized = vec![];
        value.serialize(&mut serialized).unwrap();
        let (field, _) = eat_field(&serialized);
        let deserialized = IndexValue::deserialize(field).unwrap();
        assert_eq!(deserialized, value);
    }

    #[test]
    fn test_index_value_string() {
        let value = IndexValue::String(Cow::Borrowed("hello"));
        let mut serialized = vec![];
        value.serialize(&mut serialized).unwrap();
        let (field, _) = eat_field(&serialized);
        let deserialized = IndexValue::deserialize(field).unwrap();
        assert_eq!(deserialized, value);
    }

    #[test]
    fn test_record_index_value_string_serde_deserialize() {
        let serialized = r#""hello""#;
        let deserialized = serde_json::from_str(serialized).unwrap();

        match deserialized {
            RecordValue::IndexValue(IndexValue::String(Cow::Owned(_))) => {
                panic!("should not be owned")
            }
            RecordValue::IndexValue(IndexValue::String(Cow::Borrowed(_))) => {}
            _ => panic!("should be string"),
        }
    }

    #[test]
    fn test_record_value_map_serde_deserialize() {
        let serialized = r#"{"hello": "world"}"#;
        let deserialized: RecordValue = serde_json::from_str(serialized).unwrap();

        match deserialized {
            RecordValue::Map(m) => {
                assert_eq!(m.len(), 1);
                let (k, v) = m.iter().next().unwrap();

                match k {
                    Cow::Borrowed("hello") => {}
                    Cow::Borrowed(s) => panic!("should be hello, got {s}"),
                    Cow::Owned(_) => panic!("should not be owned"),
                }

                match v {
                    RecordValue::IndexValue(IndexValue::String(Cow::Borrowed("world"))) => {}
                    RecordValue::IndexValue(IndexValue::String(Cow::Borrowed(s))) => {
                        panic!("should be world, got {s}")
                    }
                    RecordValue::IndexValue(IndexValue::String(Cow::Owned(_))) => {
                        panic!("should not be owned")
                    }
                    _ => panic!("should be string"),
                }
            }
            _ => panic!("should be map"),
        }
    }

    macro_rules! test_comparator {
        ($name:ident, $a_key:expr, $b_key:expr, $expected:expr) => {
            #[test]
            fn $name() {
                let got = comparator(&$a_key.serialize().unwrap(), &$b_key.serialize().unwrap());
                assert_eq!(got, $expected);
            }
        };
    }

    test_comparator!(
        test_comparator_data_key_equal,
        Key::new_data("namespace".to_string(), "rec1".to_string()).unwrap(),
        Key::new_data("namespace".to_string(), "rec1".to_string()).unwrap(),
        Ordering::Equal
    );

    test_comparator!(
        test_comparator_data_key_less,
        Key::new_data("namespace".to_string(), "rec1".to_string()).unwrap(),
        Key::new_data("namespace".to_string(), "rec2".to_string()).unwrap(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_data_key_greater,
        Key::new_data("namespace".to_string(), "rec2".to_string()).unwrap(),
        Key::new_data("namespace".to_string(), "rec1".to_string()).unwrap(),
        Ordering::Greater
    );

    test_comparator!(
        test_comparator_index_key_equal,
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Descending],
            vec![
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("hello"))),
                Cow::Borrowed(&IndexValue::Number(1.0)),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Descending],
            vec![
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("hello"))),
                Cow::Borrowed(&IndexValue::Number(1.0)),
            ],
        )
        .unwrap(),
        Ordering::Equal
    );

    test_comparator!(
        test_comparator_index_key_less,
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("hello"))),
                Cow::Borrowed(&IndexValue::Number(1.0)),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("hello"))),
                Cow::Borrowed(&IndexValue::Number(2.0)),
            ],
        )
        .unwrap(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_index_key_greater,
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("hello"))),
                Cow::Borrowed(&IndexValue::Number(2.0)),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("hello"))),
                Cow::Borrowed(&IndexValue::Number(1.0)),
            ],
        )
        .unwrap(),
        Ordering::Greater
    );

    test_comparator!(
        test_comparator_same_keys_with_wildcard,
        Key::new_data("namespace".to_string(), "rec1".to_string())
            .unwrap()
            .wildcard(),
        Key::new_data("namespace".to_string(), "rec1".to_string())
            .unwrap()
            .wildcard(),
        Ordering::Equal
    );

    test_comparator!(
        test_comparator_with_wildcard_vs_without,
        Key::new_data("namespace".to_string(), "rec1".to_string())
            .unwrap()
            .wildcard(),
        Key::new_data("namespace".to_string(), "rec1".to_string()).unwrap(),
        Ordering::Greater
    );

    test_comparator!(
        test_comparator_without_wildcard_vs_with,
        Key::new_data("namespace".to_string(), "rec1".to_string()).unwrap(),
        Key::new_data("namespace".to_string(), "rec1".to_string())
            .unwrap()
            .wildcard(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_30_lt_40,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(40.0))],
        )
        .unwrap()
        .wildcard(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_wildcard_in_b,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::Number(30.0)),
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("John"))),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .wildcard(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_wildcard_in_a,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .wildcard(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::Number(30.0)),
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("John"))),
            ],
        )
        .unwrap(),
        Ordering::Greater
    );

    test_comparator!(
        test_comparator_wildcard_in_a_and_b,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .wildcard(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .wildcard(),
        Ordering::Equal
    );

    test_comparator!(
        test_comparator_with_immediate_successor_is_more_than_without,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .immediate_successor_value()
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap(),
        Ordering::Greater
    );

    test_comparator!(
        test_comparator_with_immediate_successor_is_more_than_without_but_with_flipped_order,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Descending, Direction::Descending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Descending, Direction::Descending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .immediate_successor_value()
        .unwrap(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_without_immediate_successor_is_less_than_with,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["name"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .immediate_successor_value()
        .unwrap(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_1,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .wildcard(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::Number(30.0)),
                Cow::Borrowed(&IndexValue::Number(3.0)),
            ],
        )
        .unwrap(),
        Ordering::Greater
    );

    test_comparator!(
        test_comparator_2,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::Number(30.0)),
                Cow::Borrowed(&IndexValue::Number(3.0)),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))],
        )
        .unwrap()
        .wildcard(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_3,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Descending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::Number(30.0)),
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("1"))),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Descending, Direction::Ascending],
            vec![],
        )
        .unwrap()
        .wildcard(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_4,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Descending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(31.0))],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Descending, Direction::Ascending],
            vec![],
        )
        .unwrap()
        .wildcard(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_5,
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Descending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::Number(40.0)),
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("2"))),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Descending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::Number(39.0)),
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("2"))),
            ],
        )
        .unwrap(),
        Ordering::Less
    );

    test_comparator!(
        test_comparator_6,
        Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["id"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("John"))),
                Cow::Borrowed(&IndexValue::String(Cow::Borrowed("rec1"))),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["id"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::String(Cow::Borrowed("Jane")))],
        )
        .unwrap()
        .wildcard(),
        Ordering::Greater
    );
}
