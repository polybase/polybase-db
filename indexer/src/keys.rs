use std::{borrow::Cow, cmp::Ordering, fmt};

use cid::multihash::{Hasher, MultihashDigest};
use prost::Message;

use crate::{
    proto,
    record::{IndexValue, RecordRoot},
};

pub type Result<T> = std::result::Result<T, KeysError>;

#[derive(Debug, thiserror::Error)]
pub enum KeysError {
    #[error("invalid direction byte {n}")]
    InvalidDirection { n: u8 },

    #[error("path and directions must be the same length")]
    PathAndDirectionsLengthMismatch,

    #[error("record is missing fields: {fields:?}")]
    RecordIsMissingFields { fields: Vec<String> },

    #[error("invalid key type byte {n}")]
    InvalidKeyType { n: u8 },

    #[error("key does not have immediate successor")]
    KeyDoesNotHaveImmediateSuccessor,

    #[error("record error")]
    RecordError(#[from] crate::record::RecordError),

    #[error("try from int error")]
    TryFromIntError(#[from] std::num::TryFromIntError),

    #[error("CID error")]
    CIDError(#[from] cid::Error),
}

const MULTICODEC_PROTOBUF: u64 = 0x50;

// 1 byte: prefix
// 36 bytes: CID
const KEY_COMPARE_PREFIX: usize = 1 + 36;

// Key type prefixes
const BYTE_DATA: u8 = 0x01;
const BYTE_INDEX: u8 = 0x02;
const BYTE_WILDCARD: u8 = 0x03;
const BYTE_SYSTEM_DATA: u8 = 0x04;

// Data type prefixes
pub(crate) const BYTE_NULL: u8 = 0x00;
pub(crate) const BYTE_STRING: u8 = 0x04;
pub(crate) const BYTE_NUMBER: u8 = 0x05;
pub(crate) const BYTE_BOOLEAN: u8 = 0x06;
pub(crate) const BYTE_BYTES: u8 = 0x07;
pub(crate) const BYTE_PUBLIC_KEY: u8 = 0x08;

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

fn generate_cid(data: &[u8], out: &mut Vec<u8>) -> std::result::Result<(), cid::Error> {
    let mut hasher = cid::multihash::Sha2_256::default();
    hasher.update(data);
    let hash = cid::multihash::Code::Sha2_256.wrap(hasher.finalize())?;
    let cid = cid::Cid::new_v1(MULTICODEC_PROTOBUF, hash);

    cid.write_bytes(out)?;

    Ok(())
}

#[derive(PartialEq, Clone)]
pub enum Key<'a> {
    Wildcard(Box<Key<'a>>),
    Data {
        cid: Cow<'a, [u8]>,
    },
    SystemData {
        cid: Cow<'a, [u8]>,
    },
    Index {
        cid: Cow<'a, [u8]>,
        directions: Cow<'a, [Direction]>,
        values: Vec<Cow<'a, IndexValue>>,
    },
}

impl<'a> fmt::Debug for Key<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Wildcard(k) => write!(f, "Wildcard({k:?})"),
            Key::Data { cid } => write!(f, "Data({cid:?})"),
            Key::SystemData { cid } => write!(f, "SystemData({cid:?})"),
            Key::Index {
                cid,
                directions,
                values,
            } => write!(f, "Index({cid:?}, {directions:?}, {values:?})"),
        }
    }
}

impl<'a> Key<'a> {
    pub fn new_data(namespace: String, id: String) -> Result<Self> {
        let data = proto::DataKey { namespace, id };
        let mut cid = Vec::with_capacity(36);
        generate_cid(&data.encode_to_vec(), &mut cid)?;

        Ok(Key::Data {
            cid: Cow::Owned(cid),
        })
    }

    pub(crate) fn new_system_data(id: String) -> Result<Self> {
        let data = proto::SystemDataKey { id };
        let mut cid = Vec::with_capacity(36);
        generate_cid(&data.encode_to_vec(), &mut cid)?;

        Ok(Key::SystemData {
            cid: Cow::Owned(cid),
        })
    }

    pub(crate) fn new_index(
        namespace: String,
        paths: &[&[impl AsRef<str>]],
        directions: &[Direction],
        values: Vec<Cow<'a, IndexValue>>,
    ) -> Result<Self> {
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

    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
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
            Key::SystemData { cid } => {
                let mut key = Vec::with_capacity(cid.len() + 1);
                key.push(BYTE_SYSTEM_DATA);
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

    pub(crate) fn deserialize(key: &'a [u8]) -> Result<Self> {
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
            _ => Err(KeysError::InvalidKeyType { n: key_type }),
        }
    }

    pub(crate) fn with_static(self) -> Key<'static> {
        match self {
            Key::Wildcard(k) => Key::Wildcard(Box::new(k.with_static())),
            Key::Data { cid } => Key::Data {
                cid: Cow::Owned(cid.into_owned()),
            },
            Key::SystemData { cid } => Key::SystemData {
                cid: Cow::Owned(cid.into_owned()),
            },
            Key::Index {
                cid,
                directions,
                values,
            } => Key::Index {
                cid: Cow::Owned(cid.into_owned()),
                directions: Cow::Owned(directions.into_owned()),
                values: values
                    .into_iter()
                    .map(|v| Cow::Owned(v.into_owned()))
                    .collect(),
            },
        }
    }

    pub(crate) fn immediate_successor_value_mut(&mut self) -> Result<()> {
        match self {
            Key::Wildcard(_) => Err(KeysError::KeyDoesNotHaveImmediateSuccessor),
            Key::Data { .. } => Err(KeysError::KeyDoesNotHaveImmediateSuccessor),
            Key::SystemData { .. } => Err(KeysError::KeyDoesNotHaveImmediateSuccessor),
            Key::Index {
                cid: _,
                directions: _,
                values,
            } => {
                values.push(Cow::Borrowed(&IndexValue::Null));
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
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
    type Error = KeysError;

    fn try_from(d: u8) -> Result<Self> {
        match d {
            0x00 => Ok(Direction::Ascending),
            0x01 => Ok(Direction::Descending),
            _ => Err(KeysError::InvalidDirection { n: d }),
        }
    }
}

pub(crate) fn index_record_key_with_record<'a, T>(
    namespace: String,
    paths: &[&[T]],
    directions: &[Direction],
    record: &'a RecordRoot,
) -> Result<Key<'a>>
where
    T: AsRef<str> + PartialEq + for<'other> PartialEq<&'other str>,
{
    if paths.len() != directions.len() {
        return Err(KeysError::PathAndDirectionsLengthMismatch);
    }

    let mut found_values = vec![];
    for (k, v) in record {
        v.walk::<std::convert::Infallible>(&mut vec![Cow::Borrowed(k)], &mut |path, value| {
            if let Some(found) = paths.iter().find(|p| {
                p.len() == path.len()
                    && p.iter()
                        .zip(path.iter())
                        .all(|(p, v)| p.as_ref() == v.as_ref())
            }) {
                found_values.push((found, value));
            }

            Ok(())
        })
        .unwrap();
    }

    if found_values.len() != paths.len() {
        let missing_fields = paths
            .iter()
            .filter(|p| !found_values.iter().any(|(fp, _)| fp == p));
        return Err(KeysError::RecordIsMissingFields {
            fields: missing_fields
                .map(|x| {
                    let mut s = String::new();
                    for p in x.iter() {
                        s.push_str(p.as_ref());
                        s.push('.');
                    }
                    s.pop();
                    s
                })
                .collect::<Vec<_>>(),
        });
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

#[cfg(test)]
mod test {
    use crate::record::RecordValue;

    use super::*;

    impl Key<'_> {
        pub(crate) fn immediate_successor_value(mut self) -> Result<Self> {
            self.immediate_successor_value_mut()?;
            Ok(self)
        }
    }

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
        let value = IndexValue::String("hello".to_string());
        let mut serialized = vec![];
        value.serialize(&mut serialized).unwrap();
        let (field, _) = eat_field(&serialized);
        let deserialized = IndexValue::deserialize(field).unwrap();
        assert_eq!(deserialized, value);
    }

    #[test]
    fn test_record_value_map_serde_deserialize() {
        let serialized = r#"{"hello": "world"}"#;
        let deserialized: RecordValue = serde_json::from_str(serialized).unwrap();

        match deserialized {
            RecordValue::Map(m) => {
                assert_eq!(m.len(), 1);
                let (k, v) = m.iter().next().unwrap();

                match k.as_str() {
                    "hello" => {}
                    s => panic!("should be hello, got {s}"),
                }

                match v {
                    RecordValue::IndexValue(IndexValue::String(s)) if s == "world" => {}
                    RecordValue::IndexValue(IndexValue::String(s)) => {
                        panic!("should be world, got {s}")
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
                Cow::Borrowed(&IndexValue::String("hello".to_string())),
                Cow::Borrowed(&IndexValue::Number(1.0)),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Descending],
            vec![
                Cow::Borrowed(&IndexValue::String("hello".to_string())),
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
                Cow::Borrowed(&IndexValue::String("hello".to_string())),
                Cow::Borrowed(&IndexValue::Number(1.0)),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::String("hello".to_string())),
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
                Cow::Borrowed(&IndexValue::String("hello".to_string())),
                Cow::Borrowed(&IndexValue::Number(2.0)),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["a"], &["b"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::String("hello".to_string())),
                Cow::Borrowed(&IndexValue::Number(1.0)),
            ],
        )
        .unwrap(),
        Ordering::Greater
    );

    test_comparator!(
        test_comparator_index_key_greater_string,
        Key::new_index(
            "namespace".to_string(),
            &[&["a"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::String("hello2".to_string()))]
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["a"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::String("hello".to_string()))]
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
                Cow::Borrowed(&IndexValue::String("John".to_string())),
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
                Cow::Borrowed(&IndexValue::String("John".to_string())),
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
                Cow::Borrowed(&IndexValue::String("1".to_string())),
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
                Cow::Borrowed(&IndexValue::String("2".to_string())),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["age"], &["id"]],
            &[Direction::Descending, Direction::Ascending],
            vec![
                Cow::Borrowed(&IndexValue::Number(39.0)),
                Cow::Borrowed(&IndexValue::String("2".to_string())),
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
                Cow::Borrowed(&IndexValue::String("John".to_string())),
                Cow::Borrowed(&IndexValue::String("rec1".to_string())),
            ],
        )
        .unwrap(),
        Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["id"]],
            &[Direction::Ascending, Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::String("Jane".to_string()))],
        )
        .unwrap()
        .wildcard(),
        Ordering::Greater
    );
}
