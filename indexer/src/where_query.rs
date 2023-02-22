use std::{borrow::Cow, collections::HashMap};

use serde::{Deserialize, Serialize};

use crate::keys::{self, Direction};
use crate::publickey;
use crate::record::IndexValue;

pub type Result<T> = std::result::Result<T, WhereQueryError>;

#[derive(Debug, thiserror::Error)]
pub enum WhereQueryError {
    #[error(transparent)]
    UserError(#[from] WhereQueryUserError),

    #[error("keys error")]
    KeysError(#[from] keys::KeysError),
}

#[derive(Debug, thiserror::Error)]
pub enum WhereQueryUserError {
    #[error("paths and directions must have the same length")]
    PathsAndDirectionsLengthMismatch,

    #[error("inequality can only be the last condition")]
    InequalityNotLast,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub(crate) struct FieldPath(pub(crate) Vec<String>);

impl PartialEq<&[&str]> for FieldPath {
    fn eq(&self, other: &&[&str]) -> bool {
        self.0.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<'de> Deserialize<'de> for FieldPath {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = Cow::<'de, str>::deserialize(deserializer)?;
        let mut path = Vec::new();
        for part in s.split('.') {
            path.push(part.to_string());
        }
        Ok(FieldPath(path))
    }
}

impl Serialize for FieldPath {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = String::new();
        for (i, part) in self.0.iter().enumerate() {
            if i > 0 {
                s.push('.');
            }
            s.push_str(part);
        }
        serializer.serialize_str(&s)
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct WhereQuery(pub(crate) HashMap<FieldPath, WhereNode>);

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum WhereNode {
    Equality(WhereValue),
    Inequality(WhereInequality),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum WhereValue {
    String(String),
    Number(f64),
    Boolean(bool),
    PublicKey(Box<publickey::PublicKey>),
}

impl From<WhereValue> for IndexValue<'_> {
    fn from(value: WhereValue) -> Self {
        match value {
            WhereValue::String(s) => IndexValue::String(Cow::Owned(s)),
            WhereValue::Number(n) => IndexValue::Number(n),
            WhereValue::Boolean(b) => IndexValue::Boolean(b),
            WhereValue::PublicKey(pk) => IndexValue::PublicKey(Cow::Owned(*pk)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub(crate) struct WhereInequality {
    #[serde(rename = "$gt")]
    pub(crate) gt: Option<WhereValue>,
    #[serde(rename = "$gte")]
    pub(crate) gte: Option<WhereValue>,
    #[serde(rename = "$lt")]
    pub(crate) lt: Option<WhereValue>,
    #[serde(rename = "$lte")]
    pub(crate) lte: Option<WhereValue>,
}

#[derive(Debug)]
pub(crate) struct KeyRange<'a> {
    pub(crate) lower: keys::Key<'a>,
    pub(crate) upper: keys::Key<'a>,
}

impl WhereQuery {
    pub(crate) fn key_range<T>(
        self,
        namespace: String,
        paths: &[&[T]],
        directions: &[keys::Direction],
    ) -> Result<KeyRange<'static>>
    where
        T: for<'other> PartialEq<String> + AsRef<str>,
    {
        if paths.len() != directions.len() {
            return Err(WhereQueryUserError::PathsAndDirectionsLengthMismatch)?;
        }

        let mut lower_values = Vec::<Cow<IndexValue>>::with_capacity(paths.len());
        let mut lower_exclusive = false;
        let mut upper_values = Vec::<Cow<IndexValue>>::with_capacity(paths.len());
        let mut upper_exclusive = false;

        let mut ineq_found = false;
        for (path, direction) in paths.iter().zip(directions.iter()) {
            if let Some((_, node)) = self.0.iter().find(|(field_path, _)| *path == field_path.0) {
                if ineq_found {
                    return Err(WhereQueryUserError::InequalityNotLast)?;
                }

                match node {
                    WhereNode::Equality(value) => {
                        lower_values.push(Cow::Owned(IndexValue::from(value.clone())));
                        upper_values.push(Cow::Owned(IndexValue::from(value.clone())));
                    }
                    WhereNode::Inequality(inequality) => {
                        ineq_found = true;

                        if let Some(value) = &inequality.gt {
                            if direction == &Direction::Ascending {
                                lower_exclusive = true;
                                lower_values.push(Cow::Owned(IndexValue::from(value.clone())));
                            } else {
                                upper_exclusive = true;
                                upper_values.push(Cow::Owned(IndexValue::from(value.clone())));
                            }
                        }

                        if let Some(value) = &inequality.gte {
                            if direction == &Direction::Ascending {
                                lower_values.push(Cow::Owned(IndexValue::from(value.clone())));
                            } else {
                                upper_values.push(Cow::Owned(IndexValue::from(value.clone())));
                            }
                        }

                        if let Some(value) = &inequality.lt {
                            if direction == &Direction::Ascending {
                                upper_exclusive = true;
                                upper_values.push(Cow::Owned(IndexValue::from(value.clone())));
                            } else {
                                lower_exclusive = true;
                                lower_values.push(Cow::Owned(IndexValue::from(value.clone())));
                            }
                        }

                        if let Some(value) = &inequality.lte {
                            if direction == &Direction::Ascending {
                                upper_values.push(Cow::Owned(IndexValue::from(value.clone())));
                            } else {
                                lower_values.push(Cow::Owned(IndexValue::from(value.clone())));
                            }
                        }
                    }
                }
            }
        }

        let lower_key = keys::Key::new_index(namespace.clone(), paths, directions, lower_values)?;
        let lower_key = if lower_exclusive {
            lower_key.wildcard()
        } else {
            lower_key
        };

        let upper_key = keys::Key::new_index(namespace, paths, directions, upper_values)?;
        let upper_key = if upper_exclusive {
            upper_key
        } else {
            upper_key.wildcard()
        };

        Ok(KeyRange {
            lower: lower_key,
            upper: upper_key,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! test_to_key_range {
        ($name:ident, $query:expr, $fields:expr, $directions:expr, $lower:expr, $upper:expr) => {
            #[test]
            fn $name() {
                let query = $query;

                let key_range = query
                    .key_range("namespace".to_string(), $fields, $directions)
                    .unwrap();

                assert_eq!(key_range.lower, $lower, "lower");

                assert_eq!(key_range.upper, $upper, "upper");
            }
        };
    }

    test_to_key_range!(
        test_to_key_range_name_eq_john,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["name".to_string()]),
            WhereNode::Equality(WhereValue::String("john".to_string())),
        )])),
        &[&["name"]],
        &[keys::Direction::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"]],
            &[keys::Direction::Ascending],
            vec![Cow::Owned(IndexValue::String("john".to_string().into()))]
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"]],
            &[keys::Direction::Ascending],
            vec![Cow::Owned(IndexValue::String("john".to_string().into()))]
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_gt_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(WhereInequality {
                gt: Some(WhereValue::Number(30.0)),
                ..Default::default()
            }),
        )])),
        &[&["age"]],
        &[keys::Direction::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))]
        )
        .unwrap()
        .wildcard(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Ascending],
            Vec::new(),
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_gte_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(WhereInequality {
                gte: Some(WhereValue::Number(30.0)),
                ..Default::default()
            }),
        )])),
        &[&["age"]],
        &[keys::Direction::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))]
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Ascending],
            Vec::new(),
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_lt_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(WhereInequality {
                lt: Some(WhereValue::Number(30.0)),
                ..Default::default()
            }),
        )])),
        &[&["age"]],
        &[keys::Direction::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Ascending],
            Vec::new(),
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))]
        )
        .unwrap()
    );

    test_to_key_range!(
        test_to_key_range_age_lte_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(WhereInequality {
                lte: Some(WhereValue::Number(30.0)),
                ..Default::default()
            }),
        )])),
        &[&["age"]],
        &[keys::Direction::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Ascending],
            Vec::new(),
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))]
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_lt_50_desc,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(WhereInequality {
                lt: Some(WhereValue::Number(50.0)),
                ..Default::default()
            }),
        )])),
        &[&["age"]],
        &[keys::Direction::Descending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Descending],
            vec![Cow::Borrowed(&IndexValue::Number(50.0))]
        )
        .unwrap()
        .wildcard(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["age"]],
            &[keys::Direction::Descending],
            Vec::new(),
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_gt_30_name_eq_john,
        WhereQuery(HashMap::from_iter(vec![
            (
                FieldPath(vec!["age".to_string()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue::Number(30.0)),
                    ..Default::default()
                }),
            ),
            (
                FieldPath(vec!["name".to_string()]),
                WhereNode::Equality(WhereValue::String("John".into())),
            ),
        ])),
        &[&["name"], &["age"]],
        &[keys::Direction::Ascending, keys::Direction::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["age"]],
            &[keys::Direction::Ascending, keys::Direction::Ascending],
            vec![
                Cow::Owned(IndexValue::String("John".to_string().into())),
                Cow::Borrowed(&IndexValue::Number(30.0)),
            ]
        )
        .unwrap()
        .wildcard(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["age"]],
            &[keys::Direction::Ascending, keys::Direction::Ascending],
            vec![Cow::Owned(IndexValue::String("John".into())),]
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_name_eq_john_id_eq_rec1,
        WhereQuery(HashMap::from_iter(vec![
            (
                FieldPath(vec!["name".to_string()]),
                WhereNode::Equality(WhereValue::String("John".into())),
            ),
            (
                FieldPath(vec!["id".to_string()]),
                WhereNode::Equality(WhereValue::String("rec1".into())),
            ),
        ])),
        &[&["name"], &["id"]],
        &[keys::Direction::Ascending, keys::Direction::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["id"]],
            &[keys::Direction::Ascending, keys::Direction::Ascending],
            vec![
                Cow::Owned(IndexValue::String("John".to_string().into())),
                Cow::Owned(IndexValue::String("rec1".to_string().into())),
            ]
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["id"]],
            &[keys::Direction::Ascending, keys::Direction::Ascending],
            vec![
                Cow::Owned(IndexValue::String("John".to_string().into())),
                Cow::Owned(IndexValue::String("rec1".to_string().into())),
            ]
        )
        .unwrap()
        .wildcard()
    );
}
