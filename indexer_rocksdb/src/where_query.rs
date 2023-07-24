use std::{borrow::Cow, collections::HashMap};

use serde::{Deserialize, Serialize};

use crate::keys::{self, Direction};
use crate::FieldWalker;

use indexer_db_adaptor::record::{self, IndexValue};

pub type Result<T> = std::result::Result<T, WhereQueryError>;

#[derive(Debug, thiserror::Error)]
pub enum WhereQueryError {
    #[error(transparent)]
    UserError(#[from] WhereQueryUserError),

    #[error("keys error")]
    KeysError(#[from] keys::KeysError),

    #[error("record error")]
    RecordError(#[from] record::RecordError),
}

#[derive(Debug, thiserror::Error)]
pub enum WhereQueryUserError {
    #[error("paths and directions must have the same length")]
    PathsAndDirectionsLengthMismatch,

    #[error("inequality can only be the last condition")]
    InequalityNotLast,

    #[error("you cannot filter/sort by field {0}")]
    CannotFilterOrSortByField(String),
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
    Inequality(WhereInequality),
    Equality(WhereValue),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WhereValue(pub(crate) serde_json::Value);

impl WhereValue {
    fn into_record_value<T>(
        self,
        collection_ast: &polylang::stableast::Collection,
        path: &[T],
    ) -> Result<record::RecordValue>
    where
        for<'a> &'a str: std::cmp::PartialEq<T>,
        T: AsRef<str>,
    {
        let field = collection_ast.find_field(path).ok_or_else(|| {
            WhereQueryError::UserError(WhereQueryUserError::CannotFilterOrSortByField(
                path.iter()
                    .map(|x| x.as_ref())
                    .collect::<Vec<_>>()
                    .join("."),
            ))
        })?;

        // Only implicitly cast string to PublicKey. We can relax this in the future.
        // Relaxing this would mean that if the user provides an invalid value,
        // they will search by the defualt value instead of returning nothing or getting an error.
        let always_cast =
            matches!(field.type_(), polylang::stableast::Type::PublicKey(_)) && self.0.is_string();

        Ok(record::Converter::convert(
            (field.type_(), self.0),
            &mut path
                .iter()
                .map(|x| Cow::Borrowed(x.as_ref()))
                .collect::<Vec<_>>(),
            always_cast,
        )?)
    }

    fn into_index_value<'a, T>(
        self,
        collection_ast: &polylang::stableast::Collection,
        path: &[T],
    ) -> Result<IndexValue<'a>>
    where
        for<'b> &'b str: std::cmp::PartialEq<T>,
        T: AsRef<str>,
    {
        record::IndexValue::try_from(self.into_record_value(collection_ast, path)?).map_err(|_| {
            WhereQueryError::UserError(WhereQueryUserError::CannotFilterOrSortByField(
                path.iter()
                    .map(|x| x.as_ref())
                    .collect::<Vec<_>>()
                    .join("."),
            ))
        })
    }
}

#[derive(Debug, Serialize, Default, Clone)]
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

impl<'de> Deserialize<'de> for WhereInequality {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut map = serde_json::Map::deserialize(deserializer)?;
        let mut inequality = WhereInequality::default();

        if let Some(value) = map.remove("$gt") {
            inequality.gt = Some(
                serde_json::from_value(value)
                    .map_err(|e| serde::de::Error::custom(format!("invalid $gt: {}", e)))?,
            );
        }

        if let Some(value) = map.remove("$gte") {
            inequality.gte = Some(
                serde_json::from_value(value)
                    .map_err(|e| serde::de::Error::custom(format!("invalid $gte: {}", e)))?,
            );
        }

        if let Some(value) = map.remove("$lt") {
            inequality.lt = Some(
                serde_json::from_value(value)
                    .map_err(|e| serde::de::Error::custom(format!("invalid $lt: {}", e)))?,
            );
        }

        if let Some(value) = map.remove("$lte") {
            inequality.lte = Some(
                serde_json::from_value(value)
                    .map_err(|e| serde::de::Error::custom(format!("invalid $lte: {}", e)))?,
            );
        }

        if !map.is_empty() {
            return Err(serde::de::Error::custom("too many fields in inequality"));
        }

        Ok(inequality)
    }
}

#[derive(Debug)]
pub(crate) struct KeyRange<'a> {
    pub(crate) lower: keys::Key<'a>,
    pub(crate) upper: keys::Key<'a>,
}

impl WhereQuery {
    pub(crate) fn key_range<T>(
        self,
        collection_ast: &polylang::stableast::Collection,
        namespace: String,
        paths: &[&[T]],
        directions: &[keys::Direction],
    ) -> Result<KeyRange<'static>>
    where
        T: AsRef<str>,
        T: PartialEq<String>,
        for<'a> &'a str: std::cmp::PartialEq<T>,
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
                        lower_values.push(Cow::Owned(
                            value.clone().into_index_value(collection_ast, path)?,
                        ));
                        upper_values.push(Cow::Owned(
                            value.clone().into_index_value(collection_ast, path)?,
                        ));
                    }
                    WhereNode::Inequality(inequality) => {
                        ineq_found = true;

                        if let Some(value) = &inequality.gt {
                            if direction == &Direction::Ascending {
                                lower_exclusive = true;
                                lower_values.push(Cow::Owned(
                                    value.clone().into_index_value(collection_ast, path)?,
                                ));
                            } else {
                                upper_exclusive = true;
                                upper_values.push(Cow::Owned(
                                    value.clone().into_index_value(collection_ast, path)?,
                                ));
                            }
                        }

                        if let Some(value) = &inequality.gte {
                            if direction == &Direction::Ascending {
                                lower_values.push(Cow::Owned(
                                    value.clone().into_index_value(collection_ast, path)?,
                                ));
                            } else {
                                upper_values.push(Cow::Owned(
                                    value.clone().into_index_value(collection_ast, path)?,
                                ));
                            }
                        }

                        if let Some(value) = &inequality.lt {
                            if direction == &Direction::Ascending {
                                upper_exclusive = true;
                                upper_values.push(Cow::Owned(
                                    value.clone().into_index_value(collection_ast, path)?,
                                ));
                            } else {
                                lower_exclusive = true;
                                lower_values.push(Cow::Owned(
                                    value.clone().into_index_value(collection_ast, path)?,
                                ));
                            }
                        }

                        if let Some(value) = &inequality.lte {
                            if direction == &Direction::Ascending {
                                upper_values.push(Cow::Owned(
                                    value.clone().into_index_value(collection_ast, path)?,
                                ));
                            } else {
                                lower_values.push(Cow::Owned(
                                    value.clone().into_index_value(collection_ast, path)?,
                                ));
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
                    .key_range(
                        &polylang::stableast::Collection {
                            namespace: polylang::stableast::Namespace {
                                value: "test".into(),
                            },
                            name: "Sample".into(),
                            attributes: vec![
                                polylang::stableast::CollectionAttribute::Property(
                                    polylang::stableast::Property {
                                        name: "id".into(),
                                        type_: polylang::stableast::Type::Primitive(
                                            polylang::stableast::Primitive {
                                                value: polylang::stableast::PrimitiveType::String,
                                            },
                                        ),
                                        directives: vec![],
                                        required: false,
                                    },
                                ),
                                polylang::stableast::CollectionAttribute::Property(
                                    polylang::stableast::Property {
                                        name: "name".into(),
                                        type_: polylang::stableast::Type::Primitive(
                                            polylang::stableast::Primitive {
                                                value: polylang::stableast::PrimitiveType::String,
                                            },
                                        ),
                                        directives: vec![],
                                        required: false,
                                    },
                                ),
                                polylang::stableast::CollectionAttribute::Property(
                                    polylang::stableast::Property {
                                        name: "age".into(),
                                        type_: polylang::stableast::Type::Primitive(
                                            polylang::stableast::Primitive {
                                                value: polylang::stableast::PrimitiveType::Number,
                                            },
                                        ),
                                        directives: vec![],
                                        required: false,
                                    },
                                ),
                            ],
                        },
                        "namespace".to_string(),
                        $fields,
                        $directions,
                    )
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
            WhereNode::Equality(WhereValue("john".into())),
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
                gt: Some(WhereValue(30.0.into())),
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
                gte: Some(WhereValue(30.0.into())),
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
                lt: Some(WhereValue(30.0.into())),
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
                lte: Some(WhereValue(30.0.into())),
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
                lt: Some(WhereValue(50.0.into())),
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
                    gt: Some(WhereValue(30.0.into())),
                    ..Default::default()
                }),
            ),
            (
                FieldPath(vec!["name".to_string()]),
                WhereNode::Equality(WhereValue("John".into())),
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
                WhereNode::Equality(WhereValue("John".into())),
            ),
            (
                FieldPath(vec!["id".to_string()]),
                WhereNode::Equality(WhereValue("rec1".into())),
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
