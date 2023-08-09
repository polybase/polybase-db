use crate::keys;
use indexer_db_adaptor::where_query::{WhereNode, WhereQuery};
use schema::{field_path::FieldPath, index::IndexDirection, index_value::IndexValue, Schema};
use std::borrow::Cow;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    UserError(#[from] UserError),

    #[error("keys error")]
    KeysError(#[from] keys::KeysError),
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error("paths and directions must have the same length")]
    PathsAndDirectionsLengthMismatch,

    #[error("inequality can only be the last condition")]
    InequalityNotLast,

    #[error("you cannot filter/sort by field {0}")]
    CannotFilterOrSortByField(String),
}

#[derive(Debug)]
pub(crate) struct KeyRange<'a> {
    pub(crate) lower: keys::Key<'a>,
    pub(crate) upper: keys::Key<'a>,
}

pub(crate) fn key_range<'a>(
    where_query: &'a WhereQuery,
    schema: &Schema,
    namespace: String,
    paths: &[&FieldPath],
    directions: &[IndexDirection],
) -> Result<KeyRange<'a>> {
    if paths.len() != directions.len() {
        return Err(UserError::PathsAndDirectionsLengthMismatch)?;
    }

    let mut lower_values = Vec::<Cow<IndexValue>>::with_capacity(paths.len());
    let mut lower_exclusive = false;
    let mut upper_values = Vec::<Cow<IndexValue>>::with_capacity(paths.len());
    let mut upper_exclusive = false;

    let mut ineq_found = false;
    for (path, direction) in paths.iter().zip(directions.iter()) {
        if let Some((_, node)) = where_query
            .0
            .iter()
            .find(|(field_path, _)| path == field_path)
        {
            if ineq_found {
                return Err(UserError::InequalityNotLast)?;
            }

            match node {
                WhereNode::Equality(value) => {
                    lower_values.push(Cow::Owned(value.0.clone()));
                    upper_values.push(Cow::Owned(value.0.clone()));
                }
                WhereNode::Inequality(inequality) => {
                    ineq_found = true;

                    if let Some(value) = &inequality.gt {
                        if direction == &IndexDirection::Ascending {
                            lower_exclusive = true;
                            lower_values.push(Cow::Owned(value.0.clone()));
                        } else {
                            upper_exclusive = true;
                            upper_values.push(Cow::Owned(value.0.clone()));
                        }
                    }

                    if let Some(value) = &inequality.gte {
                        if direction == &IndexDirection::Ascending {
                            lower_values.push(Cow::Owned(value.0.clone()));
                        } else {
                            upper_values.push(Cow::Owned(value.0.clone()));
                        }
                    }

                    if let Some(value) = &inequality.lt {
                        if direction == &IndexDirection::Ascending {
                            upper_exclusive = true;
                            upper_values.push(Cow::Owned(value.0.clone()));
                        } else {
                            lower_exclusive = true;
                            lower_values.push(Cow::Owned(value.0.clone()));
                        }
                    }

                    if let Some(value) = &inequality.lte {
                        if direction == &IndexDirection::Ascending {
                            upper_values.push(Cow::Owned(value.0.clone()));
                        } else {
                            lower_values.push(Cow::Owned(value.0.clone()));
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

#[cfg(test)]
mod test {
    use super::*;

    use indexer_db_adaptor::where_query::{WhereInequality, WhereQuery, WhereValue};
    use schema::{field_path::FieldPath, index::IndexDirection};
    use std::collections::HashMap;

    macro_rules! test_to_key_range {
        ($name:ident, $query:expr, $fields:expr, $directions:expr, $lower:expr, $upper:expr) => {
            #[test]
            fn $name() {
                let query = $query;

                let key_range = key_range(
                    &query,
                    &Schema::new(&polylang::stableast::Collection {
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
                    }),
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
        &WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["name".to_string()]),
            WhereNode::Equality(WhereValue("john".into())),
        )])),
        &[&"name".into()],
        &[IndexDirection::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"name".into()],
            &[IndexDirection::Ascending],
            vec![Cow::Owned(IndexValue::String("john".to_string().into()))]
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"name".into()],
            &[IndexDirection::Ascending],
            vec![Cow::Owned(IndexValue::String("john".to_string().into()))]
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_gt_30,
        &WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(Box::new(WhereInequality {
                gt: Some(WhereValue(30.0.into())),
                ..Default::default()
            })),
        )])),
        &[&"age".into()],
        &[IndexDirection::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))]
        )
        .unwrap()
        .wildcard(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Ascending],
            Vec::new(),
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_gte_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(Box::new(WhereInequality {
                gte: Some(WhereValue(30.0.into())),
                ..Default::default()
            })),
        )])),
        &[&"age".into()],
        &[IndexDirection::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))]
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Ascending],
            Vec::new(),
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_lt_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(Box::new(WhereInequality {
                lt: Some(WhereValue(30.0.into())),
                ..Default::default()
            })),
        )])),
        &[&"age".into()],
        &[IndexDirection::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Ascending],
            Vec::new(),
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))]
        )
        .unwrap()
    );

    test_to_key_range!(
        test_to_key_range_age_lte_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(Box::new(WhereInequality {
                lte: Some(WhereValue(30.0.into())),
                ..Default::default()
            })),
        )])),
        &[&"age".into()],
        &[IndexDirection::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Ascending],
            Vec::new(),
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Ascending],
            vec![Cow::Borrowed(&IndexValue::Number(30.0))]
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_lt_50_desc,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec!["age".to_string()]),
            WhereNode::Inequality(Box::new(WhereInequality {
                lt: Some(WhereValue(50.0.into())),
                ..Default::default()
            })),
        )])),
        &[&"age".into()],
        &[IndexDirection::Descending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Descending],
            vec![Cow::Borrowed(&IndexValue::Number(50.0))]
        )
        .unwrap()
        .wildcard(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"age".into()],
            &[IndexDirection::Descending],
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
                WhereNode::Inequality(Box::new(WhereInequality {
                    gt: Some(WhereValue(30.0.into())),
                    ..Default::default()
                })),
            ),
            (
                FieldPath(vec!["name".to_string()]),
                WhereNode::Equality(WhereValue("John".into())),
            ),
        ])),
        &[&"name".into(), &"age".into()],
        &[IndexDirection::Ascending, IndexDirection::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"name".into(), &"age".into()],
            &[IndexDirection::Ascending, IndexDirection::Ascending],
            vec![
                Cow::Owned(IndexValue::String("John".to_string().into())),
                Cow::Borrowed(&IndexValue::Number(30.0)),
            ]
        )
        .unwrap()
        .wildcard(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"name".into(), &"age".into()],
            &[IndexDirection::Ascending, IndexDirection::Ascending],
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
        &[&"name".into(), &"id".into()],
        &[IndexDirection::Ascending, IndexDirection::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"name".into(), &"id".into()],
            &[IndexDirection::Ascending, IndexDirection::Ascending],
            vec![
                Cow::Owned(IndexValue::String("John".to_string().into())),
                Cow::Owned(IndexValue::String("rec1".to_string().into())),
            ]
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&"name".into(), &"id".into()],
            &[IndexDirection::Ascending, IndexDirection::Ascending],
            vec![
                Cow::Owned(IndexValue::String("John".to_string().into())),
                Cow::Owned(IndexValue::String("rec1".to_string().into())),
            ]
        )
        .unwrap()
        .wildcard()
    );
}
