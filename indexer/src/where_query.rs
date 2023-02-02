use std::{borrow::Cow, collections::HashMap};

use serde::{Deserialize, Serialize};

use crate::keys::{self, Direction};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Clone)]
pub(crate) struct FieldPath<'a>(pub(crate) Vec<Cow<'a, str>>);

impl PartialEq<&[&str]> for FieldPath<'_> {
    fn eq(&self, other: &&[&str]) -> bool {
        self.0.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct WhereQuery<'a>(pub(crate) HashMap<FieldPath<'a>, WhereNode<'a>>);

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum WhereNode<'a> {
    Equality(WhereValue<'a>),
    Inequality(WhereInequality<'a>),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum WhereValue<'a> {
    String(Cow<'a, str>),
    Number(f64),
    Boolean(bool),
}

impl<'a> From<&'a WhereValue<'a>> for keys::IndexValue<'a> {
    fn from(value: &'a WhereValue<'a>) -> Self {
        match value {
            WhereValue::String(s) => keys::IndexValue::String(Cow::Borrowed(s)),
            WhereValue::Number(n) => keys::IndexValue::Number(*n),
            WhereValue::Boolean(b) => keys::IndexValue::Boolean(*b),
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct WhereInequality<'a> {
    #[serde(rename = "$gt")]
    pub(crate) gt: Option<WhereValue<'a>>,
    #[serde(rename = "$gte")]
    pub(crate) gte: Option<WhereValue<'a>>,
    #[serde(rename = "$lt")]
    pub(crate) lt: Option<WhereValue<'a>>,
    #[serde(rename = "$lte")]
    pub(crate) lte: Option<WhereValue<'a>>,
}

pub(crate) struct KeyRange<'a> {
    pub(crate) lower: keys::Key<'a>,
    pub(crate) upper: keys::Key<'a>,
}

impl WhereQuery<'_> {
    pub(crate) fn to_key_range<T>(
        &self,
        namespace: String,
        paths: &[&[T]],
        directions: &[keys::Direction],
    ) -> Result<KeyRange<'_>, Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        T: for<'other> PartialEq<Cow<'other, str>> + AsRef<str>,
    {
        if paths.len() != directions.len() {
            return Err("Paths and directions must have the same length".into());
        }

        let mut lower_values = Vec::<Cow<keys::IndexValue>>::with_capacity(paths.len());
        let mut lower_exclusive = false;
        let mut upper_values = Vec::<Cow<keys::IndexValue>>::with_capacity(paths.len());
        let mut upper_exclusive = false;

        let mut ineq_found = false;
        for (path, direction) in paths.iter().zip(directions.iter()) {
            if let Some((_, node)) = self.0.iter().find(|(field_path, _)| *path == field_path.0) {
                if ineq_found {
                    return Err("Inequality can only be the last condition".into());
                }

                match node {
                    WhereNode::Equality(value) => {
                        lower_values.push(Cow::Owned(keys::IndexValue::from(value)));
                        upper_values.push(Cow::Owned(keys::IndexValue::from(value)));
                    }
                    WhereNode::Inequality(inequality) => {
                        ineq_found = true;

                        if let Some(value) = &inequality.gt {
                            if direction == &Direction::Ascending {
                                lower_exclusive = true;
                                lower_values.push(Cow::Owned(keys::IndexValue::from(value)));
                            } else {
                                upper_exclusive = true;
                                upper_values.push(Cow::Owned(keys::IndexValue::from(value)));
                            }
                        }

                        if let Some(value) = &inequality.gte {
                            if direction == &Direction::Ascending {
                                lower_values.push(Cow::Owned(keys::IndexValue::from(value)));
                            } else {
                                upper_values.push(Cow::Owned(keys::IndexValue::from(value)));
                            }
                        }

                        if let Some(value) = &inequality.lt {
                            if direction == &Direction::Ascending {
                                upper_exclusive = true;
                                upper_values.push(Cow::Owned(keys::IndexValue::from(value)));
                            } else {
                                lower_exclusive = true;
                                lower_values.push(Cow::Owned(keys::IndexValue::from(value)));
                            }
                        }

                        if let Some(value) = &inequality.lte {
                            if direction == &Direction::Ascending {
                                upper_values.push(Cow::Owned(keys::IndexValue::from(value)));
                            } else {
                                lower_values.push(Cow::Owned(keys::IndexValue::from(value)));
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
                    .to_key_range("namespace".to_string(), $fields, $directions)
                    .unwrap();

                assert_eq!(key_range.lower, $lower, "lower");

                assert_eq!(key_range.upper, $upper, "upper");
            }
        };
    }

    test_to_key_range!(
        test_to_key_range_name_eq_john,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec![Cow::Borrowed("name")]),
            WhereNode::Equality(WhereValue::String(Cow::Borrowed("john"))),
        )])),
        &[&["name"]],
        &[keys::Direction::Ascending],
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"]],
            &[keys::Direction::Ascending],
            vec![Cow::Borrowed(&keys::IndexValue::String(Cow::Borrowed(
                "john"
            )))]
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"]],
            &[keys::Direction::Ascending],
            vec![Cow::Borrowed(&keys::IndexValue::String(Cow::Borrowed(
                "john"
            )))]
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_gt_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec![Cow::Borrowed("age")]),
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
            vec![Cow::Borrowed(&keys::IndexValue::Number(30.0))]
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
            FieldPath(vec![Cow::Borrowed("age")]),
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
            vec![Cow::Borrowed(&keys::IndexValue::Number(30.0))]
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
            FieldPath(vec![Cow::Borrowed("age")]),
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
            vec![Cow::Borrowed(&keys::IndexValue::Number(30.0))]
        )
        .unwrap()
    );

    test_to_key_range!(
        test_to_key_range_age_lte_30,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec![Cow::Borrowed("age")]),
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
            vec![Cow::Borrowed(&keys::IndexValue::Number(30.0))]
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_age_lt_50_desc,
        WhereQuery(HashMap::from_iter(vec![(
            FieldPath(vec![Cow::Borrowed("age")]),
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
            vec![Cow::Borrowed(&keys::IndexValue::Number(50.0))]
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
                FieldPath(vec![Cow::Borrowed("age")]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue::Number(30.0)),
                    ..Default::default()
                }),
            ),
            (
                FieldPath(vec![Cow::Borrowed("name")]),
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
                Cow::Borrowed(&keys::IndexValue::String(Cow::Borrowed("John"))),
                Cow::Borrowed(&keys::IndexValue::Number(30.0)),
            ]
        )
        .unwrap()
        .wildcard(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["age"]],
            &[keys::Direction::Ascending, keys::Direction::Ascending],
            vec![Cow::Borrowed(&keys::IndexValue::String("John".into())),]
        )
        .unwrap()
        .wildcard()
    );

    test_to_key_range!(
        test_to_key_range_name_eq_john_id_eq_rec1,
        WhereQuery(HashMap::from_iter(vec![
            (
                FieldPath(vec![Cow::Borrowed("name")]),
                WhereNode::Equality(WhereValue::String("John".into())),
            ),
            (
                FieldPath(vec![Cow::Borrowed("id")]),
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
                Cow::Borrowed(&keys::IndexValue::String(Cow::Borrowed("John"))),
                Cow::Borrowed(&keys::IndexValue::String(Cow::Borrowed("rec1"))),
            ]
        )
        .unwrap(),
        keys::Key::new_index(
            "namespace".to_string(),
            &[&["name"], &["id"]],
            &[keys::Direction::Ascending, keys::Direction::Ascending],
            vec![
                Cow::Borrowed(&keys::IndexValue::String(Cow::Borrowed("John"))),
                Cow::Borrowed(&keys::IndexValue::String(Cow::Borrowed("rec1"))),
            ]
        )
        .unwrap()
        .wildcard()
    );
}
