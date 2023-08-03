use super::{
    cursor::{Cursor, CursorDirection},
    record::{self, IndexValue},
};
use schema::{
    field_path::FieldPath,
    index::{EitherIndexField, Index, IndexDirection, IndexField},
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cmp::Ordering, collections::HashMap};

pub type Result<T> = std::result::Result<T, WhereQueryError>;

#[derive(Debug, thiserror::Error)]
pub enum WhereQueryError {
    #[error(transparent)]
    UserError(#[from] WhereQueryUserError),

    #[error("record error")]
    RecordError(#[from] record::RecordError),

    #[error("can only sort by inequality if it's the same direction")]
    InequalitySortDirectionMismatch,
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

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct WhereQuery<'a>(pub(crate) HashMap<FieldPath, WhereNode<'a>>);

impl<'a> WhereQuery<'a> {
    // Determines if the query matches the given index
    pub fn matches(&self, index: &Index, sort: &[IndexField]) -> bool {
        let Ok(mut requirements) = index_requirements(self, sort) else { return false; };

        if requirements.len() > index.fields.len() {
            return false;
        }

        // equality requirements should be first
        requirements.sort_by(|a, b| match b.equality.cmp(&a.equality) {
            Ordering::Equal => {
                let matching_fields_b = index
                    .fields
                    .iter()
                    .map(|f| b.matches(Some(f)))
                    .take_while(|m| *m)
                    .count();
                let matching_fields_a: usize = index
                    .fields
                    .iter()
                    .map(|f| a.matches(Some(f)))
                    .take_while(|m| *m)
                    .count();

                matching_fields_b.cmp(&matching_fields_a)
            }
            ord => ord,
        });

        let mut ignore_rights = false;
        for (field, requirement) in index.fields.iter().zip(requirements.iter()) {
            match ignore_rights {
                false if !requirement.matches(Some(field)) => return false,
                true if requirement.left != *field => return false,
                _ => {}
            }

            if (requirement.left != *field || requirement.inequality) && !requirement.equality {
                ignore_rights = true;
            }
        }

        true
    }

    /// Applies a cursor to the query, updating the query to only return records after the cursor.
    ///
    /// # Example
    ///
    /// ## Cursor (ASC / After)
    ///
    /// Given the original query:
    /// ```sql
    /// WHERE
    ///     name == calum && group > 0 and group <= 3 && age > 10
    /// ORDER BY name, group, age ASC
    /// ```
    ///
    /// After applying the cursor, it would look like:
    /// ```sql
    /// WHERE
    ///     name == calum && group >= 2 and group <= 3 && age >= 30
    /// ORDER BY name, group, age ASC
    /// ```
    ///
    /// The record list (before applying the cursor) would look like this:
    /// ```
    /// calum, 1, 20, 4  <- lower bound
    /// calum, 2, 20, 2
    /// calum, 2, 30, 1  <- this is the cursor
    /// calum, 2, 40, 7
    /// calum, 3, 10, 3  <- upper bound
    /// ---
    /// john, 1, 20, 5
    /// ```
    ///
    /// ## Cursor (DESC / After)
    ///
    /// Given the original query:
    /// ```sql
    /// WHERE
    ///     name == calum && group > 0 and group <= 3 && age > 10
    /// ORDER BY name, group, age DESC
    /// ```
    ///
    /// After applying the cursor, it would look like:
    /// ```sql
    /// WHERE
    ///     name == calum && group >= 2 and group <= 3 && age > 10 && age <= 30
    /// ORDER BY name, group, age DESC
    /// ```
    ///
    /// The record list (DESC) (before applying the cursor) would look like this:
    /// ```
    ///
    /// calum, 2, 40, 7  <- lower bound
    /// calum, 2, 30, 1  <- this is the cursor
    /// calum, 1, 20, 4
    /// calum, 2, 20, 2
    /// calum, 3, 10, 3  <- upper bound
    /// ---
    /// john, 1, 20, 5
    /// ```
    /// ## Filter Conditions
    /// * If equality filter, leave as is
    /// * If range filter (>, >=, <, <=):
    ///     * If ASC + (>, >=), update to >= `<cursor_record_value>`
    ///     * If DESC + (<, <=), update to <= `<cursor_record_value>`
    ///  
    /// `index selection` - Determined by `where_query` + `order_by`
    ///
    /// `direction` - Determined by `order_by`
    ///
    /// `lower bound` - Determined by `cursor`
    ///
    /// `upper bound` - Determined by `where_query`
    pub fn apply_cursor(
        &mut self,
        cursor: Cursor,
        dir: CursorDirection,
        // TODO: does this include ID?
        order_by: &[IndexField],
    ) {
        // let values = cursor.values.with_static();
        for (key, value) in &mut self.0 {
            // We only care about inequality filters
            if let WhereNode::Inequality(node) = value {
                // Determine which direction we want to continue in (which determines
                // the inequality filter to update)
                let forward = is_inequality_forwards(key, order_by, &dir);

                // TODO: Only add fields in the cursor, or should we add these as Null?
                if let Some(cursor_field_value) = cursor.values.get(key) {
                    if forward && (node.gt.is_some() || node.gte.is_some()) {
                        // Only update if the cursor has the value for the field
                        node.gte = Some(WhereValue(cursor_field_value.clone().with_static()));
                        node.gt = None;
                    }

                    if !forward && (node.lt.is_some() || node.lte.is_some()) {
                        // Only update if the cursor has the value for the field
                        node.lte = Some(WhereValue(cursor_field_value.clone().with_static()));
                        node.lt = None;
                    }
                }
            }
        }

        // If id field not present, we should add it to the query so we don't end up
        // sending the last record in the previous query back to the user
        let id = FieldPath::id();
        if let std::collections::hash_map::Entry::Vacant(e) = self.0.entry(id.clone()) {
            let forward = is_inequality_forwards(&id, order_by, &dir);
            let where_value = Some(WhereValue(cursor.record_id.with_static()));

            e.insert(match forward {
                true => WhereNode::Inequality(WhereInequality {
                    gt: where_value,
                    gte: None,
                    lt: None,
                    lte: None,
                }),
                false => WhereNode::Inequality(WhereInequality {
                    gt: None,
                    gte: None,
                    lt: where_value,
                    lte: None,
                }),
            });
        }
    }
}

/// Determines if the inequality projection should be forwards (gt/gte) or backwards (lt/lte)
fn is_inequality_forwards(key: &FieldPath, order_by: &[IndexField], dir: &CursorDirection) -> bool {
    // Find the sort order direction for a key
    let order_for_key = order_by
        .iter()
        .find(|field| &field.path == key)
        .map(|field| field.direction)
        .unwrap_or(IndexDirection::Ascending);

    // Determine which direction we want to continue in (which determines
    // the inequality filter to update)
    match (order_for_key, &dir) {
        (IndexDirection::Ascending, CursorDirection::After) => false,
        (IndexDirection::Ascending, CursorDirection::Before) => true,
        (IndexDirection::Descending, CursorDirection::After) => true,
        (IndexDirection::Descending, CursorDirection::Before) => false,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum WhereNode<'a> {
    Inequality(WhereInequality<'a>),
    Equality(WhereValue<'a>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WhereValue<'a>(pub(crate) IndexValue<'a>);

#[derive(Debug, Serialize, Default, Clone)]
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

// Implementing Deserialize manually, so we can provide better error messages
impl<'de, 'a> Deserialize<'de> for WhereInequality<'a> {
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

fn index_requirements(
    where_query: &WhereQuery,
    sorts: &[IndexField],
) -> Result<Vec<EitherIndexField>> {
    let mut requirements = vec![];

    for (field, node) in &where_query.0 {
        match node {
            WhereNode::Equality(_) => {
                let path: Vec<String> = field.0.iter().map(|x| x.to_string()).collect();

                requirements.push(EitherIndexField {
                    equality: true,
                    inequality: false,
                    left: IndexField {
                        path: path.clone().into(),
                        direction: IndexDirection::Ascending,
                    },
                    right: Some(IndexField {
                        path: path.into(),
                        direction: IndexDirection::Descending,
                    }),
                });
            }
            WhereNode::Inequality(_) => {}
        }
    }

    for (field, node) in &where_query.0 {
        match node {
            WhereNode::Equality(_) => {}
            WhereNode::Inequality(ineq) => {
                let direction = if ineq.lt.is_some() || ineq.lte.is_some() {
                    IndexDirection::Descending
                } else {
                    IndexDirection::Ascending
                };

                requirements.push(EitherIndexField {
                    equality: false,
                    inequality: true,
                    left: IndexField {
                        path: field
                            .0
                            .iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<String>>()
                            .into(),
                        direction,
                    },
                    right: None,
                });
            }
        }
    }

    for (i, sort) in sorts.iter().enumerate() {
        let mut requirement = EitherIndexField {
            inequality: false,
            equality: false,
            left: IndexField {
                path: sort.path.clone(),
                direction: sort.direction,
            },
            right: None,
        };

        let is_last = i == sorts.len() - 1;
        if is_last {
            let opposite_direction = match sort.direction {
                IndexDirection::Ascending => IndexDirection::Descending,
                IndexDirection::Descending => IndexDirection::Ascending,
            };

            requirement.right = Some(IndexField {
                path: sort.path.clone(),
                direction: opposite_direction,
            });
        } else if requirements
            .last()
            .map(|r| r.inequality && r.left.path == sort.path && r.left.direction != sort.direction)
            .unwrap_or(false)
        {
            return Err(WhereQueryError::InequalitySortDirectionMismatch);
        }

        if let Some(last_req) = requirements.last_mut() {
            if last_req.matches(Some(&requirement.left))
                || last_req.matches(requirement.right.as_ref())
            {
                last_req.left = requirement.left;
                last_req.right = requirement.right;
                continue;
            }
        }

        requirements.push(requirement);
    }

    if let Some(last) = requirements.last_mut() {
        if last.inequality {
            let opposite_direction = match last.left.direction {
                IndexDirection::Ascending => IndexDirection::Descending,
                IndexDirection::Descending => IndexDirection::Ascending,
            };

            last.right = Some(IndexField {
                path: last.left.path.clone(),
                direction: opposite_direction,
            });
        }
    }

    Ok(requirements)
}

#[allow(dead_code)]
fn index_recommendation<'a>(where_query: &'a WhereQuery, sorts: &[IndexField]) -> Result<Index> {
    let mut index_fields = vec![];
    let requirements = index_requirements(where_query, sorts)?;

    for requirement in requirements {
        if requirement.equality {
            index_fields.push(IndexField {
                path: requirement.left.path,
                direction: IndexDirection::Ascending,
            });
        } else {
            index_fields.push(requirement.left);
        }
    }

    Ok(Index {
        fields: index_fields,
    })
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::index::IndexDirection;

//     macro_rules! test_to_key_range {
//         ($name:ident, $query:expr, $fields:expr, $directions:expr, $lower:expr, $upper:expr) => {
//             #[test]
//             fn $name() {
//                 let query = $query;

//                 let key_range = query
//                     .key_range(
//                         &polylang::stableast::Collection {
//                             namespace: polylang::stableast::Namespace {
//                                 value: "test".into(),
//                             },
//                             name: "Sample".into(),
//                             attributes: vec![
//                                 polylang::stableast::CollectionAttribute::Property(
//                                     polylang::stableast::Property {
//                                         name: "id".into(),
//                                         type_: polylang::stableast::Type::Primitive(
//                                             polylang::stableast::Primitive {
//                                                 value: polylang::stableast::PrimitiveType::String,
//                                             },
//                                         ),
//                                         directives: vec![],
//                                         required: false,
//                                     },
//                                 ),
//                                 polylang::stableast::CollectionAttribute::Property(
//                                     polylang::stableast::Property {
//                                         name: "name".into(),
//                                         type_: polylang::stableast::Type::Primitive(
//                                             polylang::stableast::Primitive {
//                                                 value: polylang::stableast::PrimitiveType::String,
//                                             },
//                                         ),
//                                         directives: vec![],
//                                         required: false,
//                                     },
//                                 ),
//                                 polylang::stableast::CollectionAttribute::Property(
//                                     polylang::stableast::Property {
//                                         name: "age".into(),
//                                         type_: polylang::stableast::Type::Primitive(
//                                             polylang::stableast::Primitive {
//                                                 value: polylang::stableast::PrimitiveType::Number,
//                                             },
//                                         ),
//                                         directives: vec![],
//                                         required: false,
//                                     },
//                                 ),
//                             ],
//                         },
//                         "namespace".to_string(),
//                         $fields,
//                         $directions,
//                     )
//                     .unwrap();

//                 assert_eq!(key_range.lower, $lower, "lower");

//                 assert_eq!(key_range.upper, $upper, "upper");
//             }
//         };
//     }

//     test_to_key_range!(
//         test_to_key_range_name_eq_john,
//         WhereQuery(HashMap::from_iter(vec![(
//             FieldPath(vec!["name".to_string()]),
//             WhereNode::Equality(WhereValue("john".into())),
//         )])),
//         &[&["name"]],
//         &[IndexDirection::Ascending],
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["name"]],
//             &[IndexDirection::Ascending],
//             vec![Cow::Owned(IndexValue::String("john".to_string().into()))]
//         )
//         .unwrap(),
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["name"]],
//             &[IndexDirection::Ascending],
//             vec![Cow::Owned(IndexValue::String("john".to_string().into()))]
//         )
//         .unwrap()
//         .wildcard()
//     );

//     test_to_key_range!(
//         test_to_key_range_age_gt_30,
//         WhereQuery(HashMap::from_iter(vec![(
//             FieldPath(vec!["age".to_string()]),
//             WhereNode::Inequality(WhereInequality {
//                 gt: Some(WhereValue(30.0.into())),
//                 ..Default::default()
//             }),
//         )])),
//         &[&["age"]],
//         &[IndexDirection::Ascending],
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Ascending],
//             vec![Cow::Borrowed(&IndexValue::Number(30.0))]
//         )
//         .unwrap()
//         .wildcard(),
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Ascending],
//             Vec::new(),
//         )
//         .unwrap()
//         .wildcard()
//     );

//     test_to_key_range!(
//         test_to_key_range_age_gte_30,
//         WhereQuery(HashMap::from_iter(vec![(
//             FieldPath(vec!["age".to_string()]),
//             WhereNode::Inequality(WhereInequality {
//                 gte: Some(WhereValue(30.0.into())),
//                 ..Default::default()
//             }),
//         )])),
//         &[&["age"]],
//         &[IndexDirection::Ascending],
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Ascending],
//             vec![Cow::Borrowed(&IndexValue::Number(30.0))]
//         )
//         .unwrap(),
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Ascending],
//             Vec::new(),
//         )
//         .unwrap()
//         .wildcard()
//     );

//     test_to_key_range!(
//         test_to_key_range_age_lt_30,
//         WhereQuery(HashMap::from_iter(vec![(
//             FieldPath(vec!["age".to_string()]),
//             WhereNode::Inequality(WhereInequality {
//                 lt: Some(WhereValue(30.0.into())),
//                 ..Default::default()
//             }),
//         )])),
//         &[&["age"]],
//         &[IndexDirection::Ascending],
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Ascending],
//             Vec::new(),
//         )
//         .unwrap(),
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Ascending],
//             vec![Cow::Borrowed(&IndexValue::Number(30.0))]
//         )
//         .unwrap()
//     );

//     test_to_key_range!(
//         test_to_key_range_age_lte_30,
//         WhereQuery(HashMap::from_iter(vec![(
//             FieldPath(vec!["age".to_string()]),
//             WhereNode::Inequality(WhereInequality {
//                 lte: Some(WhereValue(30.0.into())),
//                 ..Default::default()
//             }),
//         )])),
//         &[&["age"]],
//         &[IndexDirection::Ascending],
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Ascending],
//             Vec::new(),
//         )
//         .unwrap(),
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Ascending],
//             vec![Cow::Borrowed(&IndexValue::Number(30.0))]
//         )
//         .unwrap()
//         .wildcard()
//     );

//     test_to_key_range!(
//         test_to_key_range_age_lt_50_desc,
//         WhereQuery(HashMap::from_iter(vec![(
//             FieldPath(vec!["age".to_string()]),
//             WhereNode::Inequality(WhereInequality {
//                 lt: Some(WhereValue(50.0.into())),
//                 ..Default::default()
//             }),
//         )])),
//         &[&["age"]],
//         &[IndexDirection::Descending],
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Descending],
//             vec![Cow::Borrowed(&IndexValue::Number(50.0))]
//         )
//         .unwrap()
//         .wildcard(),
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["age"]],
//             &[IndexDirection::Descending],
//             Vec::new(),
//         )
//         .unwrap()
//         .wildcard()
//     );

//     test_to_key_range!(
//         test_to_key_range_age_gt_30_name_eq_john,
//         WhereQuery(HashMap::from_iter(vec![
//             (
//                 FieldPath(vec!["age".to_string()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue(30.0.into())),
//                     ..Default::default()
//                 }),
//             ),
//             (
//                 FieldPath(vec!["name".to_string()]),
//                 WhereNode::Equality(WhereValue("John".into())),
//             ),
//         ])),
//         &[&["name"], &["age"]],
//         &[IndexDirection::Ascending, IndexDirection::Ascending],
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["name"], &["age"]],
//             &[IndexDirection::Ascending, IndexDirection::Ascending],
//             vec![
//                 Cow::Owned(IndexValue::String("John".to_string().into())),
//                 Cow::Borrowed(&IndexValue::Number(30.0)),
//             ]
//         )
//         .unwrap()
//         .wildcard(),
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["name"], &["age"]],
//             &[IndexDirection::Ascending, IndexDirection::Ascending],
//             vec![Cow::Owned(IndexValue::String("John".into())),]
//         )
//         .unwrap()
//         .wildcard()
//     );

//     test_to_key_range!(
//         test_to_key_range_name_eq_john_id_eq_rec1,
//         WhereQuery(HashMap::from_iter(vec![
//             (
//                 FieldPath(vec!["name".to_string()]),
//                 WhereNode::Equality(WhereValue("John".into())),
//             ),
//             (
//                 FieldPath(vec!["id".to_string()]),
//                 WhereNode::Equality(WhereValue("rec1".into())),
//             ),
//         ])),
//         &[&["name"], &["id"]],
//         &[IndexDirection::Ascending, IndexDirection::Ascending],
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["name"], &["id"]],
//             &[IndexDirection::Ascending, IndexDirection::Ascending],
//             vec![
//                 Cow::Owned(IndexValue::String("John".to_string().into())),
//                 Cow::Owned(IndexValue::String("rec1".to_string().into())),
//             ]
//         )
//         .unwrap(),
//         keys::Key::new_index(
//             "namespace".to_string(),
//             &[&["name"], &["id"]],
//             &[IndexDirection::Ascending, IndexDirection::Ascending],
//             vec![
//                 Cow::Owned(IndexValue::String("John".to_string().into())),
//                 Cow::Owned(IndexValue::String("rec1".to_string().into())),
//             ]
//         )
//         .unwrap()
//         .wildcard()
//     );
// }
