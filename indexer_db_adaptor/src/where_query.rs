use super::cursor::{Cursor, CursorDirection};
use schema::{
    field_path::FieldPath,
    index::{self, EitherIndexField, Index, IndexDirection, IndexField},
    index_value::IndexValue,
    record::{self, RecordRoot, RecordUserError, RecordValue},
    types::Type,
    Schema,
};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap};

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

    #[error("unexpected query field: {}", .field.as_deref().unwrap_or("unknown"))]
    InvalidWhereQueryField { field: Option<String> },

    #[error("where query value at field \"{}\" does not match the schema type, expected type: {expected_type}, got value: {value}", .field.as_deref().unwrap_or("unknown"))]
    InvalidWhereQueryValue {
        value: serde_json::Value,
        expected_type: String,
        field: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct WhereQuery<'a>(pub HashMap<FieldPath, WhereNode<'a>>);

impl<'a> WhereQuery<'a> {
    /// Determines if the query matches the given index
    ///
    /// Indexes must be able to select records as a contiguous block. Sort order of indexes
    /// impacts the matching of an index.
    ///
    /// - Equality requirements must match front index fields (i.e.), sort order (ASC/DESC) of index does not matter
    /// - Only one inequality filter can be used at once (although the same field can have an upper and lower bound),
    ///   after an inequality filter no more filters can be used
    /// - The first sort order or inequality filter used does not need to match index sort order, but subsequent sort
    ///   orders must match index sort order
    ///
    /// [Name ASC, Age ASC, Group DESC]
    ///
    /// - Name == "calum" && Age > 10                  // MATCH
    /// - Name == "calum" && Age > 10 && Age < 20      // MATCH
    /// - Name == "calum" && Age > 10 && Group > 3     // INVALID, multiple inequality filters
    ///
    /// [Age ASC, Name ASC, Group DESC]
    ///
    /// - Name == "calum" && Age > 10                  // NO MATCH, no matches after inequality filter
    /// - Name == "calum"                              // NO MATCH, equality requirements must match from front of index
    ///
    pub fn matches(&self, index: &Index, sort: &[IndexField]) -> bool {
        let Ok(mut requirements) = self.index_requirements(sort) else { return false; };

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

    // Applies a cursor to the query, updating the query to only return records after the cursor.
    //
    // # Example
    //
    // ## Cursor (ASC / After)
    //
    // Given the original query:
    // ```sql
    // WHERE
    //     name == calum && group > 0 and group <= 3 && age > 10
    // ORDER BY name, group, age ASC
    // ```
    //
    // After applying the cursor, it would look like:
    // ```sql
    // WHERE
    //     name == calum && group >= 2 and group <= 3 && age >= 30
    // ORDER BY name, group, age ASC
    // ```
    //
    // The record list (before applying the cursor) would look like this:
    // ```
    // calum, 1, 20, 4  <- lower bound
    // calum, 2, 20, 2
    // calum, 2, 30, 1  <- this is the cursor
    // calum, 2, 40, 7
    // calum, 3, 10, 3  <- upper bound
    // ---
    // john, 1, 20, 5
    // ```
    //
    // ## Cursor (DESC / After)
    //
    // Given the original query:
    // ```sql
    // WHERE
    //     name == calum && group > 0 and group <= 3 && age > 10
    // ORDER BY name, group, age DESC
    // ```
    //
    // After applying the cursor, it would look like:
    // ```sql
    // WHERE
    //     name == calum && group >= 2 and group <= 3 && age > 10 && age <= 30
    // ORDER BY name, group, age DESC
    // ```
    //
    // The record list (DESC) (before applying the cursor) would look like this:
    // ```
    //
    // calum, 2, 40, 7  <- lower bound
    // calum, 2, 30, 1  <- this is the cursor
    // calum, 1, 20, 4
    // calum, 2, 20, 2
    // calum, 3, 10, 3  <- upper bound
    // ---
    // john, 1, 20, 5
    // ```
    // ## Filter Conditions
    // * If equality filter, leave as is
    // * If range filter (>, >=, <, <=):
    //     * If ASC + (>, >=), update to >= `<cursor_record_value>`
    //     * If DESC + (<, <=), update to <= `<cursor_record_value>`
    //
    // `index selection` - Determined by `where_query` + `order_by`
    //
    // `direction` - Determined by `order_by`
    //
    // `lower bound` - Determined by `cursor`
    //
    // `upper bound` - Determined by `where_query`
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
                if let Some(cursor_field_value) = cursor.0.values.get(key) {
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
            let where_value = Some(WhereValue(cursor.0.record_id.with_static()));

            e.insert(match forward {
                true => WhereNode::Inequality(Box::new(WhereInequality {
                    gt: where_value,
                    gte: None,
                    lt: None,
                    lte: None,
                })),
                false => WhereNode::Inequality(Box::new(WhereInequality {
                    gt: None,
                    gte: None,
                    lt: where_value,
                    lte: None,
                })),
            });
        }
    }

    fn index_requirements(&self, sorts: &[IndexField]) -> Result<Vec<EitherIndexField>> {
        let mut requirements = vec![];

        for (field, node) in &self.0 {
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

        for (field, node) in &self.0 {
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
                .map(|r| {
                    r.inequality && r.left.path == sort.path && r.left.direction != sort.direction
                })
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

    pub fn index_recommendation(&self, sorts: &[IndexField]) -> Result<Index> {
        let mut index_fields = vec![];
        let requirements = self.index_requirements(sorts)?;

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

    // TODO: consider
    pub fn cast(&mut self, schema: &Schema) -> Result<()> {
        for (path, node) in &mut self.0 {
            let prop = schema.properties.get_path(path).ok_or(
                WhereQueryUserError::InvalidWhereQueryField {
                    field: Some(path.to_string()),
                },
            )?;

            match node {
                WhereNode::Equality(val) => val.cast(&prop.type_)?,
                WhereNode::Inequality(ineq) => ineq.cast(&prop.type_)?,
            }
        }

        self.0.iter_mut().for_each(|(k, v)| {});
        Ok(())
    }

    /// Create a RecordRoot from the where_query using the equality filters
    pub fn to_record_root(&self, schema: &Schema) -> RecordRoot {
        let mut record_root = RecordRoot::default();

        self.0
            .iter()
            .filter_map(|(k, values)| match values {
                WhereNode::Equality(WhereValue(v)) => {
                    let rv: RecordValue = RecordValue::from(v.clone());
                    let prop = schema.properties.get_path(k)?;
                    // TODO: we should return the error
                    let v = rv.cast(&prop.type_).ok()?;
                    Some((k, v))
                }
                _ => None,
            })
            .for_each(|(k, v)| {
                record_root.insert_path(k, v);
            });

        record_root
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
pub enum WhereNode<'a> {
    Equality(WhereValue<'a>),
    Inequality(Box<WhereInequality<'a>>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WhereValue<'a>(pub IndexValue<'a>);

impl<'a> WhereValue<'a> {
    fn cast(&mut self, type_: &Type) -> Result<()> {
        let rv: RecordValue = RecordValue::from(self.0.clone());
        let v = rv.cast(type_)?;
        // We've just converted from a RecordValue to a IndexValue
        #[allow(clippy::unwrap_used)]
        let index_value: IndexValue = v.try_into().unwrap();
        self.0 = index_value;
        Ok(())
    }
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct WhereInequality<'a> {
    #[serde(rename = "$gt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gt: Option<WhereValue<'a>>,
    #[serde(rename = "$gte")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gte: Option<WhereValue<'a>>,
    #[serde(rename = "$lt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lt: Option<WhereValue<'a>>,
    #[serde(rename = "$lte")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lte: Option<WhereValue<'a>>,
}

impl WhereInequality<'_> {
    pub fn cast(&mut self, type_: &Type) -> Result<()> {
        if let Some(gt) = &mut self.gt {
            gt.cast(type_)?;
        }

        if let Some(gte) = &mut self.gte {
            gte.cast(type_)?;
        }

        if let Some(lt) = &mut self.lt {
            lt.cast(type_)?;
        }

        if let Some(lte) = &mut self.lte {
            lte.cast(type_)?;
        }

        Ok(())
    }
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

#[cfg(test)]
mod test {
    use super::*;
    use schema::index_value::IndexValue;

    #[test]
    fn test_equality_serialization() {
        let query: WhereQuery<'_> = WhereQuery(
            [
                (
                    "name".into(),
                    WhereNode::Equality(WhereValue(IndexValue::String("John".into()))),
                ), // ("isActive".to_string(), json!(true),
            ]
            .into(),
        );
        let query_str = r#"{"name":"John"}"#;

        assert_eq!(query_str, serde_json::to_string(&query).unwrap());

        let _: WhereQuery = serde_json::from_str(query_str).unwrap();
    }

    #[test]
    fn test_inequality_serialization() {
        let query: WhereQuery<'_> = WhereQuery(
            [
                (
                    "name".into(),
                    WhereNode::Inequality(
                        WhereInequality {
                            gt: Some(WhereValue(IndexValue::String("John".into()))),
                            gte: None,
                            lt: None,
                            lte: None,
                        }
                        .into(),
                    ),
                ), // ("isActive".to_string(), json!(true),
            ]
            .into(),
        );
        let query_str = r#"{"name":{"$gt":"John"}}"#;

        assert_eq!(query_str, serde_json::to_string(&query).unwrap());

        let _: WhereQuery = serde_json::from_str(query_str).unwrap();
    }
}
