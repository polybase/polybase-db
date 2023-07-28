use super::record::{self, IndexValue};
use super::stableast_ext::FieldWalker;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap};

pub type Result<T> = std::result::Result<T, WhereQueryError>;

#[derive(Debug, thiserror::Error)]
pub enum WhereQueryError {
    #[error(transparent)]
    UserError(#[from] WhereQueryUserError),

    // #[error("keys error")]
    // KeysError(#[from] keys::KeysError),
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
