use std::fmt::Display;

use crate::field_path::FieldPath;
use polylang::stableast;
use serde::{Deserialize, Serialize};

// TODO: can we make these private?
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct IndexField {
    pub path: FieldPath,
    pub direction: IndexDirection,
}

impl IndexField {
    pub fn new(path: FieldPath, direction: IndexDirection) -> Self {
        Self { path, direction }
    }

    pub fn new_asc(path: FieldPath) -> Self {
        Self::new(path, IndexDirection::Ascending)
    }

    pub fn new_desc(path: FieldPath) -> Self {
        Self::new(path, IndexDirection::Descending)
    }
}

impl From<IndexField> for Vec<String> {
    fn from(field: IndexField) -> Self {
        field.path.0.iter().map(|s| s.to_string()).collect()
    }
}

impl From<Vec<String>> for IndexField {
    fn from(vec: Vec<String>) -> Self {
        IndexField {
            path: vec.into(),
            direction: IndexDirection::Ascending,
        }
    }
}

impl Display for IndexField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.path, self.direction)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum IndexDirection {
    Ascending,
    Descending,
}

impl Display for IndexDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexDirection::Ascending => write!(f, "ASC"),
            IndexDirection::Descending => write!(f, "DESC"),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Index {
    pub fields: Vec<IndexField>,
}

impl Index {
    pub fn new(mut fields: Vec<IndexField>) -> Self {
        let id_field: IndexField = IndexField::new(FieldPath::id(), IndexDirection::Ascending);

        if let Some(true) = fields.last().map(|f| f == &id_field) {
        } else {
            fields.push(id_field);
        }

        Self { fields }
    }

    pub fn iter(&self) -> impl Iterator<Item = &IndexField> {
        self.fields.iter()
    }

    pub fn should_list_in_reverse(&self, sort: &[IndexField]) -> bool {
        let Some(last_sort) = sort.last() else {
            return false;
        };

        self.fields
            .iter()
            .find(|f| f.path == last_sort.path)
            .map(|f| f.direction)
            != Some(last_sort.direction)
    }
}

// TODO: can we make these field not public
#[derive(Debug, PartialEq)]
pub struct EitherIndexField {
    pub equality: bool,
    pub inequality: bool,
    pub left: IndexField,
    pub right: Option<IndexField>,
}

impl EitherIndexField {
    pub fn matches(&self, field: Option<&IndexField>) -> bool {
        match field {
            Some(field) => &self.left == field || self.right.as_ref() == Some(field),
            None => false,
        }
    }
}

pub fn custom_indexes_from_ast(collection_ast: &stableast::Collection) -> Vec<Index> {
    collection_ast
        .attributes
        .iter()
        .filter_map(|attr| match attr {
            stableast::CollectionAttribute::Index(index) => Some(Index::new(
                index
                    .fields
                    .iter()
                    .map(|field| {
                        IndexField::new(
                            field.field_path.iter().map(|p| p.to_string()).collect(),
                            match field.direction {
                                stableast::Direction::Asc => IndexDirection::Ascending,
                                stableast::Direction::Desc => IndexDirection::Descending,
                            },
                        )
                    })
                    .collect(),
            )),
            _ => None,
        })
        // .chain([Index::new(vec![])].into_iter())
        .collect::<Vec<_>>()
}

// #[cfg(test)]
// mod test {
//     use std::collections::HashMap;

//     use super::super::{
//         field_path::FieldPath,
//         where_query::{WhereInequality, WhereValue},
//     };
//     use super::*;

//     macro_rules! test_index_requirements {
//         ($name:ident, $where:expr, $sort:expr, $expected:expr) => {
//             #[test]
//             fn $name() {
//                 let where_query = WhereQuery($where);
//                 let sort = $sort;
//                 let res = index_requirements(&where_query, &sort);
//                 assert!(res.is_ok());
//                 assert_eq!(res.unwrap(), $expected);
//             }
//         };
//     }

//     test_index_requirements!(
//         test_index_requirements_name_gt_cal_age_asc,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue("cal".into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![IndexField {
//             path: vec!["age".into()],
//             direction: IndexDirection::Ascending,
//         }],
//         vec![
//             EitherIndexField {
//                 equality: false,
//                 inequality: true,
//                 left: IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 right: None,
//             },
//             EitherIndexField {
//                 equality: false,
//                 inequality: false,
//                 left: IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 right: Some(IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 }),
//             },
//         ]
//     );

//     test_index_requirements!(
//         test_index_requirements_name_eq_cal_age_asc,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map
//         },
//         vec![IndexField {
//             path: vec!["age".into()],
//             direction: IndexDirection::Ascending,
//         }],
//         vec![
//             EitherIndexField {
//                 equality: true,
//                 inequality: false,
//                 left: IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 right: Some(IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Descending,
//                 }),
//             },
//             EitherIndexField {
//                 equality: false,
//                 inequality: false,
//                 left: IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 right: Some(IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 }),
//             },
//         ]
//     );

//     test_index_requirements!(
//         test_index_requirements_v_lt_2_v_desc,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["v".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     lt: Some(WhereValue(2.0.into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![IndexField {
//             path: vec!["v".into()],
//             direction: IndexDirection::Descending,
//         }],
//         vec![EitherIndexField {
//             equality: false,
//             inequality: true,
//             left: IndexField {
//                 path: vec!["v".into()],
//                 direction: IndexDirection::Descending,
//             },
//             right: Some(IndexField {
//                 path: vec!["v".into()],
//                 direction: IndexDirection::Ascending,
//             }),
//         }]
//     );

//     test_index_requirements!(
//         test_index_requirements_age_lt_40,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["age".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     lt: Some(WhereValue(40.0.into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![],
//         vec![EitherIndexField {
//             equality: false,
//             inequality: true,
//             left: IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Descending,
//             },
//             right: Some(IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Ascending,
//             }),
//         }]
//     );

//     macro_rules! test_index_recommendation {
//         ($name:ident, $where:expr, $sort:expr, $expected:expr) => {
//             #[test]
//             fn $name() {
//                 let where_query = WhereQuery($where);
//                 let sort = $sort;
//                 let res = index_recommendation(&where_query, &sort);
//                 assert!(res.is_ok());
//                 assert_eq!(res.unwrap(), $expected);
//             }
//         };
//         (error, $name:ident, $where:expr, $sort:expr, $expected:expr) => {
//             #[test]
//             fn $name() {
//                 let where_query = WhereQuery($where);
//                 let sort = $sort;
//                 let res = index_recommendation(&where_query, &sort);
//                 assert!(res.is_err());
//                 assert_eq!(res.unwrap_err().to_string(), $expected);
//             }
//         };
//     }

//     test_index_recommendation!(
//         test_index_recommendation_name_eq_cal,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map
//         },
//         vec![],
//         Index {
//             fields: vec![IndexField {
//                 path: vec!["name".into()],
//                 direction: IndexDirection::Ascending,
//             }],
//         }
//     );

//     test_index_recommendation!(
//         test_index_recommendation_name_gt_cal,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue("cal".into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![],
//         Index {
//             fields: vec![IndexField {
//                 path: vec!["name".into()],
//                 direction: IndexDirection::Ascending,
//             }],
//         }
//     );

//     test_index_recommendation!(
//         test_index_recommendation_name_eq_cal_age_asc_place_desc,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map
//         },
//         vec![
//             IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Ascending,
//             },
//             IndexField {
//                 path: vec!["place".into()],
//                 direction: IndexDirection::Descending,
//             },
//         ],
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Descending,
//                 },
//             ],
//         }
//     );

//     test_index_recommendation!(
//         test_index_recommendation_name_gt_cal_name_asc_age_desc,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue("cal".into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![
//             IndexField {
//                 path: vec!["name".into()],
//                 direction: IndexDirection::Ascending,
//             },
//             IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Descending,
//             },
//         ],
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 },
//             ],
//         }
//     );

//     test_index_recommendation!(
//         error,
//         test_index_recommendation_name_lt_cal_name_asc_age_desc,
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     lt: Some(WhereValue("cal".into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![
//             IndexField {
//                 path: vec!["name".into()],
//                 direction: IndexDirection::Ascending,
//             },
//             IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Descending,
//             },
//         ],
//         "can only sort by inequality if it's the same direction"
//     );

//     macro_rules! test_index_matching {
//         ($name:ident, $index:expr, $where:expr, $sort:expr, $expected:expr) => {
//             #[test]
//             fn $name() {
//                 let index = $index;
//                 let where_query = WhereQuery($where);
//                 let sort = $sort;

//                 assert_eq!(index.matches(&where_query, &sort), $expected);
//             }
//         };
//     }

//     test_index_matching!(
//         test_index_matching_name_eq_cal_age_gt_20,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map.insert(
//                 FieldPath(vec!["age".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue(20.into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![],
//         true
//     );

//     test_index_matching!(
//         test_index_matching_name_eq_cal_country_eq_uk_age_lt_20,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["country".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map.insert(
//                 FieldPath(vec!["country".into()]),
//                 WhereNode::Equality(WhereValue("uk".into())),
//             );
//             map.insert(
//                 FieldPath(vec!["age".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     lt: Some(WhereValue(20.into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![],
//         true
//     );

//     test_index_matching!(
//         test_index_matching_age_gt_20,
//         Index {
//             fields: vec![IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Ascending,
//             }],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["age".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue(20.into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![],
//         true
//     );

//     test_index_matching!(
//         test_index_matching_age_gt_20_name_eq_cal,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["age".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue(20.into())),
//                     ..Default::default()
//                 }),
//             );
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map
//         },
//         vec![],
//         false
//     );

//     test_index_matching!(
//         test_index_matching_name_eq_cal_age_desc,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map
//         },
//         vec![IndexField {
//             path: vec!["age".into()],
//             direction: IndexDirection::Descending,
//         }],
//         true
//     );

//     test_index_matching!(
//         test_index_matching_name_eq_cal_age_asc,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map
//         },
//         vec![IndexField {
//             path: vec!["age".into()],
//             direction: IndexDirection::Ascending,
//         }],
//         true
//     );

//     test_index_matching!(
//         test_index_matching_name_eq_cal_age_eq_10_place_desc,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map.insert(
//                 FieldPath(vec!["age".into()]),
//                 WhereNode::Equality(WhereValue(10.into())),
//             );
//             map
//         },
//         vec![IndexField {
//             path: vec!["place".into()],
//             direction: IndexDirection::Descending,
//         }],
//         true
//     );

//     test_index_matching!(
//         test_index_matching_name_eq_cal_age_asc_place_asc,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map
//         },
//         vec![
//             IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Ascending,
//             },
//             IndexField {
//                 path: vec!["place".into()],
//                 direction: IndexDirection::Ascending,
//             }
//         ],
//         false
//     );

//     test_index_matching!(
//         test_index_matching_name_gt_cal_age_desc_place_asc,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue("cal".into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![
//             IndexField {
//                 path: vec!["name".into()],
//                 direction: IndexDirection::Ascending,
//             },
//             IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Descending,
//             }
//         ],
//         true
//     );

//     test_index_matching!(
//         test_index_matching_name_gt_cal_age_asc_place_asc,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue("cal".into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![
//             IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Ascending,
//             },
//             IndexField {
//                 path: vec!["place".into()],
//                 direction: IndexDirection::Ascending,
//             }
//         ],
//         false
//     );

//     test_index_matching!(
//         test_index_matching_name_eq_cal_age_asc_place_desc,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Descending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Equality(WhereValue("cal".into())),
//             );
//             map
//         },
//         vec![
//             IndexField {
//                 path: vec!["age".into()],
//                 direction: IndexDirection::Ascending,
//             },
//             IndexField {
//                 path: vec!["place".into()],
//                 direction: IndexDirection::Descending,
//             }
//         ],
//         true
//     );

//     test_index_matching!(
//         test_index_matching_name_gt_cal_age_desc_place_asc_2,
//         Index {
//             fields: vec![
//                 IndexField {
//                     path: vec!["name".into()],
//                     direction: IndexDirection::Ascending,
//                 },
//                 IndexField {
//                     path: vec!["age".into()],
//                     direction: IndexDirection::Descending,
//                 },
//                 IndexField {
//                     path: vec!["place".into()],
//                     direction: IndexDirection::Ascending,
//                 }
//             ],
//         },
//         {
//             let mut map = HashMap::new();
//             map.insert(
//                 FieldPath(vec!["name".into()]),
//                 WhereNode::Inequality(WhereInequality {
//                     gt: Some(WhereValue("cal".into())),
//                     ..Default::default()
//                 }),
//             );
//             map
//         },
//         vec![IndexField {
//             path: vec!["name".into()],
//             direction: IndexDirection::Descending,
//         }],
//         true
//     );
// }
