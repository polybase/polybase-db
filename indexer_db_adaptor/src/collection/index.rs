use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cmp::Ordering};

use super::where_query::{WhereNode, WhereQuery};

pub type Result<T> = std::result::Result<T, IndexError>;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("can only sort by inequality if it's the same direction")]
    InequalitySortDirectionMismatch,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct IndexField<'a> {
    pub(crate) path: Vec<Cow<'a, str>>,
    pub(crate) direction: IndexDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum IndexDirection {
    Ascending,
    Descending,
}

impl<'a> IndexField<'a> {
    pub fn new(path: Vec<Cow<'a, str>>, direction: IndexDirection) -> Self {
        Self { path, direction }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Index<'a> {
    pub(crate) fields: Vec<IndexField<'a>>,
}

impl<'a> Index<'a> {
    pub fn new(mut fields: Vec<IndexField<'a>>) -> Self {
        let id_field: IndexField<'_> =
            IndexField::new(vec!["id".into()], IndexDirection::Ascending);

        if let Some(true) = fields.last().map(|f| f == &id_field) {
        } else {
            fields.push(id_field);
        }

        Self { fields }
    }

    pub fn should_list_in_reverse(&self, sort: &[IndexField<'a>]) -> bool {
        let Some(last_sort) = sort.last() else {
            return false;
        };

        self.fields
            .iter()
            .find(|f| f.path == last_sort.path)
            .map(|f| f.direction)
            != Some(last_sort.direction)
    }

    pub fn matches(&self, where_query: &WhereQuery, sort: &[IndexField<'a>]) -> bool {
        let Ok(mut requirements) = index_requirements(where_query, sort) else { return false; };

        if requirements.len() > self.fields.len() {
            return false;
        }

        // equality requirements should be first
        requirements.sort_by(|a, b| match b.equality.cmp(&a.equality) {
            Ordering::Equal => {
                let matching_fields_b = self
                    .fields
                    .iter()
                    .map(|f| b.matches(Some(f)))
                    .take_while(|m| *m)
                    .count();
                let matching_fields_a = self
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
        for (field, requirement) in self.fields.iter().zip(requirements.iter()) {
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
}

#[derive(Debug, PartialEq)]
struct EitherIndexField<'a> {
    equality: bool,
    inequality: bool,
    left: IndexField<'a>,
    right: Option<IndexField<'a>>,
}

impl EitherIndexField<'_> {
    fn matches(&self, field: Option<&IndexField>) -> bool {
        match field {
            Some(field) => &self.left == field || self.right.as_ref() == Some(field),
            None => false,
        }
    }
}

fn index_requirements<'a>(
    where_query: &'a WhereQuery,
    sorts: &[IndexField<'a>],
) -> Result<Vec<EitherIndexField<'a>>> {
    let mut requirements = vec![];

    for (field, node) in &where_query.0 {
        match node {
            WhereNode::Equality(_) => {
                let path: Vec<Cow<str>> =
                    field.0.iter().map(|x| Cow::Borrowed(x.as_str())).collect();

                requirements.push(EitherIndexField {
                    equality: true,
                    inequality: false,
                    left: IndexField {
                        path: path.clone(),
                        direction: IndexDirection::Ascending,
                    },
                    right: Some(IndexField {
                        path,
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
                        path: field.0.iter().map(|x| Cow::Borrowed(x.as_str())).collect(),
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
            return Err(IndexError::InequalitySortDirectionMismatch);
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
fn index_recommendation<'a>(
    where_query: &'a WhereQuery,
    sorts: &[IndexField<'a>],
) -> Result<Index<'a>> {
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

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::super::where_query::{FieldPath, WhereInequality, WhereValue};
    use super::*;

    macro_rules! test_index_requirements {
        ($name:ident, $where:expr, $sort:expr, $expected:expr) => {
            #[test]
            fn $name() {
                let where_query = WhereQuery($where);
                let sort = $sort;
                let res = index_requirements(&where_query, &sort);
                assert!(res.is_ok());
                assert_eq!(res.unwrap(), $expected);
            }
        };
    }

    test_index_requirements!(
        test_index_requirements_name_gt_cal_age_asc,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![IndexField {
            path: vec!["age".into()],
            direction: IndexDirection::Ascending,
        }],
        vec![
            EitherIndexField {
                equality: false,
                inequality: true,
                left: IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                right: None,
            },
            EitherIndexField {
                equality: false,
                inequality: false,
                left: IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Ascending,
                },
                right: Some(IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                }),
            },
        ]
    );

    test_index_requirements!(
        test_index_requirements_name_eq_cal_age_asc,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map
        },
        vec![IndexField {
            path: vec!["age".into()],
            direction: IndexDirection::Ascending,
        }],
        vec![
            EitherIndexField {
                equality: true,
                inequality: false,
                left: IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                right: Some(IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Descending,
                }),
            },
            EitherIndexField {
                equality: false,
                inequality: false,
                left: IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Ascending,
                },
                right: Some(IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                }),
            },
        ]
    );

    test_index_requirements!(
        test_index_requirements_v_lt_2_v_desc,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["v".into()]),
                WhereNode::Inequality(WhereInequality {
                    lt: Some(WhereValue(2.0.into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![IndexField {
            path: vec!["v".into()],
            direction: IndexDirection::Descending,
        }],
        vec![EitherIndexField {
            equality: false,
            inequality: true,
            left: IndexField {
                path: vec!["v".into()],
                direction: IndexDirection::Descending,
            },
            right: Some(IndexField {
                path: vec!["v".into()],
                direction: IndexDirection::Ascending,
            }),
        }]
    );

    test_index_requirements!(
        test_index_requirements_age_lt_40,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    lt: Some(WhereValue(40.0.into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![],
        vec![EitherIndexField {
            equality: false,
            inequality: true,
            left: IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Descending,
            },
            right: Some(IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Ascending,
            }),
        }]
    );

    macro_rules! test_index_recommendation {
        ($name:ident, $where:expr, $sort:expr, $expected:expr) => {
            #[test]
            fn $name() {
                let where_query = WhereQuery($where);
                let sort = $sort;
                let res = index_recommendation(&where_query, &sort);
                assert!(res.is_ok());
                assert_eq!(res.unwrap(), $expected);
            }
        };
        (error, $name:ident, $where:expr, $sort:expr, $expected:expr) => {
            #[test]
            fn $name() {
                let where_query = WhereQuery($where);
                let sort = $sort;
                let res = index_recommendation(&where_query, &sort);
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().to_string(), $expected);
            }
        };
    }

    test_index_recommendation!(
        test_index_recommendation_name_eq_cal,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map
        },
        vec![],
        Index {
            fields: vec![IndexField {
                path: vec!["name".into()],
                direction: IndexDirection::Ascending,
            }],
        }
    );

    test_index_recommendation!(
        test_index_recommendation_name_gt_cal,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![],
        Index {
            fields: vec![IndexField {
                path: vec!["name".into()],
                direction: IndexDirection::Ascending,
            }],
        }
    );

    test_index_recommendation!(
        test_index_recommendation_name_eq_cal_age_asc_place_desc,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map
        },
        vec![
            IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Ascending,
            },
            IndexField {
                path: vec!["place".into()],
                direction: IndexDirection::Descending,
            },
        ],
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Descending,
                },
            ],
        }
    );

    test_index_recommendation!(
        test_index_recommendation_name_gt_cal_name_asc_age_desc,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![
            IndexField {
                path: vec!["name".into()],
                direction: IndexDirection::Ascending,
            },
            IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Descending,
            },
        ],
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                },
            ],
        }
    );

    test_index_recommendation!(
        error,
        test_index_recommendation_name_lt_cal_name_asc_age_desc,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    lt: Some(WhereValue("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![
            IndexField {
                path: vec!["name".into()],
                direction: IndexDirection::Ascending,
            },
            IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Descending,
            },
        ],
        "can only sort by inequality if it's the same direction"
    );

    macro_rules! test_index_matching {
        ($name:ident, $index:expr, $where:expr, $sort:expr, $expected:expr) => {
            #[test]
            fn $name() {
                let index = $index;
                let where_query = WhereQuery($where);
                let sort = $sort;

                assert_eq!(index.matches(&where_query, &sort), $expected);
            }
        };
    }

    test_index_matching!(
        test_index_matching_name_eq_cal_age_gt_20,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue(20.into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![],
        true
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_country_eq_uk_age_lt_20,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["country".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map.insert(
                FieldPath(vec!["country".into()]),
                WhereNode::Equality(WhereValue("uk".into())),
            );
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    lt: Some(WhereValue(20.into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![],
        true
    );

    test_index_matching!(
        test_index_matching_age_gt_20,
        Index {
            fields: vec![IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Ascending,
            }],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue(20.into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![],
        true
    );

    test_index_matching!(
        test_index_matching_age_gt_20_name_eq_cal,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue(20.into())),
                    ..Default::default()
                }),
            );
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map
        },
        vec![],
        false
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_desc,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map
        },
        vec![IndexField {
            path: vec!["age".into()],
            direction: IndexDirection::Descending,
        }],
        true
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_asc,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map
        },
        vec![IndexField {
            path: vec!["age".into()],
            direction: IndexDirection::Ascending,
        }],
        true
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_eq_10_place_desc,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Equality(WhereValue(10.into())),
            );
            map
        },
        vec![IndexField {
            path: vec!["place".into()],
            direction: IndexDirection::Descending,
        }],
        true
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_asc_place_asc,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map
        },
        vec![
            IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Ascending,
            },
            IndexField {
                path: vec!["place".into()],
                direction: IndexDirection::Ascending,
            }
        ],
        false
    );

    test_index_matching!(
        test_index_matching_name_gt_cal_age_desc_place_asc,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![
            IndexField {
                path: vec!["name".into()],
                direction: IndexDirection::Ascending,
            },
            IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Descending,
            }
        ],
        true
    );

    test_index_matching!(
        test_index_matching_name_gt_cal_age_asc_place_asc,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![
            IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Ascending,
            },
            IndexField {
                path: vec!["place".into()],
                direction: IndexDirection::Ascending,
            }
        ],
        false
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_asc_place_desc,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Descending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue("cal".into())),
            );
            map
        },
        vec![
            IndexField {
                path: vec!["age".into()],
                direction: IndexDirection::Ascending,
            },
            IndexField {
                path: vec!["place".into()],
                direction: IndexDirection::Descending,
            }
        ],
        true
    );

    test_index_matching!(
        test_index_matching_name_gt_cal_age_desc_place_asc_2,
        Index {
            fields: vec![
                IndexField {
                    path: vec!["name".into()],
                    direction: IndexDirection::Ascending,
                },
                IndexField {
                    path: vec!["age".into()],
                    direction: IndexDirection::Descending,
                },
                IndexField {
                    path: vec!["place".into()],
                    direction: IndexDirection::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![IndexField {
            path: vec!["name".into()],
            direction: IndexDirection::Descending,
        }],
        true
    );
}
