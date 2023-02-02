use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cmp::Ordering, collections::HashMap};

use crate::{
    keys,
    where_query::{WhereNode, WhereQuery},
};

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct CollectionIndexField<'a> {
    pub(crate) path: Vec<Cow<'a, str>>,
    pub(crate) direction: keys::Direction,
}

impl<'a> CollectionIndexField<'a> {
    pub(crate) fn new(path: Vec<Cow<'a, str>>, direction: keys::Direction) -> Self {
        Self { path, direction }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct CollectionIndex<'a> {
    pub(crate) fields: Vec<CollectionIndexField<'a>>,
}

impl<'a> CollectionIndex<'a> {
    pub(crate) fn new(mut fields: Vec<CollectionIndexField<'a>>) -> Self {
        let id_field = CollectionIndexField::new(vec!["id".into()], keys::Direction::Ascending);

        if let Some(true) = fields.last().map(|f| f == &id_field) {
        } else {
            fields.push(id_field);
        }

        Self { fields }
    }

    pub(crate) fn should_list_in_reverse(&self, sort: &[CollectionIndexField<'a>]) -> bool {
        let Some(last_sort) = sort.last() else {
            return false;
        };

        self.fields
            .iter()
            .find(|f| f.path == last_sort.path)
            .map(|f| f.direction)
            != Some(last_sort.direction)
    }

    pub(crate) fn matches(
        &self,
        where_query: &WhereQuery<'a>,
        sort: &[CollectionIndexField<'a>],
    ) -> bool {
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
    left: CollectionIndexField<'a>,
    right: Option<CollectionIndexField<'a>>,
}

impl EitherIndexField<'_> {
    fn matches(&self, field: Option<&CollectionIndexField>) -> bool {
        match field {
            Some(field) => &self.left == field || self.right.as_ref() == Some(field),
            None => false,
        }
    }
}

fn index_requirements<'a>(
    where_query: &'a WhereQuery<'a>,
    sorts: &[CollectionIndexField<'a>],
) -> Result<Vec<EitherIndexField<'a>>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut requirements = vec![];

    for (field, node) in &where_query.0 {
        match node {
            WhereNode::Equality(_) => {
                requirements.push(EitherIndexField {
                    equality: true,
                    inequality: false,
                    left: CollectionIndexField {
                        path: field.0.clone(),
                        direction: keys::Direction::Ascending,
                    },
                    right: Some(CollectionIndexField {
                        path: field.0.clone(),
                        direction: keys::Direction::Descending,
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
                    keys::Direction::Descending
                } else {
                    keys::Direction::Ascending
                };

                requirements.push(EitherIndexField {
                    equality: false,
                    inequality: true,
                    left: CollectionIndexField {
                        path: field.0.clone(),
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
            left: CollectionIndexField {
                path: sort.path.clone(),
                direction: sort.direction,
            },
            right: None,
        };

        let is_last = i == sorts.len() - 1;
        if is_last {
            let opposite_direction = match sort.direction {
                keys::Direction::Ascending => keys::Direction::Descending,
                keys::Direction::Descending => keys::Direction::Ascending,
            };

            requirement.right = Some(CollectionIndexField {
                path: sort.path.clone(),
                direction: opposite_direction,
            });
        } else if requirements
            .last()
            .map(|r| r.inequality && r.left.path == sort.path && r.left.direction != sort.direction)
            .unwrap_or(false)
        {
            return Err("Can only sort by inequality if it's the same direction".into());
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
                keys::Direction::Ascending => keys::Direction::Descending,
                keys::Direction::Descending => keys::Direction::Ascending,
            };

            last.right = Some(CollectionIndexField {
                path: last.left.path.clone(),
                direction: opposite_direction,
            });
        }
    }

    Ok(requirements)
}

fn index_recommendation<'a>(
    where_query: &'a WhereQuery<'a>,
    sorts: &[CollectionIndexField<'a>],
) -> Result<CollectionIndex<'a>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut index_fields = vec![];
    let requirements = index_requirements(where_query, sorts)?;

    for requirement in requirements {
        if requirement.equality {
            index_fields.push(CollectionIndexField {
                path: requirement.left.path,
                direction: keys::Direction::Ascending,
            });
        } else {
            index_fields.push(requirement.left);
        }
    }

    Ok(CollectionIndex {
        fields: index_fields,
    })
}

#[cfg(test)]
mod test {
    use crate::where_query::{FieldPath, WhereInequality, WhereValue};

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
                    gt: Some(WhereValue::String("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![CollectionIndexField {
            path: vec!["age".into()],
            direction: keys::Direction::Ascending,
        }],
        vec![
            EitherIndexField {
                equality: false,
                inequality: true,
                left: CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                right: None,
            },
            EitherIndexField {
                equality: false,
                inequality: false,
                left: CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Ascending,
                },
                right: Some(CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
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
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map
        },
        vec![CollectionIndexField {
            path: vec!["age".into()],
            direction: keys::Direction::Ascending,
        }],
        vec![
            EitherIndexField {
                equality: true,
                inequality: false,
                left: CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                right: Some(CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Descending,
                }),
            },
            EitherIndexField {
                equality: false,
                inequality: false,
                left: CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Ascending,
                },
                right: Some(CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
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
                    lt: Some(WhereValue::Number(2.0)),
                    ..Default::default()
                }),
            );
            map
        },
        vec![CollectionIndexField {
            path: vec!["v".into()],
            direction: keys::Direction::Descending,
        }],
        vec![EitherIndexField {
            equality: false,
            inequality: true,
            left: CollectionIndexField {
                path: vec!["v".into()],
                direction: keys::Direction::Descending,
            },
            right: Some(CollectionIndexField {
                path: vec!["v".into()],
                direction: keys::Direction::Ascending,
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
                    lt: Some(WhereValue::Number(40.0)),
                    ..Default::default()
                }),
            );
            map
        },
        vec![],
        vec![EitherIndexField {
            equality: false,
            inequality: true,
            left: CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Descending,
            },
            right: Some(CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Ascending,
            }),
        }]
    );

    // rewrite as a macro
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
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map
        },
        vec![],
        CollectionIndex {
            fields: vec![CollectionIndexField {
                path: vec!["name".into()],
                direction: keys::Direction::Ascending,
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
                    gt: Some(WhereValue::String("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![],
        CollectionIndex {
            fields: vec![CollectionIndexField {
                path: vec!["name".into()],
                direction: keys::Direction::Ascending,
            }],
        }
    );

    test_index_recommendation!(
        test_index_recommendation_name_eq_cal_age_asc_place_desc,
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map
        },
        vec![
            CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Ascending,
            },
            CollectionIndexField {
                path: vec!["place".into()],
                direction: keys::Direction::Descending,
            },
        ],
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Descending,
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
                    gt: Some(WhereValue::String("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![
            CollectionIndexField {
                path: vec!["name".into()],
                direction: keys::Direction::Ascending,
            },
            CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Descending,
            },
        ],
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
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
                    lt: Some(WhereValue::String("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![
            CollectionIndexField {
                path: vec!["name".into()],
                direction: keys::Direction::Ascending,
            },
            CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Descending,
            },
        ],
        "Can only sort by inequality if it's the same direction"
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
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue::Number(20.into())),
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
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["country".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map.insert(
                FieldPath(vec!["country".into()]),
                WhereNode::Equality(WhereValue::String("uk".into())),
            );
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    lt: Some(WhereValue::Number(20.into())),
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
        CollectionIndex {
            fields: vec![CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Ascending,
            }],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue::Number(20.into())),
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
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue::Number(20.into())),
                    ..Default::default()
                }),
            );
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map
        },
        vec![],
        false
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_desc,
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map
        },
        vec![CollectionIndexField {
            path: vec!["age".into()],
            direction: keys::Direction::Descending,
        }],
        true
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_asc,
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map
        },
        vec![CollectionIndexField {
            path: vec!["age".into()],
            direction: keys::Direction::Ascending,
        }],
        true
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_eq_10_place_desc,
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map.insert(
                FieldPath(vec!["age".into()]),
                WhereNode::Equality(WhereValue::Number(10.into())),
            );
            map
        },
        vec![CollectionIndexField {
            path: vec!["place".into()],
            direction: keys::Direction::Descending,
        }],
        true
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_asc_place_asc,
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map
        },
        vec![
            CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Ascending,
            },
            CollectionIndexField {
                path: vec!["place".into()],
                direction: keys::Direction::Ascending,
            }
        ],
        false
    );

    test_index_matching!(
        test_index_matching_name_gt_cal_age_desc_place_asc,
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue::String("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![
            CollectionIndexField {
                path: vec!["name".into()],
                direction: keys::Direction::Ascending,
            },
            CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Descending,
            }
        ],
        true
    );

    test_index_matching!(
        test_index_matching_name_gt_cal_age_asc_place_asc,
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue::String("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![
            CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Ascending,
            },
            CollectionIndexField {
                path: vec!["place".into()],
                direction: keys::Direction::Ascending,
            }
        ],
        false
    );

    test_index_matching!(
        test_index_matching_name_eq_cal_age_asc_place_desc,
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Descending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Equality(WhereValue::String("cal".into())),
            );
            map
        },
        vec![
            CollectionIndexField {
                path: vec!["age".into()],
                direction: keys::Direction::Ascending,
            },
            CollectionIndexField {
                path: vec!["place".into()],
                direction: keys::Direction::Descending,
            }
        ],
        true
    );

    test_index_matching!(
        test_index_matching_name_gt_cal_age_desc_place_asc_2,
        CollectionIndex {
            fields: vec![
                CollectionIndexField {
                    path: vec!["name".into()],
                    direction: keys::Direction::Ascending,
                },
                CollectionIndexField {
                    path: vec!["age".into()],
                    direction: keys::Direction::Descending,
                },
                CollectionIndexField {
                    path: vec!["place".into()],
                    direction: keys::Direction::Ascending,
                }
            ],
        },
        {
            let mut map = HashMap::new();
            map.insert(
                FieldPath(vec!["name".into()]),
                WhereNode::Inequality(WhereInequality {
                    gt: Some(WhereValue::String("cal".into())),
                    ..Default::default()
                }),
            );
            map
        },
        vec![CollectionIndexField {
            path: vec!["name".into()],
            direction: keys::Direction::Descending,
        }],
        true
    );
}
