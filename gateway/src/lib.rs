#![warn(clippy::unwrap_used, clippy::expect_used)]

use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap};
use tracing::debug;

use indexer::{
    collection::validate_collection_record, Converter, FieldWalker, Indexer, IndexerError,
    PathFinder, RecordValue,
};

pub type Result<T> = std::result::Result<T, GatewayError>;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("gateway user error")]
    UserError(#[from] GatewayUserError),

    #[error("collection has no AST")]
    CollectionHasNoAST,

    #[error("collection AST is not a string")]
    CollectionASTNotString,

    #[error("collection not found in AST")]
    CollectionNotFoundInAST,

    #[error("failed to create a v8 string")]
    FailedToCreateV8String,

    #[error("failed to compile script")]
    FailedToCompileScript,

    #[error("indexer error")]
    IndexerError(#[from] indexer::IndexerError),

    #[error("serde_json error")]
    SerdeJsonError(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayUserError {
    #[error("record {record_id:?} was not found in collection {collection_id:?}")]
    RecordNotFound {
        record_id: String,
        collection_id: String,
    },

    #[error("record ID field is not a string")]
    RecordIdNotString,

    #[error("record does not have a collectionId field")]
    RecordCollectionIdNotFound,

    #[error("record field is not an object")]
    RecordFieldNotObject,

    #[error("record ID was modified")]
    RecordIDModified,

    #[error("collection {collection_id:?} was not found")]
    CollectionNotFound { collection_id: String },

    #[error("record id already exists in collection")]
    CollectionIdExists,

    #[error("record does not have an ID field")]
    CollectionRecordIdNotFound,

    #[error("collection mismatch, expected record in collection {expected_collection_id:?}, got {actual_collection_id:?}")]
    CollectionMismatch {
        expected_collection_id: String,
        actual_collection_id: String,
    },

    #[error("method {method_name:?} not found in collection {collection_id:?}")]
    FunctionNotFound {
        method_name: String,
        collection_id: String,
    },

    #[error("incorrect number of arguments, expected {expected:?}, got {actual:?}")]
    FunctionIncorrectNumberOfArguments { expected: usize, actual: usize },

    #[error("invalid argument type for parameter {parameter_name:?}: {source}")]
    FunctionInvalidArgumentType {
        parameter_name: String,
        source: indexer::RecordError,
    },

    #[error("function timed out")]
    FunctionTimedOut,

    #[error("you do not have permission to call this function")]
    UnauthorizedCall,

    #[error("JavaScript exception error: {message}")]
    JavaScriptException { message: String },

    #[error("collection function error: {message}")]
    CollectionFunctionError { message: String },

    #[error("constructor must assign id")]
    ConstructorMustAssignId,

    // #[error("constructor must assign required fields")]
    // ConstructorMustAssignRequired,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FunctionOutput {
    args: Vec<serde_json::Value>,
    instance: serde_json::Value,
    #[serde(rename = "selfdestruct")]
    self_destruct: bool,
}

pub struct Gateway {
    // This is so the consumer of this library can't create a Gateway without calling initialize
    _x: (),
}

pub fn initialize() -> Gateway {
    let platform = v8::new_default_platform(0, false).make_shared();
    v8::V8::initialize_platform(platform);
    v8::V8::initialize();

    Gateway { _x: () }
}

async fn dereference_args(
    indexer: &Indexer,
    collection: &indexer::Collection<'_>,
    args: Vec<RecordValue>,
    auth: Option<&indexer::AuthUser>,
) -> Result<Vec<RecordValue>> {
    let mut dereferenced_args = Vec::<RecordValue>::new();

    for arg in args {
        let (collection, record_id) = match arg {
            RecordValue::RecordReference(r) => (Cow::Borrowed(collection), r.id),
            RecordValue::ForeignRecordReference(fr) => (
                Cow::Owned(
                    indexer
                        .collection(fr.collection_id)
                        .await
                        .map_err(IndexerError::from)?,
                ),
                fr.id,
            ),
            _ => {
                dereferenced_args.push(arg);
                continue;
            }
        };

        let record = collection
            .get(record_id.clone(), auth)
            .await
            .map_err(IndexerError::from)?
            .ok_or_else(|| GatewayUserError::RecordNotFound {
                record_id,
                collection_id: collection.id().to_string(),
            })?;

        dereferenced_args.push(RecordValue::Map(record));
    }

    Ok(dereferenced_args)
}

fn find_record_fields<'a>(
    collection: &'a polylang::stableast::Collection<'a>,
) -> Vec<(Vec<&'a str>, polylang::stableast::Type<'a>)> {
    let mut fields = Vec::new();

    collection.walk_fields(&mut vec![], &mut |path, ty| match ty.type_() {
        ty @ polylang::stableast::Type::Record(_) => fields.push((path.to_vec(), ty.clone())),
        ty @ polylang::stableast::Type::ForeignRecord(_) => {
            fields.push((path.to_vec(), ty.clone()))
        }
        _ => {}
    });

    fields
}

/// Dereferences records/foreign records in record fields.
async fn dereference_fields(
    indexer: &Indexer,
    collection: &indexer::Collection<'_>,
    collection_ast: &polylang::stableast::Collection<'_>,
    mut record: indexer::RecordRoot,
    auth: Option<&indexer::AuthUser>,
) -> Result<indexer::RecordRoot> {
    let record_fields = find_record_fields(collection_ast);

    for (path, type_) in record_fields {
        let map = match record.find_path_mut(&path) {
            Some(RecordValue::Map(m)) => m,
            _ => continue,
        };

        let Some(RecordValue::String(value)) = map.get("id") else {
            return Err(GatewayUserError::CollectionRecordIdNotFound)?;
        };

        let collection = if let polylang::stableast::Type::ForeignRecord(fr) = type_ {
            let Some(RecordValue::String(collection_id)) = map.get("collectionId") else { 
                return Err(GatewayUserError::RecordCollectionIdNotFound)?;
            };

            let foreign_collection_id = collection.namespace().to_string() + "/" + &fr.collection;

            if collection_id != &foreign_collection_id {
                return Err(GatewayUserError::CollectionMismatch {
                    expected_collection_id: foreign_collection_id,
                    actual_collection_id: collection_id.to_string(),
                })?;
            }

            Cow::Owned(
                indexer
                    .collection(foreign_collection_id)
                    .await
                    .map_err(IndexerError::from)?,
            )
        } else {
            Cow::Borrowed(collection)
        };

        let record = collection
            .get(value.to_string(), auth)
            .await
            .map_err(IndexerError::from)?
            .ok_or(GatewayUserError::RecordNotFound {
                record_id: value.to_string(),
                collection_id: collection.id().to_string(),
            })?;

        *map = record;
    }

    Ok(record)
}

/// Turns dereferenced records back into references.
fn reference_records(
    collection: &indexer::Collection,
    collection_ast: &polylang::stableast::Collection,
    record: serde_json::Value,
) -> Result<serde_json::Value> {
    fn visitor(
        collection_namespace: &str,
        collection_ast: &polylang::stableast::Collection,
        path: &mut Vec<String>,
        value: serde_json::Value,
    ) -> Result<serde_json::Value> {
        if let Some(field) = collection_ast.find_field(path) {
            match field.type_() {
                polylang::stableast::Type::Record(_) => {
                    let serde_json::Value::Object(o) = value else {
                        return Err(GatewayUserError::RecordFieldNotObject)?;
                    };

                    let id = o
                        .get("id")
                        .ok_or(GatewayUserError::CollectionRecordIdNotFound)?
                        .as_str()
                        .ok_or(GatewayUserError::RecordIdNotString)?;

                    return Ok(serde_json::json!({ "id": id }));
                }
                polylang::stableast::Type::ForeignRecord(fr) => {
                    let serde_json::Value::Object(o) = value else {
                        return Err(GatewayUserError::RecordFieldNotObject)?;
                    };

                    let id = o
                        .get("id")
                        .ok_or(GatewayUserError::CollectionRecordIdNotFound)?
                        .as_str()
                        .ok_or(GatewayUserError::RecordIdNotString)?;

                    let foreign_collection_id =
                        collection_namespace.to_string() + "/" + &fr.collection;

                    return Ok(
                        serde_json::json!({ "id": id, "collectionId": foreign_collection_id }),
                    );
                }
                polylang::stableast::Type::Array(t) => match t.value.as_ref() {
                    polylang::stableast::Type::ForeignRecord(fr) => {
                        let serde_json::Value::Array(ref arr) = value else {
                            return Err(GatewayUserError::RecordFieldNotObject.into());
                        };

                        let arr = arr.iter().map(|o| {
                            let serde_json::Value::Object(o) = o else {
                                return Err(GatewayUserError::RecordFieldNotObject.into());
                            };

                            let id = o
                                .get("id")
                                .ok_or(GatewayUserError::CollectionRecordIdNotFound)?
                                .as_str()
                                .ok_or(GatewayUserError::RecordIdNotString)?;
    
                            let foreign_collection_id =
                                collection_namespace.to_string() + "/" + &fr.collection;
    
                            Ok(
                                serde_json::json!({ "id": id, "collectionId": foreign_collection_id }),
                            )
                        });

                        return Ok(serde_json::Value::from(arr.collect::<Result<Vec<_>>>()?));
                    }
                    // Record types are not allowed in collection fields, only function parameters
                    polylang::stableast::Type::Record(_) => unreachable!(),
                    _ => {},
                },
                _ => {}
            }
        }

        match value {
            serde_json::Value::Object(o) => {
                let mut new_o = serde_json::Map::new();

                for (k, v) in o.into_iter() {
                    path.push(k.clone());
                    let res = visitor(collection_namespace, collection_ast, path, v)?;
                    path.pop();

                    new_o.insert(k, res);
                }

                Ok(serde_json::Value::Object(new_o))
            }
            serde_json::Value::Array(a) => {
                let mut new_a = Vec::new();

                for (i, v) in a.into_iter().enumerate() {
                    path.push(i.to_string());
                    new_a.push(visitor(collection_namespace, collection_ast, path, v)?);
                    path.pop();
                }

                Ok(serde_json::Value::Array(new_a))
            }
            serde_json::Value::Bool(_) => Ok(value),
            serde_json::Value::Number(_) => Ok(value),
            serde_json::Value::String(_) => Ok(value),
            serde_json::Value::Null => Ok(value),
        }
    }

    let record = visitor(collection.namespace(), collection_ast, &mut vec![], record)?;

    Ok(record)
}

async fn has_permission_to_call(
    indexer: &Indexer,
    collection: &indexer::Collection<'_>,
    collection_ast: &polylang::stableast::Collection<'_>,
    method_ast: &polylang::stableast::Method<'_>,
    record: &indexer::RecordRoot,
    auth: Option<&indexer::AuthUser>,
) -> Result<bool> {
    let is_col_public = collection_ast.attributes.iter().any(|attr| matches!(attr, polylang::stableast::CollectionAttribute::Directive(d) if d.name == "public"));
    // a @public collection is the same as a @read + @call collection
    let is_col_call_any = is_col_public || collection_ast.attributes.iter().any(|attr| matches!(attr, polylang::stableast::CollectionAttribute::Directive(d) if d.name == "call" && d.arguments.is_empty()));

    if method_ast.name == "constructor" {
        return Ok(true);
    }

    let Some(method_call_directive) = method_ast
        .attributes
        .iter()
        .find_map(|attr| match attr {
            polylang::stableast::MethodAttribute::Directive(d) if d.name == "call" => Some(d),
            _ => None,
        }) else {
            // Method doesn't have a @call directive
            return Ok(is_col_call_any);
        };

    // An empty @call directive with no arguments means that the method can be called by anyone.
    if method_call_directive.arguments.is_empty() {
        return Ok(true);
    }

    let callers = method_call_directive
        .arguments
        .iter()
        .filter_map(|a| match a {
            polylang::stableast::DirectiveArgument::FieldReference(fr) => Some(fr.path.clone()),
            polylang::stableast::DirectiveArgument::Unknown => None,
        })
        .collect::<Vec<_>>();

    let Some(auth) = auth else {
        return Ok(false);
    };

    for caller in callers {
        let Some(value) = record.find_path(&caller) else {
            continue;
        };

        #[async_recursion]
        async fn can_call(
            indexer: &Indexer,
            collection: &indexer::Collection<'_>,
            value: &RecordValue,
            auth: &indexer::AuthUser,
        ) -> Result<bool> {
            match value {
                RecordValue::PublicKey(pk) if pk == auth.public_key() => {
                    return Ok(true);
                }
                RecordValue::RecordReference(r) => {
                    let record = collection
                        .get(r.id.clone(), Some(auth))
                        .await
                        .map_err(IndexerError::from)?
                        .ok_or_else(|| GatewayUserError::RecordNotFound {
                            record_id: r.id.clone(),
                            collection_id: collection.id().to_string(),
                        })?;

                    if collection
                        .has_delegate_access(&record, &Some(auth))
                        .await
                        .map_err(IndexerError::from)?
                    {
                        return Ok(true);
                    }
                }
                RecordValue::ForeignRecordReference(fr) => {
                    let collection = indexer
                        .collection(fr.collection_id.clone())
                        .await
                        .map_err(IndexerError::from)?;

                    let record = collection
                        .get(fr.id.clone(), Some(auth))
                        .await
                        .map_err(IndexerError::from)?
                        .ok_or_else(|| GatewayUserError::RecordNotFound {
                            record_id: fr.id.clone(),
                            collection_id: collection.id().to_string(),
                        })?;

                    if collection
                        .has_delegate_access(&record, &Some(auth))
                        .await
                        .map_err(IndexerError::from)?
                    {
                        return Ok(true);
                    }
                }
                RecordValue::Array(arr) => {
                    for v in arr {
                        if can_call(indexer, collection, v, auth).await? {
                            return Ok(true);
                        }
                    }
                }
                _ => {}
            }

            Ok(false)
        }

        if can_call(indexer, collection, value, auth).await? {
            return Ok(true);
        }
    }

    Ok(false)
}

#[derive(Debug, PartialEq)]
pub enum Change {
    Create {
        collection_id: String,
        record_id: String,
        record: indexer::RecordRoot,
    },
    Update {
        collection_id: String,
        record_id: String,
        record: indexer::RecordRoot,
    },
    Delete {
        collection_id: String,
        record_id: String,
    },
}

impl Change {
    pub fn get_path(&self) -> (&String, &String) {
        match self {
            Change::Create {
                collection_id,
                record_id,
                ..
            } => (collection_id, record_id),
            Change::Update {
                collection_id,
                record_id,
                ..
            } => (collection_id, record_id),
            Change::Delete {
                collection_id,
                record_id,
                ..
            } => (collection_id, record_id),
        }
    }
}

fn get_collection_ast<'a>(
    collection_name: &str,
    collection_meta_record: &'a indexer::RecordRoot,
) -> Result<(&'a str, polylang::stableast::Collection<'a>)> {
    let Some(ast) = collection_meta_record.get("ast") else {
        return Err(GatewayError::CollectionHasNoAST)?;
    };

    let RecordValue::String(ast_str) = ast else {
        return Err(GatewayError::CollectionASTNotString)?;
    };

    let ast = serde_json::from_str::<polylang::stableast::Root>(ast_str)?;
    let Some(collection_ast) = ast.0.into_iter().find_map(|a| {
        if let polylang::stableast::RootNode::Collection(col) = a {
            if col.name.as_ref() == collection_name { Some(col) } else { None }
        } else {
            None
        }
    }) else {
        return Err(GatewayError::CollectionNotFoundInAST)?;
    };

    Ok((ast_str, collection_ast))
}

impl Gateway {
    #[tracing::instrument(skip(self, indexer))]
    pub async fn call(
        &self,
        indexer: &Indexer,
        collection_id: String,
        function_name: &str,
        record_id: String,
        args: Vec<serde_json::Value>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<Vec<Change>> {
        let mut changes = Vec::new();
        let collection_collection = indexer
            .collection("Collection".to_string())
            .await
            .map_err(IndexerError::from)?;
        let collection = indexer
            .collection(collection_id.clone())
            .await
            .map_err(IndexerError::from)?;

        let Some(meta) = collection_collection.get(collection.id().to_string(), None).await.map_err(IndexerError::from)? else {
            return Err(GatewayUserError::RecordNotFound {
                record_id: collection.id().to_string(),
                collection_id: collection_collection.id().to_string()
            })?;
        };

        let (collection_ast_json, collection_ast) = get_collection_ast(collection.name().as_str(), &meta)?;
        let collection_polylang_code = meta.get("code").and_then(|v| match v {
            RecordValue::String(s) => Some(s),
            _ => None,
        });

        let js_collection = polylang::js::generate_js_collection(&collection_ast);

        let Some(method) = collection_ast.attributes.iter().find_map(|a| {
            if let polylang::stableast::CollectionAttribute::Method(f) = a {
                if f.name.as_ref() == function_name { Some(f) } else { None }
            } else {
                None
            }
        }) else {
            return Err(GatewayUserError::FunctionNotFound {
                method_name: function_name.to_owned(),
                collection_id: collection.id().to_string()
            })?;
        };

        let instance_record = if function_name == "constructor" {
            indexer::RecordRoot::new()
        } else {
            collection
                .get(record_id.clone(), auth)
                .await
                .map_err(IndexerError::from)?
                .ok_or_else(|| GatewayUserError::RecordNotFound {
                    record_id,
                    collection_id: collection.id().to_string(),
                })?
        };

        if !has_permission_to_call(
            indexer,
            &collection,
            &collection_ast,
            method,
            &instance_record,
            auth,
        )
        .await?
        {
            return Err(GatewayUserError::UnauthorizedCall)?;
        }

        let params = method
            .attributes
            .iter()
            .filter_map(|a| match a {
                polylang::stableast::MethodAttribute::Parameter(p) => Some(p),
                _ => None,
            })
            .collect::<Vec<_>>();
        let required_params_len = params.iter().filter(|p| p.required).count();
        if args.len() < required_params_len {
            return Err(GatewayUserError::FunctionIncorrectNumberOfArguments {
                expected: required_params_len,
                actual: args.len(),
            })?;
        }
        if args.len() > params.len() {
            return Err(GatewayUserError::FunctionIncorrectNumberOfArguments {
                expected: params.len(),
                actual: args.len(),
            })?;
        }

        let args = params
            .iter()
            .zip(args.into_iter())
            .map(|(param, arg)| {
                if !param.required & arg.is_null() {
                    return Ok(RecordValue::Null);
                }

                Converter::convert((&param.type_, arg), &mut vec![param.name.clone()], false).map_err(|e| {
                    GatewayUserError::FunctionInvalidArgumentType {
                        parameter_name: param.name.to_string(),
                        source: e,
                    }
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let dereferenced_args = dereference_args(indexer, &collection, args, auth).await?;
        let instance_record =
            dereference_fields(indexer, &collection, &collection_ast, instance_record, auth)
                .await?;
        let output = {
            let instance_json = indexer::record_to_json(instance_record.clone()).map_err(IndexerError::from)?;
            let args = dereferenced_args
                .clone()
                .into_iter()
                .map(|a| a.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(IndexerError::from)?;

            let output = self.run(
                &collection_id,
                &js_collection.code,
                function_name,
                &instance_json,
                &args,
                auth,
            );

            debug!(
                collection_id = &collection_id,
                collection_ast = collection_ast_json,
                collection_code = &collection_polylang_code,
                function_name = function_name,
                instance = serde_json::to_string(&instance_json).unwrap_or_default(),
                args = serde_json::to_string(&args).unwrap_or_default(),
                auth = serde_json::to_string(&auth).unwrap_or_default(),
                output = serde_json::to_string(&output.as_ref().map_err(|e| e.to_string())).unwrap_or_default(),
                "function output"
            );

            let mut output = output?;

            output.instance = reference_records(&collection, &collection_ast, output.instance)?;

            output
        };
        let instance = indexer::json_to_record(&collection_ast, output.instance, false).map_err(
            |e| match e {
                indexer::RecordError::UserError(indexer::RecordUserError::MissingField {
                    field,
                }) if field == "id" && function_name == "constructor" => {
                    GatewayError::UserError(GatewayUserError::ConstructorMustAssignId)
                }
                e => GatewayError::IndexerError(IndexerError::from(e)),
            },
        )?;

        if function_name != "constructor" && instance.get("id") != instance_record.get("id") {
            return Err(GatewayUserError::RecordIDModified)?;
        }

        let Some(output_instance_id) = instance.get("id") else {
            return Err(GatewayUserError::CollectionRecordIdNotFound)?;
        };
        let RecordValue::String(output_instance_id) = output_instance_id else {
            return Err(GatewayUserError::RecordIdNotString)?;
        };

        let records_to_update = {
            let mut records_to_update = vec![];

            let dereferenced_args_len = dereferenced_args.len();
            for (i, (output_arg, input_arg)) in output
                .args
                .into_iter()
                .zip(dereferenced_args.into_iter())
                .enumerate()
            {
                let input_arg =
                    serde_json::Value::try_from(input_arg).map_err(IndexerError::from)?;
                if output_arg == input_arg {
                    continue;
                }

                let Some(parameter) = method
                    .attributes
                    .iter()
                    .filter_map(|a| {
                        if let polylang::stableast::MethodAttribute::Parameter(p) = a {
                            Some(p)
                        } else {
                            None
                        }
                    })
                    .nth(i) else {
                    return Err(GatewayUserError::FunctionIncorrectNumberOfArguments {
                        expected: method
                            .attributes
                            .iter()
                            .filter_map(|a| {
                                if let polylang::stableast::MethodAttribute::Parameter(p) = a {
                                    Some(p)
                                } else {
                                    None
                                }
                            })
                            .count(),
                        actual: dereferenced_args_len,
                    })?;
                };

                let Some((collection_id, record)) = (match &parameter.type_ {
                    polylang::stableast::Type::Record(_) => {
                        let Some(output_id) = output_arg.get("id") else {
                            return Err(GatewayUserError::CollectionRecordIdNotFound)?;
                        };

                        if Some(output_id) != input_arg.get("id") {
                            return Err(GatewayUserError::RecordIDModified)?;
                        }

                        Some((collection.id().to_owned(), indexer::json_to_record(&collection_ast, output_arg, false).map_err(IndexerError::from)?))
                    },
                    polylang::stableast::Type::ForeignRecord(fr) => {
                        let Some(output_id) = output_arg.get("id") else {
                            return Err(GatewayUserError::CollectionRecordIdNotFound)?;
                        };

                        if Some(output_id) != input_arg.get("id") {
                            return Err(GatewayUserError::RecordIDModified)?;
                        }

                        let collection_id = collection.namespace().to_string() + "/" + &fr.collection;

                        let Some(collection_meta) = collection_collection.get(collection_id.clone(), auth).await.map_err(IndexerError::from)? else {
                            return Err(GatewayUserError::CollectionNotFound {
                                collection_id: collection_id.clone(),
                            })?;
                        };

                        let (_, ast) = get_collection_ast(fr.collection.as_ref(), &collection_meta)?;

                        Some((collection_id, indexer::json_to_record(&ast, output_arg, false).map_err(IndexerError::from)?))
                    }
                    _ => None,
                }) else {
                    continue;
                };

                records_to_update.push((collection_id, record));
            }

            records_to_update
        };

        if function_name == "constructor" {
            if collection
                .get_without_auth_check(output_instance_id.to_string())
                .await
                .map_err(IndexerError::from)?
                .is_some()
            {
                return Err(GatewayUserError::CollectionIdExists)?;
            }

            if collection_id == "Collection" {
                validate_collection_record(&instance).map_err(IndexerError::from)?;
            }

            changes.push(Change::Create {
                collection_id,
                record_id: output_instance_id.to_string(),
                record: instance,
            });
        } else if output.self_destruct {
            changes.push(Change::Delete {
                collection_id,
                record_id: output_instance_id.to_string(),
            });
        } else {
            changes.push(Change::Update {
                collection_id,
                record_id: output_instance_id.to_string(),
                record: instance,
            });
        }

        for (collection_id, record) in records_to_update {
            let Some(id) = record.get("id") else {
                return Err(GatewayUserError::CollectionRecordIdNotFound)?;
            };

            let RecordValue::String(id) = id else {
                return Err(GatewayUserError::RecordIdNotString)?;
            };

            changes.push(Change::Update {
                collection_id,
                record_id: id.to_string(),
                record,
            });
        }

        Ok(changes)
    }

    fn run(
        &self,
        collection_id: &str,
        collection_code: &str,
        function_name: &str,
        instance: &serde_json::Value,
        args: &[serde_json::Value],
        auth: Option<&indexer::AuthUser>,
    ) -> Result<FunctionOutput> {
        let mut isolate = v8::Isolate::new(Default::default());
        let terminate_handle = isolate.thread_safe_handle();

        // If the script takes more than 5 seconds to run, terminate it.
        let terminated = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (finished_tx, finished_rx) = std::sync::mpsc::channel::<()>();
        let terminated_clone = terminated.clone();
        let script_termination = std::thread::spawn(move || {
            let timeout = std::time::Duration::from_secs(5);
            if finished_rx.recv_timeout(timeout).is_err() {
                terminated_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                terminate_handle.terminate_execution();
            }
        });

        let mut scope = v8::HandleScope::new(&mut isolate);

        let global = v8::ObjectTemplate::new(&mut scope);

        if collection_id == "Collection" {
            global.set(
                v8::String::new(&mut scope, "parse").ok_or(GatewayError::FailedToCreateV8String)?.into(),
                v8::FunctionTemplate::new(
                    &mut scope,
                    |scope: &mut v8::HandleScope,
                     args: v8::FunctionCallbackArguments,
                     mut retval: v8::ReturnValue| {
                        let mut get_string_arg = |i: i32| {
                            let Some(arg) = args
                                .get(i)
                                .to_string(scope) else {
                                #[allow(clippy::unwrap_used)] // we can't recover from this
                                let error_msg = v8::String::new(scope, "Argument is not a string").unwrap();
                                let exception = v8::Exception::error(scope, error_msg);
                                scope.throw_exception(exception);
                                return None;
                            };

                            Some(arg.to_rust_string_lossy(scope))
                        };

                        let Some(code) = get_string_arg(0) else { return; };
                        let Some(collection_id) = get_string_arg(1) else { return; };

                        let namespace = {
                            let mut parts = collection_id.split('/').collect::<Vec<_>>();
                            if parts.len() > 1 {
                                parts.pop();
                            }
                            parts.join("/")
                        };

                        let mut program = None;
                        let (_, stable_ast) = match polylang::parse(&code, &namespace, &mut program)
                        {
                            Ok(x) => x,
                            Err(e) => {
                                #[allow(clippy::unwrap_used)] // we can't recover from this
                                let error_msg = v8::String::new(scope, &e.message).unwrap();
                                let exception = v8::Exception::error(scope, error_msg);
                                scope.throw_exception(exception);
                                return;
                            }
                        };
                        let json = match serde_json::to_string(&stable_ast) {
                            Ok(json) => json,
                            Err(e) => {
                                #[allow(clippy::unwrap_used)] // we can't recover from this
                                let error_msg = v8::String::new(scope, &format!("{e:?}")).unwrap();
                                let exception = v8::Exception::error(scope, error_msg);
                                scope.throw_exception(exception);
                                return;
                            }
                        };

                        #[allow(clippy::unwrap_used)] // we can't recover from this
                        retval.set(v8::String::new(scope, &json).unwrap().into());
                    },
                )
                .into(),
            );
        }

        global.set(
            v8::String::new(&mut scope, "$$__publicKeyToHex")
                .ok_or(GatewayError::FailedToCreateV8String)?
                .into(),
            v8::FunctionTemplate::new(
                &mut scope,
                |scope: &mut v8::HandleScope,
                 args: v8::FunctionCallbackArguments,
                 mut retval: v8::ReturnValue| {
                    let Some(public_key_json) = args
                        .get(0)
                        .to_string(scope) else {
                        #[allow(clippy::unwrap_used)] // we can't recover from this
                        let error = v8::String::new(scope, "Argument is not a string").unwrap();
                        let exception = v8::Exception::error(scope, error);
                        scope.throw_exception(exception);
                        return;
                    };
                    let public_key_json = public_key_json.to_rust_string_lossy(scope);

                    let public_key =
                        match serde_json::from_str::<indexer::PublicKey>(&public_key_json) {
                            Ok(pk) => pk,
                            Err(e) => {
                                #[allow(clippy::unwrap_used)] // we can't recover from this
                                let error = v8::String::new(scope, &format!("{e:?}")).unwrap();
                                let exception = v8::Exception::error(scope, error);
                                scope.throw_exception(exception);
                                return;
                            }
                        };

                    let hex = match public_key.to_hex() {
                        Ok(hex) => hex,
                        Err(e) => {
                            #[allow(clippy::unwrap_used)] // we can't recover from this
                            let error = v8::String::new(scope, &format!("{e:?}")).unwrap();
                            let exception = v8::Exception::error(scope, error);
                            scope.throw_exception(exception);
                            return;
                        }
                    };

                    #[allow(clippy::unwrap_used)] // we can't recover from this
                    retval.set(v8::String::new(scope, &hex).unwrap().into());
                },
            )
            .into(),
        );

        global.set(
            v8::String::new(&mut scope, "instanceJSON")
                .ok_or(GatewayError::FailedToCreateV8String)?
                .into(),
            v8::String::new(&mut scope, &serde_json::to_string(instance)?)
                .ok_or(GatewayError::FailedToCreateV8String)?
                .into(),
        );

        global.set(
            v8::String::new(&mut scope, "authJSON")
                .ok_or(GatewayError::FailedToCreateV8String)?
                .into(),
            v8::String::new(
                &mut scope,
                &serde_json::to_string(&{
                    if let Some(auth) = auth {
                        HashMap::from([("publicKey".to_string(), auth.public_key().clone())])
                    } else {
                        HashMap::new()
                    }
                })?,
            )
            .ok_or(GatewayError::FailedToCreateV8String)?
            .into(),
        );

        global.set(
            v8::String::new(&mut scope, "argsJSON")
                .ok_or(GatewayError::FailedToCreateV8String)?
                .into(),
            v8::String::new(&mut scope, &serde_json::to_string(args)?)
                .ok_or(GatewayError::FailedToCreateV8String)?
                .into(),
        );

        let context = v8::Context::new_from_template(&mut scope, global);
        let mut scope = v8::ContextScope::new(&mut scope, context);

        let code = r#"
            // To prevent recursion, we limit (shared counter) the number of calls to each function
            let calls = 0;
            function limitMethods(obj) {
                for (const key in obj) {
                    if (typeof obj[key] === "function") {
                        const originalFn = obj[key];
                        obj[key] = function replaced(...args) {
                            if (calls >= 100) {
                                throw new Error("call limit exceeded");
                            }

                            calls++;
                            return originalFn.bind(this)(...args);
                        };
                    }
                }
            }

            // To allow comparison using "==", we intern all public keys.
            // We also freeze them to prevent modification.
            // You can only replace entire objects, you can't change their fields.
            const uniquePublicKeys = {};
            function internPublicKeys(obj) {
                if (!obj || typeof obj !== "object") return obj;

                if (obj["kty"] === "EC" && obj["crv"] === "secp256k1") {
                    const json = JSON.stringify(Object.entries(obj).sort((a, b) => a[0] > b[0] ? -1 : 1));
                    if (uniquePublicKeys[json]) {
                        return uniquePublicKeys[json];
                    }

                    obj["toHex"] = function () {
                        return $$__publicKeyToHex(JSON.stringify(this));
                    };
                    Object.freeze(obj);
                    uniquePublicKeys[json] = obj;
                } else {
                    for (const key in obj) {
                        obj[key] = internPublicKeys(obj[key]);
                    }
                }

                return obj;
            }

            // Turns previously dereferenced records into references.
            // A record reference is { id: "record-id" }.
            const dereferencedRecordSymbol = Symbol("dereferenced-record");
            function turnRecordsToReferences(obj) {
                if (!obj || typeof obj !== "object") return obj;

                if (obj[dereferencedRecordSymbol]) {
                    return { id: obj.id };
                }

                for (const key in obj) {
                    obj[key] = turnRecordsToReferences(obj[key]);
                }

                return obj;
            }

            const $$__instance = JSON.parse(instanceJSON);
            $FUNCTION_CODE
            limitMethods($$__instance);
            internPublicKeys($$__instance);
            function error(str) {{
                    throw new Error("$$__USER_ERROR:" + str);
            }}
            ctx = JSON.parse(authJSON);
            internPublicKeys(ctx);
            $auth = ctx;
            args = JSON.parse(argsJSON);
            for (const i in args) {
                if (args[i] && typeof args[i] === "object" && args[i].$$__type === "record") {
                    args[i] = eval(args[i].$$__fn)(args[i].$$__data);
                    limitMethods(args[i]);
                    args[i][dereferencedRecordSymbol] = true;
                }

                args[i] = internPublicKeys(args[i]);
            }
            $$__selfdestruct = false;
            const selfdestruct = () => { $$__selfdestruct = true };
            instance.$FUNCTION_NAME($FUNCTION_ARGS);
            turnRecordsToReferences(instance);
            
            JSON.stringify({
                args,
                instance,
                selfdestruct: $$__selfdestruct,
            });
        "#.replace("$FUNCTION_CODE", collection_code)
            .replace("$FUNCTION_NAME", function_name)
            .replace("$FUNCTION_ARGS", &args.iter().enumerate().map(|(i, _)| format!("args[{i}]")).collect::<Vec<_>>().join(", "));

        let Some(code) = v8::String::new(&mut scope, &code) else {
            return Err(GatewayError::FailedToCreateV8String);
        };

        let mut try_catch = v8::TryCatch::new(&mut scope);
        let script = v8::Script::compile(&mut try_catch, code, None)
            .ok_or(GatewayError::FailedToCompileScript)?;
        let result = script.run(&mut try_catch);
        let _ = finished_tx.send(());
        #[allow(clippy::unwrap_used)] // This will never panic
        script_termination.join().unwrap();

        if terminated.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(GatewayUserError::FunctionTimedOut.into());
        }

        match (result, try_catch.exception()) {
            (_, Some(exception)) => {
                let msg = (|| {
                    // Extract `message` property from exception object
                    let message_str = v8::String::new(&mut try_catch, "message")
                        .ok_or(GatewayError::FailedToCreateV8String)?;

                    if let Some(object) = exception.to_object(&mut try_catch) {
                        if let Some(message) = object.get(&mut try_catch, message_str.into()) {
                            return Ok::<_, GatewayError>(message);
                        }
                    }

                    Ok(exception)
                })()?;

                let exception_string = msg
                    .to_string(&mut try_catch)
                    .ok_or(GatewayError::FailedToCreateV8String)?
                    .to_rust_string_lossy(&mut try_catch);

                let s = exception_string.replace("$$__USER_ERROR:", "");
                if exception_string == s {
                    Err(GatewayUserError::JavaScriptException {
                        message: exception_string,
                    }
                    .into())
                } else {
                    Err(GatewayUserError::CollectionFunctionError { message: s }.into())
                }
            }
            (Some(result), _) => {
                let result = result.to_rust_string_lossy(&mut try_catch);
                Ok(serde_json::from_str::<FunctionOutput>(&result)?)
            }
            (None, None) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Deref, DerefMut};

    use super::*;

    pub(crate) struct TestIndexer(Option<Indexer>);

    impl Default for TestIndexer {
        fn default() -> Self {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!(
                "test-gateway-rocksdb-store-{}",
                rand::random::<u32>()
            ));

            Self(Some(Indexer::new(path).unwrap()))
        }
    }

    impl Drop for TestIndexer {
        fn drop(&mut self) {
            if let Some(indexer) = self.0.take() {
                indexer.destroy().unwrap();
            }
        }
    }

    impl Deref for TestIndexer {
        type Target = Indexer;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl DerefMut for TestIndexer {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }

    #[tokio::test]
    async fn it_works() {
        let user_col_code = r#"
            @public
            collection User {
                id: string;
                name: string;

                changeName (newName: string) {
                    this.name = newName;
                }
            }
        "#;
        let mut program = None;
        let (_, stable_ast) = polylang::parse(user_col_code, "ns", &mut program).unwrap();

        let indexer = TestIndexer::default();

        let collection_collection = indexer.collection("Collection".to_string()).await.unwrap();
        collection_collection
            .set(
                "ns/User".to_string(),
                &[
                    ("id".into(), indexer::RecordValue::String("ns/User".into())),
                    (
                        "ast".into(),
                        indexer::RecordValue::String(serde_json::to_string(&stable_ast).unwrap()),
                    ),
                ]
                .into(),
            )
            .await
            .unwrap();

        indexer.commit().await.unwrap();

        let user_collection = indexer.collection("ns/User".to_string()).await.unwrap();
        user_collection
            .set(
                "1".to_string(),
                &[
                    ("id".into(), indexer::RecordValue::String("1".into())),
                    ("name".into(), indexer::RecordValue::String("John".into())),
                ]
                .into(),
            )
            .await
            .unwrap();

        indexer.commit().await.unwrap();

        let gateway = initialize();
        let changes = gateway
            .call(
                &indexer,
                "ns/User".to_string(),
                "changeName",
                "1".to_string(),
                vec!["Tim".into()],
                None,
            )
            .await
            .unwrap();

        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0],
            Change::Update {
                collection_id: "ns/User".to_string(),
                record_id: "1".to_string(),
                record: HashMap::from([
                    ("id".into(), indexer::RecordValue::String("1".into())),
                    ("name".into(), indexer::RecordValue::String("Tim".into()))
                ])
            }
        );
    }
}
