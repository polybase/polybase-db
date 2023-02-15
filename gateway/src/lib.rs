use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap};

use indexer::{FieldWalker, IndexValue, Indexer, PathFinder, RecordReference, RecordValue};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FunctionOutput {
    args: Vec<RecordValue>,
    instance: indexer::RecordRoot,
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

fn type_check_single_value(
    parameter_type: &polylang::stableast::Type,
    value: &RecordValue,
) -> Result<(), String> {
    match (parameter_type, value) {
        (polylang::stableast::Type::Primitive(p), arg) => match (&p.value, arg) {
            (
                polylang::stableast::PrimitiveType::String,
                RecordValue::IndexValue(IndexValue::String(_)),
            ) => Ok(()),
            (
                polylang::stableast::PrimitiveType::Number,
                RecordValue::IndexValue(IndexValue::Number(_)),
            ) => Ok(()),
            (
                polylang::stableast::PrimitiveType::Boolean,
                RecordValue::IndexValue(IndexValue::Boolean(_)),
            ) => Ok(()),
            _ => Err(format!("Expected {parameter_type:?}, but got {value:?}")),
        },
        (polylang::stableast::Type::Array(pt), RecordValue::Array(at)) => {
            for (i, v) in at.iter().enumerate() {
                type_check_single_value(&pt.value, v)
                    .map_err(|e| format!("Array element {i} does not match parameter type: {e}"))?;
            }

            Ok(())
        }
        (polylang::stableast::Type::Map(pt), RecordValue::Map(at)) => {
            for (k, v) in at.iter() {
                type_check_single_value(&pt.value, v)
                    .map_err(|e| format!("Map element {k} does not match parameter type: {e}"))?;
            }

            Ok(())
        }
        (polylang::stableast::Type::Object(_), RecordValue::Map(_)) => Ok(()),
        (polylang::stableast::Type::Record(_), RecordValue::Map(m)) => {
            // `m` must be a { id: string }

            let Some(id) = m.get("id") else { return Err(
                "Record does not have an id field".to_string());
            };

            type_check_single_value(
                &polylang::stableast::Type::Primitive(polylang::stableast::Primitive {
                    value: polylang::stableast::PrimitiveType::String,
                }),
                id,
            )
            .map_err(|e| format!("Record id does not match parameter type: {e}"))?;

            if m.len() != 1 {
                return Err(format!("Record has {} fields, but expected 1", m.len()));
            }

            Ok(())
        }
        (polylang::stableast::Type::ForeignRecord(_), RecordValue::Map(m)) => {
            // `m` must be a { id: string, collectionId: string }

            let Some(id) = m.get("id") else { return Err(
                "Record does not have an id field".to_string());
            };

            type_check_single_value(
                &polylang::stableast::Type::Primitive(polylang::stableast::Primitive {
                    value: polylang::stableast::PrimitiveType::String,
                }),
                id,
            )
            .map_err(|e| format!("Record id does not match parameter type: {e}"))?;

            let Some(collection_id) = m.get("collectionId") else { return Err(
                "Record does not have a collectionId field".to_string());
            };

            type_check_single_value(
                &polylang::stableast::Type::Primitive(polylang::stableast::Primitive {
                    value: polylang::stableast::PrimitiveType::String,
                }),
                collection_id,
            )
            .map_err(|e| format!("Record collectionId does not match parameter type: {e}"))?;

            if m.len() != 2 {
                return Err(format!("Record has {} fields, but expected 2", m.len(),));
            }

            Ok(())
        }
        (
            polylang::stableast::Type::PublicKey(_),
            RecordValue::IndexValue(IndexValue::PublicKey(_)),
        ) => Ok(()),
        (polylang::stableast::Type::Unknown, _) => Ok(()),
        _ => Err(format!("Expected {parameter_type:?}, but got {value:?}")),
    }
}

fn type_check_args(
    method: &polylang::stableast::Method,
    args: &[RecordValue],
) -> Result<(), String> {
    for (i, param) in method
        .attributes
        .iter()
        .filter_map(|a| {
            if let polylang::stableast::MethodAttribute::Parameter(p) = a {
                Some(p)
            } else {
                None
            }
        })
        .enumerate()
    {
        let Some(arg) = args.get(i) else { return Err(
            format!(
                "Method {} expects {} arguments, but only {} were provided",
                method.name,
                method.attributes.iter().filter(|a| {
                    matches!(a, polylang::stableast::MethodAttribute::Parameter(_))
                }).count(),
                args.len()
            ));
        };

        type_check_single_value(&param.type_, arg).map_err(|e| {
            format!(
                "Argument {} to method {} does not match parameter type: {}",
                i, method.name, e
            )
        })?;
    }

    Ok(())
}

fn dereference_args(
    indexer: &Indexer,
    collection: &indexer::Collection,
    method: &polylang::stableast::Method,
    args: Vec<RecordValue>,
    auth: Option<&indexer::AuthUser>,
) -> Result<Vec<RecordValue>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut dereferenced_args = Vec::<RecordValue>::new();

    let parameters = method
        .attributes
        .iter()
        .filter_map(|a| {
            if let polylang::stableast::MethodAttribute::Parameter(p) = a {
                Some(p)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if parameters.len() != args.len() {
        return Err(format!(
            "Method {} expects {} arguments, but only {} were provided",
            method.name,
            parameters.len(),
            args.len()
        )
        .into());
    }

    for (arg, param) in args.into_iter().zip(parameters.into_iter()) {
        match &param.type_ {
            polylang::stableast::Type::Record(_) => {
                let record_id = match arg {
                    RecordValue::Map(m) => {
                        let Some(id) = m.get("id") else { return Err(
                            "Record does not have an id field".to_string().into());
                        };

                        match id {
                            RecordValue::IndexValue(IndexValue::String(s)) => s.to_string(),
                            _ => return Err("Record id is not a string".to_string().into()),
                        }
                    }
                    _ => return Err("Record is not a map".to_string().into()),
                };

                let Some(record) = collection.get(record_id.clone(), auth)? else {
                    return Err(format!("Record {record_id} not found").into());
                };

                dereferenced_args.push(RecordValue::Map(record));
            }
            polylang::stableast::Type::ForeignRecord(fr) => {
                let foreign_collection_id =
                    collection.namespace().to_string() + "/" + &fr.collection;

                let (collection_id, record_id) = match arg {
                    RecordValue::Map(m) => {
                        let Some(id) = m.get("id") else { return Err(
                            "Record does not have an id field".to_string().into());
                        };

                        let Some(collection_id) = m.get("collectionId") else { return Err(
                            "Record does not have a collectionId field".to_string().into());
                        };

                        let id = match id {
                            RecordValue::IndexValue(IndexValue::String(s)) => s.to_string(),
                            _ => return Err("Record id is not a string".to_string().into()),
                        };

                        let collection_id = match collection_id {
                            RecordValue::IndexValue(IndexValue::String(s)) => s.to_string(),
                            _ => {
                                return Err("Record collectionId is not a string"
                                    .to_string()
                                    .into())
                            }
                        };

                        (collection_id, id)
                    }
                    _ => return Err("Record is not a map".to_string().into()),
                };

                if collection_id != foreign_collection_id {
                    return Err(format!(
                        "Collection mismatch, expected record in collection {}",
                        &foreign_collection_id
                    )
                    .into());
                }

                let foreign_collection = indexer.collection(foreign_collection_id.clone())?;
                let record = foreign_collection
                    .get(record_id.clone(), auth)?
                    .ok_or_else(|| {
                        format!(
                            "Record {} not found in collection {}",
                            record_id, &foreign_collection_id
                        )
                    })?;

                dereferenced_args.push(RecordValue::Map(record));
            }
            _ => dereferenced_args.push(arg),
        }
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
fn dereference_fields(
    indexer: &Indexer,
    collection: &indexer::Collection,
    collection_ast: &polylang::stableast::Collection,
    record: indexer::RecordRoot,
    auth: Option<&indexer::AuthUser>,
) -> Result<indexer::RecordRoot, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let record_fields = find_record_fields(collection_ast);

    let mut rv = RecordValue::Map(record);
    rv.walk_maps_mut::<Box<dyn std::error::Error + Send + Sync + 'static>>(&mut vec![], &mut |path, map| {
        let Some((_, type_)) = record_fields.iter().find(|(p, _)| *p == path) else { return Ok(()); };

        let Some(RecordValue::IndexValue(IndexValue::String(value))) = map.get("id") else { return Err(
            "Record does not have an id field".to_string().into());
        };

        let collection = if let polylang::stableast::Type::ForeignRecord(fr) = type_ {
            let Some(RecordValue::IndexValue(IndexValue::String(collection_id))) =
                map.get("collectionId") else { return Err(
                    "Record does not have a collectionId field".to_string().into());
                };

            let foreign_collection_id =
                collection.namespace().to_string() + "/" + &fr.collection;

            if collection_id != &foreign_collection_id {
                return Err(format!(
                    "Collection mismatch, expected record in collection {}",
                    &foreign_collection_id
                )
                .into());
            }

            Cow::Owned(indexer.collection(foreign_collection_id)?)
        } else {
            Cow::Borrowed(collection)
        };

        let record = collection
            .get(value.to_string(), auth)?
            .ok_or(format!("Record {} not found in collection {}", value, collection.id()))?;

        *map = record;

        Ok(())
    })?;

    match rv {
        RecordValue::Map(m) => Ok(m),
        _ => unreachable!(),
    }
}

/// Turns dereferenced records back into references.
fn reference_records(
    collection: &indexer::Collection,
    collection_ast: &polylang::stableast::Collection,
    record: &mut indexer::RecordRoot,
) -> Result<indexer::RecordRoot, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let record_fields = find_record_fields(collection_ast);

    let mut rv = RecordValue::Map(record.clone());
    rv.walk_maps_mut::<Box<dyn std::error::Error + Send + Sync + 'static>>(&mut vec![], &mut |path, map| {
        let Some((_, type_)) = record_fields.iter().find(|(p, _)| *p == path) else { return Ok(()); };

        let Some(RecordValue::IndexValue(IndexValue::String(value))) = map.get("id") else { return Err(
            "Record does not have an id field".to_string().into());
        };

        let collection = if let polylang::stableast::Type::ForeignRecord(fr) = type_ {
            let foreign_collection_id =
                collection.namespace().to_string() + "/" + &fr.collection;

            Some(foreign_collection_id)
        } else {
            None
        };

        let mut reference = indexer::RecordRoot::new();
        reference.insert("id".to_string(), RecordValue::IndexValue(IndexValue::String(value.to_string())));
        if let Some(collection_id) = collection {
            reference.insert("collectionId".to_string(), RecordValue::IndexValue(IndexValue::String(collection_id)));
        }

        *map = reference;

        Ok(())
    })?;

    match rv {
        RecordValue::Map(m) => Ok(m),
        _ => unreachable!(),
    }
}

fn has_permission_to_call(
    indexer: &Indexer,
    collection: &indexer::Collection,
    collection_ast: &polylang::stableast::Collection,
    method_ast: &polylang::stableast::Method,
    record: &indexer::RecordRoot,
    auth: Option<&indexer::AuthUser>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let is_col_public = collection_ast.attributes.iter().any(|attr| matches!(attr, polylang::stableast::CollectionAttribute::Directive(d) if d.name == "public"));
    if is_col_public {
        return Ok(true);
    }

    if method_ast.name == "constructor" {
        return Ok(true);
    }

    let Some(callers) = method_ast.attributes.iter().find_map(|attr| match attr {
        polylang::stableast::MethodAttribute::Directive(d) if d.name == "call" => Some(
            d.arguments
                .iter()
                .filter_map(|a| match a {
                    polylang::stableast::DirectiveArgument::FieldReference(fr) => {
                        Some(fr.path.clone())
                    }
                    polylang::stableast::DirectiveArgument::Unknown => None,
                })
                .collect::<Vec<_>>(),
        ),
        _ => None,
    }) else {
        return Ok(false);
    };

    let Some(auth) = auth else {
        return Ok(false);
    };

    for caller in callers {
        let Some(value) = record.find_path(&caller) else {
            continue;
        };

        match value {
            RecordValue::IndexValue(indexer::IndexValue::PublicKey(pk))
                if pk == auth.public_key() =>
            {
                return Ok(true);
            }
            RecordValue::Map(_) => {
                let Ok(record_ref) = RecordReference::try_from(value) else {
                    continue;
                };

                let collection = match record_ref.collection_id {
                    Some(collection_id) => Cow::Owned(indexer.collection(collection_id)?),
                    None => Cow::Borrowed(collection),
                };

                let record = collection
                    .get(record_ref.id.clone(), Some(auth))?
                    .ok_or_else(|| {
                        format!(
                            "Record {} not found in collection {}",
                            record_ref.id,
                            collection.id()
                        )
                    })?;

                if collection.has_delegate_access(&record, &Some(auth))? {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }

    Ok(false)
}

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
        record_id: String,
    },
}

impl Gateway {
    pub fn call(
        &self,
        indexer: &Indexer,
        collection_id: String,
        function_name: &str,
        record_id: String,
        args: Vec<RecordValue>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<Vec<Change>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut changes = Vec::new();
        let collection_collection = indexer.collection("Collection".to_string())?;
        let collection = indexer.collection(collection_id.clone())?;

        let Some(meta) = collection_collection.get(collection.id().to_string(), None)? else {
            return Err("Collection not found".into());
        };

        let Some(ast) = meta.get("ast") else {
            return Err("Collection has no AST".into());
        };

        let RecordValue::IndexValue(IndexValue::String(ast_str)) = ast else {
            return Err("Collection AST is not a string".into());
        };

        let ast = serde_json::from_str::<polylang::stableast::Root>(ast_str)?;
        let Some(collection_ast) = ast.0.iter().find_map(|a| {
            if let polylang::stableast::RootNode::Collection(col) = a {
                if col.name.as_ref() == collection.name() { Some(col) } else { None }
            } else {
                None
            }
        }) else {
            return Err("Collection not found in AST".into());
        };

        let js_collection = polylang::js::generate_js_collection(collection_ast);

        let Some(method) = collection_ast.attributes.iter().find_map(|a| {
            if let polylang::stableast::CollectionAttribute::Method(f) = a {
                if f.name.as_ref() == function_name { Some(f) } else { None }
            } else {
                None
            }
        }) else {
            return Err("Method not found in Collection AST".into());
        };

        let instance_record = if function_name == "constructor" {
            indexer::RecordRoot::new()
        } else {
            collection.get(record_id.clone(), auth)?.ok_or_else(|| {
                format!(
                    "Record {} not found in collection {}",
                    record_id,
                    collection.name()
                )
            })?
        };

        if !has_permission_to_call(
            indexer,
            &collection,
            collection_ast,
            method,
            &instance_record,
            auth,
        )? {
            return Err("You do not have permission to call this function".into());
        }

        type_check_args(method, &args)?;

        let dereferenced_args = dereference_args(indexer, &collection, method, args, auth)?;
        let instance_record =
            dereference_fields(indexer, &collection, collection_ast, instance_record, auth)?;
        let mut output = self.run(
            &collection_id,
            &js_collection.code,
            function_name,
            &instance_record,
            &dereferenced_args,
            auth,
        )?;
        output.instance = reference_records(&collection, collection_ast, &mut output.instance)?;

        if function_name != "constructor" && output.instance.get("id") != instance_record.get("id")
        {
            return Err("Record id was modified".into());
        }

        let Some(output_instance_id) = output.instance.get("id") else {
            return Err("Record id was not returned".into());
        };
        let RecordValue::IndexValue(IndexValue::String(output_instance_id)) =
            output_instance_id else {
            return Err("Record id was not a string".into());
        };

        let records_to_update = {
            let mut records_to_update = vec![];

            for (i, (output_arg, input_arg)) in
                output.args.iter().zip(dereferenced_args.iter()).enumerate()
            {
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
                    return Err(format!(
                        "Method {} has {} parameters, but only {} were provided",
                        method.name,
                        method.attributes.len(),
                        dereferenced_args.len()
                    ).into());
                };

                match parameter.type_ {
                    polylang::stableast::Type::Record(_)
                    | polylang::stableast::Type::ForeignRecord(_) => {
                        match (output_arg, input_arg) {
                            (RecordValue::Map(output_map), RecordValue::Map(input_map)) => {
                                let Some(output_id) = output_map.get("id") else {
                                    return Err("Record id is missing".into());
                                };
                                let Some(input_id) = input_map.get("id") else {
                                    return Err("Record id is missing".into());
                                };

                                if output_id != input_id {
                                    return Err("Record id was modified".into());
                                }

                                records_to_update.push(output_arg);
                            }
                            _ => {
                                return Err("Record input and output argument must be a map".into());
                            }
                        }
                    }
                    _ => {}
                }
            }

            records_to_update
        };

        if function_name == "constructor" {
            if collection
                .get(output_instance_id.to_string(), None)?
                .is_some()
            {
                return Err("Record id already exists".into());
            }

            changes.push(Change::Create {
                collection_id,
                record_id: output_instance_id.to_string(),
                record: output.instance,
            });
        } else {
            changes.push(Change::Update {
                collection_id,
                record_id: output_instance_id.to_string(),
                record: output.instance,
            });
        }

        for record in records_to_update {
            let RecordValue::Map(m) = record else {
                return Err("Record output argument must be a map".into());
            };

            let Some(id) = m.get("id") else {
                return Err("Record id is missing".into());
            };

            let RecordValue::IndexValue(IndexValue::String(id)) = id else {
                return Err("Record id is not a string".into());
            };

            changes.push(Change::Update {
                collection_id: todo!("get the collection id before dereferencing"),
                record_id: id.to_string(),
                record: m.clone(),
            });
        }

        Ok(changes)
    }

    fn run(
        &self,
        collection_id: &str,
        collection_code: &str,
        function_name: &str,
        instance: &indexer::RecordRoot,
        args: &[RecordValue],
        auth: Option<&indexer::AuthUser>,
    ) -> Result<FunctionOutput, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut isolate = v8::Isolate::new(Default::default());
        let mut scope = v8::HandleScope::new(&mut isolate);

        let global = v8::ObjectTemplate::new(&mut scope);

        if collection_id == "Collection" {
            global.set(
                v8::String::new(&mut scope, "parse").unwrap().into(),
                v8::FunctionTemplate::new(
                    &mut scope,
                    |scope: &mut v8::HandleScope,
                     args: v8::FunctionCallbackArguments,
                     mut retval: v8::ReturnValue| {
                        let code = args
                            .get(0)
                            .to_string(scope)
                            .unwrap()
                            .to_rust_string_lossy(scope);
                        let collection_id = args
                            .get(1)
                            .to_string(scope)
                            .unwrap()
                            .to_rust_string_lossy(scope);

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
                                let error = v8::String::new(scope, &format!("{e:?}")).unwrap();
                                let exception = v8::Exception::type_error(scope, error);
                                scope.throw_exception(exception);
                                return;
                            }
                        };
                        let json = serde_json::to_string(&stable_ast).unwrap();

                        retval.set(v8::String::new(scope, &json).unwrap().into());
                    },
                )
                .into(),
            );
        }

        global.set(
            v8::String::new(&mut scope, "instanceJSON").unwrap().into(),
            v8::String::new(&mut scope, &serde_json::to_string(instance).unwrap())
                .unwrap()
                .into(),
        );

        global.set(
            v8::String::new(&mut scope, "authJSON").unwrap().into(),
            v8::String::new(
                &mut scope,
                &serde_json::to_string(&{
                    if let Some(auth) = auth {
                        HashMap::from([(
                            "publicKey".to_string(),
                            RecordValue::IndexValue(IndexValue::PublicKey(
                                auth.public_key().clone(),
                            )),
                        )])
                    } else {
                        HashMap::new()
                    }
                })
                .unwrap(),
            )
            .unwrap()
            .into(),
        );

        global.set(
            v8::String::new(&mut scope, "argsJSON").unwrap().into(),
            v8::String::new(&mut scope, &serde_json::to_string(args).unwrap())
                .unwrap()
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
                    throw new Error(str);
            }}
            ctx = JSON.parse(authJSON);
            internPublicKeys(ctx);
            $auth = ctx;
            args = JSON.parse(argsJSON);
            for (const i in args) {
                if (typeof args[i] === "object" && args[i].$$__type === "record") {
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
            return Err("Failed to create a v8 code string".into());
        };

        let mut try_catch = v8::TryCatch::new(&mut scope);
        let script = v8::Script::compile(&mut try_catch, code, None).unwrap();
        let result = script.run(&mut try_catch);

        match (result, try_catch.exception()) {
            (_, Some(exception)) => {
                let exception_string = exception
                    .to_string(&mut try_catch)
                    .unwrap()
                    .to_rust_string_lossy(&mut try_catch);

                Err(exception_string.into())
            }
            (Some(result), _) => {
                let result = result.to_rust_string_lossy(&mut try_catch);
                Ok(serde_json::from_str::<FunctionOutput>(&result)?)
            }
            (None, None) => Err("Unknown error".into()),
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

    #[test]
    fn it_works() {
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

        let collection_collection = indexer.collection("Collection".to_string()).unwrap();
        collection_collection
            .set(
                "ns/User".to_string(),
                &[
                    (
                        "id".into(),
                        indexer::RecordValue::IndexValue(indexer::IndexValue::String(
                            "ns/User".into(),
                        )),
                    ),
                    (
                        "ast".into(),
                        indexer::RecordValue::IndexValue(indexer::IndexValue::String(
                            serde_json::to_string(&stable_ast).unwrap(),
                        )),
                    ),
                ]
                .into(),
                None,
            )
            .unwrap();

        let user_collection = indexer.collection("ns/User".to_string()).unwrap();
        user_collection
            .set(
                "1".to_string(),
                &[
                    (
                        "id".into(),
                        indexer::RecordValue::IndexValue(indexer::IndexValue::String("1".into())),
                    ),
                    (
                        "name".into(),
                        indexer::RecordValue::IndexValue(indexer::IndexValue::String(
                            "John".into(),
                        )),
                    ),
                ]
                .into(),
                None,
            )
            .unwrap();

        let gateway = initialize();
        gateway
            .call(
                &indexer,
                "ns/User".to_string(),
                "changeName",
                "1".to_string(),
                vec![indexer::RecordValue::IndexValue(
                    indexer::IndexValue::String("Tim".into()),
                )],
                None,
            )
            .unwrap();

        let user = user_collection.get("1".to_string(), None).unwrap().unwrap();
        assert_eq!(
            user,
            HashMap::from([
                (
                    "id".into(),
                    indexer::RecordValue::IndexValue(indexer::IndexValue::String("1".into()))
                ),
                (
                    "name".into(),
                    indexer::RecordValue::IndexValue(indexer::IndexValue::String("Tim".into()))
                )
            ])
        );
    }
}
