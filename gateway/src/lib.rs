use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap};

use indexer::{
    Converter, FieldWalker, ForeignRecordReference, IndexValue, Indexer, PathFinder, RecordValue,
};

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
) -> Result<Vec<RecordValue>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut dereferenced_args = Vec::<RecordValue>::new();

    for arg in args {
        let (collection, record_id) = match arg {
            RecordValue::RecordReference(r) => (Cow::Borrowed(collection), r.id),
            RecordValue::ForeignRecordReference(fr) => (
                Cow::Owned(indexer.collection(fr.collection_id).await?),
                fr.id,
            ),
            _ => {
                dereferenced_args.push(arg);
                continue;
            }
        };

        let record = collection
            .get(record_id.clone(), auth)
            .await?
            .ok_or_else(|| {
                format!(
                    "Record {} not found in collection {}",
                    record_id,
                    collection.id(),
                )
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
) -> Result<indexer::RecordRoot, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let record_fields = find_record_fields(collection_ast);

    for (path, type_) in record_fields {
        let map = match record.find_path_mut(&path) {
            Some(RecordValue::Map(m)) => m,
            _ => continue,
        };

        let Some(RecordValue::IndexValue(IndexValue::String(value))) = map.get("id") else { return Err(
            "Record does not have an id field".to_string().into());
        };

        let collection = if let polylang::stableast::Type::ForeignRecord(fr) = type_ {
            let Some(RecordValue::IndexValue(IndexValue::String(collection_id))) =
                map.get("collectionId") else { return Err(
                    "Record does not have a collectionId field".to_string().into());
                };

            let foreign_collection_id = collection.namespace().to_string() + "/" + &fr.collection;

            if collection_id != &foreign_collection_id {
                return Err(format!(
                    "Collection mismatch, expected record in collection {}",
                    &foreign_collection_id
                )
                .into());
            }

            Cow::Owned(indexer.collection(foreign_collection_id).await?)
        } else {
            Cow::Borrowed(collection)
        };

        let record = collection
            .get(value.to_string(), auth)
            .await?
            .ok_or(format!(
                "Record {} not found in collection {}",
                value,
                collection.id()
            ))?;

        *map = record;
    }

    Ok(record)
}

/// Turns dereferenced records back into references.
fn reference_records(
    collection: &indexer::Collection,
    collection_ast: &polylang::stableast::Collection,
    record: serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let record_fields = find_record_fields(collection_ast);

    fn visitor(
        collection_namespace: &str,
        record_fields: &[(Vec<&str>, polylang::stableast::Type)],
        path: &mut Vec<String>,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
        if let Some((_, type_)) = record_fields.iter().find(|(p, _)| p == path) {
            match type_ {
                polylang::stableast::Type::Record(_) => {
                    let serde_json::Value::Object(o) = value else {
                        return Err("Record field is not an object".to_string().into());
                    };

                    let id = o
                        .get("id")
                        .ok_or_else(|| format!("Record field does not have an id field: {path:?}"))?
                        .as_str()
                        .ok_or_else(|| format!("Record field id is not a string: {path:?}"))?;

                    return Ok(serde_json::json!({ "id": id }));
                }
                polylang::stableast::Type::ForeignRecord(fr) => {
                    let serde_json::Value::Object(o) = value else {
                        return Err("Record field is not an object".to_string().into());
                    };

                    let id = o
                        .get("id")
                        .ok_or_else(|| format!("Record field does not have an id field: {path:?}"))?
                        .as_str()
                        .ok_or_else(|| format!("Record field id is not a string: {path:?}"))?;

                    let foreign_collection_id =
                        collection_namespace.to_string() + "/" + &fr.collection;

                    return Ok(
                        serde_json::json!({ "id": id, "collectionId": foreign_collection_id }),
                    );
                }
                _ => {}
            }
        }

        match value {
            serde_json::Value::Object(o) => {
                let mut new_o = serde_json::Map::new();

                for (k, v) in o.into_iter() {
                    path.push(k.clone());
                    let res = visitor(collection_namespace, record_fields, path, v)?;
                    path.pop();

                    new_o.insert(k, res);
                }

                Ok(serde_json::Value::Object(new_o))
            }
            serde_json::Value::Array(a) => {
                let mut new_a = Vec::new();

                for (i, v) in a.into_iter().enumerate() {
                    path.push(i.to_string());
                    new_a.push(visitor(collection_namespace, record_fields, path, v)?);
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

    let record = visitor(collection.namespace(), &record_fields, &mut vec![], record)?;

    Ok(record)
}

async fn has_permission_to_call(
    indexer: &Indexer,
    collection: &indexer::Collection<'_>,
    collection_ast: &polylang::stableast::Collection<'_>,
    method_ast: &polylang::stableast::Method<'_>,
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
            RecordValue::RecordReference(r) => {
                let record = collection
                    .get(r.id.clone(), Some(auth))
                    .await?
                    .ok_or_else(|| {
                        format!(
                            "Record {} not found in collection {}",
                            r.id,
                            collection.id()
                        )
                    })?;

                if collection.has_delegate_access(&record, &Some(auth)).await? {
                    return Ok(true);
                }
            }
            RecordValue::ForeignRecordReference(fr) => {
                let collection = indexer.collection(fr.collection_id.clone()).await?;

                let record = collection
                    .get(fr.id.clone(), Some(auth))
                    .await?
                    .ok_or_else(|| {
                        format!(
                            "Record {} not found in collection {}",
                            fr.id,
                            collection.id()
                        )
                    })?;

                if collection.has_delegate_access(&record, &Some(auth)).await? {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }

    Ok(false)
}

#[derive(Debug)]
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

fn get_collection_ast<'a>(
    collection_id: String,
    collection_name: &str,
    collection_meta_record: &'a indexer::RecordRoot,
) -> Result<polylang::stableast::Collection<'a>, Box<dyn std::error::Error + Send + Sync + 'static>>
{
    let Some(ast) = collection_meta_record.get("ast") else {
        return Err("Collection has no AST".into());
    };

    let RecordValue::IndexValue(IndexValue::String(ast_str)) = ast else {
        return Err("Collection AST is not a string".into());
    };

    let ast = serde_json::from_str::<polylang::stableast::Root>(ast_str)?;
    let Some(collection_ast) = ast.0.into_iter().find_map(|a| {
        if let polylang::stableast::RootNode::Collection(col) = a {
            if col.name.as_ref() == collection_name { Some(col) } else { None }
        } else {
            None
        }
    }) else {
        return Err("Collection not found in AST".into());
    };

    Ok(collection_ast)
}

impl Gateway {
    pub async fn call(
        &self,
        indexer: &Indexer,
        collection_id: String,
        function_name: &str,
        record_id: String,
        args: Vec<serde_json::Value>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<Vec<Change>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut changes = Vec::new();
        let collection_collection = indexer.collection("Collection".to_string()).await?;
        let collection = indexer.collection(collection_id.clone()).await?;

        let Some(meta) = collection_collection.get(collection.id().to_string(), None).await? else {
            return Err("Collection not found".into());
        };

        let collection_ast =
            get_collection_ast(collection.id().to_string(), &collection.name(), &meta)?;

        let js_collection = polylang::js::generate_js_collection(&collection_ast);

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
            collection
                .get(record_id.clone(), auth)
                .await?
                .ok_or_else(|| {
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
            &collection_ast,
            method,
            &instance_record,
            auth,
        )
        .await?
        {
            return Err("You do not have permission to call this function".into());
        }

        let params = method
            .attributes
            .iter()
            .filter_map(|a| match a {
                polylang::stableast::MethodAttribute::Parameter(p) => Some(p),
                _ => None,
            })
            .collect::<Vec<_>>();
        if params.len() != args.len() {
            return Err("Incorrect number of arguments".into());
        }

        let args = params
            .iter()
            .zip(args.into_iter())
            .map(|(param, arg)| {
                // TODO: consider what to do with optional arguments
                Converter::convert((&param.type_, arg), false)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let dereferenced_args = dereference_args(indexer, &collection, args, auth).await?;
        let instance_record =
            dereference_fields(indexer, &collection, &collection_ast, instance_record, auth)
                .await?;
        let mut output = self.run(
            &collection_id,
            &js_collection.code,
            function_name,
            &indexer::record_to_json(instance_record.clone())?,
            &dereferenced_args
                .clone()
                .into_iter()
                .map(|a| a.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            auth,
        )?;
        output.instance = reference_records(&collection, &collection_ast, output.instance)?;
        let instance = indexer::json_to_record(&collection_ast, output.instance, false)?;

        if function_name != "constructor" && instance.get("id") != instance_record.get("id") {
            return Err("Record id was modified".into());
        }

        let Some(output_instance_id) = instance.get("id") else {
            return Err("Record id was not returned".into());
        };
        let RecordValue::IndexValue(IndexValue::String(output_instance_id)) = output_instance_id else {
            return Err("Record id was not a string".into());
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
                let input_arg = serde_json::Value::try_from(input_arg)?;
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
                        dereferenced_args_len,
                    ).into());
                };

                let Some(record) = (match &parameter.type_ {
                    polylang::stableast::Type::Record(_) => {
                        let Some(output_id) = output_arg.get("id") else {
                            return Err("Record id is missing".into());
                        };

                        if Some(output_id) != input_arg.get("id") {
                            return Err("Record id was modified".into());
                        }

                        Some(indexer::json_to_record(&collection_ast, output_arg, false)?)
                    },
                    polylang::stableast::Type::ForeignRecord(fr) => {
                        let Some(output_id) = output_arg.get("id") else {
                            return Err("Record id is missing".into());
                        };

                        if Some(output_id) != input_arg.get("id") {
                            return Err("Record id was modified".into());
                        }

                        let collection_id = collection.namespace().to_string() + "/" + &fr.collection;

                        let Some(collection_meta) = collection_collection.get(collection_id.clone(), auth).await? else {
                            return Err(format!("Collection {collection_id} not found").into());
                        };

                        let ast = get_collection_ast(collection_id, fr.collection.as_ref(), &collection_meta)?;

                        Some(indexer::json_to_record(&ast, output_arg, false)?)
                    }
                    _ => None,
                }) else {
                    continue;
                };

                records_to_update.push(record);
            }

            records_to_update
        };

        if function_name == "constructor" {
            if collection
                .get(output_instance_id.to_string(), None)
                .await?
                .is_some()
            {
                return Err("Record id already exists".into());
            }

            changes.push(Change::Create {
                collection_id,
                record_id: output_instance_id.to_string(),
                record: instance,
            });
        } else {
            changes.push(Change::Update {
                collection_id,
                record_id: output_instance_id.to_string(),
                record: instance,
            });
        }

        for record in records_to_update {
            let Some(id) = record.get("id") else {
                return Err("Record id is missing".into());
            };

            let RecordValue::IndexValue(IndexValue::String(id)) = id else {
                return Err("Record id is not a string".into());
            };

            changes.push(Change::Update {
                collection_id: todo!("get the collection id before dereferencing"),
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
                        HashMap::from([("publicKey".to_string(), auth.public_key().clone())])
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
            )
            .await
            .unwrap();

        let user_collection = indexer.collection("ns/User".to_string()).await.unwrap();
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
            )
            .await
            .unwrap();

        let gateway = initialize();
        gateway
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

        let user = user_collection
            .get("1".to_string(), None)
            .await
            .unwrap()
            .unwrap();
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
