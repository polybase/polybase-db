#![warn(clippy::unwrap_used, clippy::expect_used)]

use indexer::auth_user::AuthUser;
use schema::{self, publickey::PublicKey};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Once};
use tracing::debug;

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

    #[error("invalid output args")]
    InvalidOutputArgs,

    #[error("serde_json error")]
    SerdeJsonError(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayUserError {
    // #[error("record {record_id:?} was not found in collection {collection_id:?}")]
    // RecordNotFound {
    //     record_id: String,
    //     collection_id: String,
    // },

    // #[error("record ID field is not a string")]
    // RecordIdNotString,

    // #[error("record does not have a collectionId field")]
    // RecordCollectionIdNotFound,

    // #[error("record field is not an object")]
    // RecordFieldNotObject,
    #[error("record ID was modified")]
    RecordIDModified,

    // #[error("collection {collection_id:?} was not found")]
    // CollectionNotFound { collection_id: String },

    // #[error("record id already exists in collection")]
    // CollectionIdExists,
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FunctionOutput {
    pub args: Vec<serde_json::Value>,
    pub instance: serde_json::Value,
    #[serde(rename = "selfdestruct")]
    pub self_destruct: bool,
}

pub struct Gateway {
    // This is so the consumer of this library can't create a Gateway without calling initialize
    _x: (),
}

static INIT: Once = Once::new();

pub fn initialize() -> Gateway {
    INIT.call_once(|| {
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    });

    Gateway { _x: () }
}

impl Gateway {
    #[tracing::instrument(skip(self))]
    pub async fn call<'a>(
        &self,
        collection_id: &str,
        js_code: &str,
        method: &str,
        instance: &serde_json::Value,
        args: &[serde_json::Value],
        auth: Option<&AuthUser>,
    ) -> Result<FunctionOutput> {
        // Run the function
        let output = self.run(collection_id, js_code, method, instance, args, auth)?;

        // Log the function call
        debug!(
            collection_id = &collection_id,
            collection_code = &js_code,
            function_name = method,
            instance = serde_json::to_string(&instance).unwrap_or_default(),
            args = serde_json::to_string(&args).unwrap_or_default(),
            auth = serde_json::to_string(&auth).unwrap_or_default(),
            output = serde_json::to_string(&output).unwrap_or_default(),
            "function after"
        );

        if method == "constructor" && output.instance.get("id").is_none() {
            return Err(GatewayUserError::ConstructorMustAssignId)?;
        }

        if method != "constructor" && instance.get("id") != output.instance.get("id") {
            return Err(GatewayUserError::RecordIDModified)?;
        }

        if args.len() != output.args.len() {
            return Err(GatewayError::InvalidOutputArgs)?;
        }

        Ok(output)
    }

    fn run(
        &self,
        collection_id: &str,
        collection_code: &str,
        method: &str,
        instance: &serde_json::Value,
        args: &[serde_json::Value],
        auth: Option<&AuthUser>,
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

                    let public_key = match serde_json::from_str::<PublicKey>(&public_key_json) {
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
            .replace("$FUNCTION_NAME", method)
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
    use super::*;
    use schema::{ast::collection_ast_from_root, Schema};
    use serde_json::json;

    fn get_code(collection_name: &str, collection_schema: &str) -> String {
        let mut program = None;
        let (_, stable_ast) = polylang::parse(collection_schema, "ns", &mut program).unwrap();

        let collection = collection_ast_from_root(collection_name, stable_ast).unwrap();
        let schema = Schema::new(&collection);
        schema.generate_js()
    }

    #[tokio::test]
    async fn test_constructor() {
        let user_col_code = r#"
            @public
            collection User {
                id: string;
                name: string;

                constructor (name: string) {
                    this.id = "1";
                    this.name = name;
                }
            }
        "#;
        let js_code = get_code("User", user_col_code);

        let gateway = initialize();
        let output = gateway
            .call(
                "ns/User",
                &js_code,
                "constructor",
                &json!({}),
                &[json!("new name")],
                None,
            )
            .await
            .unwrap();

        assert_eq!(output.instance, json!({ "id": "1", "name": "new name" }));
        assert_eq!(output.args, vec![json!("new name")]);
        assert!(!output.self_destruct);
    }

    #[tokio::test]
    async fn test_change_instance() {
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
        let js_code = get_code("User", user_col_code);

        let gateway = initialize();
        let output = gateway
            .call(
                "ns/User",
                &js_code,
                "changeName",
                &json!({
                    "id": "1",
                    "name": "old name",
                }),
                &[json!("new name")],
                None,
            )
            .await
            .unwrap();

        assert_eq!(output.instance, json!({ "id": "1", "name": "new name" }));
        assert_eq!(output.args, vec![json!("new name")]);
        assert!(!output.self_destruct, "selfdestruct() was called");
    }

    #[tokio::test]
    async fn test_nested_fields() {
        let user_col_code = r#"
            @public
            collection Account {
                id: string;
                info: {
                    name: string;
                };
            
                @index(info.name);
            
                constructor (id: string, name: string) {
                    this.id = id;
                    this.info = { name: name };
                }
            }
        "#;
        let js_code = get_code("Account", user_col_code);

        let gateway = initialize();
        let output = gateway
            .call(
                "ns/Account",
                &js_code,
                "constructor",
                &json!({}),
                &[json!("1"), json!("new name")],
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            output.instance,
            json!({ "id": "1", "info": { "name": "new name" } })
        );
        assert_eq!(output.args, vec![json!("1"), json!("new name")]);
        assert!(!output.self_destruct, "selfdestruct() was called");
    }

    #[tokio::test]
    async fn test_self_destruct() {
        let user_col_code = r#"
            @public
            collection User {
                id: string;
            
                del () {
                    selfdestruct();
                }
            }
        "#;
        let js_code = get_code("User", user_col_code);

        let gateway = initialize();
        let output = gateway
            .call("ns/User", &js_code, "del", &json!({}), &[], None)
            .await
            .unwrap();

        assert_eq!(output.instance, json!({}));
        assert_eq!(output.args, Vec::<serde_json::Value>::new());
        assert!(output.self_destruct, "selfdestruct() was not called");
    }
}
