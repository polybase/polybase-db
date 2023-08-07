#![warn(clippy::unwrap_used, clippy::expect_used)]

use crate::db::{Db, DbWaitResult};
use crate::errors::http::HTTPError;
use crate::errors::logger::SlogMiddleware;
use crate::errors::metrics::MetricsData;
use crate::errors::reason::ReasonCode;
use crate::errors::AppError;
use crate::txn::CallTxn;
use crate::{auth, util::hash};
use actix_cors::Cors;
use actix_server::Server;
use actix_web::{get, post, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use base64::Engine;
use indexer::AuthUser;
use polylang_prover::{compile_program, Inputs, ProgramExt};
use serde::{de::IntoDeserializer, Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use std::{borrow::Cow, cmp::min, sync::Arc, time::Duration};

struct RouteState {
    db: Arc<Db>,
    whitelist: Arc<Option<Vec<String>>>,
    restrict_namespaces: Arc<bool>,
}

#[get("/")]
async fn root() -> impl Responder {
    HttpResponse::Ok()
        .content_type("application/json")
        .body(format!(
            "{{ \"server\": \"Polybase\", \"version\": \"{}\" }}",
            env!("CARGO_PKG_VERSION")
        ))
}

#[derive(Default)]
struct PrefixedHex([u8; 32]);

impl Serialize for PrefixedHex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut hex = hex::encode(self.0);
        hex.insert_str(0, "0x");

        serializer.serialize_str(&hex)
    }
}

#[derive(Default, Serialize)]
struct Block {
    hash: PrefixedHex,
}

#[derive(Deserialize)]
struct GetRecordQuery {
    since: Option<f64>,
    #[serde(rename = "waitFor", default = "Seconds::sixty")]
    wait_for: Seconds,
    format: Option<String>,
}

#[derive(Serialize)]
struct GetRecordResponse {
    data: serde_json::Value,
    block: Block,
}

#[get("/{collection}/records/{id}")]
async fn get_record(
    state: web::Data<RouteState>,
    path: web::Path<(String, String)>,
    query: web::Query<GetRecordQuery>,
    body: auth::SignedJSON<()>,
) -> Result<impl Responder, HTTPError> {
    let (collection, record_id) = path.into_inner();
    let auth = body.auth;
    let auth: Option<indexer::AuthUser> = auth.map(|a| a.into());

    let record = if let Some(since) = query.since {
        match state
            .db
            .get_wait(
                collection,
                record_id,
                auth,
                since,
                Duration::from(query.wait_for),
            )
            .await?
        {
            DbWaitResult::Updated(record) => record,
            DbWaitResult::NotModified => return Ok(HttpResponse::NotModified().finish()),
        }
    } else {
        state.db.get(collection, record_id, auth).await?
    };

    match record {
        Some(record) => {
            let data = indexer::record_to_json(record).map_err(indexer::IndexerError::from)?;
            if let Some(f) = &query.format {
                if f == "nft" {
                    return Ok(HttpResponse::Ok().json(data));
                }
            }
            Ok(HttpResponse::Ok().json(GetRecordResponse {
                data,
                block: Default::default(),
            }))
        }
        None => Err(HTTPError::new(ReasonCode::RecordNotFound, None)),
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Direction {
    #[serde(rename = "asc")]
    Ascending,
    #[serde(rename = "desc")]
    Descending,
}

impl From<Direction> for indexer::Direction {
    fn from(dir: Direction) -> Self {
        match dir {
            Direction::Ascending => indexer::Direction::Ascending,
            Direction::Descending => indexer::Direction::Descending,
        }
    }
}

/// Deserialized from "<number>s"
#[derive(Debug, Clone, Copy)]
struct Seconds(u64);

impl Seconds {
    fn sixty() -> Self {
        Self(60)
    }
}

impl From<Seconds> for std::time::Duration {
    fn from(s: Seconds) -> Self {
        std::time::Duration::from_secs(s.0)
    }
}

impl<'de> Deserialize<'de> for Seconds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if !s.ends_with('s') {
            return Err(serde::de::Error::custom("missing 's'"));
        }
        let s = &s[..s.len() - 1];
        let s = s.parse::<u64>().map_err(serde::de::Error::custom)?;

        Ok(Seconds(s))
    }
}

#[derive(Debug)]
struct OptionalCursor(Option<indexer::Cursor>);

impl<'de> Deserialize<'de> for OptionalCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // if there's nothing or it's an empty string, return None
        // if there's a string, delegate to Cursor::deserialize

        let cursor = Option::<String>::deserialize(deserializer)?
            .filter(|s| !s.is_empty())
            .map(|s| indexer::Cursor::deserialize(s.into_deserializer()))
            .transpose()?;

        Ok(OptionalCursor(cursor))
    }
}

#[serde_as]
#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<usize>,
    #[serde(default, rename = "where")]
    #[serde_as(as = "serde_with::json::JsonString")]
    where_query: indexer::WhereQuery,
    #[serde(default)]
    #[serde_as(as = "serde_with::json::JsonString")]
    sort: Vec<(String, Direction)>,
    before: OptionalCursor,
    after: OptionalCursor,
    /// UNIX timestamp in seconds
    since: Option<f64>,
    #[serde(rename = "waitFor", default = "Seconds::sixty")]
    wait_for: Seconds,
}

#[derive(Debug, Serialize)]
struct Cursors {
    before: Option<indexer::Cursor>,
    after: Option<indexer::Cursor>,
}

#[derive(Serialize)]
struct ListResponse {
    data: Vec<GetRecordResponse>,
    cursor: Cursors,
}

#[tracing::instrument(skip(state, body))]
#[get("/{collection}/records")]
async fn get_records(
    req: HttpRequest,
    state: web::Data<RouteState>,
    path: web::Path<String>,
    query: web::Query<ListQuery>,
    body: auth::SignedJSON<()>,
) -> Result<impl Responder, HTTPError> {
    let collection = path.into_inner();
    let auth = body.auth;
    let auth: Option<indexer::AuthUser> = auth.map(|a| a.into());

    let sort_indexes = query
        .sort
        .iter()
        .map(|(path, dir)| {
            indexer::CollectionIndexField::new(
                path.split('.').map(|p| Cow::Owned(p.to_string())).collect(),
                (*dir).into(),
            )
        })
        .collect::<Vec<_>>();

    let list_query = indexer::ListQuery {
        limit: Some(min(1000, query.limit.unwrap_or(100))),
        where_query: query.where_query.clone(),
        order_by: &sort_indexes,
        cursor_after: query.after.0.clone(),
        cursor_before: query.before.0.clone(),
    };

    let records = if let Some(since) = query.since {
        match state
            .db
            .list_wait(
                collection,
                list_query,
                auth,
                since,
                Duration::from(query.wait_for),
            )
            .await?
        {
            DbWaitResult::Updated(record) => record,
            DbWaitResult::NotModified => return Ok(HttpResponse::NotModified().finish()),
        }
    } else {
        state.db.list(collection, list_query, auth).await?
    };

    // for metrics data collection
    let req_uri = req.uri().to_string();
    let mut num_records = 0;

    let list_response: Result<ListResponse, HTTPError> = async {
        num_records = records.len();

        Ok(ListResponse {
            cursor: Cursors {
                before: records
                    .first()
                    .map(|(c, _)| c.clone())
                    .or_else(|| query.before.0.clone())
                    // TODO: is this right?
                    // The `after` cursor is the key of the last record the user received,
                    // if they don't receive any records,
                    // then querying again with the returned `before` should return the `after` record,
                    // not just records before it.
                    .or_else(|| {
                        query.after.0.clone().map(|a| {
                            #[allow(clippy::unwrap_used)]
                            // Unwrap is safe because `a` is an index key, immediate_sucessor works on index keys
                            a.immediate_successor().unwrap()
                        })
                    }),
                after: records
                    .last()
                    .map(|(c, _)| c.clone())
                    .or_else(|| query.after.0.clone()),
            },
            data: records
                .into_iter()
                .map(|(_, r)| {
                    Ok(GetRecordResponse {
                        data: indexer::record_to_json(r)?,
                        block: Default::default(),
                    })
                })
                .collect::<Result<_, indexer::RecordError>>()
                .map_err(indexer::IndexerError::from)?,
        })
    }
    .await;

    let mut resp = HttpResponse::Ok()
        .content_type("application/json")
        .json(list_response?);

    // update metrics data
    resp.extensions_mut()
        .insert(MetricsData::NumberOfRecordsBeingReturned {
            req_uri,
            num_records,
        });

    Ok(resp)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionCall {
    args: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionResponse {
    data: serde_json::Value,
}

#[tracing::instrument(skip(state, body))]
#[post("/{collection}/records")]
async fn post_record(
    state: web::Data<RouteState>,
    path: web::Path<String>,
    body: auth::SignedJSON<FunctionCall>,
) -> Result<web::Json<FunctionResponse>, HTTPError> {
    let collection_id = path.into_inner();

    let auth = body.auth.map(|a| a.into());
    let db: Arc<_> = Arc::clone(&state.db);

    // New collection is being created
    if collection_id == "Collection" {
        // Validate whitelist (if it exists)
        validate_new_collection(
            &body.data.args[0],
            &state.whitelist,
            &state.restrict_namespaces,
            &auth,
        )?;
    }

    let txn = CallTxn::new(
        collection_id.clone(),
        "constructor",
        "".to_string(),
        body.data.args,
        auth,
    );

    let record_id = db.call(txn).await?;

    let Some(record) = state.db.get_without_auth_check(collection_id, record_id).await? else {
        return Err(HTTPError::new(
            ReasonCode::RecordNotFound,
            None,
        ));
    };

    Ok(web::Json(FunctionResponse {
        data: indexer::record_to_json(record).map_err(indexer::IndexerError::from)?,
    }))
}

#[tracing::instrument(skip(state, body))]
#[post("/{collection}/records/{record}/call/{function}")]
async fn call_function(
    state: web::Data<RouteState>,
    path: web::Path<(String, String, String)>,
    body: auth::SignedJSON<FunctionCall>,
) -> Result<web::Json<FunctionResponse>, HTTPError> {
    let (collection_id, record_id, function) = path.into_inner();

    let auth = body.auth.map(indexer::AuthUser::from);
    let db = Arc::clone(&state.db);

    let txn = CallTxn::new(
        collection_id.clone(),
        &function,
        record_id,
        body.data.args,
        auth,
    );

    let record_id = db.call(txn).await?;
    let record = state
        .db
        .get_without_auth_check(collection_id, record_id)
        .await?;

    Ok(web::Json(FunctionResponse {
        data: match record {
            Some(record) => indexer::record_to_json(record).map_err(indexer::IndexerError::from)?,
            None => serde_json::Value::Null,
        },
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProveRequest {
    miden_code: String,
    abi: abi::Abi,
    ctx_public_key: Option<abi::publickey::Key>,
    this: Option<serde_json::Value>,
    this_salts: Vec<u32>,
    args: Vec<serde_json::Value>,
    other_records: HashMap<String, Vec<(serde_json::Value, Vec<u32>)>>,
}

#[tracing::instrument(skip_all, fields(
    miden_code_hash = %hash(&req.miden_code),
    abi = ?req.abi,
    ctx_public_key = ?req.ctx_public_key,
    this = ?req.this,
    args = ?req.args,
))]
#[post("/v0/prove")]
async fn prove(req: web::Json<ProveRequest>) -> Result<impl Responder, HTTPError> {
    let program = compile_program(&req.abi, &req.miden_code).map_err(|e| {
        HTTPError::new(
            ReasonCode::Internal,
            Some(Box::new(AppError::MidenCompile(Box::new(e)))),
        )
    })?;

    let this = req.this.clone().unwrap_or(
        req.abi
            .default_this_value()
            .map_err(|err| {
                HTTPError::new(
                    ReasonCode::Internal,
                    Some(Box::new(AppError::ABIError(err))),
                )
            })?
            .try_into()
            .map_err(|err| {
                HTTPError::new(
                    ReasonCode::Internal,
                    Some(Box::new(AppError::ABIError(Box::new(err)))),
                )
            })?,
    );

    let inputs = Inputs::new(
        req.abi.clone(),
        req.ctx_public_key.clone(),
        req.this_salts.clone(),
        this.clone(),
        req.args.clone(),
        req.other_records.clone(),
    )
    .map_err(|err| {
        HTTPError::new(
            ReasonCode::Internal,
            Some(Box::new(AppError::ProveError(Box::new(err)))),
        )
    })?;

    let output = polylang_prover::prove(&program, &inputs).map_err(|err| {
        HTTPError::new(
            ReasonCode::Internal,
            Some(Box::new(AppError::ProveError(Box::new(err)))),
        )
    })?;

    let program_info = program.to_program_info_bytes();
    let new_this = TryInto::<serde_json::Value>::try_into(output.new_this).map_err(|err| {
        HTTPError::new(
            ReasonCode::Internal,
            Some(Box::new(AppError::ProveError(Box::new(err)))),
        )
    })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "old": {
            "this": this,
            "hashes": inputs.this_field_hashes,
        },
        "new": {
            "selfDestructed": output.self_destructed,
            "this": new_this,
            "hashes": output.new_hashes,
        },
        "stack": {
            "input": output.input_stack,
            "output": output.stack,
        },
        "programInfo": base64::engine::general_purpose::STANDARD.encode(program_info),
        "proof": base64::engine::general_purpose::STANDARD.encode(output.proof),
        "debug": {
            "logs": output.run_output.logs(),
        }
    })))
}

#[get("/v0/health")]
async fn health(state: web::Data<RouteState>) -> impl Responder {
    if state.db.is_healthy() {
        HttpResponse::Ok()
    } else {
        HttpResponse::ServiceUnavailable()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusResponse {
    status: String,
    root: String,
    height: usize,
    peers: usize,
}

#[tracing::instrument(skip(state))]
#[get("/v0/status")]
async fn status(state: web::Data<RouteState>) -> Result<web::Json<StatusResponse>, HTTPError> {
    let manifest = state.db.get_manifest().await?;
    let height = manifest.as_ref().map(|m| m.height).unwrap_or(0);
    let hash = manifest
        .as_ref()
        .map(|m| m.hash().to_string())
        .unwrap_or("0x0".to_string());
    Ok(web::Json(StatusResponse {
        status: "OK".to_string(),
        root: hash,
        height,
        peers: 23,
    }))
}

#[tracing::instrument(skip(db))]
pub fn create_rpc_server(
    rpc_laddr: String,
    db: Arc<Db>,
    whitelist: Arc<Option<Vec<String>>>,
    restrict_namespaces: Arc<bool>,
) -> Result<Server, std::io::Error> {
    Ok(HttpServer::new(move || {
        let cors = Cors::permissive();

        App::new()
            .app_data(web::Data::new(RouteState {
                db: Arc::clone(&db),
                whitelist: Arc::clone(&whitelist),
                restrict_namespaces: Arc::clone(&restrict_namespaces),
            }))
            .wrap(SlogMiddleware)
            .wrap(cors)
            .service(root)
            .service(health)
            .service(status)
            .service(prove)
            .service(
                web::scope("/v0/collections")
                    .service(get_record)
                    .service(get_records)
                    .service(post_record)
                    .service(call_function),
            )
    })
    .bind(rpc_laddr)?
    .run())
}

fn validate_new_collection(
    collection_id: &serde_json::Value,
    whitelist: &Option<Vec<String>>,
    restrict_namespaces: &bool,
    auth: &Option<AuthUser>,
) -> Result<(), HTTPError> {
    let pk = auth
        .as_ref()
        .map(|a| a.public_key().to_hex().unwrap_or("".to_string()))
        .unwrap_or("".to_string());

    // Check collection whitelist
    if let Some(whitelist) = whitelist {
        if pk.is_empty() || !whitelist.contains(&pk) {
            return Err(HTTPError::new(
                ReasonCode::Unauthorized,
                Some(Box::new(AppError::Whitelist)),
            ));
        }
    }

    // Check namespace is valid (only pk/<pk> currently allowed)
    if *restrict_namespaces {
        match collection_id {
            serde_json::Value::String(id) => {
                let parts: Vec<&str> = id.split('/').collect();

                if pk.is_empty() {
                    return Err(HTTPError::new(
                        ReasonCode::Unauthorized,
                        Some(Box::new(AppError::AnonNamespace)),
                    ));
                }

                if parts.len() <= 2 || parts[0] != "pk" {
                    return Err(HTTPError::new(
                        ReasonCode::Unauthorized,
                        Some(Box::new(AppError::InvalidNamespace(id.clone()))),
                    ));
                }

                if parts[1] != pk {
                    return Err(HTTPError::new(
                        ReasonCode::Unauthorized,
                        Some(Box::new(AppError::InvalidNamespacePublicKey(
                            pk,
                            parts[1].to_string(),
                        ))),
                    ));
                }
            }
            _ => {
                return Err(HTTPError::new(
                    ReasonCode::Unauthorized,
                    Some(Box::new(AppError::InvalidNamespace(format!(
                        "{:?}",
                        collection_id
                    )))),
                ));
            }
        }
    }

    Ok(())
}
