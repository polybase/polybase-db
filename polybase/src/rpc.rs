#![warn(clippy::unwrap_used, clippy::expect_used)]

use crate::auth;
use crate::db::Db;
use crate::errors::http::HTTPError;
use crate::errors::logger::SlogMiddleware;
use crate::errors::metrics::MetricsData;
use crate::errors::reason::ReasonCode;
use crate::errors::AppError;
use crate::txn::CallTxn;
use actix_cors::Cors;
use actix_server::Server;
use actix_web::{
    get, http::StatusCode, post, web, App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use futures::TryStreamExt;
use indexer::{AuthUser, Indexer};
use serde::{de::IntoDeserializer, Deserialize, Serialize};
use serde_with::serde_as;
use std::{
    borrow::Cow,
    cmp::min,
    sync::Arc,
    time::{Duration, SystemTime},
};

struct RouteState {
    db: Arc<Db>,
    indexer: Arc<Indexer>,
    whitelist: Arc<Option<Vec<String>>>,
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
    let (collection, id) = path.into_inner();
    let auth = body.auth;

    let collection = state.indexer.collection(collection).await?;

    if let Some(since) = query.since {
        enum UpdateCheckResult {
            Updated,
            NotFound,
            NotModified,
        }

        let was_updated: Result<UpdateCheckResult, HTTPError> = async {
            let wait_for = min(Duration::from(query.wait_for), Duration::from_secs(60));
            let wait_until = SystemTime::now() + wait_for;
            let since = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(since);

            let mut record_exists = false;
            while wait_until > SystemTime::now() {
                if let Some(metadata) = collection.get_record_metadata(&id).await? {
                    record_exists = true;
                    if metadata.updated_at > since {
                        return Ok(UpdateCheckResult::Updated);
                    }
                }

                tokio::time::sleep(Duration::from_millis(1000)).await;
            }

            Ok(if record_exists {
                UpdateCheckResult::NotModified
            } else {
                UpdateCheckResult::NotFound
            })
        }
        .await;

        match was_updated? {
            UpdateCheckResult::Updated => {}
            UpdateCheckResult::NotModified => {
                return Ok(HttpResponse::Ok().status(StatusCode::NOT_MODIFIED).finish())
            }
            UpdateCheckResult::NotFound => return Ok(HttpResponse::NotFound().finish()),
        }
    }

    let record = collection.get(id, auth.map(|a| a.into()).as_ref()).await?;

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
    let collection = state.indexer.collection(collection).await?;

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

    if let Some(since) = query.since {
        let was_updated = async {
            let wait_for = min(Duration::from(query.wait_for), Duration::from_secs(60));
            let wait_until = SystemTime::now() + wait_for;
            let since = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(since);

            while wait_until > SystemTime::now() {
                if collection
                    .get_metadata()
                    .await?
                    .map(|m| m.last_record_updated_at > since)
                    .unwrap_or(false)
                {
                    return Ok(true);
                }

                tokio::time::sleep(Duration::from_millis(1000)).await;
            }

            Ok(false)
        }
        .await;

        match was_updated {
            Ok(true) => {}
            Ok(false) => return Ok(HttpResponse::Ok().status(StatusCode::NOT_MODIFIED).finish()),
            Err(e) => return Err(e),
        }
    }

    // for metrics data collection
    let req_uri = req.uri().to_string();
    let mut num_records = 0;

    let list_response: Result<ListResponse, HTTPError> = async {
        let records = collection
            .list(
                indexer::ListQuery {
                    limit: Some(min(1000, query.limit.unwrap_or(100))),
                    where_query: query.where_query.clone(),
                    order_by: &sort_indexes,
                    cursor_after: query.after.0.clone(),
                    cursor_before: query.before.0.clone(),
                },
                &auth.map(indexer::AuthUser::from).as_ref(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;

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

#[post("/{collection}/records")]
async fn post_record(
    state: web::Data<RouteState>,
    path: web::Path<String>,
    body: auth::SignedJSON<FunctionCall>,
) -> Result<web::Json<FunctionResponse>, HTTPError> {
    let collection_id = path.into_inner();

    let auth = body.auth.map(|a| a.into());
    let db: Arc<_> = Arc::clone(&state.db);

    // Check whitelist
    if collection_id == "Collection" {
        validate_whitelist(&state.whitelist, &auth)?;
    }

    let txn = CallTxn::new(
        collection_id.clone(),
        "constructor",
        "".to_string(),
        body.data.args,
        auth,
    );

    let record_id = db.call(txn).await?;

    let Some(record) = state.db.get(collection_id, record_id).await? else {
        return Err(HTTPError::new(
            ReasonCode::RecordNotFound,
            None,
        ));
    };

    Ok(web::Json(FunctionResponse {
        data: indexer::record_to_json(record).map_err(indexer::IndexerError::from)?,
    }))
}

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
    let record = state.db.get(collection_id, record_id).await?;

    Ok(web::Json(FunctionResponse {
        data: match record {
            Some(record) => indexer::record_to_json(record).map_err(indexer::IndexerError::from)?,
            None => serde_json::Value::Null,
        },
    }))
}

#[get("/v0/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusResponse {
    status: String,
    root: String,
    peers: usize,
    leader: usize,
}

#[get("/v0/status")]
async fn status(state: web::Data<RouteState>) -> Result<web::Json<StatusResponse>, HTTPError> {
    Ok(web::Json(StatusResponse {
        status: "OK".to_string(),
        root: hex::encode(state.db.rollup.root()?),
        peers: 23,
        leader: 12,
    }))
}

pub fn create_rpc_server(
    rpc_laddr: String,
    indexer: Arc<indexer::Indexer>,
    db: Arc<Db>,
    whitelist: Arc<Option<Vec<String>>>,
    logger: slog::Logger,
) -> Result<Server, std::io::Error> {
    Ok(HttpServer::new(move || {
        let cors = Cors::permissive();

        App::new()
            .app_data(web::Data::new(RouteState {
                db: Arc::clone(&db),
                indexer: Arc::clone(&indexer),
                whitelist: Arc::clone(&whitelist),
            }))
            .wrap(SlogMiddleware::new(logger.clone()))
            .wrap(cors)
            .service(root)
            .service(health)
            .service(status)
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

fn validate_whitelist(
    whitelist: &Option<Vec<String>>,
    auth: &Option<AuthUser>,
) -> Result<(), HTTPError> {
    // Check whitelist
    if let Some(whitelist) = whitelist {
        if let Some(auth_user) = auth {
            // Convert the key to hex for easier comparison
            let pk = auth_user.public_key().to_hex().unwrap_or("".to_string());
            if pk.is_empty() || !whitelist.contains(&pk) {
                return Err(HTTPError::new(
                    ReasonCode::Unauthorized,
                    Some(Box::new(AppError::Whitelist)),
                ));
            }
        } else {
            return Err(HTTPError::new(
                ReasonCode::Unauthorized,
                Some(Box::new(AppError::Whitelist)),
            ));
        }
    }
    Ok(())
}
