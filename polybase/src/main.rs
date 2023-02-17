mod auth;
mod config;

use std::{
    borrow::Cow,
    cmp::min,
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::config::Config;
use actix_web::{get, http::StatusCode, post, web, App, HttpResponse, HttpServer, Responder};
use clap::Parser;
use futures::TryStreamExt;
use gateway::{Change, Gateway};
use indexer::Indexer;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

struct AppState {
    indexer: Arc<Indexer>,
    gateway: Arc<Gateway>,
}

#[get("/")]
async fn root() -> impl Responder {
    HttpResponse::Ok()
        .content_type("application/json")
        .body(r#"{ "server": "Polybase", "version": "0.1.0" }"#)
}

#[derive(Deserialize)]
struct GetRecordQuery {
    since: Option<f64>,
    #[serde(rename = "waitFor", default = "Seconds::sixty")]
    wait_for: Seconds,
}

#[get("/{collection}/records/{id}")]
async fn get_record(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    query: web::Query<GetRecordQuery>,
    body: auth::SignedJSON<()>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let (collection, id) = path.into_inner();
    let auth = body.auth;

    let collection = state.indexer.collection(collection).await.unwrap();

    if let Some(since) = query.since {
        enum UpdateCheckResult {
            Updated,
            NotFound,
            NotModified,
        }

        let was_updated = async {
            let wait_for = min(Duration::from(query.wait_for), Duration::from_secs(60));
            let wait_until = SystemTime::now() + wait_for;
            let since = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(since);

            let mut record_exists = false;
            while wait_until > SystemTime::now() {
                if let Some(metadata) = collection.get_record_metadata(&id).await.unwrap() {
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

        match was_updated {
            Ok(UpdateCheckResult::Updated) => {}
            Ok(UpdateCheckResult::NotModified) => {
                return Ok(HttpResponse::Ok().status(StatusCode::NOT_MODIFIED).finish())
            }
            Ok(UpdateCheckResult::NotFound) => return Ok(HttpResponse::NotFound().finish()),
            Err(e) => return Err(e),
        }
    }

    let record = collection.get(id, auth.map(|a| a.into()).as_ref()).await;

    match record {
        Ok(Some(record)) => Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(indexer::record_to_json(record).unwrap())),
        Ok(None) => Ok(HttpResponse::NotFound().body("Record not found")),
        Err(e) => Err(e),
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
#[derive(Clone, Copy)]
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

#[serde_as]
#[derive(Deserialize)]
struct ListQuery {
    limit: Option<usize>,
    #[serde(default, rename = "where")]
    #[serde_as(as = "serde_with::json::JsonString")]
    where_query: indexer::WhereQuery,
    #[serde(default)]
    #[serde_as(as = "serde_with::json::JsonString")]
    sort: Vec<(String, Direction)>,
    before: Option<indexer::Cursor>,
    after: Option<indexer::Cursor>,
    /// UNIX timestamp in seconds
    since: Option<f64>,
    #[serde(rename = "waitFor", default = "Seconds::sixty")]
    wait_for: Seconds,
}

#[derive(Serialize)]
struct ListResponse {
    data: Vec<serde_json::Value>,
    cursor_before: Option<indexer::Cursor>,
    cursor_after: Option<indexer::Cursor>,
}

#[get("/{collection}/records")]
async fn get_records(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<ListQuery>,
    body: auth::SignedJSON<()>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let collection = path.into_inner();
    let auth = body.auth;
    let collection = state.indexer.collection(collection).await.unwrap();

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

            Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(false)
        }
        .await;

        match was_updated {
            Ok(true) => {}
            Ok(false) => return Ok(HttpResponse::Ok().status(StatusCode::NOT_MODIFIED).finish()),
            Err(e) => return Err(e),
        }
    }

    let list_response = async {
        let records = collection
            .list(
                indexer::ListQuery {
                    limit: Some(min(1000, query.limit.unwrap_or(1000))),
                    where_query: query.where_query.clone(),
                    order_by: &sort_indexes,
                    cursor_after: query.after.clone(),
                    cursor_before: query.before.clone(),
                },
                &auth.map(indexer::AuthUser::from).as_ref(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(ListResponse {
            cursor_before: records.first().map(|(c, _)| c.clone()),
            cursor_after: records.last().map(|(c, _)| c.clone()),
            data: records
                .into_iter()
                .map(|(_, r)| indexer::record_to_json(r).unwrap())
                .collect(),
        })
    }
    .await;

    match list_response {
        Ok(list_response) => Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(list_response)),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionCall {
    args: Vec<serde_json::Value>,
}

#[post("/{collection}/records")]
async fn post_record(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: auth::SignedJSON<FunctionCall>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let collection = path.into_inner();
    let auth = body.auth;

    let auth = auth.map(indexer::AuthUser::from);
    let changes = state
        .gateway
        .call(
            &state.indexer,
            collection,
            "constructor",
            "".to_string(),
            body.data.args,
            auth.as_ref(),
        )
        .await;

    let changes = match changes {
        Ok(changes) => changes,
        Err(e) => return Ok(HttpResponse::InternalServerError().body(e.to_string())),
    };

    for change in changes {
        match change {
            Change::Create {
                collection_id,
                record_id,
                record,
            } => {
                let collection = state.indexer.collection(collection_id).await.unwrap();
                collection.set(record_id, &record).await.unwrap();
            }
            Change::Update {
                collection_id,
                record_id,
                record,
            } => {
                let collection = state.indexer.collection(collection_id).await.unwrap();
                collection.set(record_id, &record).await.unwrap();
            }
            Change::Delete {
                collection_id,
                record_id,
            } => {
                let collection = state.indexer.collection(collection_id).await.unwrap();
                collection.delete(record_id).await.unwrap();
            }
        }
    }

    Ok(HttpResponse::Ok().body("Record created"))
}

#[post("/{collection}/records/{record}/call/{function}")]
async fn call_function(
    state: web::Data<AppState>,
    path: web::Path<(String, String, String)>,
    body: auth::SignedJSON<FunctionCall>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let (collection, record, function) = path.into_inner();
    let auth = body.auth;

    let auth = auth.map(indexer::AuthUser::from);
    let changes = state
        .gateway
        .call(
            &state.indexer,
            collection,
            &function,
            record,
            body.data.args,
            auth.as_ref(),
        )
        .await;

    let changes = match changes {
        Ok(changes) => changes,
        Err(e) => return Ok(HttpResponse::InternalServerError().body(e.to_string())),
    };

    for change in changes {
        match change {
            Change::Create {
                collection_id,
                record_id,
                record,
            } => {
                let collection = state.indexer.collection(collection_id).await.unwrap();
                collection.set(record_id, &record).await.unwrap();
            }
            Change::Update {
                collection_id,
                record_id,
                record,
            } => {
                let collection = state.indexer.collection(collection_id).await.unwrap();
                collection.set(record_id, &record).await.unwrap();
            }
            Change::Delete {
                collection_id,
                record_id,
            } => {
                let collection = state.indexer.collection(collection_id).await.unwrap();
                collection.delete(record_id).await.unwrap();
            }
        }
    }

    Ok(HttpResponse::Ok().body("Function called"))
}

#[get("/v0/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config = Config::parse();

    let indexer = Arc::new(
        Indexer::new(format!(
            "{}/polybase-indexer-data",
            std::env::temp_dir().to_str().unwrap()
        ))
        .unwrap(),
    );

    let gateway = Arc::new(gateway::initialize());

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                indexer: Arc::clone(&indexer),
                gateway: Arc::clone(&gateway),
            }))
            .service(root)
            .service(
                web::scope("/v0/collections")
                    .service(get_record)
                    .service(get_records)
                    .service(post_record)
                    .service(call_function),
            )
    })
    .bind(config.rpc_laddr)?
    .run()
    .await
}
