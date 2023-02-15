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

    if let Some(since) = query.since {
        enum UpdateCheckResult {
            Updated,
            NotFound,
            NotModified,
        }

        let was_updated = web::block({
            let wait_for = min(Duration::from(query.wait_for), Duration::from_secs(60));
            let wait_until = SystemTime::now() + wait_for;
            let since = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(since);
            let indexer = Arc::clone(&state.indexer);
            let collection = collection.clone();
            let record_id = id.clone();

            move || {
                let collection = indexer.collection(collection)?;
                let mut record_exists = false;
                while wait_until > SystemTime::now() {
                    if let Some(metadata) = collection.get_record_metadata(&record_id)? {
                        record_exists = true;
                        if metadata.updated_at > since {
                            return Ok(UpdateCheckResult::Updated);
                        }
                    }

                    std::thread::sleep(Duration::from_millis(1000));
                }

                Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(if record_exists {
                    UpdateCheckResult::NotModified
                } else {
                    UpdateCheckResult::NotFound
                })
            }
        })
        .await?;

        match was_updated {
            Ok(UpdateCheckResult::Updated) => {}
            Ok(UpdateCheckResult::NotModified) => {
                return Ok(HttpResponse::Ok().status(StatusCode::NOT_MODIFIED).finish())
            }
            Ok(UpdateCheckResult::NotFound) => return Ok(HttpResponse::NotFound().finish()),
            Err(e) => return Err(e),
        }
    }

    let indexer = Arc::clone(&state.indexer);
    let record = web::block(move || {
        let collection = indexer.collection(collection)?;
        let record = collection.get(id, auth.map(|a| a.into()).as_ref())?;

        Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(record)
    })
    .await?;

    match record {
        Ok(Some(record)) => Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(record)),
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
    data: Vec<indexer::RecordRoot>,
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
        let was_updated = web::block({
            let wait_for = min(Duration::from(query.wait_for), Duration::from_secs(60));
            let wait_until = SystemTime::now() + wait_for;
            let since = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(since);
            let indexer = Arc::clone(&state.indexer);
            let collection = collection.clone();

            move || {
                let collection = indexer.collection(collection)?;
                while wait_until > SystemTime::now() {
                    if collection
                        .get_metadata()?
                        .map(|m| m.last_record_updated_at > since)
                        .unwrap_or(false)
                    {
                        return Ok(true);
                    }

                    std::thread::sleep(Duration::from_millis(1000));
                }

                Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(false)
            }
        })
        .await?;

        match was_updated {
            Ok(true) => {}
            Ok(false) => return Ok(HttpResponse::Ok().status(StatusCode::NOT_MODIFIED).finish()),
            Err(e) => return Err(e),
        }
    }

    let indexer = Arc::clone(&state.indexer);
    let list_response = web::block(move || {
        let collection = indexer.collection(collection)?;
        let auth = auth.map(indexer::AuthUser::from);
        let auth_ref = &auth.as_ref();
        let records = collection
            .list(
                indexer::ListQuery {
                    limit: Some(min(1000, query.limit.unwrap_or(1000))),
                    where_query: query.where_query.clone(),
                    order_by: &sort_indexes,
                    cursor_after: query.after.clone(),
                    cursor_before: query.before.clone(),
                },
                auth_ref,
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(ListResponse {
            cursor_before: records.first().map(|(c, _)| c.clone()),
            cursor_after: records.last().map(|(c, _)| c.clone()),
            data: records.into_iter().map(|(_, r)| r).collect(),
        })
    })
    .await?;

    match list_response {
        Ok(list_response) => Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(list_response)),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionCall {
    args: Vec<indexer::RecordValue>,
}

#[post("/{collection}/records")]
async fn post_record(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: auth::SignedJSON<serde_json::Value>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let collection = path.into_inner();
    let auth = body.auth;
    let body = FunctionCall::deserialize(body.data)?;

    let indexer = Arc::clone(&state.indexer);
    let gateway = Arc::clone(&state.gateway);

    let res = web::block(move || {
        let auth = auth.map(|a| a.into());

        let changes = match gateway.call(
            &indexer,
            collection,
            "constructor",
            "".to_string(),
            body.args,
            auth.as_ref(),
        ) {
            Ok(changes) => changes,
            Err(e) => return Err(e),
        };

        for change in changes {
            match change {
                Change::Create {
                    collection_id,
                    record_id,
                    record,
                } => {
                    let collection = indexer.collection(collection_id)?;
                    collection.set(record_id, &record, auth.as_ref())?;
                }
                Change::Update {
                    collection_id,
                    record_id,
                    record,
                } => {
                    let collection = indexer.collection(collection_id)?;
                    collection.set(record_id, &record, auth.as_ref())?;
                }
                Change::Delete { record_id: _ } => todo!(),
            }
        }

        Ok(())
    })
    .await?;

    match res {
        Ok(()) => Ok(HttpResponse::Ok().body("Record created")),
        Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
    }
}

#[post("/{collection}/records/{record}/call/{function}")]
async fn call_function(
    state: web::Data<AppState>,
    path: web::Path<(String, String, String)>,
    body: auth::SignedJSON<serde_json::Value>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let (collection, record, function) = path.into_inner();
    let auth = body.auth;
    let body = FunctionCall::deserialize(body.data)?;

    let indexer = Arc::clone(&state.indexer);
    let gateway = Arc::clone(&state.gateway);

    let res = web::block(move || {
        let auth = auth.map(|a| a.into());

        let changes = match gateway.call(
            &indexer,
            collection,
            &function,
            record,
            body.args,
            auth.as_ref(),
        ) {
            Ok(changes) => changes,
            Err(e) => return Err(e),
        };

        for change in changes {
            match change {
                Change::Create {
                    collection_id,
                    record_id,
                    record,
                } => {
                    let collection = indexer.collection(collection_id)?;
                    collection.set(record_id, &record, auth.as_ref())?;
                }
                Change::Update {
                    collection_id,
                    record_id,
                    record,
                } => {
                    let collection = indexer.collection(collection_id)?;
                    collection.set(record_id, &record, auth.as_ref())?;
                }
                Change::Delete { record_id: _ } => todo!(),
            }
        }

        Ok(())
    })
    .await?;

    match res {
        Ok(()) => Ok(HttpResponse::Ok().body("Function called")),
        Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
    }
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
