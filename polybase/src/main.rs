mod auth;

use std::{
    cmp::{max, min},
    sync::Arc,
};

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use gateway::Gateway;
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

#[get("/{collection}/records/{id}")]
async fn get_record(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    body: auth::SignedJSON<()>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let (collection, id) = path.into_inner();
    let auth = body.auth;

    let indexer = Arc::clone(&state.indexer);
    let record = web::block(move || {
        let collection = indexer.collection(collection)?;
        let record = collection.get(id, auth.map(|a| a.into()).as_ref())?;

        Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(
            record.map(|r| serde_json::to_string(r.borrow_record()).unwrap()),
        )
    })
    .await?;

    match record {
        Ok(Some(record)) => Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(record)),
        Ok(None) => Ok(HttpResponse::NotFound().body("Record not found")),
        Err(e) => Err(e),
    }
}

#[serde_as]
#[derive(Deserialize)]
struct ListQuery {
    limit: Option<usize>,
    #[serde(default, rename = "where")]
    #[serde_as(as = "serde_with::json::JsonString")]
    where_query: indexer::WhereQuery<'static>,
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

    let indexer = Arc::clone(&state.indexer);
    let records = web::block(move || {
        let collection = indexer.collection(collection)?;
        let auth = auth.map(indexer::AuthUser::from);
        let auth_ref = &auth.as_ref();
        let records = collection
            .list(
                &indexer::ListQuery {
                    limit: Some(min(1000, query.limit.unwrap_or(1000))),
                    where_query: &query.where_query,
                    order_by: &[],
                },
                auth_ref,
            )?
            .collect::<Result<Vec<_>, _>>()?;

        let borrowed_records = records
            .iter()
            .map(|r| r.borrow_record())
            .collect::<Vec<_>>();

        Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(serde_json::to_string(
            &borrowed_records,
        )?)
    })
    .await?;

    match records {
        Ok(records) => Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(records)),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionCall {
    #[serde(borrow)]
    args: Vec<indexer::RecordValue<'static>>,
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
        gateway.call(
            &indexer,
            collection,
            "constructor",
            "".to_string(),
            body.args,
            auth.map(|a| a.into()).as_ref(),
        )
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
        gateway.call(
            &indexer,
            collection,
            &function,
            record,
            body.args,
            auth.map(|a| a.into()).as_ref(),
        )
    })
    .await?;

    match res {
        Ok(()) => Ok(HttpResponse::Ok().body("Function called")),
        Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
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
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
