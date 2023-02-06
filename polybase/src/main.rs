use std::sync::Arc;

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use gateway::Gateway;
use indexer::Indexer;
use serde::{Deserialize, Serialize};

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
async fn get_record<'a>(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let (collection, id) = path.into_inner();

    let indexer = Arc::clone(&state.indexer);
    let record = web::block(move || {
        let collection = indexer.collection(collection)?;
        let record = collection.get(id, None)?;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionCall {
    #[serde(borrow)]
    args: Vec<indexer::RecordValue<'static>>,
}

#[post("/{collection}/records")]
async fn post_record(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let collection = path.into_inner();
    let body = FunctionCall::deserialize(body.0)?;

    let indexer = Arc::clone(&state.indexer);
    let gateway = Arc::clone(&state.gateway);

    let res = web::block(move || {
        gateway.call(
            &indexer,
            collection,
            "constructor",
            "".to_string(),
            body.args,
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
    body: web::Json<serde_json::Value>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let (collection, record, function) = path.into_inner();
    let body = FunctionCall::deserialize(body.0)?;

    let indexer = Arc::clone(&state.indexer);
    let gateway = Arc::clone(&state.gateway);

    let res = web::block(move || gateway.call(&indexer, collection, &function, record, body.args))
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
                    .service(post_record)
                    .service(call_function),
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
