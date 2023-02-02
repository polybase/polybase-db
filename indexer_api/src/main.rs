use std::{
    cell::Cell,
    sync::{Arc, Mutex, RwLock},
};

use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};

#[get("/{collection}/records/{id}")]
async fn get_record<'a>(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let (collection, id) = path.into_inner();

    let store = Arc::clone(&state.store);
    let record = web::block(move || {
        let collection = store.collection(collection)?;
        let record = collection.get(id, None)?;

        Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>(
            record.map(|r| r.get_slice().to_vec()),
        )
    })
    .await?;

    match record {
        Ok(Some(record)) => Ok(HttpResponse::Ok().body(record)),
        Ok(None) => Ok(HttpResponse::NotFound().body("Record not found")),
        Err(e) => Err(e),
    }
}

struct AppState {
    store: Arc<indexer::Store>,
}

trait StoreExt {
    fn collection(
        &self,
        name: String,
    ) -> Result<indexer::Collection, Box<dyn std::error::Error + Send + Sync + 'static>>;
}

impl StoreExt for indexer::Store {
    fn collection(
        &self,
        name: String,
    ) -> Result<indexer::Collection, Box<dyn std::error::Error + Send + Sync + 'static>> {
        indexer::Collection::load(&self, name)
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let store = Arc::new(
        indexer::Store::open(format!(
            "{}/polybase-indexer-data",
            std::env::temp_dir().to_str().unwrap()
        ))
        .unwrap(),
    );

    HttpServer::new(move || {
        let store = Arc::clone(&store);

        App::new()
            .app_data(web::Data::new(AppState { store }))
            .service(web::scope("/v0/collections").service(get_record))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
