mod config;

use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use clap::Parser;
use crate::config::Config;

#[get("/")]
async fn root() -> impl Responder {
    HttpResponse::Ok()
        .content_type("application/json")
        .body(r#"{ "server": "Polybase", "version": "0.1.0" }"#)
}

#[get("/v0/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config = Config::parse();

    HttpServer::new(|| App::new().service(root).service(health))
        .bind(config.rpc_laddr)?
        .run()
        .await
}
