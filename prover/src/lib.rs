use abi::Parser;
use actix_web::{dev::Server, get, post, web, App, HttpResponse, HttpServer, Responder};
use base64::Engine;
use polylang_prover::{compile_program, hash_this, Inputs, ProgramExt};
use serde::Deserialize;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

fn hash<T>(obj: T) -> u64
where
    T: Hash,
{
    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProveRequest {
    miden_code: String,
    abi: abi::Abi,
    ctx_public_key: Option<abi::publickey::Key>,
    this: Option<serde_json::Value>,
    args: Vec<serde_json::Value>,
}

#[tracing::instrument(skip_all, fields(
    miden_code_hash = %hash(&req.miden_code),
    abi = ?req.abi,
    ctx_public_key = ?req.ctx_public_key,
    this = ?req.this,
    args = ?req.args,
))]
#[post("/prove")]
async fn prove(req: web::Json<ProveRequest>) -> Result<impl Responder, actix_web::Error> {
    let program = compile_program(&req.abi, &req.miden_code).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("failed to compile program: {}", e))
    })?;

    let this = req
        .this
        .clone()
        .unwrap_or(req.abi.default_this_value()?.into());

    let this_hash = hash_this(
        req.abi.this_type.clone().ok_or_else(|| {
            actix_web::error::ErrorInternalServerError("ABI is missing `this` type")
        })?,
        &req.abi
            .this_type
            .as_ref()
            .ok_or_else(|| {
                actix_web::error::ErrorInternalServerError("ABI is missing `this` type")
            })?
            .parse(&this)?,
    )?;

    let inputs = Inputs {
        abi: req.abi.clone(),
        ctx_public_key: req.ctx_public_key.clone(),
        this: this.clone(),
        this_hash,
        args: req.args.clone(),
    };

    let output = polylang_prover::prove(&program, &inputs)?;

    let program_info = program.to_program_info_bytes();
    let new_this = Into::<serde_json::Value>::into(output.new_this);

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "old": {
            "this": this,
            "hash": inputs.this_hash,
        },
        "new": {
            "selfDestructed": output.self_destructed,
            "this": new_this,
            "hash": output.new_hash,
        },
        "stack": {
            "input": inputs.stack_values(),
            "output": output.stack,
        },
        "programInfo": base64::engine::general_purpose::STANDARD.encode(program_info),
        "proof": base64::engine::general_purpose::STANDARD.encode(output.proof),
        "debug": {
            "logs": output.run_output.logs(),
        }
    })))
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().body("Polybase Prover Service")
}

pub fn server(addr: &str) -> std::io::Result<Server> {
    Ok(HttpServer::new(|| App::new().service(index).service(prove))
        .bind(addr)?
        .run())
}
