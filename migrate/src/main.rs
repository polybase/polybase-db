mod config;

use anyhow::Result;
use clap::Parser;
use indexer::publickey::PublicKey;
use regex::Regex;
use secp256k1::PublicKey as Secp256k1PublicKey;
use serde::{Deserialize, Serialize};
use slog::Drain;
use std::path::PathBuf;
use std::str::FromStr;
use url::form_urlencoded::byte_serialize;

use crate::config::Config;

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse<T> {
    data: Vec<ListResponseItem<T>>,
    cursor: ListResponseCursor,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponseItem<T> {
    data: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponseCursor {
    before: String,
    after: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CollectionData {
    ast: Option<String>,
    code: String,
    id: String,
    #[serde(rename = "publicKey")]
    public_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewCollectionData {
    ast: String,
    name: String,
    code: String,
    id: String,
    #[serde(rename = "publicKey")]
    #[serde(skip_serializing_if = "Option::is_none")]
    public_key: Option<PublicKey>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog::slog_o!("version" => env!("CARGO_PKG_VERSION")));

    let config = Config::parse();

    let indexer_dir = get_indexer_dir(&config.root_dir);
    println!("Indexer store path: {}", indexer_dir.display());

    let indexer = indexer::Indexer::new(logger.clone(), indexer_dir.clone()).unwrap();
    indexer.destroy().unwrap();

    println!("Database reset");

    let indexer = indexer::Indexer::new(logger.clone(), indexer_dir).unwrap();
    let collection_collection = indexer.collection("Collection".into()).await?;

    // Get list of all collections data
    let collections = get_records::<CollectionData>(&config.migration_url, "Collection").await?;

    println!("Migrating {} collections", collections.len());

    let mut total_records = 0;

    for collection_data in &collections {
        let collection_id = &collection_data.data.id;
        let code = &collection_data.data.code;
        let public_key = &collection_data.data.public_key;

        if collection_id == "Collection" {
            continue;
        }

        println!("Migrating {:?}", &collection_id);

        let mut pk = None;
        if let Some(public_key_hex) = public_key {
            if !public_key_hex.is_empty() {
                pk = Some(convert_public_key(public_key_hex)?);
            }
        }

        let (code, ast) = migrate_code(code, namespace(collection_id));

        let col_col_as_str = collection_ast();
        let col_col_ast =
            indexer::collection::collection_ast_from_json(col_col_as_str.as_str(), "Collection")?;

        let new_collection_record = NewCollectionData {
            id: collection_id.clone(),
            name: name(collection_id).to_string(),
            ast: ast.clone(),
            code,
            public_key: pk,
        };
        let new_collection_record = serde_json::to_value(new_collection_record)?;
        let new_collection_record =
            indexer::json_to_record(&col_col_ast, new_collection_record, false)?;

        // println!("Collection record: {:?}\n", new_collection_record);

        collection_collection
            .set(collection_id.clone(), &new_collection_record)
            .await?;

        println!("Collection created: {:?}\n", &collection_id);

        // Get the indexer collection instance so we can insert records
        let collection = indexer.collection(collection_id.clone()).await?;

        let records =
            get_records::<serde_json::Value>(&config.migration_url, collection_id.as_str()).await?;
        let records_len = records.len();
        let col_ast =
            indexer::collection::collection_ast_from_json(&ast, name(collection_id).as_str())?;

        for record in records {
            let id = record.data.get("id").unwrap().as_str().unwrap();

            println!("Migrating record {:?}", &id);
            println!("Record data: {:?}\n", &record.data);

            let new_record = indexer::json_to_record(&col_ast, record.data.to_owned(), true)?;

            collection.set(id.to_string(), &new_record).await?;
        }

        println!("Migrated {:?}: {} records", collection_id, records_len);

        total_records += records_len;
    }

    println!("Migrated {} collections", collections.len());
    println!("Migrated {} records", total_records);

    Ok(())
}

fn convert_public_key(public_key_str: &str) -> Result<PublicKey> {
    let hex_str = normalize_hex(public_key_str);
    let res = hex::decode(&hex_str).unwrap();
    // hex::from_hex(hex_str.as_str()).unwrap();
    let pubkey = &Secp256k1PublicKey::from_str(hex_str.as_str()).unwrap();
    Ok(PublicKey::from_secp256k1_key(pubkey)?)
}

fn normalize_hex(hex: &str) -> String {
    let mut hex = hex.to_string();
    if hex.starts_with("0x") {
        hex = hex[2..].to_string();
    }
    format!("04{hex}")
}

fn migrate_code(code: &str, namespace: &str) -> (String, String) {
    let ctx_pk_assignment_regex = Regex::new(r"this.([\w\$]+) = ctx.publicKey;").unwrap();
    let ctx_pk_comparison_left_regex = Regex::new(r"ctx.publicKey != this.([\w\$]+)").unwrap();
    let ctx_pk_comparison_right_regex = Regex::new(r"this.([\w\$]+) != ctx.publicKey").unwrap();

    let code = ctx_pk_assignment_regex.replace_all(
        code,
        r"if (ctx.publicKey) this.${1} = ctx.publicKey.toHex(); else this.${1} = '';",
    );
    let code =
        ctx_pk_comparison_left_regex.replace_all(&code, r"ctx.publicKey.toHex() != this.${1}");
    let code =
        ctx_pk_comparison_right_regex.replace_all(&code, r"this.${1} != ctx.publicKey.toHex()");

    // Parse AST
    let ast_code = migrate_code_for_ast(&code);
    let mut program = None;
    let (_, stable_ast) = &polylang::parse(ast_code.as_str(), namespace, &mut program).unwrap();
    let stable_ast = serde_json::to_string(&stable_ast).unwrap();

    (code.to_string(), stable_ast)
}

fn migrate_code_for_ast(code: &str) -> String {
    let collection_definition_regex = Regex::new(r"collection ([\w\$]+) \{").unwrap();
    collection_definition_regex
        .replace_all(code, r"@public collection ${1} {")
        .to_string()
}

async fn get_records<T>(url: &str, collection_name: &str) -> Result<Vec<ListResponseItem<T>>>
where
    T: serde::de::DeserializeOwned,
{
    let mut records: Vec<ListResponseItem<T>> = vec![];
    let mut cursor: Option<String> = None;

    let base_url =
        format!("{}/v0/collections/{}/records", url, encode(collection_name)).to_string();

    loop {
        let mut url = base_url.clone();

        if let Some(after) = &cursor {
            url = format!("{}?after={}", url, encode(after));
        }

        let resp: ListResponse<T> = reqwest::Client::new().get(url).send().await?.json().await?;

        let len = resp.data.len();

        cursor = resp.cursor.after;

        records.extend(resp.data);

        // We've reached the end
        if len != 100 {
            break;
        }
    }

    Ok(records)
}

fn encode(s: &str) -> String {
    byte_serialize(s.as_bytes()).collect::<String>()
}

fn name(collection_id: &str) -> String {
    collection_id
        .split('/')
        .last()
        .unwrap()
        .to_string()
        .replace("-", "_")
}

fn namespace(collection_id: &str) -> &str {
    let Some(slash_index) = collection_id.rfind('/') else {
        return "";
    };

    &collection_id[0..slash_index]
}

fn collection_ast() -> String {
    let code = r#"
    @public
    collection Collection {
        id: string;
        name?: string;
        lastRecordUpdated?: string;
        code?: string;
        ast?: string;
        publicKey?: PublicKey;
    
        @index(publicKey);
        @index([lastRecordUpdated, desc]);
    
        constructor (id: string, code: string) {
            this.id = id;
            this.code = code;
            this.ast = parse(code, id);
            if (ctx.publicKey) this.publicKey = ctx.publicKey;
        }
    
        updateCode (code: string) {
            if (this.publicKey != ctx.publicKey) {
                throw error('invalid owner');
            }
            this.code = code;
            this.ast = parse(code, this.id);
        }
    }
    "#;
    let mut program = None;
    let (_, stable_ast) = polylang::parse(code, "", &mut program).unwrap();
    serde_json::to_string(&stable_ast).unwrap()
}

fn get_indexer_dir(dir: &str) -> PathBuf {
    let mut path_buf = PathBuf::new();
    if dir.starts_with("~/") {
        if let Some(home_dir) = dirs::home_dir() {
            path_buf.push(home_dir);
            path_buf.push(dir.strip_prefix("~/").unwrap());
        }
    }
    path_buf.push("data/indexer.db");
    path_buf
}
