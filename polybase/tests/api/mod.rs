mod array_field;
mod auth;
mod boolean_field;
mod bytes_field;
mod call;
mod collection_collection;
mod errors;
mod general_collection;
mod index_record_refs;
mod index_where_sort;
mod map_field;
mod nested_field;
mod other_collection_fns;
mod restrict_namespaces;
mod schema_field_update;
mod schema_index_update;
mod start_stop;
mod store_other_collection_records;
mod whitelist;

use std::{
    collections::HashSet,
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use once_cell::sync::Lazy;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use sha3::Digest;

fn find_binary() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../target/debug/polybase");
    path
}

#[derive(Debug, Serialize, Deserialize)]
struct RecordResponse<T> {
    data: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse<T> {
    data: Vec<RecordResponse<T>>,
    cursor: ListCursor,
}

impl<T> ListResponse<T> {
    fn into_record_data(self) -> Vec<T> {
        self.data.into_iter().map(|r| r.data).collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ListCursor {
    before: Option<String>,
    after: Option<String>,
}

#[derive(Debug, Default)]
struct ListQuery {
    where_query: Option<serde_json::Value>,
    sort: Option<serde_json::Value>,
    limit: Option<u32>,
    before: Option<String>,
    after: Option<String>,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct Error {
    error: ErrorData,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct ErrorData {
    code: String,
    reason: String,
    message: String,
}

struct PortPool {
    ports: HashSet<u16>,
}

static PORT_POOL: Lazy<Mutex<PortPool>> = once_cell::sync::Lazy::new(|| {
    Mutex::new(PortPool {
        ports: (8081..8081 + 1000).collect(),
    })
});

impl PortPool {
    fn get(&mut self) -> u16 {
        let port = *self.ports.iter().next().expect("No ports left");
        self.ports.remove(&port);
        port
    }

    fn release(&mut self, port: u16) {
        self.ports.insert(port);
    }
}

#[derive(Debug, Default)]
struct ServerConfig {
    whitelist: Option<Vec<String>>,
    keep_port_after_drop: bool,
    restrict_namespaces: bool,
}

#[derive(Debug)]
struct Server {
    process: std::process::Child,
    // Keep the root dir alive so that polybase can use it
    _root_dir: tempfile::TempDir,
    api_port: u16,
    keep_port_after_drop: bool,
    client: reqwest::Client,
    base_url: reqwest::Url,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.process.kill().expect("Failed to stop polybase");
        if !self.keep_port_after_drop {
            PORT_POOL.lock().unwrap().release(self.api_port);
        }
    }
}

impl Server {
    fn setup(config: Option<ServerConfig>) -> Arc<Self> {
        let root_dir: tempfile::TempDir =
            tempfile::tempdir().expect("Failed to create temp root dir");
        let api_port = PORT_POOL.lock().unwrap().get();

        let mut command = Command::new(find_binary());

        if let Some(ref config) = config {
            if let Some(ref whitelist) = config.whitelist {
                command.arg("--whitelist").arg(whitelist.join(","));
            }

            if config.restrict_namespaces {
                command.arg("--restrict-namespaces");
            }
        }

        command.arg("--root-dir").arg(root_dir.path());
        command
            .arg("--rpc-laddr")
            .arg(format!("127.0.0.1:{api_port}"));

        if !std::env::var("LOG_POLYBASE_OUTPUT")
            .map(|v| v == "1")
            .unwrap_or(false)
        {
            command
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
        }

        let process = command.spawn().expect("Failed to start polybase");

        Arc::new(Self {
            process,
            _root_dir: root_dir,
            client: reqwest::Client::new(),
            base_url: format!("http://localhost:{api_port}").parse().unwrap(),
            keep_port_after_drop: config.map(|c| c.keep_port_after_drop).unwrap_or(false),
            api_port,
        })
    }

    async fn setup_and_wait(config: Option<ServerConfig>) -> Arc<Self> {
        let server = Self::setup(config);
        server.wait().await.expect("Failed to wait for server");
        server
    }

    fn collection<T: DeserializeOwned>(self: &Arc<Self>, id: &str) -> Collection<T> {
        Collection::new(Arc::clone(self), id.to_owned())
    }

    fn collection_untyped(self: &Arc<Self>, id: &str) -> Collection<serde_json::Value> {
        Collection::new(Arc::clone(self), id.to_owned())
    }

    async fn wait(&self) -> Result<(), Box<dyn std::error::Error>> {
        let time_between_requests = std::time::Duration::from_millis(100);
        let max_retries = 10000 / time_between_requests.as_millis() as usize;

        let mut retry = 0;
        loop {
            let is_last_retry = retry == max_retries - 1;

            let req = self
                .client
                .get(self.base_url.join("/v0/health").unwrap())
                .build()
                .unwrap();

            match self.client.execute(req).await {
                Ok(res) if res.status().is_success() => return Ok(()),
                Ok(res) if is_last_retry => {
                    return Err(format!("Failed to get health: {}", res.status()).into())
                }
                Ok(_) => {}
                Err(err) if is_last_retry => return Err(err.into()),
                Err(_) => {}
            }

            tokio::time::sleep(time_between_requests).await;
            retry += 1;
        }
    }

    async fn get_record<T: DeserializeOwned>(
        &self,
        collection: &str,
        record: &str,
        signer: Option<&Signer>,
    ) -> Result<RecordResponse<T>, Error> {
        let req = self.client.get(
            self.base_url
                .join(&format!(
                    "/v0/collections/{}/records/{}",
                    urlencoding::encode(collection),
                    urlencoding::encode(record)
                ))
                .unwrap(),
        );

        let req = if let Some(signer) = signer {
            req.header("X-Polybase-Signature", signer("").to_header())
        } else {
            req
        };

        let req = req.build().unwrap();

        let res = self.client.execute(req).await.unwrap();

        if res.status().is_success() {
            Ok(res.json().await.unwrap())
        } else {
            Err(res.json().await.unwrap())
        }
    }

    async fn call<T: DeserializeOwned>(
        &self,
        collection: &str,
        record: &str,
        function: &str,
        args: serde_json::Value,
        signer: Option<&Signer>,
    ) -> Result<RecordResponse<T>, Error> {
        let body = json!({
            "args": args,
        });
        let body = serde_json::to_string_pretty(&body).unwrap();

        let req = self
            .client
            .post(
                self.base_url
                    .join(&format!(
                        "/v0/collections/{}/records/{}/call/{}",
                        urlencoding::encode(collection),
                        urlencoding::encode(record),
                        urlencoding::encode(function)
                    ))
                    .unwrap(),
            )
            .header("Content-Type", "application/json")
            .body(body.clone());

        let req = if let Some(signer) = signer {
            req.header("X-Polybase-Signature", signer(&body).to_header())
        } else {
            req
        };

        let req = req.build().unwrap();

        let res = self.client.execute(req).await.unwrap();

        if res.status().is_success() {
            Ok(res.json().await.unwrap())
        } else {
            Err(res.json().await.unwrap())
        }
    }

    async fn create_record<T: DeserializeOwned>(
        &self,
        collection: &str,
        args: serde_json::Value,
        signer: Option<&Signer>,
    ) -> Result<RecordResponse<T>, Error> {
        let body = json!({
            "args": args,
        });
        let body = serde_json::to_string_pretty(&body).unwrap();

        let req = self
            .client
            .post(
                self.base_url
                    .join(&format!(
                        "/v0/collections/{}/records",
                        urlencoding::encode(collection)
                    ))
                    .unwrap(),
            )
            .header("Content-Type", "application/json")
            .body(body.clone());

        let req = if let Some(signer) = signer {
            req.header("X-Polybase-Signature", signer(&body).to_header())
        } else {
            req
        };

        let req = req.build().unwrap();

        let res = self.client.execute(req).await.unwrap();

        if res.status().is_success() {
            let json = res.text().await.unwrap();
            match serde_json::from_str(&json) {
                Ok(res) => Ok(res),
                Err(err) => {
                    panic!("Failed to parse response: {}, body: {}", err, json);
                }
            }
        } else {
            Err(res.json().await.unwrap())
        }
    }

    async fn update_record<T: DeserializeOwned>(
        &self,
        collection: &str,
        record: &str,
        args: serde_json::Value,
        signer: Option<&Signer>,
    ) -> Result<RecordResponse<T>, Error> {
        let body = json!({
            "args": args,
        });
        let body = serde_json::to_string_pretty(&body).unwrap();

        let req = self
            .client
            .post(
                self.base_url
                    .join(&format!(
                        "/v0/collections/{}/records/{}/call/updateCode",
                        urlencoding::encode(collection),
                        urlencoding::encode(record),
                    ))
                    .unwrap(),
            )
            .header("Content-Type", "application/json")
            .body(body.clone());

        let req = if let Some(signer) = signer {
            req.header("X-Polybase-Signature", signer(&body).to_header())
        } else {
            req
        };

        let req = req.build().unwrap();

        let res = self.client.execute(req).await.unwrap();

        if res.status().is_success() {
            let json = res.text().await.unwrap();
            match serde_json::from_str(&json) {
                Ok(res) => Ok(res),
                Err(err) => {
                    panic!("Failed to parse response: {}, body: {}", err, json);
                }
            }
        } else {
            Err(res.json().await.unwrap())
        }
    }

    async fn list_records<T: DeserializeOwned>(
        &self,
        collection: &str,
        query: ListQuery,
        signer: Option<&Signer>,
    ) -> Result<ListResponse<T>, Error> {
        let mut query_kv = vec![];
        if let Some(where_query) = query.where_query {
            query_kv.push(("where", serde_json::to_string_pretty(&where_query).unwrap()));
        }
        if let Some(sort) = query.sort {
            query_kv.push(("sort", serde_json::to_string_pretty(&sort).unwrap()));
        }
        if let Some(cursor_before) = query.before {
            query_kv.push(("before", cursor_before));
        }
        if let Some(cursor_after) = query.after {
            query_kv.push(("after", cursor_after));
        }
        if let Some(limit) = query.limit {
            query_kv.push(("limit", limit.to_string()));
        }

        let req = self
            .client
            .get(
                self.base_url
                    .join(&format!(
                        "/v0/collections/{}/records",
                        urlencoding::encode(collection)
                    ))
                    .unwrap(),
            )
            .query(&query_kv);

        let req = if let Some(signer) = signer {
            req.header("X-Polybase-Signature", signer("").to_header())
        } else {
            req
        };

        let req = req.build().unwrap();

        let res = self.client.execute(req).await.unwrap();

        if res.status().is_success() {
            Ok(res.json().await.unwrap())
        } else {
            Err(res.json().await.unwrap())
        }
    }

    async fn create_collection<T: DeserializeOwned>(
        self: &Arc<Self>,
        collection: &str,
        schema: &str,
        signer: Option<&Signer>,
    ) -> Result<Collection<T>, Error> {
        self.create_record::<serde_json::Value>("Collection", json!([collection, schema]), signer)
            .await?;

        Ok(self.collection(collection))
    }

    async fn update_collection<T: DeserializeOwned>(
        self: &Arc<Self>,
        collection: &str,
        schema: &str,
        signer: Option<&Signer>,
    ) -> Result<Collection<T>, Error> {
        self.update_record::<serde_json::Value>("Collection", collection, json!([schema]), signer)
            .await?;

        Ok(self.collection(collection))
    }

    async fn update_collection_untyped(
        self: &Arc<Self>,
        collection: &str,
        schema: &str,
        signer: Option<&Signer>,
    ) -> Result<Collection<serde_json::Value>, Error> {
        self.update_collection(collection, schema, signer).await
    }

    async fn create_collection_untyped(
        self: &Arc<Self>,
        collection: &str,
        schema: &str,
        signer: Option<&Signer>,
    ) -> Result<Collection<serde_json::Value>, Error> {
        self.create_collection(collection, schema, signer).await
    }
}

#[derive(Debug)]
struct Collection<T: DeserializeOwned> {
    id: String,
    server: Arc<Server>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned> Collection<T> {
    fn new(server: Arc<Server>, id: String) -> Self {
        Self {
            id,
            server,
            _phantom: std::marker::PhantomData,
        }
    }

    async fn call(
        &self,
        record: &str,
        function: &str,
        args: serde_json::Value,
        signer: Option<&Signer>,
    ) -> Result<Option<T>, Error> {
        let res = self
            .server
            .call(&self.id, record, function, args, signer)
            .await?;

        Ok(res.data)
    }

    async fn create(&self, args: serde_json::Value, signer: Option<&Signer>) -> Result<T, Error> {
        let res = self.server.create_record(&self.id, args, signer).await?;

        Ok(res.data)
    }

    async fn get(&self, record: &str, signer: Option<&Signer>) -> Result<T, Error> {
        self.server
            .get_record(&self.id, record, signer)
            .await
            .map(|res| res.data)
    }

    async fn list(
        &self,
        query: ListQuery,
        signer: Option<&Signer>,
    ) -> Result<ListResponse<T>, Error> {
        self.server.list_records(&self.id, query, signer).await
    }
}
struct Signature {
    public_key: Option<indexer::PublicKey>,
    signature: secp256k1::ecdsa::RecoverableSignature,
    timestamp: SystemTime,
    version: String,
    hash: String,
}

impl Signature {
    fn to_header(&self) -> String {
        format!(
            "{pk_eq_public_key_comma}sig=0x{sig},t={timestamp},v={version},h={hash}",
            pk_eq_public_key_comma = self
                .public_key
                .as_ref()
                .map(|pk| {
                    let pk = pk.to_secp256k1_key().unwrap();
                    format!("pk=0x{},", hex::encode(pk.serialize().as_slice()))
                })
                .unwrap_or_default(),
            sig = hex::encode({
                let mut rec_sig = vec![];
                let sig = self.signature.serialize_compact();
                rec_sig.extend_from_slice(&sig.1);
                rec_sig.push(sig.0.to_i32() as u8 + 27);
                rec_sig
            }),
            timestamp = (self
                .timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                * 1000),
            version = self.version,
            hash = self.hash,
        )
    }

    fn create(key: &secp256k1::SecretKey, time: SystemTime, body: &str) -> Self {
        let timestamp = (time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            * 1000)
            .to_string();

        let message_content = format!("{timestamp}.{body}");
        let message_content_length = message_content.len().to_string();

        let message_parts = &[
            "\u{19}Ethereum Signed Message:\n".as_bytes(),
            message_content_length.as_bytes(),
            message_content.as_bytes(),
        ];

        let mut hasher = sha3::Keccak256::new();
        for part in message_parts {
            hasher.update(part);
        }

        let message_hash = hasher.finalize();
        let message = secp256k1::Message::from_slice(&message_hash).unwrap();

        let sig = secp256k1::global::SECP256K1.sign_ecdsa_recoverable(&message, key);

        let public_key = key.public_key(secp256k1::global::SECP256K1);
        let public_key = indexer::PublicKey::from_secp256k1_key(&public_key).unwrap();

        Self {
            public_key: Some(public_key),
            signature: sig,
            timestamp: time,
            version: "0".to_string(),
            hash: "eth-personal-sign".to_string(),
        }
    }
}

struct Signer(Box<dyn Fn(&str) -> Signature>);

impl<T: Fn(&str) -> Signature + 'static> From<T> for Signer {
    fn from(f: T) -> Self {
        Self(Box::new(f))
    }
}

impl std::ops::Deref for Signer {
    type Target = dyn Fn(&str) -> Signature;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ForeignRecordReference {
    collection_id: String,
    id: String,
}
