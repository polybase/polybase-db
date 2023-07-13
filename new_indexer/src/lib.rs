//! Module for the Indexer.

use indexer::indexer_server::Indexer;
use indexer::{
    CollectionRecord, CreateCollectionRecordRequest, CreateCollectionRecordResponse,
    CreateCollectionRequest, CreateCollectionResponse, GetCollectionRecordRequest,
    GetCollectionRecordResponse, GetCollectionRequest, GetCollectionResponse,
    ListCollectionRecordsRequest, ListCollectionRecordsResponse, ListCollectionsRequest,
    ListCollectionsResponse, ShutdownRequest, ShutdownResponse,
};
use std::sync::Arc;
use thiserror::Error;
use tonic::{Request, Response, Status};

pub mod indexer {
    include!(concat!(env!("OUT_DIR"), "/indexer.rs"));
}

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("indexer: could not get address to bind to")]
    IndexerAddr,

    #[error(transparent)]
    Db(#[from] db::DbError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    TonicTransport(#[from] tonic::transport::Error),

    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, IndexerError>;

// todo - visibility
pub mod db;
mod util;

use crate::indexer::Collection;
use db::Db;
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct IndexerService<D>
where
    D: Db,
{
    pub indexer: PolybaseIndexer<D>,
}

#[tonic::async_trait]
impl<D> Indexer for IndexerService<D>
where
    D: Db + Send + Sync + 'static,
{
    async fn shutdown(
        &self,
        _req: Request<ShutdownRequest>,
    ) -> std::result::Result<Response<ShutdownResponse>, Status> {
        println!("Got a request for: shutdown");

        let _ = self.indexer.shutdown().await;

        Ok(Response::new(ShutdownResponse {}))
    }

    async fn list_collections(
        &self,
        _req: Request<ListCollectionsRequest>,
    ) -> std::result::Result<Response<ListCollectionsResponse>, Status> {
        println!("Got a request for: list_collections");

        Ok(Response::new(ListCollectionsResponse {
            collections: self.indexer.list_collections().await.unwrap(), // todo
        }))
    }

    async fn create_collection(
        &self,
        req: Request<CreateCollectionRequest>,
    ) -> std::result::Result<Response<CreateCollectionResponse>, Status> {
        println!("Got a request for: create_collection");

        if let Some(collection) = req.into_inner().collection {
            match self.indexer.create_collection(collection).await {
                Ok(created_collection) => Ok(Response::new(CreateCollectionResponse {
                    collection: Some(created_collection),
                })),
                Err(e) => Err(Status::from_error(Box::new(e))),
            }
        } else {
            // todo
            Err(Status::invalid_argument("empty payload"))
        }
    }

    async fn get_collection(
        &self,
        req: Request<GetCollectionRequest>,
    ) -> std::result::Result<Response<GetCollectionResponse>, Status> {
        println!("Got a request for: get_collection");

        let collection_id = req.into_inner().collection_id;
        match self.indexer.get_collection(&collection_id).await {
            Ok(collection) => Ok(Response::new(GetCollectionResponse { collection })),
            Err(e) => Err(Status::from_error(Box::new(e))),
        }
    }

    async fn list_collection_records(
        &self,
        req: Request<ListCollectionRecordsRequest>,
    ) -> std::result::Result<Response<ListCollectionRecordsResponse>, Status> {
        println!("Got a request for: list_collection_records");

        let collection_id = req.into_inner().collection_id;
        match self.indexer.list_collection_records(&collection_id).await {
            Ok(collection_records) => Ok(Response::new(ListCollectionRecordsResponse {
                collection_records,
            })),
            Err(e) => Err(Status::from_error(Box::new(e))),
        }
    }

    async fn create_collection_record(
        &self,
        req: Request<CreateCollectionRecordRequest>,
    ) -> std::result::Result<Response<CreateCollectionRecordResponse>, Status> {
        println!("Got a request for: create_collection_record");

        let req = req.into_inner();
        let collection_id = req.collection_id;

        if let Some(collection_record) = req.collection_record {
            match self
                .indexer
                .create_collection_record(&collection_id, collection_record)
                .await
            {
                Ok(created_collection_record) => {
                    Ok(Response::new(CreateCollectionRecordResponse {
                        collection_record: Some(created_collection_record),
                    }))
                }
                Err(e) => Err(Status::from_error(Box::new(e))),
            }
        } else {
            // todo
            Err(Status::invalid_argument(
                "invalid payload - empty collection record",
            ))
        }
    }

    async fn get_collection_record(
        &self,
        req: Request<GetCollectionRecordRequest>,
    ) -> std::result::Result<Response<GetCollectionRecordResponse>, Status> {
        println!("Got a request for: get_collection_record");

        let req = req.into_inner();
        let (collection_id, collection_record_id) = (req.collection_id, req.collection_record_id);
        match self
            .indexer
            .get_collection_record(&collection_id, &collection_record_id)
            .await
        {
            Ok(collection_record) => Ok(Response::new(GetCollectionRecordResponse {
                collection_record: Some(collection_record),
            })),
            Err(e) => Err(Status::from_error(Box::new(e))),
        }
    }
}

#[derive(Debug, Default)]
pub struct Column {
    pub name: String,
    pub type_kind: String,
    pub type_value: Option<String>,
    pub required: bool,
}

/// The Indexer
#[derive(Default)]
pub struct PolybaseIndexer<D: Db> {
    db: Arc<D>,
}

impl<D> PolybaseIndexer<D>
where
    D: Db + Send + Sync,
{
    pub async fn new() -> Self {
        Self {
            db: Arc::new(D::init().await),
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.db.shutdown().await;

        Ok(())
    }

    pub async fn list_collections(&self) -> Result<Vec<Collection>> {
        println!("Indexer: in list_collections");

        Ok(self.db.get_collections().await?)
    }

    pub async fn create_collection(&self, coll: Collection) -> Result<Collection> {
        println!("Indexer: in create_collection");

        // parse the ast and create a map of fields (columns)
        let ast_str = serde_json::to_string(&coll.ast)?;
        let columns: Vec<Column> = self.parse_ast(&ast_str).await?;

        println!("columns = {columns:#?}");

        Ok(self.db.make_collection(coll, columns).await?)
    }

    async fn parse_ast(&self, ast_str: &str) -> Result<Vec<Column>> {
        let ast: Ast = serde_json::from_str(ast_str)?;

        Ok(ast
            .attributes
            .iter()
            .filter(|attr| attr.kind == "property")
            .map(|attr| Column {
                name: attr.name.clone(),
                type_kind: attr.type_.as_ref().unwrap().kind.clone(),
                type_value: attr.type_.as_ref().unwrap().value.clone(),
                required: attr.required.is_some(),
            })
            .collect::<Vec<_>>())
    }

    pub async fn get_collection(&self, collection_id: &str) -> Result<Option<Collection>> {
        println!("Indexer: in get_collection");

        Ok(self.db.get_collection(collection_id).await?)
    }

    pub async fn list_collection_records(
        &self,
        collection_id: &str,
    ) -> Result<Vec<CollectionRecord>> {
        println!("Indexer: in list_collection_records");

        Ok(self.db.get_collection_records(collection_id).await?)
    }

    pub async fn get_collection_record(
        &self,
        collection_id: &str,
        collection_record_id: &str,
    ) -> Result<CollectionRecord> {
        println!("Indexer: in get_collection_record");

        Ok(self
            .db
            .get_collection_record(collection_id, collection_record_id)
            .await?)
    }

    pub async fn create_collection_record(
        &self,
        collection_id: &str,
        collection_record: CollectionRecord,
    ) -> Result<CollectionRecord> {
        println!("Indexer: in create_collection_record");

        Ok(self
            .db
            .make_collection_record(collection_id, collection_record)
            .await?)
    }
}

// for parsing the sample JSON (the AST in particular).

#[derive(Debug, Serialize, Deserialize)]
pub struct Ast {
    pub kind: String,
    pub namespace: Namespace,
    pub name: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Namespace {
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attribute {
    pub kind: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: Option<Type>,
    pub directives: Option<Vec<Directive>>,
    pub required: Option<bool>,
    pub code: Option<String>,
    pub attributes: Option<Vec<Attribute>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Type {
    pub kind: String,
    pub value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Directive {
    pub kind: String,
    pub arguments: Vec<Argument>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Argument {
    pub kind: String,
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Default)]
pub struct Field {
    pub name: String,
    pub type_kind: String,
    pub type_value: Option<String>,
    pub required: bool,
}
