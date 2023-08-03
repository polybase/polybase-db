use indexer_db_adaptor::{
    collection::{
        ast::{collection_ast_from_json, collection_ast_from_record, fields_from_ast},
        record::RecordRoot,
        util::normalize_name,
        where_query::WhereQuery,
        CollectionError,
    },
    store::{Index, IndexDirection, IndexField, Result, Store},
};
use polylang::stableast;
use sqlx::{
    postgres::{PgPool, PgRow},
    Row,
};
use std::{pin::Pin, time::SystemTime};

mod hash;
mod queries;
mod row;
mod util;

// type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("collection error: {0}")]
    Collection(#[from] CollectionError),
}

#[derive(Debug, Clone)]
struct PostgresAdaptor {
    pool: PgPool,
}

impl PostgresAdaptor {
    pub async fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_by_id(
        &self,
        table_name: &str,
        record_id: &str,
    ) -> std::result::Result<Option<PgRow>, Error> {
        let res = sqlx::query(&format!(
            "SELECT * FROM {} WHERE id = $1",
            util::strip_invalid_chars(table_name)
        ))
        .bind(record_id)
        .fetch_one(&self.pool)
        .await;

        match res {
            Ok(row) => Ok(Some(row)),
            Err(sqlx::Error::RowNotFound) => return Ok(None),
            Err(e) => Err(e)?,
        }
    }

    pub async fn get_ast(&self, collection_id: &str) -> Result<stableast::Collection> {
        todo!()
        // let res = self.get_by_id("Collection", collection_id).await?;

        // match res {
        //     None => return Err(CollectionError::NotFound)?,
        //     Some(row) => {}
        // }

        // let code: String = res.get(0);
        // collection_ast_from_json(&code, &normalize_name(collection_id))
    }

    pub async fn create_collection(
        &self,
        collection_id: &str,
        value: &RecordRoot,
    ) -> std::result::Result<(), Error> {
        let collection_name = normalize_name(collection_id);
        // TODO: remove unwrap
        let ast = collection_ast_from_record(value, &collection_name)?;
        let fields = fields_from_ast(&ast);

        let res = sqlx::query(
            "CREATE TABLE IF NOT EXISTS $1 (
            id TEXT PRIMARY KEY,
            
        )",
        )
        .bind(pg_collection_table_name(collection_id))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // pub async fn update_collection() {
    //     // Get fields for the collection

    //     sqlx::query(
    //         "CREATE TABLE IF NOT EXISTS collection (
    //         id TEXT PRIMARY KEY,
    //         schema TEXT NOT NULL,
    //         data JSONB NOT NULL
    //     )",
    //     )
    //     .execute(&self.pool)
    //     .await?;
    // }
}

#[async_trait::async_trait]
impl Store for PostgresAdaptor {
    async fn commit(&self) -> Result<()> {
        todo!()
    }

    async fn set(&self, collection_id: &str, record_id: &str, value: &RecordRoot) -> Result<()> {
        if collection_id == "Collection" {
            let old_value = self.get(collection_id, record_id).await?;

            // return self.update_collection(record_id: &str, value: &RecordRoot).await;
        }

        Ok(())
    }

    async fn get(&self, collection_id: &str, record_id: &str) -> Result<Option<RecordRoot>> {
        let ast = self.get_ast(collection_id).await?;

        let res = sqlx::query(&format!(
            "SELECT * FROM {} WHERE id = $1",
            util::strip_invalid_chars(collection_id)
        ))
        .bind(record_id)
        .fetch_one(&self.pool)
        .await;

        match res {
            Ok(row) => Ok(Some(row::pg_row_to_record_value(row, &ast))),
            Err(sqlx::Error::RowNotFound) => return Ok(None),
            Err(e) => Err(Error::Sqlx(e))?,
        }
    }

    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField],
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        todo!()
    }

    async fn delete(&self, collection_id: &str, record_id: &str) -> Result<()> {
        todo!()
    }

    async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<SystemTime>> {
        todo!()
    }

    async fn last_collection_update(&self, collection_id: &str) -> Result<Option<SystemTime>> {
        todo!()
    }

    async fn set_system_key(&self, key: &str, data: &RecordRoot) -> Result<()> {
        todo!()
    }

    async fn get_system_key(&self, key: &str) -> Result<Option<RecordRoot>> {
        todo!()
    }

    async fn destroy(&self) -> Result<()> {
        todo!()
    }
}

impl From<Error> for indexer_db_adaptor::store::Error {
    fn from(err: Error) -> Self {
        Self(Box::new(err))
    }
}

fn pg_collection_table_name(collection_id: &str) -> String {
    let collection_name = normalize_name(collection_id);

    format!(
        "{}_{}",
        hash::rpo::hash_and_encode(collection_id),
        collection_name
    )
    .to_lowercase()
}
