extern crate lru;

use error::Result;
use indexer_db_adaptor::{
    adaptor::{self, IndexerAdaptor},
    where_query::WhereQuery,
    IndexerChange,
};
use row::CollectionRecordRow;
// use lru::LruCache;
use schema::{
    index::IndexField, record::RecordRoot, util::normalize_name, Schema, COLLECTION_SCHEMA,
};
use sqlx::postgres::{PgPool, PgRow};
use std::{pin::Pin, time::SystemTime};

mod error;
mod hash;
mod pg_type;
mod queries;
mod row;
mod util;

#[derive(Debug)]
pub struct PostgresAdaptor {
    pool: PgPool,
    // schemas: LruCache<String, Schema>,
}

impl PostgresAdaptor {
    pub async fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize the database, creating the collections table if it doesn't exist
    pub async fn init(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS Collection (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            ast TEXT NOT NULL,
            code TEXT NOT NULL,
            public_key JSONB
        )",
        )
        .execute(&self.pool)
        .await?;

        // TODO: create system table

        Ok(())
    }

    pub async fn get_row_by_id(&self, table_name: &str, record_id: &str) -> Result<Option<PgRow>> {
        Ok(sqlx::query(&format!(
            "SELECT * FROM {} WHERE id = $1",
            util::strip_invalid_chars(table_name)
        ))
        .bind(record_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_record_by_id(
        &self,
        collection_id: &str,
        record_id: &str,
        schema: &Schema,
    ) -> Result<Option<RecordRoot>> {
        let table_name = pg_collection_table_name(collection_id);
        match self.get_row_by_id(&table_name, record_id).await? {
            Some(row) => Ok(Some(row::pg_row_to_record_value(row, schema))),
            None => Ok(None),
        }
    }

    pub async fn get_collection_record_by_id(
        &self,
        collection_id: &str,
    ) -> Result<Option<CollectionRecordRow>> {
        let record =
            sqlx::query_as::<_, row::CollectionRecordRow>("SELECT * FROM Collection WHERE id = $1")
                .bind(collection_id)
                .fetch_optional(&self.pool)
                .await;
        Ok(record?)
    }

    async fn set(&self, collection_id: &str, record_id: &str, record: &RecordRoot) -> Result<()> {
        if collection_id == "Collection" {
            let old_schema = self.get_schema(record_id).await?;
            let new_schema = Schema::from_record(record);

            // return self.update_collection(record_id: &str, value: &RecordRoot).await;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl IndexerAdaptor for PostgresAdaptor {
    async fn commit(&self, height: usize, changes: Vec<IndexerChange>) -> adaptor::Result<()> {
        Ok(())
    }

    // TODO: we need to check permissions
    async fn get(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> adaptor::Result<Option<RecordRoot>> {
        if collection_id == "Collection" {
            return match self.get_row_by_id("Collection", record_id).await? {
                Some(row) => Ok(Some(row::pg_row_to_record_value(row, &COLLECTION_SCHEMA))),
                None => Ok(None),
            };
        }

        let schema = self.get_schema(collection_id).await?;
        match schema {
            Some(schema) => {
                let table_name = pg_collection_table_name(collection_id);
                match self.get_row_by_id(&table_name, record_id).await? {
                    Some(row) => Ok(Some(row::pg_row_to_record_value(row, &schema))),
                    None => Ok(None),
                }
            }
            // TODO: this should be an error that is defined by the Indexer
            None => Ok(None),
        }
    }

    async fn list(
        &self,
        collection_id: &str,
        limit: Option<usize>,
        where_query: WhereQuery<'_>,
        order_by: &[IndexField],
    ) -> adaptor::Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        todo!()
    }

    async fn get_schema(&self, collection_id: &str) -> adaptor::Result<Option<Schema>> {
        // if let Some(schema) = self.schemas.get(collection_id) {
        //     return Ok(Some(schema));
        // }

        let record = self.get_collection_record_by_id(collection_id).await?;

        match record {
            Some(record) => {
                let schema = Schema::from_json_str(&record.id, &record.ast)?;
                Ok(Some(schema))
                // self.schemas.put(collection_id.to_string(), schema);
                // Ok(self.schemas.get(collection_id))
            }
            None => Ok(None),
        }
    }

    async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> adaptor::Result<Option<SystemTime>> {
        todo!()
    }

    async fn last_collection_update(
        &self,
        collection_id: &str,
    ) -> adaptor::Result<Option<SystemTime>> {
        todo!()
    }

    async fn set_system_key(&self, key: &str, data: &RecordRoot) -> adaptor::Result<()> {
        todo!()
    }

    async fn get_system_key(&self, key: &str) -> adaptor::Result<Option<RecordRoot>> {
        todo!()
    }

    async fn destroy(&self) -> adaptor::Result<()> {
        todo!()
    }
}

// impl From<StoreError> for Error {
//     fn from(err: StoreError) -> Self {
//         Error::Store(err)
//     }
// }

// impl From<indexer_db_adaptor::collection::CollectionError> for Error {
//     fn from(err: indexer_db_adaptor::collection::CollectionError) -> Self {
//         Error::Store(StoreError::Collection(err))
//     }
// }

fn pg_collection_table_name(collection_id: &str) -> String {
    let collection_name = normalize_name(collection_id);

    format!(
        "{}_{}",
        hash::rpo::hash_and_encode(collection_id),
        collection_name
    )
    .to_lowercase()
}
