//! Module for the DB adaptor.

use crate::{indexer::Collection, CollectionRecord, Column};
use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error(transparent)]
    Postgres(#[from] postgres::PostgresError),
}

pub type Result<T> = std::result::Result<T, DbError>;

mod models {
    use std::collections::HashMap;

    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use serde_json::{Map, Number, Value};
    use sqlx::{postgres::PgRow, types::Json, Column, FromRow, Row, Type, ValueRef};

    #[derive(Debug, FromRow, Serialize, Deserialize)]
    pub struct Collection {
        pub id: String,
        pub code: String,
        pub ast: Value,
        #[serde(rename = "publicKey")]
        pub public_key: Option<Value>,
        #[serde(rename = "createdAt")]
        pub created_at: Option<DateTime<Utc>>,
        #[serde(rename = "updatedAt")]
        pub updated_at: Option<DateTime<Utc>>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct CollectionRecord {
        pub data: Value,
    }

    impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for CollectionRecord {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            let mut data = serde_json::Map::new();

            for column in row.columns() {
                let column_name = column.name();

                match row.try_get::<String, _>(column_name) {
                    Ok(val) => {
                        data.insert(column_name.to_owned(), serde_json::Value::String(val));
                    }

                    Err(e) => match row.try_get::<sqlx::types::BigDecimal, _>(column_name) {
                        Ok(val) => {
                            data.insert(
                                column_name.to_owned(),
                                serde_json::from_str(&val.to_string()).unwrap(),
                            );
                        }
                        Err(e) => panic!("{e:?}"),
                    },
                }
            }

            Ok(CollectionRecord {
                data: Value::Object(data),
            })
        }
    }
}

pub mod postgres;

#[async_trait]
pub trait DbInit {
    async fn init() -> Self;
}

#[async_trait]
pub trait Db: DbInit {
    async fn shutdown(&self) -> Result<()>;
    async fn get_collections(&self) -> Result<Vec<Collection>>;
    async fn make_collection(&self, coll: Collection, columns: Vec<Column>) -> Result<Collection>;
    async fn get_collection(&self, collection_id: &str) -> Result<Option<Collection>>;
    async fn get_collection_records(&self, collection_id: &str) -> Result<Vec<CollectionRecord>>;
    async fn get_collection_record(
        &self,
        collection_id: &str,
        collection_record_id: &str,
    ) -> Result<CollectionRecord>;
    async fn make_collection_record(
        &self,
        collection_id: &str,
        collection_record: CollectionRecord,
    ) -> Result<CollectionRecord>;
}

pub(crate) mod helpers {
    use crate::util;
    use prost_wkt_types::Struct;

    pub fn convert_to_proto_collection(
        collection: &crate::db::models::Collection,
    ) -> crate::indexer::Collection {
        let mut proto_collection = crate::indexer::Collection::default();
        proto_collection.id = collection.id.clone();
        proto_collection.code = collection.code.clone();
        proto_collection.ast = Some(convert_to_proto_struct(&collection.ast));
        proto_collection.public_key = collection
            .public_key
            .as_ref()
            .map(|value| convert_to_proto_struct(value));
        proto_collection
    }

    pub fn convert_to_proto_collection_record(
        collection_record: &crate::db::models::CollectionRecord,
    ) -> crate::indexer::CollectionRecord {
        let mut proto_collection_record = crate::indexer::CollectionRecord::default();
        proto_collection_record.data = Some(convert_to_proto_struct(&collection_record.data));
        proto_collection_record
    }

    pub fn convert_to_proto_struct(value: &sqlx::types::JsonValue) -> Struct {
        let json_string = value.to_string();
        let prost_wkt_value: prost_wkt_types::Struct = serde_json::from_str(&json_string).unwrap();
        prost_wkt_types::Struct::from(prost_wkt_value)
    }

    pub fn get_collection_table_name(collection_id: &str) -> String {
        let collection_name = collection_id.rsplit('/').next().unwrap().to_string();
        println!("Collection name = {collection_name:?}");

        format!(
            "{}_{}",
            util::rpo::hash_and_encode(collection_id),
            collection_name
        )
        .to_lowercase()
    }

    pub fn extract_value_from_struct(struct_value: &Struct, key: &str) -> Option<String> {
        struct_value
            .fields
            .get(key)
            .and_then(|value| match &value.kind {
                Some(prost_wkt_types::value::Kind::StringValue(s)) => {
                    Some(format!("'{}'", s.clone()))
                }
                Some(prost_wkt_types::value::Kind::NumberValue(num)) => Some(num.to_string()),
                _ => None,
            })
    }
}
