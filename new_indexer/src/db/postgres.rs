//! PostgreSQL service

use super::{helpers, models, Db, DbError, DbInit, Result};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PostgresError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

struct DbState {
    pool: Pool<Postgres>,
}

// todo -visibility
pub struct PostgresDB {
    state: DbState,
}

#[async_trait::async_trait]
impl DbInit for PostgresDB {
    async fn init() -> Self {
        let database_url = std::env::var("DATABASE_URL").expect("could not find DATABASE_URL");
        println!("database url = {database_url:#?}");

        let max_conns = match std::env::var("MAX_CONNECTIONS") {
            Err(_) => 100,
            Ok(mc) => mc.parse::<u32>().unwrap_or(100),
        };

        let pool = match PgPoolOptions::new()
            .max_connections(max_conns)
            .connect(&database_url)
            .await
        {
            Err(e) => {
                eprintln!("{e:?}");
                std::process::exit(1);
            }
            Ok(pool) => {
                println!("Postgres pool established successfully");
                pool
            }
        };

        PostgresDB {
            state: DbState { pool: pool.clone() },
        }
    }
}

#[async_trait::async_trait]
impl Db for PostgresDB {
    async fn shutdown(&self) -> Result<()> {
        self.state.pool.close().await;

        Ok(())
    }

    /// Retrieve the collections in the DB
    async fn get_collections(&self) -> Result<Vec<crate::indexer::Collection>> {
        println!("Inside postgres get_collections");

        match sqlx::query_as::<_, models::Collection>("select * from collections")
            .fetch_all(&self.state.pool)
            .await
        {
            Ok(colls) => Ok(colls
                .into_iter()
                .map(|coll| helpers::convert_to_proto_collection(&coll))
                .collect::<Vec<_>>()),

            Err(e) => Err(DbError::from(PostgresError::from(e))),
        }
    }

    /// Create a new collection
    async fn make_collection(
        &self,
        collection: crate::indexer::Collection,
        columns: Vec<crate::Column>,
    ) -> Result<crate::indexer::Collection> {
        println!("Inside postgres make_collection");

        // run all operations in a transaction
        let mut txn = self
            .state
            .pool
            .begin()
            .await
            .map_err(|e| DbError::from(PostgresError::from(e)))?;

        // create the collection row in the `collections` table
        let ast_json: serde_json::Value = serde_json::from_str(
            &serde_json::to_string(&collection.ast).map_err(PostgresError::from)?,
        )
        .map_err(PostgresError::from)?;

        let public_key_json: Option<serde_json::Value> = if let Some(pk) = collection.public_key {
            serde_json::from_str(&serde_json::to_string(&pk).map_err(PostgresError::from)?)
                .map_err(PostgresError::from)?
        } else {
            None
        };

        let created_collection = sqlx::query_as::<_, models::Collection>(
            "insert into collections (id, code, ast, public_key) values ($1, $2, $3, $4) returning *")
            .bind(collection.id.to_string())
            .bind(collection.code.to_string())
            .bind( ast_json.to_owned())
            .bind( public_key_json.to_owned())
            .fetch_one(&mut txn)
            .await.and_then(|coll| Ok(helpers::convert_to_proto_collection(&coll))).map_err(|e| DbError::from(PostgresError::from(e)))?;

        // So here we simply create a new table for the collection using the attributes as columns

        let collection_table_name = helpers::get_collection_table_name(&collection.id);
        println!("collection_table_name = {collection_table_name:?}");

        let mut create_coll_table_query =
            format!("create table if not exists {} (", collection_table_name);

        for column in &columns {
            if column.name == "id" {
                create_coll_table_query.push_str("id text primary key,");
                continue;
            }

            let mut col_create_query = format!(
                "{} {}",
                column.name,
                match column.type_value.clone().unwrap().as_str() {
                    "string" => "text",
                    "number" => "numeric",
                    _ => panic!("unsupported type"),
                }
            );

            if column.required {
                col_create_query.push_str(" not null,");
            } else {
                col_create_query.push_str(",");
            }

            create_coll_table_query.push_str(&col_create_query);
        }

        if create_coll_table_query.ends_with(",") {
            create_coll_table_query.pop();
        }

        create_coll_table_query.push_str(")");

        sqlx::query(&create_coll_table_query)
            .execute(&mut txn)
            .await
            .map_err(|e| DbError::from(PostgresError::from(e)))?;

        // now add indexes on all the "columns" parsed in the indexer

        for column in &columns {
            let create_col_idx_query = format!(
                "create index idx_{}_{} on {}({})",
                collection_table_name, column.name, collection_table_name, column.name
            );
            println!("{create_col_idx_query}");

            sqlx::query(&create_col_idx_query)
                .execute(&mut txn)
                .await
                .map_err(|e| DbError::from(PostgresError::from(e)))?;
        }

        txn.commit()
            .await
            .map_err(|e| DbError::from(PostgresError::from(e)))?;

        Ok(created_collection)
    }

    /// Get the collection from the database, if available.
    async fn get_collection(
        &self,
        collection_id: &str,
    ) -> Result<Option<crate::indexer::Collection>> {
        println!("Inside postgres get_collection");

        println!("collection_id = {collection_id:?}");

        Ok(
            match sqlx::query_as::<_, models::Collection>("select * from collections where id = $1")
                .bind(collection_id)
                .fetch_one(&self.state.pool)
                .await
            {
                Err(e) => {
                    eprintln!("{e:?}");
                    None
                }
                Ok(coll) => Some(helpers::convert_to_proto_collection(&coll)),
            },
        )
    }

    /// Get the collection records for the given collection
    async fn get_collection_records(
        &self,
        collection_id: &str,
    ) -> Result<Vec<crate::indexer::CollectionRecord>> {
        println!("Inside postgres get_collection_records");

        let collection_table_name = helpers::get_collection_table_name(collection_id);
        let list_coll_recs_query = format!("select * from {}", collection_table_name);

        match sqlx::query_as::<_, models::CollectionRecord>(&list_coll_recs_query)
            .fetch_all(&self.state.pool)
            .await
        {
            Ok(coll_recs) => Ok(coll_recs
                .into_iter()
                .map(|coll_rec| helpers::convert_to_proto_collection_record(&coll_rec))
                .collect::<Vec<_>>()),

            Err(e) => Err(DbError::from(PostgresError::from(e))),
        }
    }

    /// Get the collection record, if available
    async fn get_collection_record(
        &self,
        collection_id: &str,
        collection_record_id: &str,
    ) -> Result<crate::indexer::CollectionRecord> {
        println!("Inside postgres get_collection_record");

        let collection_table_name = helpers::get_collection_table_name(collection_id);
        let get_coll_rec_query = format!(
            "select * from {} where id = '{}'",
            collection_table_name, collection_record_id
        );

        match sqlx::query_as::<_, models::CollectionRecord>(&get_coll_rec_query)
            .fetch_one(&self.state.pool)
            .await
        {
            Ok(coll_rec) => Ok(helpers::convert_to_proto_collection_record(&coll_rec)),
            Err(e) => {
                eprintln!("{e:?}");
                Err(DbError::from(PostgresError::from(e)))
            }
        }
    }

    /// Make a new collection record.
    async fn make_collection_record(
        &self,
        collection_id: &str,
        collection_record: crate::indexer::CollectionRecord,
    ) -> Result<crate::indexer::CollectionRecord> {
        println!("Inside postgres make_collection_record");

        // run all operations in a transaction
        let mut txn = self
            .state
            .pool
            .begin()
            .await
            .map_err(|e| DbError::from(PostgresError::from(e)))?;

        let collection_table_name = helpers::get_collection_table_name(collection_id);
        println!("collection_table_name = {collection_table_name:?}");

        let get_col_names_query = format!(
            "select column_name 
            from information_schema.columns
            where table_schema = 'public'
            and table_name = '{}'
        ",
            collection_table_name
        );

        println!("{get_col_names_query}");

        let column_names: Vec<String> = sqlx::query(&get_col_names_query)
            .fetch_all(&mut txn)
            .await
            .map_err(|e| DbError::from(PostgresError::from(e)))?
            .into_iter()
            .map(|pg_row| pg_row.get("column_name"))
            .collect::<Vec<_>>();

        println!("column names = {column_names:#?}");

        let coll_rec_data = collection_record.data.unwrap(); // todo

        let mut create_coll_rec_values_query = format!("values(");
        let mut create_coll_rec_query = format!("insert into {}(", collection_table_name);

        for column in column_names {
            create_coll_rec_query.push_str(&format!("{},", column));
            create_coll_rec_values_query
                .push_str(&helpers::extract_value_from_struct(&coll_rec_data, &column).unwrap());
            create_coll_rec_values_query.push(',');
        }

        if create_coll_rec_query.ends_with(",") {
            create_coll_rec_query.pop();
        }
        create_coll_rec_query.push_str(") ");

        if create_coll_rec_values_query.ends_with(",") {
            create_coll_rec_values_query.pop();
        }
        create_coll_rec_values_query.push_str(")");

        create_coll_rec_query.push_str(&create_coll_rec_values_query);
        create_coll_rec_query.push_str(" returning *");

        println!("create_coll_rec_query = {create_coll_rec_query:?}");

        let created_collection_record =
            sqlx::query_as::<_, models::CollectionRecord>(&create_coll_rec_query)
                .fetch_one(&mut txn)
                .await
                .map_err(|e| DbError::from(PostgresError::from(e)))?;

        txn.commit()
            .await
            .map_err(|e| DbError::from(PostgresError::from(e)))?;

        Ok(helpers::convert_to_proto_collection_record(
            &created_collection_record,
        ))
    }
}
