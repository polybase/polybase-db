#![warn(clippy::unwrap_used, clippy::expect_used)]

// TODO: we should export schema from here, so that indexer builders
// are using the correct schema
use crate::adaptor::IndexerAdaptor;
use crate::list_query::ListQuery;
use crate::where_query::WhereQuery;
use futures::stream::{FuturesUnordered, StreamExt};
use schema::{
    directive::DirectiveKind,
    field_path::FieldPath,
    publickey::PublicKey,
    record::{ForeignRecordReference, RecordReference, RecordRoot, Reference},
    Schema, COLLECTION_RECORD, COLLECTION_SCHEMA,
};
use std::{borrow::Cow, pin::Pin, time::SystemTime};

pub mod adaptor;
pub mod auth_user;
pub mod cursor;
pub mod list_query;
pub mod memory;
pub mod where_query;

// pub use indexer::{Error, Indexer, IndexerChange, Result, UserError};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("adaptor error: {0}")]
    Adaptor(#[from] adaptor::Error),

    #[error("user error: {0}")]
    User(#[from] UserError),

    #[error("where query error: {0}")]
    WhereQuery(#[from] where_query::WhereQueryError),
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error("unauthorized read")]
    UnauthorizedRead,

    #[error("collection not found")]
    CollectionNotFound { id: String },

    #[error("invalid cursor, before and after cannot be used together")]
    InvalidCursorBeforeAndAfterSpecified,

    #[error("no index found matching the query")]
    NoIndexFoundMatchingTheQuery,
}

pub struct Indexer<A: IndexerAdaptor> {
    adaptor: A,
}

#[derive(Debug, Clone)]
pub enum IndexerChange {
    Set {
        collection_id: String,
        record_id: String,
        record: RecordRoot,
    },
    Delete {
        collection_id: String,
        record_id: String,
    },
}

impl<A: IndexerAdaptor> Indexer<A> {
    pub fn new(adaptor: A) -> Self {
        Self { adaptor }
    }

    pub async fn commit(&self, height: usize, changes: Vec<IndexerChange>) -> Result<()> {
        Ok(self.adaptor.commit(height, changes).await?)
    }

    pub async fn get(
        &self,
        collection_id: &str,
        record_id: &str,
        public_key: Option<&PublicKey>,
    ) -> Result<Option<RecordRoot>> {
        // Automatically respond with Collection collection record
        if collection_id == "Collection" && record_id == "Collection" {
            return Ok(Some(COLLECTION_RECORD.clone()));
        }

        let record = match self.adaptor.get(collection_id, record_id).await? {
            Some(record) => record,
            None => return Ok(None),
        };

        let schema = self.get_schema_required(collection_id).await?;

        if !self
            .verify_read(collection_id, &schema, &record, public_key)
            .await
        {
            return Err(UserError::UnauthorizedRead)?;
        }

        Ok(Some(record))
    }

    pub async fn get_without_auth_check(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<RecordRoot>> {
        let record = match self.adaptor.get(collection_id, record_id).await? {
            Some(record) => record,
            None => return Ok(None),
        };

        Ok(Some(record))
    }

    pub async fn list<'a>(
        &'a self,
        collection_id: &'a str,
        query: ListQuery<'a>,
        public_key: Option<&'a PublicKey>,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        let schema = self.get_schema_required(collection_id).await?;

        // Check we have a matching index
        if !schema
            .indexes
            .iter()
            .any(|index| query.where_query.matches(index, query.order_by))
        {
            return Err(UserError::NoIndexFoundMatchingTheQuery)?;
        };

        // if !self
        //     .verify_list(collection_id, &schema, &query.where_query, public_key)
        //     .await
        // {
        //     return Err(UserError::UnauthorizedRead)?;
        // };

        let ListQuery {
            limit,
            where_query,
            order_by,
            cursor_after,
            cursor_before,
        } = query;

        let mut where_query = where_query.clone();

        // Apply the cursor to the where_query
        let reverse = match (cursor_before, cursor_after) {
            (Some(cursor_before), None) => {
                where_query.apply_cursor(cursor_before, cursor::CursorDirection::Before, order_by);
                true
            }
            (None, Some(cursor_after)) => {
                where_query.apply_cursor(cursor_after, cursor::CursorDirection::After, order_by);
                false
            }
            (Some(_), Some(_)) => {
                return Err(UserError::InvalidCursorBeforeAndAfterSpecified)?;
            }
            (None, None) => false,
        };

        where_query.cast(&schema)?;

        let schema = std::sync::Arc::new(schema);

        Ok(Box::pin(
            self.adaptor
                .list(collection_id, limit, where_query, order_by, reverse)
                .await?
                .filter(move |r| {
                    let r = r.clone();
                    let schema = schema.clone();
                    async move {
                        self.verify_read(
                            collection_id,
                            &std::sync::Arc::clone(&schema),
                            &r,
                            public_key,
                        )
                        .await
                    }
                }),
        ))
    }

    pub async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<SystemTime>> {
        Ok(self
            .adaptor
            .last_record_update(collection_id, record_id)
            .await?)
    }

    pub async fn last_collection_update(&self, collection_id: &str) -> Result<Option<SystemTime>> {
        Ok(self.adaptor.last_collection_update(collection_id).await?)
    }

    pub async fn set_system_key(&self, key: &str, data: &RecordRoot) -> Result<()> {
        Ok(self.adaptor.set_system_key(key, data).await?)
    }

    pub async fn get_system_key(&self, key: &str) -> Result<Option<RecordRoot>> {
        Ok(self.adaptor.get_system_key(key).await?)
    }

    /// Verify a list query is valid
    pub async fn verify_list<'a>(
        &self,
        collection_id: &str,
        schema: &Schema,
        where_query: &WhereQuery<'a>,
        public_key: Option<&PublicKey>,
    ) -> bool {
        // Convert where query to a record, so we can verify it
        let record = where_query.to_record_root(schema);

        self.verify_read(collection_id, schema, &record, public_key)
            .await
    }

    /// Verify public_key is allowed to call method on record, we don't check record parameters
    /// here, as they will automatically be checked when they are fetched using the normal read
    /// rules.
    pub async fn verify_call(
        &self,
        collection_id: &str,
        method: &str,
        schema: &Schema,
        record: &RecordRoot,
        public_key: Option<&PublicKey>,
    ) -> bool {
        // Always allow call if schema allows any, and there are no @call directives
        // on the method be called
        if schema.call_all && schema.method_auth(method).next().is_none() {
            return true;
        }

        // If no public key and not call all, deny call
        let Some(public_key) = public_key else {
            return false;
        };

        // Check for matching public keys in record
        if schema.authorise_method_with_public_key(method, record, public_key) {
            return true;
        }

        // Otherwise, get method references
        let refs = schema.find_method_references(method, record);

        self.verify_references(collection_id, schema, public_key, refs)
            .await
    }

    /// Verify user can read a give record
    pub async fn verify_read(
        &self,
        collection_id: &str,
        schema: &Schema,
        record: &RecordRoot,
        public_key: Option<&PublicKey>,
    ) -> bool {
        // Always allow read if schema allows any
        if schema.read_all {
            return true;
        }

        // If no public key and not read all, deny read
        let public_key = match public_key {
            Some(pk) => pk,
            None => return false,
        };

        // Otherwise get read permissions
        self.verify_directives(
            collection_id,
            &[DirectiveKind::Delegate, DirectiveKind::Read],
            schema,
            record,
            public_key,
        )
        .await
    }

    #[async_recursion::async_recursion]
    pub async fn verify_directives(
        &self,
        collection_id: &str,
        directives: &[DirectiveKind],
        schema: &Schema,
        record: &RecordRoot,
        public_key: &PublicKey,
    ) -> bool {
        // Check for matching public keys in record
        if schema.authorise_directives_with_public_key(directives, record, public_key) {
            return true;
        }

        // Otherwise, get references
        let refs = schema.find_directive_references(directives, record);

        // Create a future for each reference (recursive lookup)
        self.verify_references(collection_id, schema, public_key, refs)
            .await
    }

    pub async fn verify_references<'a>(
        &self,
        collection_id: &str,
        schema: &Schema,
        public_key: &PublicKey,
        refs: impl Iterator<Item = (FieldPath, Vec<Reference<'a>>)>,
    ) -> bool {
        // Create a future for each reference (recursive lookup)
        let mut futures = FuturesUnordered::new();
        let refs = refs.flat_map(|(_, refs)| refs);
        for reference in refs {
            let (collection_id, schema, record_id) = match reference {
                Reference::Record(RecordReference { id }) => {
                    (collection_id, Cow::Borrowed(schema), id.as_str())
                }
                Reference::ForeignRecord(ForeignRecordReference {
                    ref collection_id,
                    ref id,
                }) => match self.adaptor.get_schema(collection_id).await {
                    Ok(Some(schema)) => (collection_id.as_str(), Cow::Owned(schema), id.as_str()),
                    _ => continue,
                },
            };

            // Get record, ignore if record deleted / missing
            let record = match self.adaptor.get(collection_id, record_id).await {
                Ok(Some(record)) => record,
                _ => continue,
            };

            futures.push(async move {
                self.verify_directives(
                    collection_id,
                    // When we recurse, we only look for delegates
                    &[DirectiveKind::Delegate],
                    &schema,
                    &record,
                    public_key,
                )
                .await
            });
        }

        // Check the reults of the futures, find the first one that returns true
        while let Some(result) = futures.next().await {
            if result {
                return true;
            }
        }

        false
    }

    /// Use this instead of self.adaptor.get_schema to error when schema is missing, i.e.
    /// collection does not exist
    pub async fn get_schema_required(&self, collection_id: &str) -> Result<Schema> {
        if collection_id == "Collection" {
            return Ok(COLLECTION_SCHEMA.clone());
        }

        match self.adaptor.get_schema(collection_id).await? {
            Some(schema) => Ok(schema),
            None => Err(UserError::CollectionNotFound {
                id: collection_id.to_string(),
            })?,
        }
    }
}
